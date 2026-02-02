from __future__ import annotations

import typing as t

from sqlglot import exp, generator, jsonpath, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    array_append_sql,
    array_concat_sql,
    binary_from_function,
    build_default_decimal_type,
    build_formatted_time,
    build_like,
    build_replace_with_optional_replacement,
    build_timetostr_or_tochar,
    build_trunc,
    date_delta_sql,
    date_trunc_to_time,
    datestrtodate_sql,
    groupconcat_sql,
    if_sql,
    inline_array_sql,
    map_date_part,
    max_or_greatest,
    min_or_least,
    no_make_interval_sql,
    no_timestamp_sql,
    rename_func,
    strposition_sql,
    timestampdiff_sql,
    timestamptrunc_sql,
    timestrtotime_sql,
    unit_to_str,
    var_map_sql,
)
from sqlglot.generator import unsupported_args
from sqlglot.helper import find_new_name, flatten, is_date_unit, is_int, seq_get
from sqlglot.optimizer.scope import build_scope, find_all_in_scope
from sqlglot.tokens import TokenType
from sqlglot.typing.snowflake import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot._typing import E, B


# Timestamp types used in _build_datetime
TIMESTAMP_TYPES = {
    exp.DataType.Type.TIMESTAMP: "TO_TIMESTAMP",
    exp.DataType.Type.TIMESTAMPLTZ: "TO_TIMESTAMP_LTZ",
    exp.DataType.Type.TIMESTAMPNTZ: "TO_TIMESTAMP_NTZ",
    exp.DataType.Type.TIMESTAMPTZ: "TO_TIMESTAMP_TZ",
}


def _build_strtok(args: t.List) -> exp.SplitPart:
    # Add default delimiter (space) if missing - per Snowflake docs
    if len(args) == 1:
        args.append(exp.Literal.string(" "))

    # Add default part_index (1) if missing
    if len(args) == 2:
        args.append(exp.Literal.number(1))

    return exp.SplitPart.from_arg_list(args)


def _build_approx_top_k(args: t.List) -> exp.ApproxTopK:
    """
    Normalizes APPROX_TOP_K arguments to match Snowflake semantics.

    Snowflake APPROX_TOP_K signature: APPROX_TOP_K(column [, k] [, counters])
    - k defaults to 1 if omitted (per Snowflake documentation)
    - counters is optional precision parameter
    """
    # Add default k=1 if only column is provided
    if len(args) == 1:
        args.append(exp.Literal.number(1))

    return exp.ApproxTopK.from_arg_list(args)


def _build_date_from_parts(args: t.List) -> exp.DateFromParts:
    return exp.DateFromParts(
        year=seq_get(args, 0),
        month=seq_get(args, 1),
        day=seq_get(args, 2),
        allow_overflow=True,
    )


def _build_datetime(
    name: str, kind: exp.DataType.Type, safe: bool = False
) -> t.Callable[[t.List], exp.Func]:
    def _builder(args: t.List) -> exp.Func:
        value = seq_get(args, 0)
        scale_or_fmt = seq_get(args, 1)

        int_value = value is not None and is_int(value.name)
        int_scale_or_fmt = scale_or_fmt is not None and scale_or_fmt.is_int

        if isinstance(value, (exp.Literal, exp.Neg)) or (value and scale_or_fmt):
            # Converts calls like `TO_TIME('01:02:03')` into casts
            if len(args) == 1 and value.is_string and not int_value:
                return (
                    exp.TryCast(this=value, to=exp.DataType.build(kind), requires_string=True)
                    if safe
                    else exp.cast(value, kind)
                )

            # Handles `TO_TIMESTAMP(str, fmt)` and `TO_TIMESTAMP(num, scale)` as special
            # cases so we can transpile them, since they're relatively common
            if kind in TIMESTAMP_TYPES:
                if not safe and (int_scale_or_fmt or (int_value and scale_or_fmt is None)):
                    # TRY_TO_TIMESTAMP('integer') is not parsed into exp.UnixToTime as
                    # it's not easily transpilable. Also, numeric-looking strings with
                    # format strings (e.g., TO_TIMESTAMP('20240115', 'YYYYMMDD')) should
                    # use StrToTime, not UnixToTime.
                    unix_expr = exp.UnixToTime(this=value, scale=scale_or_fmt)
                    unix_expr.set("target_type", exp.DataType.build(kind, dialect="snowflake"))
                    return unix_expr
                if scale_or_fmt and not int_scale_or_fmt:
                    # Format string provided (e.g., 'YYYY-MM-DD'), use StrToTime
                    strtotime_expr = build_formatted_time(exp.StrToTime, "snowflake")(args)
                    strtotime_expr.set("safe", safe)
                    strtotime_expr.set("target_type", exp.DataType.build(kind, dialect="snowflake"))
                    return strtotime_expr

        # Handle DATE/TIME with format strings - allow int_value if a format string is provided
        has_format_string = scale_or_fmt and not int_scale_or_fmt
        if kind in (exp.DataType.Type.DATE, exp.DataType.Type.TIME) and (
            not int_value or has_format_string
        ):
            klass = exp.TsOrDsToDate if kind == exp.DataType.Type.DATE else exp.TsOrDsToTime
            formatted_exp = build_formatted_time(klass, "snowflake")(args)
            formatted_exp.set("safe", safe)
            return formatted_exp

        return exp.Anonymous(this=name, expressions=args)

    return _builder


def _build_object_construct(args: t.List) -> t.Union[exp.StarMap, exp.Struct]:
    expression = parser.build_var_map(args)

    if isinstance(expression, exp.StarMap):
        return expression

    return exp.Struct(
        expressions=[
            exp.PropertyEQ(this=k, expression=v) for k, v in zip(expression.keys, expression.values)
        ]
    )


def _build_datediff(args: t.List) -> exp.DateDiff:
    return exp.DateDiff(
        this=seq_get(args, 2),
        expression=seq_get(args, 1),
        unit=map_date_part(seq_get(args, 0)),
        date_part_boundary=True,
    )


def _build_date_time_add(expr_type: t.Type[E]) -> t.Callable[[t.List], E]:
    def _builder(args: t.List) -> E:
        return expr_type(
            this=seq_get(args, 2),
            expression=seq_get(args, 1),
            unit=map_date_part(seq_get(args, 0)),
        )

    return _builder


def _build_bitwise(expr_type: t.Type[B], name: str) -> t.Callable[[t.List], B | exp.Anonymous]:
    def _builder(args: t.List) -> B | exp.Anonymous:
        if len(args) == 3:
            # Special handling for bitwise operations with padside argument
            if expr_type in (exp.BitwiseAnd, exp.BitwiseOr, exp.BitwiseXor):
                return expr_type(
                    this=seq_get(args, 0), expression=seq_get(args, 1), padside=seq_get(args, 2)
                )
            return exp.Anonymous(this=name, expressions=args)

        result = binary_from_function(expr_type)(args)

        # Snowflake specifies INT128 for bitwise shifts
        if expr_type in (exp.BitwiseLeftShift, exp.BitwiseRightShift):
            result.set("requires_int128", True)

        return result

    return _builder


# https://docs.snowflake.com/en/sql-reference/functions/div0
def _build_if_from_div0(args: t.List) -> exp.If:
    lhs = exp._wrap(seq_get(args, 0), exp.Binary)
    rhs = exp._wrap(seq_get(args, 1), exp.Binary)

    cond = exp.EQ(this=rhs, expression=exp.Literal.number(0)).and_(
        exp.Is(this=lhs, expression=exp.null()).not_()
    )
    true = exp.Literal.number(0)
    false = exp.Div(this=lhs, expression=rhs)
    return exp.If(this=cond, true=true, false=false)


# https://docs.snowflake.com/en/sql-reference/functions/div0null
def _build_if_from_div0null(args: t.List) -> exp.If:
    lhs = exp._wrap(seq_get(args, 0), exp.Binary)
    rhs = exp._wrap(seq_get(args, 1), exp.Binary)

    # Returns 0 when divisor is 0 OR NULL
    cond = exp.EQ(this=rhs, expression=exp.Literal.number(0)).or_(
        exp.Is(this=rhs, expression=exp.null())
    )
    true = exp.Literal.number(0)
    false = exp.Div(this=lhs, expression=rhs)
    return exp.If(this=cond, true=true, false=false)


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _build_if_from_zeroifnull(args: t.List) -> exp.If:
    cond = exp.Is(this=seq_get(args, 0), expression=exp.Null())
    return exp.If(this=cond, true=exp.Literal.number(0), false=seq_get(args, 0))


def _build_search(args: t.List) -> exp.Search:
    kwargs = {
        "this": seq_get(args, 0),
        "expression": seq_get(args, 1),
        **{arg.name.lower(): arg for arg in args[2:] if isinstance(arg, exp.Kwarg)},
    }
    return exp.Search(**kwargs)


# https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
def _build_if_from_nullifzero(args: t.List) -> exp.If:
    cond = exp.EQ(this=seq_get(args, 0), expression=exp.Literal.number(0))
    return exp.If(this=cond, true=exp.Null(), false=seq_get(args, 0))


def _regexpilike_sql(self: Snowflake.Generator, expression: exp.RegexpILike) -> str:
    flag = expression.text("flag")

    if "i" not in flag:
        flag += "i"

    return self.func(
        "REGEXP_LIKE", expression.this, expression.expression, exp.Literal.string(flag)
    )


def _build_regexp_replace(args: t.List) -> exp.RegexpReplace:
    regexp_replace = exp.RegexpReplace.from_arg_list(args)

    if not regexp_replace.args.get("replacement"):
        regexp_replace.set("replacement", exp.Literal.string(""))

    return regexp_replace


def _show_parser(*args: t.Any, **kwargs: t.Any) -> t.Callable[[Snowflake.Parser], exp.Show]:
    def _parse(self: Snowflake.Parser) -> exp.Show:
        return self._parse_show_snowflake(*args, **kwargs)

    return _parse


def _date_trunc_to_time(args: t.List) -> exp.DateTrunc | exp.TimestampTrunc:
    trunc = date_trunc_to_time(args)
    unit = map_date_part(trunc.args["unit"])
    trunc.set("unit", unit)
    is_time_input = trunc.this.is_type(exp.DataType.Type.TIME, exp.DataType.Type.TIMETZ)
    if (isinstance(trunc, exp.TimestampTrunc) and is_date_unit(unit) or is_time_input) or (
        isinstance(trunc, exp.DateTrunc) and not is_date_unit(unit)
    ):
        trunc.set("input_type_preserved", True)
    return trunc


def _unqualify_pivot_columns(expression: exp.Expression) -> exp.Expression:
    """
    Snowflake doesn't allow columns referenced in UNPIVOT to be qualified,
    so we need to unqualify them. Same goes for ANY ORDER BY <column>.

    Example:
        >>> from sqlglot import parse_one
        >>> expr = parse_one("SELECT * FROM m_sales UNPIVOT(sales FOR month IN (m_sales.jan, feb, mar, april))")
        >>> print(_unqualify_pivot_columns(expr).sql(dialect="snowflake"))
        SELECT * FROM m_sales UNPIVOT(sales FOR month IN (jan, feb, mar, april))
    """
    if isinstance(expression, exp.Pivot):
        if expression.unpivot:
            expression = transforms.unqualify_columns(expression)
        else:
            for field in expression.fields:
                field_expr = seq_get(field.expressions if field else [], 0)

                if isinstance(field_expr, exp.PivotAny):
                    unqualified_field_expr = transforms.unqualify_columns(field_expr)
                    t.cast(exp.Expression, field).set("expressions", unqualified_field_expr, 0)

    return expression


def _flatten_structured_types_unless_iceberg(expression: exp.Expression) -> exp.Expression:
    assert isinstance(expression, exp.Create)

    def _flatten_structured_type(expression: exp.DataType) -> exp.DataType:
        if expression.this in exp.DataType.NESTED_TYPES:
            expression.set("expressions", None)
        return expression

    props = expression.args.get("properties")
    if isinstance(expression.this, exp.Schema) and not (props and props.find(exp.IcebergProperty)):
        for schema_expression in expression.this.expressions:
            if isinstance(schema_expression, exp.ColumnDef):
                column_type = schema_expression.kind
                if isinstance(column_type, exp.DataType):
                    column_type.transform(_flatten_structured_type, copy=False)

    return expression


def _unnest_generate_date_array(unnest: exp.Unnest) -> None:
    generate_date_array = unnest.expressions[0]
    start = generate_date_array.args.get("start")
    end = generate_date_array.args.get("end")
    step = generate_date_array.args.get("step")

    if not start or not end or not isinstance(step, exp.Interval) or step.name != "1":
        return

    unit = step.args.get("unit")

    unnest_alias = unnest.args.get("alias")
    if unnest_alias:
        unnest_alias = unnest_alias.copy()
        sequence_value_name = seq_get(unnest_alias.columns, 0) or "value"
    else:
        sequence_value_name = "value"

    # We'll add the next sequence value to the starting date and project the result
    date_add = _build_date_time_add(exp.DateAdd)(
        [unit, exp.cast(sequence_value_name, "int"), exp.cast(start, "date")]
    )

    # We use DATEDIFF to compute the number of sequence values needed
    number_sequence = Snowflake.Parser.FUNCTIONS["ARRAY_GENERATE_RANGE"](
        [exp.Literal.number(0), _build_datediff([unit, start, end]) + 1]
    )

    unnest.set("expressions", [number_sequence])

    unnest_parent = unnest.parent
    if isinstance(unnest_parent, exp.Join):
        select = unnest_parent.parent
        if isinstance(select, exp.Select):
            replace_column_name = (
                sequence_value_name
                if isinstance(sequence_value_name, str)
                else sequence_value_name.name
            )

            scope = build_scope(select)
            if scope:
                for column in scope.columns:
                    if column.name.lower() == replace_column_name.lower():
                        column.replace(
                            date_add.as_(replace_column_name)
                            if isinstance(column.parent, exp.Select)
                            else date_add
                        )

            lateral = exp.Lateral(this=unnest_parent.this.pop())
            unnest_parent.replace(exp.Join(this=lateral))
    else:
        unnest.replace(
            exp.select(date_add.as_(sequence_value_name))
            .from_(unnest.copy())
            .subquery(unnest_alias)
        )


def _transform_generate_date_array(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Select):
        for generate_date_array in expression.find_all(exp.GenerateDateArray):
            parent = generate_date_array.parent

            # If GENERATE_DATE_ARRAY is used directly as an array (e.g passed into ARRAY_LENGTH), the transformed Snowflake
            # query is the following (it'll be unnested properly on the next iteration due to copy):
            # SELECT ref(GENERATE_DATE_ARRAY(...)) -> SELECT ref((SELECT ARRAY_AGG(*) FROM UNNEST(GENERATE_DATE_ARRAY(...))))
            if not isinstance(parent, exp.Unnest):
                unnest = exp.Unnest(expressions=[generate_date_array.copy()])
                generate_date_array.replace(
                    exp.select(exp.ArrayAgg(this=exp.Star())).from_(unnest).subquery()
                )

            if (
                isinstance(parent, exp.Unnest)
                and isinstance(parent.parent, (exp.From, exp.Join))
                and len(parent.expressions) == 1
            ):
                _unnest_generate_date_array(parent)

    return expression


def _build_regexp_extract(expr_type: t.Type[E]) -> t.Callable[[t.List, Snowflake], E]:
    def _builder(args: t.List, dialect: Snowflake) -> E:
        return expr_type(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            position=seq_get(args, 2),
            occurrence=seq_get(args, 3),
            parameters=seq_get(args, 4),
            group=seq_get(args, 5) or exp.Literal.number(0),
            **(
                {"null_if_pos_overflow": dialect.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL}
                if expr_type is exp.RegexpExtract
                else {}
            ),
        )

    return _builder


def _regexpextract_sql(self, expression: exp.RegexpExtract | exp.RegexpExtractAll) -> str:
    # Other dialects don't support all of the following parameters, so we need to
    # generate default values as necessary to ensure the transpilation is correct
    group = expression.args.get("group")

    # To avoid generating all these default values, we set group to None if
    # it's 0 (also default value) which doesn't trigger the following chain
    if group and group.name == "0":
        group = None

    parameters = expression.args.get("parameters") or (group and exp.Literal.string("c"))
    occurrence = expression.args.get("occurrence") or (parameters and exp.Literal.number(1))
    position = expression.args.get("position") or (occurrence and exp.Literal.number(1))

    return self.func(
        "REGEXP_SUBSTR" if isinstance(expression, exp.RegexpExtract) else "REGEXP_EXTRACT_ALL",
        expression.this,
        expression.expression,
        position,
        occurrence,
        parameters,
        group,
    )


def _json_extract_value_array_sql(
    self: Snowflake.Generator, expression: exp.JSONValueArray | exp.JSONExtractArray
) -> str:
    json_extract = exp.JSONExtract(this=expression.this, expression=expression.expression)
    ident = exp.to_identifier("x")

    if isinstance(expression, exp.JSONValueArray):
        this: exp.Expression = exp.cast(ident, to=exp.DataType.Type.VARCHAR)
    else:
        this = exp.ParseJSON(this=f"TO_JSON({ident})")

    transform_lambda = exp.Lambda(expressions=[ident], this=this)

    return self.func("TRANSFORM", json_extract, transform_lambda)


def _qualify_unnested_columns(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Select):
        scope = build_scope(expression)
        if not scope:
            return expression

        unnests = list(scope.find_all(exp.Unnest))

        if not unnests:
            return expression

        taken_source_names = set(scope.sources)
        column_source: t.Dict[str, exp.Identifier] = {}
        unnest_to_identifier: t.Dict[exp.Unnest, exp.Identifier] = {}

        unnest_identifier: t.Optional[exp.Identifier] = None
        orig_expression = expression.copy()

        for unnest in unnests:
            if not isinstance(unnest.parent, (exp.From, exp.Join)):
                continue

            # Try to infer column names produced by an unnest operator. This is only possible
            # when we can peek into the (statically known) contents of the unnested value.
            unnest_columns: t.Set[str] = set()
            for unnest_expr in unnest.expressions:
                if not isinstance(unnest_expr, exp.Array):
                    continue

                for array_expr in unnest_expr.expressions:
                    if not (
                        isinstance(array_expr, exp.Struct)
                        and array_expr.expressions
                        and all(
                            isinstance(struct_expr, exp.PropertyEQ)
                            for struct_expr in array_expr.expressions
                        )
                    ):
                        continue

                    unnest_columns.update(
                        struct_expr.this.name.lower() for struct_expr in array_expr.expressions
                    )
                    break

                if unnest_columns:
                    break

            unnest_alias = unnest.args.get("alias")
            if not unnest_alias:
                alias_name = find_new_name(taken_source_names, "value")
                taken_source_names.add(alias_name)

                # Produce a `TableAlias` AST similar to what is produced for BigQuery. This
                # will be corrected later, when we generate SQL for the `Unnest` AST node.
                aliased_unnest = exp.alias_(unnest, None, table=[alias_name])
                scope.replace(unnest, aliased_unnest)

                unnest_identifier = aliased_unnest.args["alias"].columns[0]
            else:
                alias_columns = getattr(unnest_alias, "columns", [])
                unnest_identifier = unnest_alias.this or seq_get(alias_columns, 0)

            if not isinstance(unnest_identifier, exp.Identifier):
                return orig_expression

            unnest_to_identifier[unnest] = unnest_identifier
            column_source.update({c.lower(): unnest_identifier for c in unnest_columns})

        for column in scope.columns:
            if column.table:
                continue

            table = column_source.get(column.name.lower())
            if (
                unnest_identifier
                and not table
                and len(scope.sources) == 1
                and column.name.lower() != unnest_identifier.name.lower()
            ):
                unnest_ancestor = column.find_ancestor(exp.Unnest, exp.Select)
                ancestor_identifier = unnest_to_identifier.get(unnest_ancestor)
                if (
                    isinstance(unnest_ancestor, exp.Unnest)
                    and ancestor_identifier
                    and ancestor_identifier.name.lower() == unnest_identifier.name.lower()
                ):
                    continue

                table = unnest_identifier

            column.set("table", table and table.copy())

    return expression


def _eliminate_dot_variant_lookup(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Select):
        # This transformation is used to facilitate transpilation of BigQuery `UNNEST` operations
        # to Snowflake. It should not affect roundtrip because `Unnest` nodes cannot be produced
        # by Snowflake's parser.
        #
        # Additionally, at the time of writing this, BigQuery is the only dialect that produces a
        # `TableAlias` node that only fills `columns` and not `this`, due to `UNNEST_COLUMN_ONLY`.
        unnest_aliases = set()
        for unnest in find_all_in_scope(expression, exp.Unnest):
            unnest_alias = unnest.args.get("alias")
            if (
                isinstance(unnest_alias, exp.TableAlias)
                and not unnest_alias.this
                and len(unnest_alias.columns) == 1
            ):
                unnest_aliases.add(unnest_alias.columns[0].name)

        if unnest_aliases:
            for c in find_all_in_scope(expression, exp.Column):
                if c.table in unnest_aliases:
                    bracket_lhs = c.args["table"]
                    bracket_rhs = exp.Literal.string(c.name)
                    bracket = exp.Bracket(this=bracket_lhs, expressions=[bracket_rhs])

                    if c.parent is expression:
                        # Retain column projection names by using aliases
                        c.replace(exp.alias_(bracket, c.this.copy()))
                    else:
                        c.replace(bracket)

    return expression


def _build_timestamp_from_parts(args: t.List) -> exp.Func:
    """Build TimestampFromParts with support for both syntaxes:
    1. TIMESTAMP_FROM_PARTS(year, month, day, hour, minute, second [, nanosecond] [, time_zone])
    2. TIMESTAMP_FROM_PARTS(date_expr, time_expr) - Snowflake specific
    """
    if len(args) == 2:
        return exp.TimestampFromParts(this=seq_get(args, 0), expression=seq_get(args, 1))

    return exp.TimestampFromParts.from_arg_list(args)


def _build_round(args: t.List) -> exp.Round:
    """
    Build Round expression, unwrapping Snowflake's named parameters.

    Maps EXPR => this, SCALE => decimals, ROUNDING_MODE => truncate.

    Note: Snowflake does not support mixing named and positional arguments.
    Arguments are either all named or all positional.
    """
    kwarg_map = {"EXPR": "this", "SCALE": "decimals", "ROUNDING_MODE": "truncate"}
    round_args = {}
    positional_keys = ["this", "decimals", "truncate"]
    positional_idx = 0

    for arg in args:
        if isinstance(arg, exp.Kwarg):
            key = arg.this.name.upper()
            round_key = kwarg_map.get(key)
            if round_key:
                round_args[round_key] = arg.expression
        else:
            if positional_idx < len(positional_keys):
                round_args[positional_keys[positional_idx]] = arg
                positional_idx += 1

    expression = exp.Round(**round_args)
    expression.set("casts_non_integer_decimals", True)
    return expression


def _build_generator(args: t.List) -> exp.Generator:
    """
    Build Generator expression, unwrapping Snowflake's named parameters.

    Maps ROWCOUNT => rowcount, TIMELIMIT => time_limit.
    """
    kwarg_map = {"ROWCOUNT": "rowcount", "TIMELIMIT": "time_limit"}
    gen_args = {}

    for arg in args:
        if isinstance(arg, exp.Kwarg):
            key = arg.this.name.upper()
            gen_key = kwarg_map.get(key)
            if gen_key:
                gen_args[gen_key] = arg.expression

    return exp.Generator(**gen_args)


def _build_try_to_number(args: t.List[exp.Expression]) -> exp.Expression:
    return exp.ToNumber(
        this=seq_get(args, 0),
        format=seq_get(args, 1),
        precision=seq_get(args, 2),
        scale=seq_get(args, 3),
        safe=True,
    )


class Snowflake(Dialect):
    # https://docs.snowflake.com/en/sql-reference/identifiers-syntax
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE
    NULL_ORDERING = "nulls_are_large"
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"
    SUPPORTS_USER_DEFINED_TYPES = False
    SUPPORTS_SEMI_ANTI_JOIN = False
    PREFER_CTE_ALIAS_COLUMN = True
    TABLESAMPLE_SIZE_IS_PERCENT = True
    COPY_PARAMS_ARE_CSV = False
    ARRAY_AGG_INCLUDES_NULLS = None
    ARRAY_FUNCS_PROPAGATES_NULLS = True
    ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = False
    TRY_CAST_REQUIRES_STRING = True
    SUPPORTS_ALIAS_REFS_IN_JOIN_CONDITIONS = True
    LEAST_GREATEST_IGNORES_NULLS = False

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    # https://docs.snowflake.com/en/en/sql-reference/functions/initcap
    INITCAP_DEFAULT_DELIMITER_CHARS = ' \t\n\r\f\v!?@"^#$&~_,.:;+\\-*%/|\\[\\](){}<>'

    INVERSE_TIME_MAPPING = {
        "T": "T",  # in TIME_MAPPING we map '"T"' with the double quotes to 'T', and we want to prevent 'T' from being mapped back to '"T"' so that 'AUTO' doesn't become 'AU"T"O'
    }

    TIME_MAPPING = {
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MMMM": "%B",
        "mmmm": "%B",
        "MON": "%b",
        "mon": "%b",
        "MM": "%m",
        "mm": "%m",
        "DD": "%d",
        "dd": "%-d",
        "DY": "%a",
        "dy": "%w",
        "HH24": "%H",
        "hh24": "%H",
        "HH12": "%I",
        "hh12": "%I",
        "MI": "%M",
        "mi": "%M",
        "SS": "%S",
        "ss": "%S",
        "FF": "%f_nine",  # %f_ internal representation with precision specified
        "ff": "%f_nine",
        "FF0": "%f_zero",
        "ff0": "%f_zero",
        "FF1": "%f_one",
        "ff1": "%f_one",
        "FF2": "%f_two",
        "ff2": "%f_two",
        "FF3": "%f_three",
        "ff3": "%f_three",
        "FF4": "%f_four",
        "ff4": "%f_four",
        "FF5": "%f_five",
        "ff5": "%f_five",
        "FF6": "%f",
        "ff6": "%f",
        "FF7": "%f_seven",
        "ff7": "%f_seven",
        "FF8": "%f_eight",
        "ff8": "%f_eight",
        "FF9": "%f_nine",
        "ff9": "%f_nine",
        "TZHTZM": "%z",
        "tzhtzm": "%z",
        "TZH:TZM": "%:z",  # internal representation for ±HH:MM
        "tzh:tzm": "%:z",
        "TZH": "%-z",  # internal representation ±HH
        "tzh": "%-z",
        '"T"': "T",  # remove the optional double quotes around the separator between the date and time
        # Seems like Snowflake treats AM/PM in the format string as equivalent,
        # only the time (stamp) value's AM/PM affects the output
        "AM": "%p",
        "am": "%p",
        "PM": "%p",
        "pm": "%p",
    }

    DATE_PART_MAPPING = {
        **Dialect.DATE_PART_MAPPING,
        "ISOWEEK": "WEEKISO",
        # The base Dialect maps EPOCH_SECOND -> EPOCH, but we need to preserve
        # EPOCH_SECOND as a distinct value for two reasons:
        # 1. Type annotation: EPOCH_SECOND returns BIGINT, while EPOCH returns DOUBLE
        # 2. Transpilation: DuckDB's EPOCH() returns float, so we cast EPOCH_SECOND
        #    to BIGINT to match Snowflake's integer behavior
        # Without this override, EXTRACT(EPOCH_SECOND FROM ts) would be normalized
        # to EXTRACT(EPOCH FROM ts) and lose the integer semantics.
        "EPOCH_SECOND": "EPOCH_SECOND",
        "EPOCH_SECONDS": "EPOCH_SECOND",
    }

    PSEUDOCOLUMNS = {"LEVEL"}

    def can_quote(self, identifier: exp.Identifier, identify: str | bool = "safe") -> bool:
        # This disables quoting DUAL in SELECT ... FROM DUAL, because Snowflake treats an
        # unquoted DUAL keyword in a special way and does not map it to a user-defined table
        return super().can_quote(identifier, identify) and not (
            isinstance(identifier.parent, exp.Table)
            and not identifier.quoted
            and identifier.name.lower() == "dual"
        )

    class JSONPathTokenizer(jsonpath.JSONPathTokenizer):
        SINGLE_TOKENS = jsonpath.JSONPathTokenizer.SINGLE_TOKENS.copy()
        SINGLE_TOKENS.pop("$")

    class Parser(parser.Parser):
        IDENTIFY_PIVOT_STRINGS = True
        DEFAULT_SAMPLING_METHOD = "BERNOULLI"
        COLON_IS_VARIANT_EXTRACT = True
        JSON_EXTRACT_REQUIRES_JSON_EXPRESSION = True

        ID_VAR_TOKENS = {
            *parser.Parser.ID_VAR_TOKENS,
            TokenType.EXCEPT,
            TokenType.MATCH_CONDITION,
        }

        TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {TokenType.WINDOW}
        TABLE_ALIAS_TOKENS.discard(TokenType.MATCH_CONDITION)

        COLON_PLACEHOLDER_TOKENS = ID_VAR_TOKENS | {TokenType.NUMBER}

        NO_PAREN_FUNCTIONS = {
            **parser.Parser.NO_PAREN_FUNCTIONS,
            TokenType.CURRENT_TIME: exp.Localtime,
        }

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ADD_MONTHS": lambda args: exp.AddMonths(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                preserve_end_of_month=True,
            ),
            "APPROX_PERCENTILE": exp.ApproxQuantile.from_arg_list,
            "CURRENT_TIME": lambda args: exp.Localtime(this=seq_get(args, 0)),
            "APPROX_TOP_K": _build_approx_top_k,
            "ARRAY_CONSTRUCT": lambda args: exp.Array(expressions=args),
            "ARRAY_CONTAINS": lambda args: exp.ArrayContains(
                this=seq_get(args, 1), expression=seq_get(args, 0), ensure_variant=False
            ),
            "ARRAY_GENERATE_RANGE": lambda args: exp.GenerateSeries(
                # ARRAY_GENERATE_RANGE has an exlusive end; we normalize it to be inclusive
                start=seq_get(args, 0),
                end=exp.Sub(this=seq_get(args, 1), expression=exp.Literal.number(1)),
                step=seq_get(args, 2),
            ),
            "ARRAY_SORT": exp.SortArray.from_arg_list,
            "ARRAY_FLATTEN": exp.Flatten.from_arg_list,
            "BITAND": _build_bitwise(exp.BitwiseAnd, "BITAND"),
            "BIT_AND": _build_bitwise(exp.BitwiseAnd, "BITAND"),
            "BITNOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BIT_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BITXOR": _build_bitwise(exp.BitwiseXor, "BITXOR"),
            "BIT_XOR": _build_bitwise(exp.BitwiseXor, "BITXOR"),
            "BITOR": _build_bitwise(exp.BitwiseOr, "BITOR"),
            "BIT_OR": _build_bitwise(exp.BitwiseOr, "BITOR"),
            "BITSHIFTLEFT": _build_bitwise(exp.BitwiseLeftShift, "BITSHIFTLEFT"),
            "BIT_SHIFTLEFT": _build_bitwise(exp.BitwiseLeftShift, "BIT_SHIFTLEFT"),
            "BITSHIFTRIGHT": _build_bitwise(exp.BitwiseRightShift, "BITSHIFTRIGHT"),
            "BIT_SHIFTRIGHT": _build_bitwise(exp.BitwiseRightShift, "BIT_SHIFTRIGHT"),
            "BITANDAGG": exp.BitwiseAndAgg.from_arg_list,
            "BITAND_AGG": exp.BitwiseAndAgg.from_arg_list,
            "BIT_AND_AGG": exp.BitwiseAndAgg.from_arg_list,
            "BIT_ANDAGG": exp.BitwiseAndAgg.from_arg_list,
            "BITORAGG": exp.BitwiseOrAgg.from_arg_list,
            "BITOR_AGG": exp.BitwiseOrAgg.from_arg_list,
            "BIT_OR_AGG": exp.BitwiseOrAgg.from_arg_list,
            "BIT_ORAGG": exp.BitwiseOrAgg.from_arg_list,
            "BITXORAGG": exp.BitwiseXorAgg.from_arg_list,
            "BITXOR_AGG": exp.BitwiseXorAgg.from_arg_list,
            "BIT_XOR_AGG": exp.BitwiseXorAgg.from_arg_list,
            "BIT_XORAGG": exp.BitwiseXorAgg.from_arg_list,
            "BITMAP_OR_AGG": exp.BitmapOrAgg.from_arg_list,
            "BOOLAND": lambda args: exp.Booland(
                this=seq_get(args, 0), expression=seq_get(args, 1), round_input=True
            ),
            "BOOLOR": lambda args: exp.Boolor(
                this=seq_get(args, 0), expression=seq_get(args, 1), round_input=True
            ),
            "BOOLNOT": lambda args: exp.Boolnot(this=seq_get(args, 0), round_input=True),
            "BOOLXOR": lambda args: exp.Xor(
                this=seq_get(args, 0), expression=seq_get(args, 1), round_input=True
            ),
            "CORR": lambda args: exp.Corr(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                null_on_zero_variance=True,
            ),
            "DATE": _build_datetime("DATE", exp.DataType.Type.DATE),
            "DATEFROMPARTS": _build_date_from_parts,
            "DATE_FROM_PARTS": _build_date_from_parts,
            "DATE_TRUNC": _date_trunc_to_time,
            "DATEADD": _build_date_time_add(exp.DateAdd),
            "DATEDIFF": _build_datediff,
            "DAYNAME": lambda args: exp.Dayname(this=seq_get(args, 0), abbreviated=True),
            "DAYOFWEEKISO": exp.DayOfWeekIso.from_arg_list,
            "DIV0": _build_if_from_div0,
            "DIV0NULL": _build_if_from_div0null,
            "EDITDISTANCE": lambda args: exp.Levenshtein(
                this=seq_get(args, 0), expression=seq_get(args, 1), max_dist=seq_get(args, 2)
            ),
            "FLATTEN": exp.Explode.from_arg_list,
            "GENERATOR": _build_generator,
            "GET": exp.GetExtract.from_arg_list,
            "GETDATE": exp.CurrentTimestamp.from_arg_list,
            "GET_PATH": lambda args, dialect: exp.JSONExtract(
                this=seq_get(args, 0),
                expression=dialect.to_json_path(seq_get(args, 1)),
                requires_json=True,
            ),
            "GREATEST_IGNORE_NULLS": lambda args: exp.Greatest(
                this=seq_get(args, 0), expressions=args[1:], ignore_nulls=True
            ),
            "LEAST_IGNORE_NULLS": lambda args: exp.Least(
                this=seq_get(args, 0), expressions=args[1:], ignore_nulls=True
            ),
            "HEX_DECODE_BINARY": exp.Unhex.from_arg_list,
            "IFF": exp.If.from_arg_list,
            "MD5_HEX": exp.MD5.from_arg_list,
            "MD5_BINARY": exp.MD5Digest.from_arg_list,
            "MD5_NUMBER_LOWER64": exp.MD5NumberLower64.from_arg_list,
            "MD5_NUMBER_UPPER64": exp.MD5NumberUpper64.from_arg_list,
            "MONTHNAME": lambda args: exp.Monthname(this=seq_get(args, 0), abbreviated=True),
            "LAST_DAY": lambda args: exp.LastDay(
                this=seq_get(args, 0), unit=map_date_part(seq_get(args, 1))
            ),
            "LEN": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
            "LENGTH": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
            "LOCALTIMESTAMP": exp.CurrentTimestamp.from_arg_list,
            "NULLIFZERO": _build_if_from_nullifzero,
            "OBJECT_CONSTRUCT": _build_object_construct,
            "OBJECT_KEYS": exp.JSONKeys.from_arg_list,
            "OCTET_LENGTH": exp.ByteLength.from_arg_list,
            "PARSE_URL": lambda args: exp.ParseUrl(
                this=seq_get(args, 0), permissive=seq_get(args, 1)
            ),
            "REGEXP_EXTRACT_ALL": _build_regexp_extract(exp.RegexpExtractAll),
            "REGEXP_REPLACE": _build_regexp_replace,
            "REGEXP_SUBSTR": _build_regexp_extract(exp.RegexpExtract),
            "REGEXP_SUBSTR_ALL": _build_regexp_extract(exp.RegexpExtractAll),
            "REPLACE": build_replace_with_optional_replacement,
            "RLIKE": exp.RegexpLike.from_arg_list,
            "ROUND": _build_round,
            "SHA1_BINARY": exp.SHA1Digest.from_arg_list,
            "SHA1_HEX": exp.SHA.from_arg_list,
            "SHA2_BINARY": exp.SHA2Digest.from_arg_list,
            "SHA2_HEX": exp.SHA2.from_arg_list,
            "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
            "STDDEV_SAMP": exp.Stddev.from_arg_list,
            "STRTOK": _build_strtok,
            "SYSDATE": lambda args: exp.CurrentTimestamp(this=seq_get(args, 0), sysdate=True),
            "TABLE": lambda args: exp.TableFromRows(this=seq_get(args, 0)),
            "TIMEADD": _build_date_time_add(exp.TimeAdd),
            "TIMEDIFF": _build_datediff,
            "TIME_FROM_PARTS": lambda args: exp.TimeFromParts(
                hour=seq_get(args, 0),
                min=seq_get(args, 1),
                sec=seq_get(args, 2),
                nano=seq_get(args, 3),
                overflow=True,
            ),
            "TIMESTAMPADD": _build_date_time_add(exp.DateAdd),
            "TIMESTAMPDIFF": _build_datediff,
            "TIMESTAMPFROMPARTS": _build_timestamp_from_parts,
            "TIMESTAMP_FROM_PARTS": _build_timestamp_from_parts,
            "TIMESTAMPNTZFROMPARTS": _build_timestamp_from_parts,
            "TIMESTAMP_NTZ_FROM_PARTS": _build_timestamp_from_parts,
            "TRUNC": build_trunc,
            "TRUNCATE": build_trunc,
            "TRY_DECRYPT": lambda args: exp.Decrypt(
                this=seq_get(args, 0),
                passphrase=seq_get(args, 1),
                aad=seq_get(args, 2),
                encryption_method=seq_get(args, 3),
                safe=True,
            ),
            "TRY_DECRYPT_RAW": lambda args: exp.DecryptRaw(
                this=seq_get(args, 0),
                key=seq_get(args, 1),
                iv=seq_get(args, 2),
                aad=seq_get(args, 3),
                encryption_method=seq_get(args, 4),
                aead=seq_get(args, 5),
                safe=True,
            ),
            "TRY_PARSE_JSON": lambda args: exp.ParseJSON(this=seq_get(args, 0), safe=True),
            "TRY_TO_BINARY": lambda args: exp.ToBinary(
                this=seq_get(args, 0), format=seq_get(args, 1), safe=True
            ),
            "TRY_TO_BOOLEAN": lambda args: exp.ToBoolean(this=seq_get(args, 0), safe=True),
            "TRY_TO_DATE": _build_datetime("TRY_TO_DATE", exp.DataType.Type.DATE, safe=True),
            **dict.fromkeys(
                ("TRY_TO_DECIMAL", "TRY_TO_NUMBER", "TRY_TO_NUMERIC"), _build_try_to_number
            ),
            "TRY_TO_DOUBLE": lambda args: exp.ToDouble(
                this=seq_get(args, 0), format=seq_get(args, 1), safe=True
            ),
            "TRY_TO_FILE": lambda args: exp.ToFile(
                this=seq_get(args, 0), path=seq_get(args, 1), safe=True
            ),
            "TRY_TO_TIME": _build_datetime("TRY_TO_TIME", exp.DataType.Type.TIME, safe=True),
            "TRY_TO_TIMESTAMP": _build_datetime(
                "TRY_TO_TIMESTAMP", exp.DataType.Type.TIMESTAMP, safe=True
            ),
            "TRY_TO_TIMESTAMP_LTZ": _build_datetime(
                "TRY_TO_TIMESTAMP_LTZ", exp.DataType.Type.TIMESTAMPLTZ, safe=True
            ),
            "TRY_TO_TIMESTAMP_NTZ": _build_datetime(
                "TRY_TO_TIMESTAMP_NTZ", exp.DataType.Type.TIMESTAMPNTZ, safe=True
            ),
            "TRY_TO_TIMESTAMP_TZ": _build_datetime(
                "TRY_TO_TIMESTAMP_TZ", exp.DataType.Type.TIMESTAMPTZ, safe=True
            ),
            "TO_CHAR": build_timetostr_or_tochar,
            "TO_DATE": _build_datetime("TO_DATE", exp.DataType.Type.DATE),
            **dict.fromkeys(
                ("TO_DECIMAL", "TO_NUMBER", "TO_NUMERIC"),
                lambda args: exp.ToNumber(
                    this=seq_get(args, 0),
                    format=seq_get(args, 1),
                    precision=seq_get(args, 2),
                    scale=seq_get(args, 3),
                ),
            ),
            "TO_TIME": _build_datetime("TO_TIME", exp.DataType.Type.TIME),
            "TO_TIMESTAMP": _build_datetime("TO_TIMESTAMP", exp.DataType.Type.TIMESTAMP),
            "TO_TIMESTAMP_LTZ": _build_datetime("TO_TIMESTAMP_LTZ", exp.DataType.Type.TIMESTAMPLTZ),
            "TO_TIMESTAMP_NTZ": _build_datetime("TO_TIMESTAMP_NTZ", exp.DataType.Type.TIMESTAMPNTZ),
            "TO_TIMESTAMP_TZ": _build_datetime("TO_TIMESTAMP_TZ", exp.DataType.Type.TIMESTAMPTZ),
            "TO_VARCHAR": build_timetostr_or_tochar,
            "TO_JSON": exp.JSONFormat.from_arg_list,
            "VECTOR_COSINE_SIMILARITY": exp.CosineDistance.from_arg_list,
            "VECTOR_INNER_PRODUCT": exp.DotProduct.from_arg_list,
            "VECTOR_L1_DISTANCE": exp.ManhattanDistance.from_arg_list,
            "VECTOR_L2_DISTANCE": exp.EuclideanDistance.from_arg_list,
            "ZEROIFNULL": _build_if_from_zeroifnull,
            "LIKE": build_like(exp.Like),
            "ILIKE": build_like(exp.ILike),
            "SEARCH": _build_search,
            "SKEW": exp.Skewness.from_arg_list,
            "SYSTIMESTAMP": exp.CurrentTimestamp.from_arg_list,
            "WEEKISO": exp.WeekOfYear.from_arg_list,
            "WEEKOFYEAR": exp.Week.from_arg_list,
        }
        FUNCTIONS.pop("PREDICT")

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "DATE_PART": lambda self: self._parse_date_part(),
            "DIRECTORY": lambda self: self._parse_directory(),
            "OBJECT_CONSTRUCT_KEEP_NULL": lambda self: self._parse_json_object(),
            "LISTAGG": lambda self: self._parse_string_agg(),
            "SEMANTIC_VIEW": lambda self: self._parse_semantic_view(),
        }
        FUNCTION_PARSERS.pop("TRIM")

        TIMESTAMPS = parser.Parser.TIMESTAMPS - {TokenType.TIME}

        ALTER_PARSERS = {
            **parser.Parser.ALTER_PARSERS,
            "SESSION": lambda self: self._parse_alter_session(),
            "UNSET": lambda self: self.expression(
                exp.Set,
                tag=self._match_text_seq("TAG"),
                expressions=self._parse_csv(self._parse_id_var),
                unset=True,
            ),
        }

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,
            TokenType.GET: lambda self: self._parse_get(),
            TokenType.PUT: lambda self: self._parse_put(),
            TokenType.SHOW: lambda self: self._parse_show(),
        }

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,
            "CREDENTIALS": lambda self: self._parse_credentials_property(),
            "FILE_FORMAT": lambda self: self._parse_file_format_property(),
            "LOCATION": lambda self: self._parse_location_property(),
            "TAG": lambda self: self._parse_tag(),
            "USING": lambda self: self._match_text_seq("TEMPLATE")
            and self.expression(exp.UsingTemplateProperty, this=self._parse_statement()),
        }

        TYPE_CONVERTERS = {
            # https://docs.snowflake.com/en/sql-reference/data-types-numeric#number
            exp.DataType.Type.DECIMAL: build_default_decimal_type(precision=38, scale=0),
        }

        SHOW_PARSERS = {
            "DATABASES": _show_parser("DATABASES"),
            "TERSE DATABASES": _show_parser("DATABASES"),
            "SCHEMAS": _show_parser("SCHEMAS"),
            "TERSE SCHEMAS": _show_parser("SCHEMAS"),
            "OBJECTS": _show_parser("OBJECTS"),
            "TERSE OBJECTS": _show_parser("OBJECTS"),
            "TABLES": _show_parser("TABLES"),
            "TERSE TABLES": _show_parser("TABLES"),
            "VIEWS": _show_parser("VIEWS"),
            "TERSE VIEWS": _show_parser("VIEWS"),
            "PRIMARY KEYS": _show_parser("PRIMARY KEYS"),
            "TERSE PRIMARY KEYS": _show_parser("PRIMARY KEYS"),
            "IMPORTED KEYS": _show_parser("IMPORTED KEYS"),
            "TERSE IMPORTED KEYS": _show_parser("IMPORTED KEYS"),
            "UNIQUE KEYS": _show_parser("UNIQUE KEYS"),
            "TERSE UNIQUE KEYS": _show_parser("UNIQUE KEYS"),
            "SEQUENCES": _show_parser("SEQUENCES"),
            "TERSE SEQUENCES": _show_parser("SEQUENCES"),
            "STAGES": _show_parser("STAGES"),
            "COLUMNS": _show_parser("COLUMNS"),
            "USERS": _show_parser("USERS"),
            "TERSE USERS": _show_parser("USERS"),
            "FILE FORMATS": _show_parser("FILE FORMATS"),
            "FUNCTIONS": _show_parser("FUNCTIONS"),
            "PROCEDURES": _show_parser("PROCEDURES"),
            "WAREHOUSES": _show_parser("WAREHOUSES"),
        }

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            "WITH": lambda self: self._parse_with_constraint(),
            "MASKING": lambda self: self._parse_with_constraint(),
            "PROJECTION": lambda self: self._parse_with_constraint(),
            "TAG": lambda self: self._parse_with_constraint(),
        }

        STAGED_FILE_SINGLE_TOKENS = {
            TokenType.DOT,
            TokenType.MOD,
            TokenType.SLASH,
        }

        FLATTEN_COLUMNS = ["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"]

        SCHEMA_KINDS = {"OBJECTS", "TABLES", "VIEWS", "SEQUENCES", "UNIQUE KEYS", "IMPORTED KEYS"}

        NON_TABLE_CREATABLES = {"STORAGE INTEGRATION", "TAG", "WAREHOUSE", "STREAMLIT"}

        LAMBDAS = {
            **parser.Parser.LAMBDAS,
            TokenType.ARROW: lambda self, expressions: self.expression(
                exp.Lambda,
                this=self._replace_lambda(
                    self._parse_assignment(),
                    expressions,
                ),
                expressions=[e.this if isinstance(e, exp.Cast) else e for e in expressions],
            ),
        }

        COLUMN_OPERATORS = {
            **parser.Parser.COLUMN_OPERATORS,
            TokenType.EXCLAMATION: lambda self, this, attr: self.expression(
                exp.ModelAttribute, this=this, expression=attr
            ),
        }

        def _parse_directory(self) -> exp.DirectoryStage:
            table = self._parse_table_parts()

            if isinstance(table, exp.Table):
                table = table.this

            return self.expression(exp.DirectoryStage, this=table)

        def _parse_use(self) -> exp.Use:
            if self._match_text_seq("SECONDARY", "ROLES"):
                this = self._match_texts(("ALL", "NONE")) and exp.var(self._prev.text.upper())
                roles = None if this else self._parse_csv(lambda: self._parse_table(schema=False))
                return self.expression(
                    exp.Use, kind="SECONDARY ROLES", this=this, expressions=roles
                )

            return super()._parse_use()

        def _negate_range(
            self, this: t.Optional[exp.Expression] = None
        ) -> t.Optional[exp.Expression]:
            if not this:
                return this

            query = this.args.get("query")
            if isinstance(this, exp.In) and isinstance(query, exp.Query):
                # Snowflake treats `value NOT IN (subquery)` as `VALUE <> ALL (subquery)`, so
                # we do this conversion here to avoid parsing it into `NOT value IN (subquery)`
                # which can produce different results (most likely a SnowFlake bug).
                #
                # https://docs.snowflake.com/en/sql-reference/functions/in
                # Context: https://github.com/tobymao/sqlglot/issues/3890
                return self.expression(
                    exp.NEQ, this=this.this, expression=exp.All(this=query.unnest())
                )

            return self.expression(exp.Not, this=this)

        def _parse_tag(self) -> exp.Tags:
            return self.expression(
                exp.Tags,
                expressions=self._parse_wrapped_csv(self._parse_property),
            )

        def _parse_with_constraint(self) -> t.Optional[exp.Expression]:
            if self._prev.token_type != TokenType.WITH:
                self._retreat(self._index - 1)

            if self._match_text_seq("MASKING", "POLICY"):
                policy = self._parse_column()
                return self.expression(
                    exp.MaskingPolicyColumnConstraint,
                    this=policy.to_dot() if isinstance(policy, exp.Column) else policy,
                    expressions=self._match(TokenType.USING)
                    and self._parse_wrapped_csv(self._parse_id_var),
                )
            if self._match_text_seq("PROJECTION", "POLICY"):
                policy = self._parse_column()
                return self.expression(
                    exp.ProjectionPolicyColumnConstraint,
                    this=policy.to_dot() if isinstance(policy, exp.Column) else policy,
                )
            if self._match(TokenType.TAG):
                return self._parse_tag()

            return None

        def _parse_with_property(self) -> t.Optional[exp.Expression] | t.List[exp.Expression]:
            if self._match(TokenType.TAG):
                return self._parse_tag()

            return super()._parse_with_property()

        def _parse_create(self) -> exp.Create | exp.Command:
            expression = super()._parse_create()
            if isinstance(expression, exp.Create) and expression.kind in self.NON_TABLE_CREATABLES:
                # Replace the Table node with the enclosed Identifier
                expression.this.replace(expression.this.this)

            return expression

        # https://docs.snowflake.com/en/sql-reference/functions/date_part.html
        # https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts
        def _parse_date_part(self: Snowflake.Parser) -> t.Optional[exp.Expression]:
            this = self._parse_var() or self._parse_type()

            if not this:
                return None

            # Handle both syntaxes: DATE_PART(part, expr) and DATE_PART(part FROM expr)
            expression = (
                self._match_set((TokenType.FROM, TokenType.COMMA)) and self._parse_bitwise()
            )
            return self.expression(
                exp.Extract, this=map_date_part(this, self.dialect), expression=expression
            )

        def _parse_bracket_key_value(self, is_map: bool = False) -> t.Optional[exp.Expression]:
            if is_map:
                # Keys are strings in Snowflake's objects, see also:
                # - https://docs.snowflake.com/en/sql-reference/data-types-semistructured
                # - https://docs.snowflake.com/en/sql-reference/functions/object_construct
                return self._parse_slice(self._parse_string()) or self._parse_assignment()

            return self._parse_slice(self._parse_alias(self._parse_assignment(), explicit=True))

        def _parse_lateral(self) -> t.Optional[exp.Lateral]:
            lateral = super()._parse_lateral()
            if not lateral:
                return lateral

            if isinstance(lateral.this, exp.Explode):
                table_alias = lateral.args.get("alias")
                columns = [exp.to_identifier(col) for col in self.FLATTEN_COLUMNS]
                if table_alias and not table_alias.args.get("columns"):
                    table_alias.set("columns", columns)
                elif not table_alias:
                    exp.alias_(lateral, "_flattened", table=columns, copy=False)

            return lateral

        def _parse_table_parts(
            self, schema: bool = False, is_db_reference: bool = False, wildcard: bool = False
        ) -> exp.Table:
            # https://docs.snowflake.com/en/user-guide/querying-stage
            if self._match(TokenType.STRING, advance=False):
                table = self._parse_string()
            elif self._match_text_seq("@", advance=False):
                table = self._parse_location_path()
            else:
                table = None

            if table:
                file_format = None
                pattern = None

                wrapped = self._match(TokenType.L_PAREN)
                while self._curr and wrapped and not self._match(TokenType.R_PAREN):
                    if self._match_text_seq("FILE_FORMAT", "=>"):
                        file_format = self._parse_string() or super()._parse_table_parts(
                            is_db_reference=is_db_reference
                        )
                    elif self._match_text_seq("PATTERN", "=>"):
                        pattern = self._parse_string()
                    else:
                        break

                    self._match(TokenType.COMMA)

                table = self.expression(exp.Table, this=table, format=file_format, pattern=pattern)
            else:
                table = super()._parse_table_parts(schema=schema, is_db_reference=is_db_reference)

            return table

        def _parse_table(
            self,
            schema: bool = False,
            joins: bool = False,
            alias_tokens: t.Optional[t.Collection[TokenType]] = None,
            parse_bracket: bool = False,
            is_db_reference: bool = False,
            parse_partition: bool = False,
            consume_pipe: bool = False,
        ) -> t.Optional[exp.Expression]:
            table = super()._parse_table(
                schema=schema,
                joins=joins,
                alias_tokens=alias_tokens,
                parse_bracket=parse_bracket,
                is_db_reference=is_db_reference,
                parse_partition=parse_partition,
            )
            if isinstance(table, exp.Table) and isinstance(table.this, exp.TableFromRows):
                table_from_rows = table.this
                for arg in exp.TableFromRows.arg_types:
                    if arg != "this":
                        table_from_rows.set(arg, table.args.get(arg))

                table = table_from_rows

            return table

        def _parse_id_var(
            self,
            any_token: bool = True,
            tokens: t.Optional[t.Collection[TokenType]] = None,
        ) -> t.Optional[exp.Expression]:
            if self._match_text_seq("IDENTIFIER", "("):
                identifier = (
                    super()._parse_id_var(any_token=any_token, tokens=tokens)
                    or self._parse_string()
                )
                self._match_r_paren()
                return self.expression(exp.Anonymous, this="IDENTIFIER", expressions=[identifier])

            return super()._parse_id_var(any_token=any_token, tokens=tokens)

        def _parse_show_snowflake(self, this: str) -> exp.Show:
            scope = None
            scope_kind = None

            # will identity SHOW TERSE SCHEMAS but not SHOW TERSE PRIMARY KEYS
            # which is syntactically valid but has no effect on the output
            terse = self._tokens[self._index - 2].text.upper() == "TERSE"

            history = self._match_text_seq("HISTORY")

            like = self._parse_string() if self._match(TokenType.LIKE) else None

            if self._match(TokenType.IN):
                if self._match_text_seq("ACCOUNT"):
                    scope_kind = "ACCOUNT"
                elif self._match_text_seq("CLASS"):
                    scope_kind = "CLASS"
                    scope = self._parse_table_parts()
                elif self._match_text_seq("APPLICATION"):
                    scope_kind = "APPLICATION"
                    if self._match_text_seq("PACKAGE"):
                        scope_kind += " PACKAGE"
                    scope = self._parse_table_parts()
                elif self._match_set(self.DB_CREATABLES):
                    scope_kind = self._prev.text.upper()
                    if self._curr:
                        scope = self._parse_table_parts()
                elif self._curr:
                    scope_kind = "SCHEMA" if this in self.SCHEMA_KINDS else "TABLE"
                    scope = self._parse_table_parts()

            return self.expression(
                exp.Show,
                terse=terse,
                this=this,
                history=history,
                like=like,
                scope=scope,
                scope_kind=scope_kind,
                starts_with=self._match_text_seq("STARTS", "WITH") and self._parse_string(),
                limit=self._parse_limit(),
                from_=self._parse_string() if self._match(TokenType.FROM) else None,
                privileges=self._match_text_seq("WITH", "PRIVILEGES")
                and self._parse_csv(lambda: self._parse_var(any_token=True, upper=True)),
            )

        def _parse_put(self) -> exp.Put | exp.Command:
            if self._curr.token_type != TokenType.STRING:
                return self._parse_as_command(self._prev)

            return self.expression(
                exp.Put,
                this=self._parse_string(),
                target=self._parse_location_path(),
                properties=self._parse_properties(),
            )

        def _parse_get(self) -> t.Optional[exp.Expression]:
            start = self._prev

            # If we detect GET( then we need to parse a function, not a statement
            if self._match(TokenType.L_PAREN):
                self._retreat(self._index - 2)
                return self._parse_expression()

            target = self._parse_location_path()

            # Parse as command if unquoted file path
            if self._curr.token_type == TokenType.URI_START:
                return self._parse_as_command(start)

            return self.expression(
                exp.Get,
                this=self._parse_string(),
                target=target,
                properties=self._parse_properties(),
            )

        def _parse_location_property(self) -> exp.LocationProperty:
            self._match(TokenType.EQ)
            return self.expression(exp.LocationProperty, this=self._parse_location_path())

        def _parse_file_location(self) -> t.Optional[exp.Expression]:
            # Parse either a subquery or a staged file
            return (
                self._parse_select(table=True, parse_subquery_alias=False)
                if self._match(TokenType.L_PAREN, advance=False)
                else self._parse_table_parts()
            )

        def _parse_location_path(self) -> exp.Var:
            start = self._curr
            self._advance_any(ignore_reserved=True)

            # We avoid consuming a comma token because external tables like @foo and @bar
            # can be joined in a query with a comma separator, as well as closing paren
            # in case of subqueries
            while self._is_connected() and not self._match_set(
                (TokenType.COMMA, TokenType.L_PAREN, TokenType.R_PAREN), advance=False
            ):
                self._advance_any(ignore_reserved=True)

            return exp.var(self._find_sql(start, self._prev))

        def _parse_lambda_arg(self) -> t.Optional[exp.Expression]:
            this = super()._parse_lambda_arg()

            if not this:
                return this

            typ = self._parse_types()

            if typ:
                return self.expression(exp.Cast, this=this, to=typ)

            return this

        def _parse_foreign_key(self) -> exp.ForeignKey:
            # inlineFK, the REFERENCES columns are implied
            if self._match(TokenType.REFERENCES, advance=False):
                return self.expression(exp.ForeignKey)

            # outoflineFK, explicitly names the columns
            return super()._parse_foreign_key()

        def _parse_file_format_property(self) -> exp.FileFormatProperty:
            self._match(TokenType.EQ)
            if self._match(TokenType.L_PAREN, advance=False):
                expressions = self._parse_wrapped_options()
            else:
                expressions = [self._parse_format_name()]

            return self.expression(
                exp.FileFormatProperty,
                expressions=expressions,
            )

        def _parse_credentials_property(self) -> exp.CredentialsProperty:
            return self.expression(
                exp.CredentialsProperty,
                expressions=self._parse_wrapped_options(),
            )

        def _parse_semantic_view(self) -> exp.SemanticView:
            kwargs: t.Dict[str, t.Any] = {"this": self._parse_table_parts()}

            while self._curr and not self._match(TokenType.R_PAREN, advance=False):
                if self._match_texts(("DIMENSIONS", "METRICS", "FACTS")):
                    keyword = self._prev.text.lower()
                    kwargs[keyword] = self._parse_csv(self._parse_disjunction)
                elif self._match_text_seq("WHERE"):
                    kwargs["where"] = self._parse_expression()
                else:
                    self.raise_error("Expecting ) or encountered unexpected keyword")
                    break

            return self.expression(exp.SemanticView, **kwargs)

        def _parse_set(self, unset: bool = False, tag: bool = False) -> exp.Set | exp.Command:
            set = super()._parse_set(unset=unset, tag=tag)

            if isinstance(set, exp.Set):
                for expr in set.expressions:
                    if isinstance(expr, exp.SetItem):
                        expr.set("kind", "VARIABLE")
            return set

    class Tokenizer(tokens.Tokenizer):
        STRING_ESCAPES = ["\\", "'"]
        HEX_STRINGS = [("x'", "'"), ("X'", "'")]
        RAW_STRINGS = ["$$"]
        COMMENTS = ["--", "//", ("/*", "*/")]
        NESTED_COMMENTS = False

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "BYTEINT": TokenType.INT,
            "FILE://": TokenType.URI_START,
            "FILE FORMAT": TokenType.FILE_FORMAT,
            "GET": TokenType.GET,
            "MATCH_CONDITION": TokenType.MATCH_CONDITION,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "MINUS": TokenType.EXCEPT,
            "NCHAR VARYING": TokenType.VARCHAR,
            "PUT": TokenType.PUT,
            "REMOVE": TokenType.COMMAND,
            "RM": TokenType.COMMAND,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "SEMANTIC VIEW": TokenType.SEMANTIC_VIEW,
            "SQL_DOUBLE": TokenType.DOUBLE,
            "SQL_VARCHAR": TokenType.VARCHAR,
            "STAGE": TokenType.STAGE,
            "STORAGE INTEGRATION": TokenType.STORAGE_INTEGRATION,
            "STREAMLIT": TokenType.STREAMLIT,
            "TAG": TokenType.TAG,
            "TIMESTAMP_TZ": TokenType.TIMESTAMPTZ,
            "TOP": TokenType.TOP,
            "WAREHOUSE": TokenType.WAREHOUSE,
            # https://docs.snowflake.com/en/sql-reference/data-types-numeric#float
            # FLOAT is a synonym for DOUBLE in Snowflake
            "FLOAT": TokenType.DOUBLE,
        }
        KEYWORDS.pop("/*+")

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
            "!": TokenType.EXCLAMATION,
        }

        VAR_SINGLE_TOKENS = {"$"}

        COMMANDS = tokens.Tokenizer.COMMANDS - {TokenType.SHOW}

    class Generator(generator.Generator):
        PARAMETER_TOKEN = "$"
        MATCHED_BY_SOURCE = False
        SINGLE_STRING_INTERVAL = True
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        AGGREGATE_FILTER_SUPPORTED = False
        SUPPORTS_TABLE_COPY = False
        COLLATE_IS_FUNC = True
        LIMIT_ONLY_LITERALS = True
        JSON_KEY_VALUE_PAIR_SEP = ","
        INSERT_OVERWRITE = " OVERWRITE INTO"
        STRUCT_DELIMITER = ("(", ")")
        COPY_PARAMS_ARE_WRAPPED = False
        COPY_PARAMS_EQ_REQUIRED = True
        STAR_EXCEPT = "EXCLUDE"
        SUPPORTS_EXPLODING_PROJECTIONS = False
        ARRAY_CONCAT_IS_VAR_LEN = False
        SUPPORTS_CONVERT_TIMEZONE = True
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        SUPPORTS_MEDIAN = True
        ARRAY_SIZE_NAME = "ARRAY_SIZE"
        SUPPORTS_DECODE_CASE = True
        IS_BOOL_ALLOWED = False
        DIRECTED_JOINS = True

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ArgMax: rename_func("MAX_BY"),
            exp.ArgMin: rename_func("MIN_BY"),
            exp.Array: transforms.preprocess([transforms.inherit_struct_field_names]),
            exp.ArrayConcat: array_concat_sql("ARRAY_CAT"),
            exp.ArrayAppend: array_append_sql("ARRAY_APPEND"),
            exp.ArrayPrepend: array_append_sql("ARRAY_PREPEND"),
            exp.ArrayContains: lambda self, e: self.func(
                "ARRAY_CONTAINS",
                e.expression
                if e.args.get("ensure_variant") is False
                else exp.cast(e.expression, exp.DataType.Type.VARIANT, copy=False),
                e.this,
            ),
            exp.ArrayIntersect: rename_func("ARRAY_INTERSECTION"),
            exp.AtTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", e.args.get("zone"), e.this
            ),
            exp.BitwiseOr: rename_func("BITOR"),
            exp.BitwiseXor: rename_func("BITXOR"),
            exp.BitwiseAnd: rename_func("BITAND"),
            exp.BitwiseAndAgg: rename_func("BITANDAGG"),
            exp.BitwiseOrAgg: rename_func("BITORAGG"),
            exp.BitwiseXorAgg: rename_func("BITXORAGG"),
            exp.BitwiseNot: rename_func("BITNOT"),
            exp.BitwiseLeftShift: rename_func("BITSHIFTLEFT"),
            exp.BitwiseRightShift: rename_func("BITSHIFTRIGHT"),
            exp.Create: transforms.preprocess([_flatten_structured_types_unless_iceberg]),
            exp.CurrentTimestamp: lambda self, e: self.func("SYSDATE")
            if e.args.get("sysdate")
            else self.function_fallback_sql(e),
            exp.Localtime: lambda self, e: self.func("CURRENT_TIME", e.this)
            if e.this
            else "CURRENT_TIME",
            exp.Localtimestamp: lambda self, e: self.func("CURRENT_TIMESTAMP", e.this)
            if e.this
            else "CURRENT_TIMESTAMP",
            exp.DateAdd: date_delta_sql("DATEADD"),
            exp.DateDiff: date_delta_sql("DATEDIFF"),
            exp.DatetimeAdd: date_delta_sql("TIMESTAMPADD"),
            exp.DatetimeDiff: timestampdiff_sql,
            exp.DateStrToDate: datestrtodate_sql,
            exp.Decrypt: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}DECRYPT",
                e.this,
                e.args.get("passphrase"),
                e.args.get("aad"),
                e.args.get("encryption_method"),
            ),
            exp.DecryptRaw: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}DECRYPT_RAW",
                e.this,
                e.args.get("key"),
                e.args.get("iv"),
                e.args.get("aad"),
                e.args.get("encryption_method"),
                e.args.get("aead"),
            ),
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfWeekIso: rename_func("DAYOFWEEKISO"),
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.DotProduct: rename_func("VECTOR_INNER_PRODUCT"),
            exp.Explode: rename_func("FLATTEN"),
            exp.Extract: lambda self, e: self.func(
                "DATE_PART", map_date_part(e.this, self.dialect), e.expression
            ),
            exp.CosineDistance: rename_func("VECTOR_COSINE_SIMILARITY"),
            exp.EuclideanDistance: rename_func("VECTOR_L2_DISTANCE"),
            exp.FileFormatProperty: lambda self,
            e: f"FILE_FORMAT=({self.expressions(e, 'expressions', sep=' ')})",
            exp.FromTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", e.args.get("zone"), "'UTC'", e.this
            ),
            exp.GenerateSeries: lambda self, e: self.func(
                "ARRAY_GENERATE_RANGE", e.args["start"], e.args["end"] + 1, e.args.get("step")
            ),
            exp.GetExtract: rename_func("GET"),
            exp.GroupConcat: lambda self, e: groupconcat_sql(self, e, sep=""),
            exp.If: if_sql(name="IFF", false_value="NULL"),
            exp.JSONExtractArray: _json_extract_value_array_sql,
            exp.JSONExtractScalar: lambda self, e: self.func(
                "JSON_EXTRACT_PATH_TEXT", e.this, e.expression
            ),
            exp.JSONKeys: rename_func("OBJECT_KEYS"),
            exp.JSONObject: lambda self, e: self.func("OBJECT_CONSTRUCT_KEEP_NULL", *e.expressions),
            exp.JSONPathRoot: lambda *_: "",
            exp.JSONValueArray: _json_extract_value_array_sql,
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost")(
                rename_func("EDITDISTANCE")
            ),
            exp.LocationProperty: lambda self, e: f"LOCATION={self.sql(e, 'this')}",
            exp.LogicalAnd: rename_func("BOOLAND_AGG"),
            exp.LogicalOr: rename_func("BOOLOR_AGG"),
            exp.Map: lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
            exp.ManhattanDistance: rename_func("VECTOR_L1_DISTANCE"),
            exp.MakeInterval: no_make_interval_sql,
            exp.Max: max_or_greatest,
            exp.Min: min_or_least,
            exp.ParseJSON: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}PARSE_JSON", e.this
            ),
            exp.ToBinary: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}TO_BINARY", e.this, e.args.get("format")
            ),
            exp.ToBoolean: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}TO_BOOLEAN", e.this
            ),
            exp.ToDouble: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}TO_DOUBLE", e.this, e.args.get("format")
            ),
            exp.ToFile: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}TO_FILE", e.this, e.args.get("path")
            ),
            exp.ToNumber: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}TO_NUMBER",
                e.this,
                e.args.get("format"),
                e.args.get("precision"),
                e.args.get("scale"),
            ),
            exp.JSONFormat: rename_func("TO_JSON"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.PercentileCont: transforms.preprocess(
                [transforms.add_within_group_for_percentiles]
            ),
            exp.PercentileDisc: transforms.preprocess(
                [transforms.add_within_group_for_percentiles]
            ),
            exp.Pivot: transforms.preprocess([_unqualify_pivot_columns]),
            exp.RegexpExtract: _regexpextract_sql,
            exp.RegexpExtractAll: _regexpextract_sql,
            exp.RegexpILike: _regexpilike_sql,
            exp.Rand: rename_func("RANDOM"),
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_window_clause,
                    transforms.eliminate_distinct_on,
                    transforms.explode_projection_to_unnest(),
                    transforms.eliminate_semi_and_anti_joins,
                    _transform_generate_date_array,
                    _qualify_unnested_columns,
                    _eliminate_dot_variant_lookup,
                ]
            ),
            exp.SHA: rename_func("SHA1"),
            exp.SHA1Digest: rename_func("SHA1_BINARY"),
            exp.MD5Digest: rename_func("MD5_BINARY"),
            exp.MD5NumberLower64: rename_func("MD5_NUMBER_LOWER64"),
            exp.MD5NumberUpper64: rename_func("MD5_NUMBER_UPPER64"),
            exp.LowerHex: rename_func("TO_CHAR"),
            exp.SortArray: rename_func("ARRAY_SORT"),
            exp.Skewness: rename_func("SKEW"),
            exp.StarMap: rename_func("OBJECT_CONSTRUCT"),
            exp.StartsWith: rename_func("STARTSWITH"),
            exp.EndsWith: rename_func("ENDSWITH"),
            exp.StrPosition: lambda self, e: strposition_sql(
                self, e, func_name="CHARINDEX", supports_position=True
            ),
            exp.StrToDate: lambda self, e: self.func("DATE", e.this, self.format_time(e)),
            exp.StringToArray: rename_func("STRTOK_TO_ARRAY"),
            exp.Stuff: rename_func("INSERT"),
            exp.StPoint: rename_func("ST_MAKEPOINT"),
            exp.TimeAdd: date_delta_sql("TIMEADD"),
            exp.TimeSlice: lambda self, e: self.func(
                "TIME_SLICE",
                e.this,
                e.expression,
                unit_to_str(e),
                e.args.get("kind"),
            ),
            exp.Timestamp: no_timestamp_sql,
            exp.TimestampAdd: date_delta_sql("TIMESTAMPADD"),
            exp.TimestampDiff: lambda self, e: self.func(
                "TIMESTAMPDIFF", e.unit, e.expression, e.this
            ),
            exp.TimestampTrunc: timestamptrunc_sql(),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeToUnix: lambda self, e: f"EXTRACT(epoch_second FROM {self.sql(e, 'this')})",
            exp.ToArray: rename_func("TO_ARRAY"),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.TsOrDsAdd: date_delta_sql("DATEADD", cast=True),
            exp.TsOrDsDiff: date_delta_sql("DATEDIFF"),
            exp.TsOrDsToDate: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}TO_DATE", e.this, self.format_time(e)
            ),
            exp.TsOrDsToTime: lambda self, e: self.func(
                f"{'TRY_' if e.args.get('safe') else ''}TO_TIME", e.this, self.format_time(e)
            ),
            exp.Unhex: rename_func("HEX_DECODE_BINARY"),
            exp.UnixToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, e.args.get("scale")),
            exp.Uuid: rename_func("UUID_STRING"),
            exp.VarMap: lambda self, e: var_map_sql(self, e, "OBJECT_CONSTRUCT"),
            exp.Booland: rename_func("BOOLAND"),
            exp.Boolor: rename_func("BOOLOR"),
            exp.WeekOfYear: rename_func("WEEKISO"),
            exp.YearOfWeek: rename_func("YEAROFWEEK"),
            exp.YearOfWeekIso: rename_func("YEAROFWEEKISO"),
            exp.Xor: rename_func("BOOLXOR"),
            exp.ByteLength: rename_func("OCTET_LENGTH"),
            exp.Flatten: rename_func("ARRAY_FLATTEN"),
            exp.ArrayConcatAgg: lambda self, e: self.func(
                "ARRAY_FLATTEN", exp.ArrayAgg(this=e.this)
            ),
            exp.SHA2Digest: lambda self, e: self.func(
                "SHA2_BINARY", e.this, e.args.get("length") or exp.Literal.number(256)
            ),
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.BIGDECIMAL: "DOUBLE",
            exp.DataType.Type.NESTED: "OBJECT",
            exp.DataType.Type.STRUCT: "OBJECT",
            exp.DataType.Type.TEXT: "VARCHAR",
        }

        TOKEN_MAPPING = {
            TokenType.AUTO_INCREMENT: "AUTOINCREMENT",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.CredentialsProperty: exp.Properties.Location.POST_WITH,
            exp.LocationProperty: exp.Properties.Location.POST_WITH,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.SetProperty: exp.Properties.Location.UNSUPPORTED,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        UNSUPPORTED_VALUES_EXPRESSIONS = {
            exp.Map,
            exp.StarMap,
            exp.Struct,
            exp.VarMap,
        }

        RESPECT_IGNORE_NULLS_UNSUPPORTED_EXPRESSIONS = (exp.ArrayAgg,)

        def with_properties(self, properties: exp.Properties) -> str:
            return self.properties(properties, wrapped=False, prefix=self.sep(""), sep=" ")

        def values_sql(self, expression: exp.Values, values_as_table: bool = True) -> str:
            if expression.find(*self.UNSUPPORTED_VALUES_EXPRESSIONS):
                values_as_table = False

            return super().values_sql(expression, values_as_table=values_as_table)

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Check if this is a FLOAT type nested inside a VECTOR type
            # VECTOR only accepts FLOAT (not DOUBLE), INT, and STRING as element types
            # https://docs.snowflake.com/en/sql-reference/data-types-vector
            if expression.is_type(exp.DataType.Type.DOUBLE):
                parent = expression.parent
                if isinstance(parent, exp.DataType) and parent.is_type(exp.DataType.Type.VECTOR):
                    # Preserve FLOAT for VECTOR types instead of mapping to synonym DOUBLE
                    return "FLOAT"

            expressions = expression.expressions
            if expressions and expression.is_type(*exp.DataType.STRUCT_TYPES):
                for field_type in expressions:
                    # The correct syntax is OBJECT [ (<key> <value_type [NOT NULL] [, ...]) ]
                    if isinstance(field_type, exp.DataType):
                        return "OBJECT"
                    if (
                        isinstance(field_type, exp.ColumnDef)
                        and field_type.this
                        and field_type.this.is_string
                    ):
                        # Doing OBJECT('foo' VARCHAR) is invalid snowflake Syntax. Moreover, besides
                        # converting 'foo' into an identifier, we also need to quote it because these
                        # keys are case-sensitive. For example:
                        #
                        # WITH t AS (SELECT OBJECT_CONSTRUCT('x', 'y') AS c) SELECT c:x FROM t -- correct
                        # WITH t AS (SELECT OBJECT_CONSTRUCT('x', 'y') AS c) SELECT c:X FROM t -- incorrect, returns NULL
                        field_type.this.replace(exp.to_identifier(field_type.name, quoted=True))

            return super().datatype_sql(expression)

        def tonumber_sql(self, expression: exp.ToNumber) -> str:
            return self.func(
                "TO_NUMBER",
                expression.this,
                expression.args.get("format"),
                expression.args.get("precision"),
                expression.args.get("scale"),
            )

        def timestampfromparts_sql(self, expression: exp.TimestampFromParts) -> str:
            milli = expression.args.get("milli")
            if milli is not None:
                milli_to_nano = milli.pop() * exp.Literal.number(1000000)
                expression.set("nano", milli_to_nano)

            return rename_func("TIMESTAMP_FROM_PARTS")(self, expression)

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            if expression.is_type(exp.DataType.Type.GEOGRAPHY):
                return self.func("TO_GEOGRAPHY", expression.this)
            if expression.is_type(exp.DataType.Type.GEOMETRY):
                return self.func("TO_GEOMETRY", expression.this)

            return super().cast_sql(expression, safe_prefix=safe_prefix)

        def trycast_sql(self, expression: exp.TryCast) -> str:
            value = expression.this

            if value.type is None:
                from sqlglot.optimizer.annotate_types import annotate_types

                value = annotate_types(value, dialect=self.dialect)

            # Snowflake requires that TRY_CAST's value be a string
            # If TRY_CAST is being roundtripped (since Snowflake is the only dialect that sets "requires_string") or
            # if we can deduce that the value is a string, then we can generate TRY_CAST
            if expression.args.get("requires_string") or value.is_type(*exp.DataType.TEXT_TYPES):
                return super().trycast_sql(expression)

            return self.cast_sql(expression)

        def log_sql(self, expression: exp.Log) -> str:
            if not expression.expression:
                return self.func("LN", expression.this)

            return super().log_sql(expression)

        def greatest_sql(self, expression: exp.Greatest) -> str:
            name = "GREATEST_IGNORE_NULLS" if expression.args.get("ignore_nulls") else "GREATEST"
            return self.func(name, expression.this, *expression.expressions)

        def least_sql(self, expression: exp.Least) -> str:
            name = "LEAST_IGNORE_NULLS" if expression.args.get("ignore_nulls") else "LEAST"
            return self.func(name, expression.this, *expression.expressions)

        def generator_sql(self, expression: exp.Generator) -> str:
            args = []
            rowcount = expression.args.get("rowcount")
            time_limit = expression.args.get("time_limit")

            if rowcount:
                args.append(exp.Kwarg(this=exp.var("ROWCOUNT"), expression=rowcount))
            if time_limit:
                args.append(exp.Kwarg(this=exp.var("TIMELIMIT"), expression=time_limit))

            return self.func("GENERATOR", *args)

        def unnest_sql(self, expression: exp.Unnest) -> str:
            unnest_alias = expression.args.get("alias")
            offset = expression.args.get("offset")

            unnest_alias_columns = unnest_alias.columns if unnest_alias else []
            value = seq_get(unnest_alias_columns, 0) or exp.to_identifier("value")

            columns = [
                exp.to_identifier("seq"),
                exp.to_identifier("key"),
                exp.to_identifier("path"),
                offset.pop() if isinstance(offset, exp.Expression) else exp.to_identifier("index"),
                value,
                exp.to_identifier("this"),
            ]

            if unnest_alias:
                unnest_alias.set("columns", columns)
            else:
                unnest_alias = exp.TableAlias(this="_u", columns=columns)

            table_input = self.sql(expression.expressions[0])
            if not table_input.startswith("INPUT =>"):
                table_input = f"INPUT => {table_input}"

            expression_parent = expression.parent

            explode = (
                f"FLATTEN({table_input})"
                if isinstance(expression_parent, exp.Lateral)
                else f"TABLE(FLATTEN({table_input}))"
            )
            alias = self.sql(unnest_alias)
            alias = f" AS {alias}" if alias else ""
            value = (
                ""
                if isinstance(expression_parent, (exp.From, exp.Join, exp.Lateral))
                else f"{value} FROM "
            )

            return f"{value}{explode}{alias}"

        def show_sql(self, expression: exp.Show) -> str:
            terse = "TERSE " if expression.args.get("terse") else ""
            history = " HISTORY" if expression.args.get("history") else ""
            like = self.sql(expression, "like")
            like = f" LIKE {like}" if like else ""

            scope = self.sql(expression, "scope")
            scope = f" {scope}" if scope else ""

            scope_kind = self.sql(expression, "scope_kind")
            if scope_kind:
                scope_kind = f" IN {scope_kind}"

            starts_with = self.sql(expression, "starts_with")
            if starts_with:
                starts_with = f" STARTS WITH {starts_with}"

            limit = self.sql(expression, "limit")

            from_ = self.sql(expression, "from_")
            if from_:
                from_ = f" FROM {from_}"

            privileges = self.expressions(expression, key="privileges", flat=True)
            privileges = f" WITH PRIVILEGES {privileges}" if privileges else ""

            return f"SHOW {terse}{expression.name}{history}{like}{scope_kind}{scope}{starts_with}{limit}{from_}{privileges}"

        def describe_sql(self, expression: exp.Describe) -> str:
            # Default to table if kind is unknown
            kind_value = expression.args.get("kind") or "TABLE"
            kind = f" {kind_value}" if kind_value else ""
            this = f" {self.sql(expression, 'this')}"
            expressions = self.expressions(expression, flat=True)
            expressions = f" {expressions}" if expressions else ""
            return f"DESCRIBE{kind}{this}{expressions}"

        def generatedasidentitycolumnconstraint_sql(
            self, expression: exp.GeneratedAsIdentityColumnConstraint
        ) -> str:
            start = expression.args.get("start")
            start = f" START {start}" if start else ""
            increment = expression.args.get("increment")
            increment = f" INCREMENT {increment}" if increment else ""

            order = expression.args.get("order")
            if order is not None:
                order_clause = " ORDER" if order else " NOORDER"
            else:
                order_clause = ""

            return f"AUTOINCREMENT{start}{increment}{order_clause}"

        def cluster_sql(self, expression: exp.Cluster) -> str:
            return f"CLUSTER BY ({self.expressions(expression, flat=True)})"

        def struct_sql(self, expression: exp.Struct) -> str:
            if len(expression.expressions) == 1:
                arg = expression.expressions[0]
                if arg.is_star or (isinstance(arg, exp.ILike) and arg.left.is_star):
                    # Wildcard syntax: https://docs.snowflake.com/en/sql-reference/data-types-semistructured#object
                    return f"{{{self.sql(expression.expressions[0])}}}"

            keys = []
            values = []

            for i, e in enumerate(expression.expressions):
                if isinstance(e, exp.PropertyEQ):
                    keys.append(
                        exp.Literal.string(e.name) if isinstance(e.this, exp.Identifier) else e.this
                    )
                    values.append(e.expression)
                else:
                    keys.append(exp.Literal.string(f"_{i}"))
                    values.append(e)

            return self.func("OBJECT_CONSTRUCT", *flatten(zip(keys, values)))

        @unsupported_args("weight", "accuracy")
        def approxquantile_sql(self, expression: exp.ApproxQuantile) -> str:
            return self.func("APPROX_PERCENTILE", expression.this, expression.args.get("quantile"))

        def alterset_sql(self, expression: exp.AlterSet) -> str:
            exprs = self.expressions(expression, flat=True)
            exprs = f" {exprs}" if exprs else ""
            file_format = self.expressions(expression, key="file_format", flat=True, sep=" ")
            file_format = f" STAGE_FILE_FORMAT = ({file_format})" if file_format else ""
            copy_options = self.expressions(expression, key="copy_options", flat=True, sep=" ")
            copy_options = f" STAGE_COPY_OPTIONS = ({copy_options})" if copy_options else ""
            tag = self.expressions(expression, key="tag", flat=True)
            tag = f" TAG {tag}" if tag else ""

            return f"SET{exprs}{file_format}{copy_options}{tag}"

        def strtotime_sql(self, expression: exp.StrToTime):
            # target_type is stored as a DataType instance
            target_type = expression.args.get("target_type")

            # Get the type enum from DataType instance or from type annotation
            if isinstance(target_type, exp.DataType):
                type_enum = target_type.this
            elif expression.type:
                type_enum = expression.type.this
            else:
                type_enum = exp.DataType.Type.TIMESTAMP

            func_name = TIMESTAMP_TYPES.get(type_enum, "TO_TIMESTAMP")

            return self.func(
                f"{'TRY_' if expression.args.get('safe') else ''}{func_name}",
                expression.this,
                self.format_time(expression),
            )

        def timestampsub_sql(self, expression: exp.TimestampSub):
            return self.sql(
                exp.TimestampAdd(
                    this=expression.this,
                    expression=expression.expression * -1,
                    unit=expression.unit,
                )
            )

        def jsonextract_sql(self, expression: exp.JSONExtract):
            this = expression.this

            # JSON strings are valid coming from other dialects such as BQ so
            # for these cases we PARSE_JSON preemptively
            if not isinstance(this, (exp.ParseJSON, exp.JSONExtract)) and not expression.args.get(
                "requires_json"
            ):
                this = exp.ParseJSON(this=this)

            return self.func(
                "GET_PATH",
                this,
                expression.expression,
            )

        def timetostr_sql(self, expression: exp.TimeToStr) -> str:
            this = expression.this
            if this.is_string:
                this = exp.cast(this, exp.DataType.Type.TIMESTAMP)

            return self.func("TO_CHAR", this, self.format_time(expression))

        def datesub_sql(self, expression: exp.DateSub) -> str:
            value = expression.expression
            if value:
                value.replace(value * (-1))
            else:
                self.unsupported("DateSub cannot be transpiled if the subtracted count is unknown")

            return date_delta_sql("DATEADD")(self, expression)

        def select_sql(self, expression: exp.Select) -> str:
            limit = expression.args.get("limit")
            offset = expression.args.get("offset")
            if offset and not limit:
                expression.limit(exp.Null(), copy=False)
            return super().select_sql(expression)

        def createable_sql(self, expression: exp.Create, locations: t.DefaultDict) -> str:
            is_materialized = expression.find(exp.MaterializedProperty)
            copy_grants_property = expression.find(exp.CopyGrantsProperty)

            if expression.kind == "VIEW" and is_materialized and copy_grants_property:
                # For materialized views, COPY GRANTS is located *before* the columns list
                # This is in contrast to normal views where COPY GRANTS is located *after* the columns list
                # We default CopyGrantsProperty to POST_SCHEMA which means we need to output it POST_NAME if a materialized view is detected
                # ref: https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#syntax
                # ref: https://docs.snowflake.com/en/sql-reference/sql/create-view#syntax
                post_schema_properties = locations[exp.Properties.Location.POST_SCHEMA]
                post_schema_properties.pop(post_schema_properties.index(copy_grants_property))

                this_name = self.sql(expression.this, "this")
                copy_grants = self.sql(copy_grants_property)
                this_schema = self.schema_columns_sql(expression.this)
                this_schema = f"{self.sep()}{this_schema}" if this_schema else ""

                return f"{this_name}{self.sep()}{copy_grants}{this_schema}"

            return super().createable_sql(expression, locations)

        def arrayagg_sql(self, expression: exp.ArrayAgg) -> str:
            this = expression.this

            # If an ORDER BY clause is present, we need to remove it from ARRAY_AGG
            # and add it later as part of the WITHIN GROUP clause
            order = this if isinstance(this, exp.Order) else None
            if order:
                expression.set("this", order.this.pop())

            expr_sql = super().arrayagg_sql(expression)

            if order:
                expr_sql = self.sql(exp.WithinGroup(this=expr_sql, expression=order))

            return expr_sql

        def array_sql(self, expression: exp.Array) -> str:
            expressions = expression.expressions

            first_expr = seq_get(expressions, 0)
            if isinstance(first_expr, exp.Select):
                # SELECT AS STRUCT foo AS alias_foo -> ARRAY_AGG(OBJECT_CONSTRUCT('alias_foo', foo))
                if first_expr.text("kind").upper() == "STRUCT":
                    object_construct_args = []
                    for expr in first_expr.expressions:
                        # Alias case: SELECT AS STRUCT foo AS alias_foo -> OBJECT_CONSTRUCT('alias_foo', foo)
                        # Column case: SELECT AS STRUCT foo -> OBJECT_CONSTRUCT('foo', foo)
                        name = expr.this if isinstance(expr, exp.Alias) else expr

                        object_construct_args.extend([exp.Literal.string(expr.alias_or_name), name])

                    array_agg = exp.ArrayAgg(
                        this=_build_object_construct(args=object_construct_args)
                    )

                    first_expr.set("kind", None)
                    first_expr.set("expressions", [array_agg])

                    return self.sql(first_expr.subquery())

            return inline_array_sql(self, expression)

        def currentdate_sql(self, expression: exp.CurrentDate) -> str:
            zone = self.sql(expression, "this")
            if not zone:
                return super().currentdate_sql(expression)

            expr = exp.Cast(
                this=exp.ConvertTimezone(target_tz=zone, timestamp=exp.CurrentTimestamp()),
                to=exp.DataType(this=exp.DataType.Type.DATE),
            )
            return self.sql(expr)

        def dot_sql(self, expression: exp.Dot) -> str:
            this = expression.this

            if not this.type:
                from sqlglot.optimizer.annotate_types import annotate_types

                this = annotate_types(this, dialect=self.dialect)

            if not isinstance(this, exp.Dot) and this.is_type(exp.DataType.Type.STRUCT):
                # Generate colon notation for the top level STRUCT
                return f"{self.sql(this)}:{self.sql(expression, 'expression')}"

            return super().dot_sql(expression)

        def modelattribute_sql(self, expression: exp.ModelAttribute) -> str:
            return f"{self.sql(expression, 'this')}!{self.sql(expression, 'expression')}"

        def format_sql(self, expression: exp.Format) -> str:
            if expression.name.lower() == "%s" and len(expression.expressions) == 1:
                return self.func("TO_CHAR", expression.expressions[0])

            return self.function_fallback_sql(expression)

        def splitpart_sql(self, expression: exp.SplitPart) -> str:
            # Set part_index to 1 if missing
            if not expression.args.get("delimiter"):
                expression.set("delimiter", exp.Literal.string(" "))

            if not expression.args.get("part_index"):
                expression.set("part_index", exp.Literal.number(1))

            return rename_func("SPLIT_PART")(self, expression)

        def uniform_sql(self, expression: exp.Uniform) -> str:
            gen = expression.args.get("gen")
            seed = expression.args.get("seed")

            # From Databricks UNIFORM(min, max, seed) -> Wrap gen in RANDOM(seed)
            if seed:
                gen = exp.Rand(this=seed)

            # No gen argument (from Databricks 2-arg UNIFORM(min, max)) -> Add RANDOM()
            if not gen:
                gen = exp.Rand()

            return self.func("UNIFORM", expression.this, expression.expression, gen)
