from __future__ import annotations

import typing as t

from sqlglot import exp, generator, jsonpath, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    array_append_sql,
    array_concat_sql,
    date_delta_sql,
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
from sqlglot.helper import find_new_name, flatten, seq_get
from sqlglot.optimizer.scope import build_scope, find_all_in_scope
from sqlglot.parsers.snowflake import (
    RANKING_WINDOW_FUNCTIONS_WITH_FRAME,
    TIMESTAMP_TYPES,
    SnowflakeParser,
    build_object_construct,
)
from sqlglot.tokens import TokenType
from sqlglot.typing.snowflake import EXPRESSION_METADATA

if t.TYPE_CHECKING:
    from sqlglot._typing import E


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


def _regexpilike_sql(self: Snowflake.Generator, expression: exp.RegexpILike) -> str:
    flag = expression.text("flag")

    if "i" not in flag:
        flag += "i"

    return self.func(
        "REGEXP_LIKE", expression.this, expression.expression, exp.Literal.string(flag)
    )


def _unqualify_pivot_columns(expression: exp.Expr) -> exp.Expr:
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
                    t.cast(exp.Expr, field).set("expressions", unqualified_field_expr, 0)

    return expression


def _flatten_structured_types_unless_iceberg(expression: exp.Expr) -> exp.Expr:
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


def _transform_generate_date_array(expression: exp.Expr) -> exp.Expr:
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
        "REGEXP_SUBSTR" if isinstance(expression, exp.RegexpExtract) else "REGEXP_SUBSTR_ALL",
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
        this: exp.Expr = exp.cast(ident, to=exp.DType.VARCHAR)
    else:
        this = exp.ParseJSON(this=f"TO_JSON({ident})")

    transform_lambda = exp.Lambda(expressions=[ident], this=this)

    return self.func("TRANSFORM", json_extract, transform_lambda)


def _qualify_unnested_columns(expression: exp.Expr) -> exp.Expr:
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
                if isinstance(unnest_ancestor, exp.Unnest):
                    ancestor_identifier = unnest_to_identifier.get(unnest_ancestor)
                    if (
                        ancestor_identifier
                        and ancestor_identifier.name.lower() == unnest_identifier.name.lower()
                    ):
                        continue

                table = unnest_identifier

            column.set("table", table and table.copy())

    return expression


def _eliminate_dot_variant_lookup(expression: exp.Expr) -> exp.Expr:
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

    Parser = SnowflakeParser

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
            "INTEGRATION": TokenType.INTEGRATION,
            "MATCH_CONDITION": TokenType.MATCH_CONDITION,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "MINUS": TokenType.EXCEPT,
            "NCHAR VARYING": TokenType.VARCHAR,
            "PACKAGE": TokenType.PACKAGE,
            "POLICY": TokenType.POLICY,
            "POOL": TokenType.POOL,
            "PUT": TokenType.PUT,
            "REMOVE": TokenType.COMMAND,
            "RM": TokenType.COMMAND,
            "ROLE": TokenType.ROLE,
            "RULE": TokenType.RULE,
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
            "VOLUME": TokenType.VOLUME,
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
                else exp.cast(e.expression, exp.DType.VARIANT, copy=False),
                e.this,
            ),
            exp.ArrayPosition: lambda self, e: self.func(
                "ARRAY_POSITION",
                e.expression,
                e.this,
            ),
            exp.ArrayIntersect: rename_func("ARRAY_INTERSECTION"),
            exp.ArrayOverlaps: rename_func("ARRAYS_OVERLAP"),
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
            exp.CurrentSchemas: lambda self, e: self.func("CURRENT_SCHEMAS"),
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
                "ARRAY_GENERATE_RANGE",
                e.args["start"],
                e.args["end"] if e.args.get("is_end_exclusive") else e.args["end"] + 1,
                e.args.get("step"),
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
            exp.Rand: lambda self, e: self.func("RANDOM", e.this),
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

        def nthvalue_sql(self, expression: exp.NthValue) -> str:
            result = self.func("NTH_VALUE", expression.this, expression.args.get("offset"))

            from_first = expression.args.get("from_first")

            if from_first is not None:
                if from_first:
                    result = result + " FROM FIRST"
                else:
                    result = result + " FROM LAST"

            return result

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DType.BIGDECIMAL: "DOUBLE",
            exp.DType.NESTED: "OBJECT",
            exp.DType.STRUCT: "OBJECT",
            exp.DType.TEXT: "VARCHAR",
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
            if expression.is_type(exp.DType.DOUBLE):
                parent = expression.parent
                if isinstance(parent, exp.DataType) and parent.is_type(exp.DType.VECTOR):
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
            if expression.is_type(exp.DType.GEOGRAPHY):
                return self.func("TO_GEOGRAPHY", expression.this)
            if expression.is_type(exp.DType.GEOMETRY):
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
            timelimit = expression.args.get("timelimit")

            if rowcount:
                args.append(exp.Kwarg(this=exp.var("ROWCOUNT"), expression=rowcount))
            if timelimit:
                args.append(exp.Kwarg(this=exp.var("TIMELIMIT"), expression=timelimit))

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
                offset.pop() if isinstance(offset, exp.Expr) else exp.to_identifier("index"),
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
            kind_value = expression.args.get("kind") or "TABLE"

            properties = expression.args.get("properties")
            if properties:
                qualifier = " ".join(self.sql(p) for p in properties.expressions)
                kind = f" {qualifier} {kind_value}"
            else:
                kind = f" {kind_value}"

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
                type_enum = exp.DType.TIMESTAMP

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
                this = exp.cast(this, exp.DType.TIMESTAMP)

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
                        this=build_object_construct(args=object_construct_args)
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
                to=exp.DataType(this=exp.DType.DATE),
            )
            return self.sql(expr)

        def dot_sql(self, expression: exp.Dot) -> str:
            this = expression.this

            if not this.type:
                from sqlglot.optimizer.annotate_types import annotate_types

                this = annotate_types(this, dialect=self.dialect)

            if not isinstance(this, exp.Dot) and this.is_type(exp.DType.STRUCT):
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

        def window_sql(self, expression: exp.Window) -> str:
            spec = expression.args.get("spec")
            this = expression.this

            if (
                (
                    isinstance(this, RANKING_WINDOW_FUNCTIONS_WITH_FRAME)
                    or (
                        isinstance(this, (exp.RespectNulls, exp.IgnoreNulls))
                        and isinstance(this.this, RANKING_WINDOW_FUNCTIONS_WITH_FRAME)
                    )
                )
                and spec
                and (
                    spec.text("kind").upper() == "ROWS"
                    and spec.text("start").upper() == "UNBOUNDED"
                    and spec.text("start_side").upper() == "PRECEDING"
                    and spec.text("end").upper() == "UNBOUNDED"
                    and spec.text("end_side").upper() == "FOLLOWING"
                )
            ):
                # omit the default window from window ranking functions
                expression.set("spec", None)
            return super().window_sql(expression)
