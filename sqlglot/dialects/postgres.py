from _future_ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    DATE_ADD_OR_SUB,
    Dialect,
    JSON_EXTRACT_TYPE,
    any_value_to_max_sql,
    bool_xor_sql,
    datestrtodate_sql,
    build_formatted_time,
    filter_array_using_unnest,
    json_extract_segments,
    json_path_key_only_name,
    max_or_greatest,
    merge_without_target_sql,
    min_or_least,
    no_last_day_sql,
    no_map_from_entries_sql,
    no_paren_current_date_sql,
    no_pivot_sql,
    no_trycast_sql,
    build_json_extract_path,
    build_timestamp_trunc,
    rename_func,
    str_position_sql,
    struct_extract_sql,
    timestamptrunc_sql,
    timestrtotime_sql,
    trim_sql,
    ts_or_ds_add_cast,
)
from sqlglot.helper import is_int, seq_get
from sqlglot.parser import binary_range_parser
from sqlglot.tokens import TokenType

DATE_DIFF_FACTOR = {
    "MICROSECOND": " * 1000000",
    "MILLISECOND": " * 1000",
    "SECOND": "",
    "MINUTE": " / 60",
    "HOUR": " / 3600",
    "DAY": " / 86400",
}

def _date_add_sql(kind: str) -> t.Callable[[Postgres.Generator, DATE_ADD_OR_SUB], str]:
    def func(self: Postgres.Generator, expression: DATE_ADD_OR_SUB) -> str:
        if isinstance(expression, exp.TsOrDsAdd):
            expression = ts_or_ds_add_cast(expression)

        this = self.sql(expression, "this")
        unit = expression.args.get("unit")

        expression = self._simplify_unless_literal(expression.expression)
        if not isinstance(expression, exp.Literal):
            self.unsupported("Cannot add non literal")

        expression.args["is_string"] = True
        return f"{this} {kind} {self.sql(exp.Interval(this=expression, unit=unit))}"

    return func

def _date_diff_sql(self: Postgres.Generator, expression: exp.DateDiff) -> str:
    unit = expression.text("unit").upper()
    factor = DATE_DIFF_FACTOR.get(unit)

    end = f"CAST({self.sql(expression, 'this')} AS TIMESTAMP)"
    start = f"CAST({self.sql(expression, 'expression')} AS TIMESTAMP)"

    if factor is not None:
        return f"CAST(EXTRACT(epoch FROM {end} - {start}){factor} AS BIGINT)"

    age = f"AGE({end}, {start})"

    if unit == "WEEK":
        unit = f"EXTRACT(days FROM ({end} - {start})) / 7"
    elif unit == "MONTH":
        unit = f"EXTRACT(year FROM {age}) * 12 + EXTRACT(month FROM {age})"
    elif unit == "QUARTER":
        unit = f"EXTRACT(year FROM {age}) * 4 + EXTRACT(month FROM {age}) / 3"
    elif unit == "YEAR":
        unit = f"EXTRACT(year FROM {age})"
    else:
        unit = age

    return f"CAST({unit} AS BIGINT)"

def _substring_sql(self: Postgres.Generator, expression: exp.Substring) -> str:
    this = self.sql(expression, "this")
    start = self.sql(expression, "start")
    length = self.sql(expression, "length")

    from_part = f" FROM {start}" if start else ""
    for_part = f" FOR {length}" if length else ""

    return f"SUBSTRING({this}{from_part}{for_part})"

def _string_agg_sql(self: Postgres.Generator, expression: exp.GroupConcat) -> str:
    separator = expression.args.get("separator") or exp.Literal.string(",")

    order = ""
    this = expression.this
    if isinstance(this, exp.Order):
        if this.this:
            this = this.this.pop()
        order = self.sql(expression.this)  # Order has a leading space

    return f"STRING_AGG({self.format_args(this, separator)}{order})"

def _datatype_sql(self: Postgres.Generator, expression: exp.DataType) -> str:
    if expression.is_type("array"):
        if expression.expressions:
            values = self.expressions(expression, key="values", flat=True)
            return f"{self.expressions(expression, flat=True)}[{values}]"
        return "ARRAY"
    return self.datatype_sql(expression)

def _auto_increment_to_serial(expression: exp.Expression) -> exp.Expression:
    auto = expression.find(exp.AutoIncrementColumnConstraint)

    if auto:
        expression.args["constraints"].remove(auto.parent)
        kind = expression.args["kind"]

        if kind.this == exp.DataType.Type.INT:
            kind.replace(exp.DataType(this=exp.DataType.Type.SERIAL))
        elif kind.this == exp.DataType.Type.SMALLINT:
            kind.replace(exp.DataType(this=exp.DataType.Type.SMALLSERIAL))
        elif kind.this == exp.DataType.Type.BIGINT:
            kind.replace(exp.DataType(this=exp.DataType.Type.BIGSERIAL))

    return expression

def _serial_to_generated(expression: exp.Expression) -> exp.Expression:
    if not isinstance(expression, exp.ColumnDef):
        return expression
    kind = expression.kind
    if not kind:
        return expression

    if kind.this == exp.DataType.Type.SERIAL:
        data_type = exp.DataType(this=exp.DataType.Type.INT)
    elif kind.this == exp.DataType.Type.SMALLSERIAL:
        data_type = exp.DataType(this=exp.DataType.Type.SMALLINT)
    elif kind.this == exp.DataType.Type.BIGSERIAL:
        data_type = exp.DataType(this=exp.DataType.Type.BIGINT)
    else:
        data_type = None

    if data_type:
        expression.args["kind"].replace(data_type)
        constraints = expression.args["constraints"]
        generated = exp.ColumnConstraint(kind=exp.GeneratedAsIdentityColumnConstraint(this=False))
        notnull = exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())

        if notnull not in constraints:
            constraints.insert(0, notnull)
        if generated not in constraints:
            constraints.insert(0, generated)

    return expression

def _build_generate_series(args: t.List) -> exp.GenerateSeries:
    # The goal is to convert step values like '1 day' or INTERVAL '1 day' into INTERVAL '1' day
    step = seq_get(args, 2)

    if step is None:
        # Postgres allows calls with just two arguments -- the "step" argument defaults to 1
        return exp.GenerateSeries.from_arg_list(args)

    if step.is_string:
        args[2] = exp.to_interval(step.this)
    elif isinstance(step, exp.Interval) and not step.args.get("unit"):
        args[2] = exp.to_interval(step.this.this)

    return exp.GenerateSeries.from_arg_list(args)

def _build_to_timestamp(args: t.List) -> exp.UnixToTime | exp.StrToTime:
    # TO_TIMESTAMP accepts either a single double argument or (text, text)
    if len(args) == 1:
        # https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-TABLE
        return exp.UnixToTime.from_arg_list(args)

    # https://www.postgresql.org/docs/current/functions-formatting.html
    return build_formatted_time(exp.StrToTime, "postgres")(args)

def _json_extract_sql(
    name: str, op: str
) -> t.Callable[[Postgres.Generator, JSON_EXTRACT_TYPE], str]:
    def _generate(self: Postgres.Generator, expression: JSON_EXTRACT_TYPE) -> str:
        if expression.args.get("only_json_types"):
            return json_extract_segments(name, quoted_index=False, op=op)(self, expression)
        return json_extract_segments(name)(self, expression)

    return _generate

def _build_regexp_replace(args: t.List) -> exp.RegexpReplace:
    # The signature of REGEXP_REPLACE is:
    # regexp_replace(source, pattern, replacement [, start [, N ]] [, flags ])
    #
    # Any one of start, N and flags can be column references, meaning that
    # unless we can statically see that the last argument is a non-integer string
    # (eg. not '0'), then it's not possible to construct the correct AST
    if len(args) > 3:
        last = args[-1]
        if not is_int(last.name):
            if not last.type or last.is_type(exp.DataType.Type.UNKNOWN, exp.DataType.Type.NULL):
                from sqlglot.optimizer.annotate_types import annotate_types

                last = annotate_types(last)

            if last.is_type(*exp.DataType.TEXT_TYPES):
                regexp_replace = exp.RegexpReplace.from_arg_list(args[:-1])
                regexp_replace.set("modifiers", last)
                return regexp_replace

    return exp.RegexpReplace.from_arg_list(args)

def _unix_to_time_sql(self: Postgres.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func("TO_TIMESTAMP", timestamp, self.format_time(expression))

    return self.func(
        "TO_TIMESTAMP",
        exp.Div(this=timestamp, expression=exp.func("POWER", 10, scale)),
        self.format_time(expression),
    )

def _top_sql(self, expression: exp.Top) -> str:
    return f"LIMIT {self.sql(expression, 'value')}"

class Postgres(Dialect):
    index_offset = 1
    null_ordering = "nulls_are_last"
    time_trie_mapping = {"i": "minute"}
    time_format = "'YYYY-MM-DD HH24:MI:SS'"
    time_mapping = {
        "d": "day",
        "day": "day",
        "dd": "day",
        "hh24": "hour",
        "hour": "hour",
        "mi": "minute",
        "minute": "minute",
        "mon": "month",
        "month": "month",
        "ss": "second",
        "second": "second",
        "yyyy": "year",
        "yy": "year",
        "year": "year",
    }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "DATE_PART": lambda args: exp.Extract(
                this=exp.Var(this=exp.Literal.string(seq_get(args, 0).name.lower())),
                expression=seq_get(args, 1),
            ),
            "GENERATE_SERIES": _build_generate_series,
            "TO_TIMESTAMP": _build_to_timestamp,
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "STRING_AGG": lambda self: self.expression(
                exp.GroupConcat,
                this=self._parse_lambda(),
                order=self._parse_order(),
                separator=self._match(TokenType.COMMA) and self._parse_bitwise(),
            ),
            "DATE_TRUNC": lambda self: self.expression(
                exp.TimestampTrunc,
                this=self._parse_var(),
                unit=self._parse_var(),
                expression=self._parse_bitwise(),
            ),
        }

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,
            TokenType.DOUBLE_COLON: binary_range_parser(exp.Cast),
        }

        COLUMN_CONSTRAINT_PARSERS = {
            **parser.Parser.COLUMN_CONSTRAINT_PARSERS,
            "GENERATED": lambda self: self._generated(self._column_constraint()),
        }

        TABLE_END = {*parser.Parser.TABLE_END, TokenType.USING}

    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "BOOLEAN": TokenType.BOOLEAN,
            "BYTEA": TokenType.BINARY,
            "DOUBLE PRECISION": TokenType.DOUBLE,
            "DOUBLE_PRECISION": TokenType.DOUBLE,
            "HSTORE": TokenType.MAP,
            "JSONB": TokenType.JSON,
            "SERIAL": TokenType.SEQUENCE,
            "SMALLSERIAL": TokenType.SEQUENCE,
            "BIGSERIAL": TokenType.SEQUENCE,
        }

    class Generator(generator.Generator):
        INTERVAL_KEYWORDS = {
            **generator.Generator.INTERVAL_KEYWORDS,
            "MICROSECOND": "MICROSECONDS",
            "MILLISECOND": "MILLISECONDS",
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.BINARY: "BYTEA",
            exp.DataType.Type.MAP: "HSTORE",
            exp.DataType.Type.VARBINARY: "BYTEA",
            exp.DataType.Type.JSON: "JSONB",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.FLOAT: "DOUBLE PRECISION",
            exp.DataType.Type.SMALLINT: "SMALLINT",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            **transforms.UNIX_TO_TIME,
            **transforms.FORMAT_TIME_MAPPING.get("postgres", {}),
            exp.AnonymousProperty: lambda self, e: f"{self.sql(e, 'this')}.{self.sql(e, 'value')}",
            exp.AnyValue: any_value_to_max_sql,
            exp.AutoIncrementColumnConstraint: lambda *args: "",
            exp.BoolAnd: lambda self, e: f"BOOL_AND({self.sql(e, 'this')})",
            exp.BoolOr: lambda self, e: f"BOOL_OR({self.sql(e, 'this')})",
            exp.BoolXor: bool_xor_sql,
            exp.DataType: _datatype_sql,
            exp.DateAdd: _date_add_sql("+"),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateSub: _date_add_sql("-"),
            exp.DateDiff: _date_diff_sql,
            exp.Dummy: lambda *args: "SELECT 1",
            exp.Field: lambda self, e: f"{self.sql(e, 'this')}#{self.sql(e, 'expression')}",
            exp.GroupConcat: _string_agg_sql,
            exp.JSONExtract: _json_extract_sql("->", "->"),
            exp.JSONExtractScalar: _json_extract_sql("->>", "->>"),
            exp.JSONPathExtract: _json_extract_sql("#>", "->"),
            exp.JSONPathExtractScalar: _json_extract_sql("#>>", "->>"),
            exp.Max: max_or_greatest,
            exp.Min: min_or_least,
            exp.Pivot: no_pivot_sql,
            exp.RegexpLike: rename_func("REGEXP_MATCHES"),
            exp.RegexpReplace: _build_regexp_replace,
            exp.Select: lambda self, e: self.select_sql(e),
            exp.Substring: _substring_sql,
            exp.StrPosition: str_position_sql,
            exp.TableSample: transforms.no_tablesample_sql,
            exp.TableAlias: lambda self, e: f"{self.sql(e, 'this')} {self.sql(e, 'alias')}",
            exp.TableFunction: lambda self, e: f"{self.sql(e, 'this')}({self.sql(e, 'alias')})",
            exp.TimestampStrToTime: timestrtotime_sql,
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TsOrDsAdd: _date_add_sql("+"),
            exp.TsOrDsSub: _date_add_sql("-"),
            exp.UnixToTime: _unix_to_time_sql,
            exp.Unpivot: no_pivot_sql,
            exp.Merge: merge_without_target_sql,
            exp.AnyValue: any_value_to_max_sql,
            exp.Top: _top_sql,  # Add support for TOP keyword
        }

        def select_sql(self, expression: exp.Select) -> str:
            sql = super().select_sql(expression)
            if expression.args.get("top"):
                sql = f"{sql} {self.sql(expression, 'top')}"  # Add TOP clause if present
            return sql
