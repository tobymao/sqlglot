from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    approx_count_distinct_sql,
    create_with_partitions_sql,
    format_time_lambda,
    if_sql,
    locate_to_strposition,
    max_or_greatest,
    min_or_least,
    no_ilike_sql,
    no_recursive_cte_sql,
    no_safe_divide_sql,
    no_trycast_sql,
    rename_func,
    strposition_to_locate_sql,
    struct_extract_sql,
    timestrtotime_sql,
    var_map_sql,
)
from sqlglot.helper import seq_get
from sqlglot.parser import parse_var_map
from sqlglot.tokens import TokenType

# (FuncType, Multiplier)
DATE_DELTA_INTERVAL = {
    "YEAR": ("ADD_MONTHS", 12),
    "MONTH": ("ADD_MONTHS", 1),
    "QUARTER": ("ADD_MONTHS", 3),
    "WEEK": ("DATE_ADD", 7),
    "DAY": ("DATE_ADD", 1),
}

TIME_DIFF_FACTOR = {
    "MILLISECOND": " * 1000",
    "SECOND": "",
    "MINUTE": " / 60",
    "HOUR": " / 3600",
}

DIFF_MONTH_SWITCH = ("YEAR", "QUARTER", "MONTH")


def _add_date_sql(self: generator.Generator, expression: exp.DateAdd | exp.DateSub) -> str:
    unit = expression.text("unit").upper()
    func, multiplier = DATE_DELTA_INTERVAL.get(unit, ("DATE_ADD", 1))

    if isinstance(expression, exp.DateSub):
        multiplier *= -1

    if expression.expression.is_number:
        modified_increment = exp.Literal.number(int(expression.text("expression")) * multiplier)
    else:
        modified_increment = expression.expression
        if multiplier != 1:
            modified_increment = exp.Mul(  # type: ignore
                this=modified_increment, expression=exp.Literal.number(multiplier)
            )

    return self.func(func, expression.this, modified_increment)


def _date_diff_sql(self: generator.Generator, expression: exp.DateDiff) -> str:
    unit = expression.text("unit").upper()

    factor = TIME_DIFF_FACTOR.get(unit)
    if factor is not None:
        left = self.sql(expression, "this")
        right = self.sql(expression, "expression")
        sec_diff = f"UNIX_TIMESTAMP({left}) - UNIX_TIMESTAMP({right})"
        return f"({sec_diff}){factor}" if factor else sec_diff

    sql_func = "MONTHS_BETWEEN" if unit in DIFF_MONTH_SWITCH else "DATEDIFF"
    _, multiplier = DATE_DELTA_INTERVAL.get(unit, ("", 1))
    multiplier_sql = f" / {multiplier}" if multiplier > 1 else ""
    diff_sql = f"{sql_func}({self.format_args(expression.this, expression.expression)})"
    return f"{diff_sql}{multiplier_sql}"


def _json_format_sql(self: generator.Generator, expression: exp.JSONFormat) -> str:
    this = expression.this

    if not this.type:
        from sqlglot.optimizer.annotate_types import annotate_types

        annotate_types(this)

    if this.type.is_type(exp.DataType.Type.JSON):
        return self.sql(this)
    return self.func("TO_JSON", this, expression.args.get("options"))


def _array_sort_sql(self: generator.Generator, expression: exp.ArraySort) -> str:
    if expression.expression:
        self.unsupported("Hive SORT_ARRAY does not support a comparator")
    return f"SORT_ARRAY({self.sql(expression, 'this')})"


def _property_sql(self: generator.Generator, expression: exp.Property) -> str:
    return f"'{expression.name}'={self.sql(expression, 'value')}"


def _str_to_unix_sql(self: generator.Generator, expression: exp.StrToUnix) -> str:
    return self.func("UNIX_TIMESTAMP", expression.this, _time_format(self, expression))


def _str_to_date_sql(self: generator.Generator, expression: exp.StrToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format not in (Hive.time_format, Hive.date_format):
        this = f"FROM_UNIXTIME(UNIX_TIMESTAMP({this}, {time_format}))"
    return f"CAST({this} AS DATE)"


def _str_to_time_sql(self: generator.Generator, expression: exp.StrToTime) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format not in (Hive.time_format, Hive.date_format):
        this = f"FROM_UNIXTIME(UNIX_TIMESTAMP({this}, {time_format}))"
    return f"CAST({this} AS TIMESTAMP)"


def _time_format(
    self: generator.Generator, expression: exp.UnixToStr | exp.StrToUnix
) -> t.Optional[str]:
    time_format = self.format_time(expression)
    if time_format == Hive.time_format:
        return None
    return time_format


def _time_to_str(self: generator.Generator, expression: exp.TimeToStr) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    return f"DATE_FORMAT({this}, {time_format})"


def _to_date_sql(self: generator.Generator, expression: exp.TsOrDsToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format and time_format not in (Hive.time_format, Hive.date_format):
        return f"TO_DATE({this}, {time_format})"
    return f"TO_DATE({this})"


def _index_sql(self: generator.Generator, expression: exp.Index) -> str:
    this = self.sql(expression, "this")
    table = self.sql(expression, "table")
    columns = self.sql(expression, "columns")
    return f"{this} ON TABLE {table} {columns}"


class Hive(Dialect):
    alias_post_tablesample = True

    time_mapping = {
        "y": "%Y",
        "Y": "%Y",
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "M": "%-m",
        "dd": "%d",
        "d": "%-d",
        "HH": "%H",
        "H": "%-H",
        "hh": "%I",
        "h": "%-I",
        "mm": "%M",
        "m": "%-M",
        "ss": "%S",
        "s": "%-S",
        "SSSSSS": "%f",
        "a": "%p",
        "DD": "%j",
        "D": "%-j",
        "E": "%a",
        "EE": "%a",
        "EEE": "%a",
        "EEEE": "%A",
    }

    date_format = "'yyyy-MM-dd'"
    dateint_format = "'yyyyMMdd'"
    time_format = "'yyyy-MM-dd HH:mm:ss'"

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["\\"]
        ENCODE = "utf-8"
        IDENTIFIER_CAN_START_WITH_DIGIT = True

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ADD ARCHIVE": TokenType.COMMAND,
            "ADD ARCHIVES": TokenType.COMMAND,
            "ADD FILE": TokenType.COMMAND,
            "ADD FILES": TokenType.COMMAND,
            "ADD JAR": TokenType.COMMAND,
            "ADD JARS": TokenType.COMMAND,
            "MSCK REPAIR": TokenType.COMMAND,
            "WITH SERDEPROPERTIES": TokenType.SERDE_PROPERTIES,
        }

        NUMERIC_LITERALS = {
            "L": "BIGINT",
            "S": "SMALLINT",
            "Y": "TINYINT",
            "D": "DOUBLE",
            "F": "FLOAT",
            "BD": "DECIMAL",
        }

    class Parser(parser.Parser):
        LOG_DEFAULTS_TO_LN = True
        STRICT_CAST = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "BASE64": exp.ToBase64.from_arg_list,
            "COLLECT_LIST": exp.ArrayAgg.from_arg_list,
            "DATE_ADD": lambda args: exp.TsOrDsAdd(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                unit=exp.Literal.string("DAY"),
            ),
            "DATEDIFF": lambda args: exp.DateDiff(
                this=exp.TsOrDsToDate(this=seq_get(args, 0)),
                expression=exp.TsOrDsToDate(this=seq_get(args, 1)),
            ),
            "DATE_SUB": lambda args: exp.TsOrDsAdd(
                this=seq_get(args, 0),
                expression=exp.Mul(
                    this=seq_get(args, 1),
                    expression=exp.Literal.number(-1),
                ),
                unit=exp.Literal.string("DAY"),
            ),
            "DATE_FORMAT": lambda args: format_time_lambda(exp.TimeToStr, "hive")(
                [
                    exp.TimeStrToTime(this=seq_get(args, 0)),
                    seq_get(args, 1),
                ]
            ),
            "DAY": lambda args: exp.Day(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
            "FROM_UNIXTIME": format_time_lambda(exp.UnixToStr, "hive", True),
            "GET_JSON_OBJECT": exp.JSONExtractScalar.from_arg_list,
            "LOCATE": locate_to_strposition,
            "MAP": parse_var_map,
            "MONTH": lambda args: exp.Month(this=exp.TsOrDsToDate.from_arg_list(args)),
            "PERCENTILE": exp.Quantile.from_arg_list,
            "PERCENTILE_APPROX": exp.ApproxQuantile.from_arg_list,
            "COLLECT_SET": exp.SetAgg.from_arg_list,
            "SIZE": exp.ArraySize.from_arg_list,
            "SPLIT": exp.RegexpSplit.from_arg_list,
            "TO_DATE": format_time_lambda(exp.TsOrDsToDate, "hive"),
            "TO_JSON": exp.JSONFormat.from_arg_list,
            "UNBASE64": exp.FromBase64.from_arg_list,
            "UNIX_TIMESTAMP": format_time_lambda(exp.StrToUnix, "hive", True),
            "YEAR": lambda args: exp.Year(this=exp.TsOrDsToDate.from_arg_list(args)),
        }

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,  # type: ignore
            "WITH SERDEPROPERTIES": lambda self: exp.SerdeProperties(
                expressions=self._parse_wrapped_csv(self._parse_property)
            ),
        }

        QUERY_MODIFIER_PARSERS = {
            **parser.Parser.QUERY_MODIFIER_PARSERS,
            "distribute": lambda self: self._parse_sort(TokenType.DISTRIBUTE_BY, exp.Distribute),
            "sort": lambda self: self._parse_sort(TokenType.SORT_BY, exp.Sort),
            "cluster": lambda self: self._parse_sort(TokenType.CLUSTER_BY, exp.Cluster),
        }

    class Generator(generator.Generator):
        LIMIT_FETCH = "LIMIT"
        TABLESAMPLE_WITH_METHOD = False
        TABLESAMPLE_SIZE_IS_PERCENT = True
        JOIN_HINTS = False
        TABLE_HINTS = False

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.VARBINARY: "BINARY",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.BIT: "BOOLEAN",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.Group: transforms.preprocess([transforms.unalias_group]),
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_qualify,
                    transforms.eliminate_distinct_on,
                    transforms.unnest_to_explode,
                ]
            ),
            exp.Property: _property_sql,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ArrayConcat: rename_func("CONCAT"),
            exp.ArrayJoin: lambda self, e: self.func("CONCAT_WS", e.expression, e.this),
            exp.ArraySize: rename_func("SIZE"),
            exp.ArraySort: _array_sort_sql,
            exp.With: no_recursive_cte_sql,
            exp.DateAdd: _add_date_sql,
            exp.DateDiff: _date_diff_sql,
            exp.DateStrToDate: rename_func("TO_DATE"),
            exp.DateSub: _add_date_sql,
            exp.DateToDi: lambda self, e: f"CAST(DATE_FORMAT({self.sql(e, 'this')}, {Hive.dateint_format}) AS INT)",
            exp.DiToDate: lambda self, e: f"TO_DATE(CAST({self.sql(e, 'this')} AS STRING), {Hive.dateint_format})",
            exp.FileFormatProperty: lambda self, e: f"STORED AS {self.sql(e, 'this') if isinstance(e.this, exp.InputOutputFormat) else e.name.upper()}",
            exp.FromBase64: rename_func("UNBASE64"),
            exp.If: if_sql,
            exp.Index: _index_sql,
            exp.ILike: no_ilike_sql,
            exp.JSONExtract: rename_func("GET_JSON_OBJECT"),
            exp.JSONExtractScalar: rename_func("GET_JSON_OBJECT"),
            exp.JSONFormat: _json_format_sql,
            exp.Map: var_map_sql,
            exp.Max: max_or_greatest,
            exp.Min: min_or_least,
            exp.VarMap: var_map_sql,
            exp.Create: create_with_partitions_sql,
            exp.Quantile: rename_func("PERCENTILE"),
            exp.ApproxQuantile: rename_func("PERCENTILE_APPROX"),
            exp.RegexpLike: lambda self, e: self.binary(e, "RLIKE"),
            exp.RegexpSplit: rename_func("SPLIT"),
            exp.SafeDivide: no_safe_divide_sql,
            exp.SchemaCommentProperty: lambda self, e: self.naked_property(e),
            exp.SetAgg: rename_func("COLLECT_SET"),
            exp.Split: lambda self, e: f"SPLIT({self.sql(e, 'this')}, CONCAT('\\\\Q', {self.sql(e, 'expression')}))",
            exp.StrPosition: strposition_to_locate_sql,
            exp.StrToDate: _str_to_date_sql,
            exp.StrToTime: _str_to_time_sql,
            exp.StrToUnix: _str_to_unix_sql,
            exp.StructExtract: struct_extract_sql,
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeToStr: _time_to_str,
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.ToBase64: rename_func("BASE64"),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS STRING), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.TsOrDsToDate: _to_date_sql,
            exp.TryCast: no_trycast_sql,
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, _time_format(self, e)
            ),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.UnixToTimeStr: rename_func("FROM_UNIXTIME"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITIONED BY {self.sql(e, 'this')}",
            exp.RowFormatSerdeProperty: lambda self, e: f"ROW FORMAT SERDE {self.sql(e, 'this')}",
            exp.SerdeProperties: lambda self, e: self.properties(e, prefix="WITH SERDEPROPERTIES"),
            exp.NumberToStr: rename_func("FORMAT_NUMBER"),
            exp.LastDateOfMonth: rename_func("LAST_DAY"),
            exp.National: lambda self, e: self.sql(e, "this"),
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.FileFormatProperty: exp.Properties.Location.POST_SCHEMA,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def arrayagg_sql(self, expression: exp.ArrayAgg) -> str:
            return self.func(
                "COLLECT_LIST",
                expression.this.this if isinstance(expression.this, exp.Order) else expression.this,
            )

        def with_properties(self, properties: exp.Properties) -> str:
            return self.properties(
                properties,
                prefix=self.seg("TBLPROPERTIES"),
            )

        def datatype_sql(self, expression: exp.DataType) -> str:
            if (
                expression.this in (exp.DataType.Type.VARCHAR, exp.DataType.Type.NVARCHAR)
                and not expression.expressions
            ):
                expression = exp.DataType.build("text")
            elif expression.this in exp.DataType.TEMPORAL_TYPES:
                expression = exp.DataType.build(expression.this)

            return super().datatype_sql(expression)

        def after_having_modifiers(self, expression):
            return super().after_having_modifiers(expression) + [
                self.sql(expression, "distribute"),
                self.sql(expression, "sort"),
                self.sql(expression, "cluster"),
            ]
