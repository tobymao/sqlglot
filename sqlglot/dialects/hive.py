from __future__ import annotations

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    approx_count_distinct_sql,
    create_with_partitions_sql,
    format_time_lambda,
    if_sql,
    locate_to_strposition,
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

DIFF_MONTH_SWITCH = ("YEAR", "QUARTER", "MONTH")


def _add_date_sql(self, expression):
    unit = expression.text("unit").upper()
    func, multiplier = DATE_DELTA_INTERVAL.get(unit, ("DATE_ADD", 1))
    modified_increment = (
        int(expression.text("expression")) * multiplier
        if expression.expression.is_number
        else expression.expression
    )
    modified_increment = exp.Literal.number(modified_increment)
    return f"{func}({self.format_args(expression.this, modified_increment.this)})"


def _date_diff_sql(self, expression):
    unit = expression.text("unit").upper()
    sql_func = "MONTHS_BETWEEN" if unit in DIFF_MONTH_SWITCH else "DATEDIFF"
    _, multiplier = DATE_DELTA_INTERVAL.get(unit, ("", 1))
    multiplier_sql = f" / {multiplier}" if multiplier > 1 else ""
    diff_sql = f"{sql_func}({self.format_args(expression.this, expression.expression)})"
    return f"{diff_sql}{multiplier_sql}"


def _array_sort(self, expression):
    if expression.expression:
        self.unsupported("Hive SORT_ARRAY does not support a comparator")
    return f"SORT_ARRAY({self.sql(expression, 'this')})"


def _property_sql(self, expression):
    return f"'{expression.name}'={self.sql(expression, 'value')}"


def _str_to_unix(self, expression):
    return f"UNIX_TIMESTAMP({self.format_args(expression.this, _time_format(self, expression))})"


def _str_to_date(self, expression):
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format not in (Hive.time_format, Hive.date_format):
        this = f"FROM_UNIXTIME(UNIX_TIMESTAMP({this}, {time_format}))"
    return f"CAST({this} AS DATE)"


def _str_to_time(self, expression):
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format not in (Hive.time_format, Hive.date_format):
        this = f"FROM_UNIXTIME(UNIX_TIMESTAMP({this}, {time_format}))"
    return f"CAST({this} AS TIMESTAMP)"


def _time_format(self, expression):
    time_format = self.format_time(expression)
    if time_format == Hive.time_format:
        return None
    return time_format


def _time_to_str(self, expression):
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    return f"DATE_FORMAT({this}, {time_format})"


def _to_date_sql(self, expression):
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format and time_format not in (Hive.time_format, Hive.date_format):
        return f"TO_DATE({this}, {time_format})"
    return f"TO_DATE({this})"


def _unnest_to_explode_sql(self, expression):
    unnest = expression.this
    if isinstance(unnest, exp.Unnest):
        alias = unnest.args.get("alias")
        udtf = exp.Posexplode if unnest.args.get("ordinality") else exp.Explode
        return "".join(
            self.sql(
                exp.Lateral(
                    this=udtf(this=expression),
                    view=True,
                    alias=exp.TableAlias(this=alias.this, columns=[column]),
                )
            )
            for expression, column in zip(unnest.expressions, alias.columns if alias else [])
        )
    return self.join_sql(expression)


def _index_sql(self, expression):
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
        ESCAPES = ["\\"]
        ENCODE = "utf-8"

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

        IDENTIFIER_CAN_START_WITH_DIGIT = True

    class Parser(parser.Parser):
        STRICT_CAST = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "APPROX_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
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
            "LOG": (
                lambda args: exp.Log.from_arg_list(args)
                if len(args) > 1
                else exp.Ln.from_arg_list(args)
            ),
            "MAP": parse_var_map,
            "MONTH": lambda args: exp.Month(this=exp.TsOrDsToDate.from_arg_list(args)),
            "PERCENTILE": exp.Quantile.from_arg_list,
            "PERCENTILE_APPROX": exp.ApproxQuantile.from_arg_list,
            "COLLECT_SET": exp.SetAgg.from_arg_list,
            "SIZE": exp.ArraySize.from_arg_list,
            "SPLIT": exp.RegexpSplit.from_arg_list,
            "TO_DATE": format_time_lambda(exp.TsOrDsToDate, "hive"),
            "UNIX_TIMESTAMP": format_time_lambda(exp.StrToUnix, "hive", True),
            "YEAR": lambda args: exp.Year(this=exp.TsOrDsToDate.from_arg_list(args)),
        }

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,  # type: ignore
            TokenType.SERDE_PROPERTIES: lambda self: exp.SerdeProperties(
                expressions=self._parse_wrapped_csv(self._parse_property)
            ),
        }

    class Generator(generator.Generator):
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.VARBINARY: "BINARY",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            **transforms.UNALIAS_GROUP,  # type: ignore
            exp.Property: _property_sql,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.ArrayConcat: rename_func("CONCAT"),
            exp.ArraySize: rename_func("SIZE"),
            exp.ArraySort: _array_sort,
            exp.With: no_recursive_cte_sql,
            exp.DateAdd: _add_date_sql,
            exp.DateDiff: _date_diff_sql,
            exp.DateStrToDate: rename_func("TO_DATE"),
            exp.DateToDi: lambda self, e: f"CAST(DATE_FORMAT({self.sql(e, 'this')}, {Hive.dateint_format}) AS INT)",
            exp.DiToDate: lambda self, e: f"TO_DATE(CAST({self.sql(e, 'this')} AS STRING), {Hive.dateint_format})",
            exp.FileFormatProperty: lambda self, e: f"STORED AS {e.name.upper()}",
            exp.If: if_sql,
            exp.Index: _index_sql,
            exp.ILike: no_ilike_sql,
            exp.Join: _unnest_to_explode_sql,
            exp.JSONExtract: rename_func("GET_JSON_OBJECT"),
            exp.JSONExtractScalar: rename_func("GET_JSON_OBJECT"),
            exp.Map: var_map_sql,
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
            exp.StrToDate: _str_to_date,
            exp.StrToTime: _str_to_time,
            exp.StrToUnix: _str_to_unix,
            exp.StructExtract: struct_extract_sql,
            exp.TableFormatProperty: lambda self, e: f"USING {self.sql(e, 'this')}",
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeToStr: _time_to_str,
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS STRING), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.TsOrDsToDate: _to_date_sql,
            exp.TryCast: no_trycast_sql,
            exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({self.format_args(e.this, _time_format(self, e))})",
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.UnixToTimeStr: rename_func("FROM_UNIXTIME"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITIONED BY {self.sql(e, 'this')}",
            exp.RowFormatSerdeProperty: lambda self, e: f"ROW FORMAT SERDE {self.sql(e, 'this')}",
            exp.SerdeProperties: lambda self, e: self.properties(e, prefix="WITH SERDEPROPERTIES"),
            exp.NumberToStr: rename_func("FORMAT_NUMBER"),
            exp.LastDateOfMonth: rename_func("LAST_DAY"),
        }

        WITH_PROPERTIES = {exp.Property}

        ROOT_PROPERTIES = {
            exp.PartitionedByProperty,
            exp.FileFormatProperty,
            exp.SchemaCommentProperty,
            exp.LocationProperty,
            exp.TableFormatProperty,
            exp.RowFormatDelimitedProperty,
            exp.RowFormatSerdeProperty,
            exp.SerdeProperties,
        }

        def with_properties(self, properties):
            return self.properties(
                properties,
                prefix=self.seg("TBLPROPERTIES"),
            )

        def datatype_sql(self, expression):
            if (
                expression.this in (exp.DataType.Type.VARCHAR, exp.DataType.Type.NVARCHAR)
                and not expression.expressions
            ):
                expression = exp.DataType.build("text")
            elif expression.this in exp.DataType.TEMPORAL_TYPES:
                expression = exp.DataType.build(expression.this)
            return super().datatype_sql(expression)
