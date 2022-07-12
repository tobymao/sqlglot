from sqlglot import exp, constants as c
from sqlglot.dialects.dialect import (
    Dialect,
    approx_count_distinct_sql,
    format_time_lambda,
    if_sql,
    no_ilike_sql,
    no_safe_divide_sql,
    no_recursive_cte_sql,
    no_trycast_sql,
    rename_func,
    struct_extract_sql,
)
from sqlglot.generator import Generator
from sqlglot.helper import csv, list_get
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer


def _parse_map(args):
    keys = []
    values = []
    for i in range(0, len(args), 2):
        keys.append(args[i])
        values.append(args[i + 1])
    return HiveMap(
        keys=exp.Array(expressions=keys),
        values=exp.Array(expressions=values),
    )


def _map_sql(self, expression):
    keys = expression.args["keys"]
    values = expression.args["values"]

    if not isinstance(keys, exp.Array) or not isinstance(values, exp.Array):
        self.unsupported("Cannot convert array columns into map use SparkSQL instead.")
        return f"MAP({self.sql(keys)}, {self.sql(values)})"

    args = []
    for key, value in zip(keys.args["expressions"], values.args["expressions"]):
        args.append(self.sql(key))
        args.append(self.sql(value))
    return f"MAP({csv(*args)})"


def _array_sort(self, expression):
    if expression.args.get("expression"):
        self.unsupported("Hive SORT_ARRAY does not support a comparator")
    return f"SORT_ARRAY({self.sql(expression, 'this')})"


def _properties_sql(self, expression):
    expression = expression.copy()
    properties = expression.args["expressions"]

    stored_as = ""
    partitioned_by = ""

    for p in properties:
        if p.text("this").upper() == c.FORMAT:
            stored_as = p
        if isinstance(p.args["value"], exp.Schema):
            partitioned_by = p

    if partitioned_by:
        properties.remove(partitioned_by)
        partitioned_by = self.seg(
            f"PARTITIONED BY {self.sql(partitioned_by.args['value'])}"
        )
    if stored_as:
        properties.remove(stored_as)
        stored_as = self.seg(f"STORED AS {stored_as.text('value').upper()}")

    return f"{partitioned_by}{stored_as}{self.properties('TBLPROPERTIES', expression)}"


def _property_sql(self, expression):
    key = expression.text("this")
    value = self.sql(expression, "value")
    return f"'{key}' = {value}"


def _str_to_unix(self, expression):
    return f"UNIX_TIMESTAMP({csv(self.sql(expression, 'this'), _time_format(self, expression))})"


def _str_to_time(self, expression):
    time_format = self.sql(expression, "format")
    if time_format in (Hive.time_format, Hive.date_format):
        return f"DATE_FORMAT({self.sql(expression, 'this')}, {Hive.time_format})"
    return f"FROM_UNIXTIME({_str_to_unix(self, expression)})"


def _time_format(self, expression):
    time_format = self.format_time(expression)
    if time_format == Hive.time_format:
        return None
    return time_format


def _time_to_str(self, expression):
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format == Hive.date_format:
        return f"TO_DATE({this})"
    return f"DATE_FORMAT({this}, {time_format})"


def _unnest_to_explode_sql(self, expression):
    if isinstance(expression.this, exp.Unnest):
        unnest = expression.this
        udtf = exp.Posexplode if unnest.args.get("ordinality") else exp.Explode
        return "".join(
            self.sql(
                exp.Lateral(
                    this=udtf(this=expression),
                    table=unnest.args.get("table"),
                    columns=[column],
                )
            )
            for expression, column in zip(
                unnest.args["expressions"], unnest.args.get("columns", [])
            )
        )
    return self.join_sql(expression)


class HiveMap(exp.Map):
    is_var_len_args = True


class Hive(Dialect):
    identifier = "`"
    escape = "\\"

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
        "S": "%f",
    }

    date_format = "'yyyy-MM-dd'"
    dateint_format = "'yyyyMMdd'"
    time_format = "'yyyy-MM-dd HH:mm:ss'"

    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"']

        ENCODE = "utf-8"

        NUMERIC_LITERALS = {
            "L": "BIGINT",
            "S": "SMALLINT",
            "Y": "TINYINT",
            "D": "DOUBLE",
            "F": "FLOAT",
            "BD": "DECIMAL",
        }

    class Parser(Parser):
        STRICT_CAST = False

        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "APPROX_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "COLLECT_LIST": exp.ArrayAgg.from_arg_list,
            "DATE_ADD": lambda args: exp.TsOrDsAdd(
                this=list_get(args, 0),
                expression=list_get(args, 1),
                unit=exp.Literal.string("DAY"),
            ),
            "DATEDIFF": lambda args: exp.DateDiff(
                this=exp.TsOrDsToDate(this=list_get(args, 0)),
                expression=exp.TsOrDsToDate(this=list_get(args, 1)),
            ),
            "DATE_SUB": lambda args: exp.TsOrDsAdd(
                this=list_get(args, 0),
                expression=exp.Mul(
                    this=list_get(args, 1),
                    expression=exp.Literal.number(-1),
                ),
                unit=exp.Literal.string("DAY"),
            ),
            "DATE_FORMAT": format_time_lambda(exp.TimeToStr, "hive"),
            "DAY": lambda args: exp.Day(this=exp.TsOrDsToDate(this=list_get(args, 0))),
            "FROM_UNIXTIME": format_time_lambda(exp.UnixToStr, "hive", True),
            "GET_JSON_OBJECT": exp.JSONExtractScalar.from_arg_list,
            "LOCATE": lambda args: exp.StrPosition(
                this=list_get(args, 1),
                substr=list_get(args, 0),
                position=list_get(args, 2),
            ),
            "LOG": (
                lambda args: exp.Log.from_arg_list(args)
                if len(args) > 1
                else exp.Ln.from_arg_list(args)
            ),
            "MAP": _parse_map,
            "MONTH": lambda args: exp.Month(this=exp.TsOrDsToDate.from_arg_list(args)),
            "PERCENTILE": exp.Quantile.from_arg_list,
            "COLLECT_SET": exp.SetAgg.from_arg_list,
            "SIZE": exp.ArraySize.from_arg_list,
            "SPLIT": exp.RegexpSplit.from_arg_list,
            "TO_DATE": exp.TsOrDsToDateStr.from_arg_list,
            "UNIX_TIMESTAMP": format_time_lambda(exp.StrToUnix, "hive", True),
            "YEAR": lambda args: exp.Year(this=exp.TsOrDsToDate.from_arg_list(args)),
        }

    class Generator(Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.VARCHAR: "STRING",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.ArraySize: rename_func("SIZE"),
            exp.ArraySort: _array_sort,
            exp.With: no_recursive_cte_sql,
            exp.DateAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.DateDiff: lambda self, e: f"DATEDIFF({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.DateStrToDate: rename_func("TO_DATE"),
            exp.DateToDateStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {Hive.date_format})",
            exp.DateToDi: lambda self, e: f"CAST(DATE_FORMAT({self.sql(e, 'this')}, {Hive.dateint_format}) AS INT)",
            exp.DiToDate: lambda self, e: f"TO_DATE(CAST({self.sql(e, 'this')} AS STRING), {Hive.dateint_format})",
            exp.Properties: _properties_sql,
            exp.Property: _property_sql,
            exp.If: if_sql,
            exp.ILike: no_ilike_sql,
            exp.Join: _unnest_to_explode_sql,
            exp.JSONExtract: rename_func("GET_JSON_OBJECT"),
            exp.JSONExtractScalar: rename_func("GET_JSON_OBJECT"),
            exp.Map: _map_sql,
            HiveMap: _map_sql,
            exp.Quantile: rename_func("PERCENTILE"),
            exp.RegexpLike: lambda self, e: self.binary(e, "RLIKE"),
            exp.RegexpSplit: rename_func("SPLIT"),
            exp.SafeDivide: no_safe_divide_sql,
            exp.SetAgg: rename_func("COLLECT_SET"),
            exp.Split: lambda self, e: f"SPLIT({self.sql(e, 'this')}, CONCAT('\\\\Q', {self.sql(e, 'expression')}))",
            exp.StrPosition: lambda self, e: f"LOCATE({csv(self.sql(e, 'substr'), self.sql(e, 'this'), self.sql(e, 'position'))})",
            exp.StrToTime: _str_to_time,
            exp.StrToUnix: _str_to_unix,
            exp.StructExtract: struct_extract_sql,
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.TimeStrToTime: lambda self, e: f"CAST({self.sql(e, 'this')} AS TIMESTAMP)",
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeToStr: _time_to_str,
            exp.TimeToTimeStr: lambda self, e: f"CAST({self.sql(e, 'this')} AS STRING)",
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS STRING), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.TsOrDsToDateStr: rename_func("TO_DATE"),
            exp.TsOrDsToDate: rename_func("TO_DATE"),
            exp.TryCast: no_trycast_sql,
            exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({csv(self.sql(e, 'this'), _time_format(self, e))})",
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.UnixToTimeStr: rename_func("FROM_UNIXTIME"),
        }
