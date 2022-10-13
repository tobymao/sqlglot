from sqlglot import exp, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    approx_count_distinct_sql,
    create_with_partitions_sql,
    format_time_lambda,
    if_sql,
    no_ilike_sql,
    no_recursive_cte_sql,
    no_safe_divide_sql,
    no_trycast_sql,
    rename_func,
    struct_extract_sql,
    var_map_sql,
)
from sqlglot.generator import Generator
from sqlglot.helper import list_get
from sqlglot.parser import Parser, parse_var_map
from sqlglot.tokens import Tokenizer


def _array_sort(self, expression):
    if expression.expression:
        self.unsupported("Hive SORT_ARRAY does not support a comparator")
    return f"SORT_ARRAY({self.sql(expression, 'this')})"


def _property_sql(self, expression):
    key = expression.name
    value = self.sql(expression, "value")
    return f"'{key}'={value}"


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
        "S": "%f",
    }

    date_format = "'yyyy-MM-dd'"
    dateint_format = "'yyyyMMdd'"
    time_format = "'yyyy-MM-dd HH:mm:ss'"

    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]
        ESCAPE = "\\"
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
            "LOG": (lambda args: exp.Log.from_arg_list(args) if len(args) > 1 else exp.Ln.from_arg_list(args)),
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

    class Generator(Generator):
        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            **transforms.UNALIAS_GROUP,
            exp.AnonymousProperty: _property_sql,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.ArraySize: rename_func("SIZE"),
            exp.ArraySort: _array_sort,
            exp.With: no_recursive_cte_sql,
            exp.DateAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.DateDiff: lambda self, e: f"DATEDIFF({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.DateStrToDate: rename_func("TO_DATE"),
            exp.DateToDi: lambda self, e: f"CAST(DATE_FORMAT({self.sql(e, 'this')}, {Hive.dateint_format}) AS INT)",
            exp.DiToDate: lambda self, e: f"TO_DATE(CAST({self.sql(e, 'this')} AS STRING), {Hive.dateint_format})",
            exp.FileFormatProperty: lambda self, e: f"STORED AS {e.text('value').upper()}",
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
            exp.StrPosition: lambda self, e: f"LOCATE({self.format_args(e.args.get('substr'), e.this, e.args.get('position'))})",
            exp.StrToDate: _str_to_date,
            exp.StrToTime: _str_to_time,
            exp.StrToUnix: _str_to_unix,
            exp.StructExtract: struct_extract_sql,
            exp.TableFormatProperty: lambda self, e: f"USING {self.sql(e, 'value')}",
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.TimeStrToTime: lambda self, e: f"CAST({self.sql(e, 'this')} AS TIMESTAMP)",
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
            exp.PartitionedByProperty: lambda self, e: f"PARTITIONED BY {self.sql(e, 'value')}",
        }

        WITH_PROPERTIES = {exp.AnonymousProperty}

        ROOT_PROPERTIES = {
            exp.PartitionedByProperty,
            exp.FileFormatProperty,
            exp.SchemaCommentProperty,
            exp.LocationProperty,
            exp.TableFormatProperty,
        }

        def with_properties(self, properties):
            return self.properties(
                properties,
                prefix="TBLPROPERTIES",
            )

        def datatype_sql(self, expression):
            if (
                expression.this in (exp.DataType.Type.VARCHAR, exp.DataType.Type.NVARCHAR)
                and not expression.expressions
            ):
                expression = exp.DataType.build("text")
            return super().datatype_sql(expression)
