# pylint: disable=no-member, protected-access
import sqlglot.expressions as exp
from sqlglot.generator import Generator
from sqlglot.helper import RegisteringMeta, csv, list_get
from sqlglot.parser import Parser
from sqlglot.time import format_time
from sqlglot.tokens import Tokenizer
from sqlglot.trie import new_trie


class Dialect(metaclass=RegisteringMeta):
    identifier = None
    quotes = None
    escape = None
    encode = None
    numeric_literals = None
    functions = {}
    transforms = {}
    type_mapping = {}
    index_offset = 0

    time_mapping = {}
    # automatically created
    time_trie = None
    inverse_time_mapping = None
    inverse_time_trie = None

    @classmethod
    def get_or_raise(cls, dialect):
        if not dialect:
            return cls
        result = cls.get(dialect, None)
        if not result:
            raise ValueError(f"Unknown dialect '{dialect}'")
        return result

    @classmethod
    def format_time(cls, expression):
        if isinstance(expression, str):
            return exp.Literal.string(
                format_time(
                    expression[1:-1],  # the time formats are quoted
                    cls.time_mapping,
                    cls.time_trie,
                )
            )
        if isinstance(expression, exp.Literal) and expression.is_string:
            return exp.Literal.string(
                format_time(
                    expression.this,
                    cls.time_mapping,
                    cls.time_trie,
                )
            )
        return expression

    def parse(self, code, **opts):
        return self.parser(**opts).parse(self.tokenizer().tokenize(code), code)

    def generate(self, expression, **opts):
        return self.generator(**opts).generate(expression)

    def transpile(self, code, **opts):
        return self.generate(self.parse(code), **opts)

    def generator(self, **opts):
        return Generator(
            **{
                "identifier": self.identifier,
                "escape": self.escape,
                "index_offset": self.index_offset,
                "transforms": {**self.transforms, **opts.pop("transforms", {})},
                "type_mapping": {
                    **self.type_mapping,
                    **opts.pop("type_mapping", {}),
                },
                "time_mapping": self.inverse_time_mapping,
                "time_trie": self.inverse_time_trie,
                **opts,
            }
        )

    def parser(self, **opts):
        return Parser(
            functions=self.functions, **{"index_offset": self.index_offset, **opts}
        )

    def tokenizer(self):
        return Tokenizer(
            identifier=self.identifier,
            quotes=self.quotes,
            escape=self.escape,
            encode=self.encode,
            numeric_literals=self.numeric_literals,
        )


def _approx_count_distinct_sql(self, expression):
    if expression.args.get("accuracy"):
        self.unsupported("APPROX_COUNT_DISTINCT does not support accuracy")
    return f"APPROX_COUNT_DISTINCT({self.sql(expression, 'this')})"


def _if_sql(self, expression):
    expressions = csv(
        self.sql(expression, "this"),
        self.sql(expression, "true"),
        self.sql(expression, "false"),
    )
    return f"IF({expressions})"


def _no_ilike_sql(self, expression):
    return self.like_sql(
        exp.Like(
            this=exp.Lower(this=expression.this),
            expression=expression.args["expression"],
        )
    )


def _no_recursive_cte_sql(self, expression):
    if expression.args.get("recursive"):
        self.unsupported("Recursive CTEs are unsupported")
        expression.args["recursive"] = False
    return self.cte_sql(expression)


def _no_tablesample_sql(self, expression):
    self.unsupported("TABLESAMPLE unsupported")
    return self.sql(expression.this)


def _explode_to_unnest_sql(self, expression):
    if isinstance(expression.this, (exp.Explode, exp.Posexplode)):
        return self.sql(
            exp.Join(
                this=exp.Unnest(
                    expressions=[expression.this.this],
                    table=expression.args.get("table"),
                    columns=expression.args.get("columns"),
                    ordinality=isinstance(expression.this, exp.Posexplode),
                ),
                kind="cross",
            )
        )
    return self.lateral_sql(expression)


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


def _struct_extract_sql(self, expression):
    this = self.sql(expression, "this")
    struct_key = self.sql(expression, "expression").replace(self.quote, self.identifier)
    return f"{this}.{struct_key}"


def _format_time(exp_class, dialect, default=None):
    return lambda args: exp_class(
        this=list_get(args, 0),
        format=dialect.format_time(list_get(args, 1) or default),
    )


# https://prestodb.io/docs/current/functions/datetime.html#mysql-date-functions
MYSQL_TIME_MAPPING = {
    "%M": "%B",
    "%c": "%-m",
    "%e": "%-d",
    "%h": "%I",
    "%i": "%M",
    "%s": "%S",
    "%S": "%S",
}


class DuckDB(Dialect):
    # https://duckdb.org/docs/sql/functions/dateformat
    DATE_FORMAT = "'%Y-%m-%d'"
    TIME_FORMAT = "'%Y-%m-%d %H:%M:%S'"

    def _unix_to_time(self, expression):
        return f"TO_TIMESTAMP(CAST({self.sql(expression, 'this')} AS BIGINT))"

    def _ts_or_ds_add(self, expression):
        this = self.sql(expression, "this")
        e = self.sql(expression, "expression")
        unit = self.sql(expression, "unit").strip("'") or "DAY"
        return f"STRFTIME(CAST({this} AS DATE) + INTERVAL {e} {unit}, {DuckDB.DATE_FORMAT})"

    def _date_add(self, expression):
        this = self.sql(expression, "this")
        e = self.sql(expression, "expression")
        unit = self.sql(expression, "unit").strip("'") or "DAY"
        return f"{this} + INTERVAL {e} {unit}"

    transforms = {
        exp.ApproxDistinct: _approx_count_distinct_sql,
        exp.Array: lambda self, e: f"LIST_VALUE({self.expressions(e, flat=True)})",
        exp.DateAdd: _date_add,
        exp.DateDiff: lambda self, e: f"{self.sql(e, 'this')} - {self.sql(e, 'expression')}",
        exp.DateStrToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
        exp.Explode: lambda self, e: f"UNNEST({self.sql(e, 'this')})",
        exp.Quantile: lambda self, e: f"QUANTILE({self.sql(e, 'this')}, {self.sql(e, 'quantile')})",
        exp.RegexLike: lambda self, e: f"REGEXP_MATCHES({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.StrToTime: lambda self, e: f"STRPTIME({self.sql(e, 'this')}, {self.format_time(e)})",
        exp.StrToUnix: lambda self, e: f"EPOCH(STRPTIME({self.sql(e, 'this')}, {self.format_time(e)}))",
        exp.TableSample: _no_tablesample_sql,
        exp.TimeStrToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
        exp.TimeStrToTime: lambda self, e: f"CAST({self.sql(e, 'this')} AS TIMESTAMP)",
        exp.TimeStrToUnix: lambda self, e: f"EPOCH(CAST({self.sql(e, 'this')} AS TIMESTAMP))",
        exp.TimeToStr: lambda self, e: f"STRFTIME({self.sql(e, 'this')}, {self.format_time(e)})",
        exp.TimeToTimeStr: lambda self, e: f"STRFTIME({self.sql(e, 'this')}, {DuckDB.TIME_FORMAT})",
        exp.TimeToUnix: lambda self, e: f"EPOCH({self.sql(e, 'this')})",
        exp.TsOrDsAdd: _ts_or_ds_add,
        exp.TsOrDsToDateStr: lambda self, e: f"STRFTIME(CAST({self.sql(e, 'this')} AS DATE), {DuckDB.DATE_FORMAT})",
        exp.TsOrDsToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
        exp.UnixToStr: lambda self, e: f"STRFTIME({DuckDB._unix_to_time(self, e)}, {self.format_time(e)})",
        exp.UnixToTime: _unix_to_time,
        exp.UnixToTimeStr: lambda self, e: f"STRFTIME({DuckDB._unix_to_time(self, e)}, {DuckDB.TIME_FORMAT})",
    }


DuckDB.functions = {
    "APPROX_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
    "EPOCH": exp.TimeToUnix.from_arg_list,
    "EPOCH_MS": lambda args: exp.UnixToTime(
        this=exp.Div(
            this=list_get(args, 0),
            expression=exp.Literal.number(1000),
        )
    ),
    "LIST_VALUE": exp.Array.from_arg_list,
    "QUANTILE": exp.Quantile.from_arg_list,
    "REGEXP_MATCHES": exp.RegexLike.from_arg_list,
    "STRFTIME": _format_time(exp.TimeToStr, DuckDB),
    "STRPTIME": _format_time(exp.StrToTime, DuckDB),
    "TO_TIMESTAMP": exp.TimeStrToTime.from_arg_list,
    "UNNEST": exp.Explode.from_arg_list,
}


class Hive(Dialect):
    identifier = "`"
    quotes = {"'", '"'}
    escape = "\\"
    encode = "utf-8"
    numeric_literals = {
        "L": "BIGINT",
        "S": "SMALLINT",
        "Y": "TINYINT",
        "D": "DOUBLE",
        "F": "FLOAT",
        "BD": "DECIMAL",
    }

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

    DATE_FORMAT = "'yyyy-MM-dd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    def _parse_map(args):
        keys = []
        values = []
        for i in range(0, len(args), 2):
            keys.append(args[i])
            values.append(args[i + 1])
        return exp.Map(
            keys=exp.Array(expressions=keys),
            values=exp.Array(expressions=values),
        )

    def _time_format(self, expression):
        time_format = self.format_time(expression)
        if time_format == Hive.TIME_FORMAT:
            return None
        return time_format

    def _fileformat_sql(self, expression):
        file_format = self.sql(expression, "this").replace(self.quote, "")
        if file_format:
            return f"STORED AS {file_format}"
        return ""

    def _str_to_unix(self, expression):
        return f"UNIX_TIMESTAMP({csv(self.sql(expression, 'this'), Hive._time_format(self, expression))})"

    def _str_to_time(self, expression):
        time_format = self.sql(expression, "format")
        if time_format in (Hive.TIME_FORMAT, Hive.DATE_FORMAT):
            return f"DATE_FORMAT({self.sql(expression, 'this')}, {Hive.TIME_FORMAT})"
        return f"FROM_UNIXTIME({Hive._str_to_unix(self, expression)})"

    def _time_to_str(self, expression):
        this = self.sql(expression, "this")
        time_format = self.format_time(expression)
        if time_format == Hive.DATE_FORMAT:
            return f"TO_DATE({this})"
        return f"DATE_FORMAT({this}, {time_format})"

    def _time_to_unix(self, expression):
        return f"UNIX_TIMESTAMP({self.sql(expression, 'this')})"

    def _unix_to_time(self, expression):
        return f"FROM_UNIXTIME({self.sql(expression, 'this')})"

    type_mapping = {
        exp.DataType.Type.TEXT: "STRING",
        exp.DataType.Type.VARCHAR: "STRING",
    }

    transforms = {
        exp.ApproxDistinct: _approx_count_distinct_sql,
        exp.ArrayAgg: lambda self, e: f"COLLECT_LIST({self.sql(e, 'this')})",
        exp.ArraySize: lambda self, e: f"SIZE({self.sql(e, 'this')})",
        exp.CTE: _no_recursive_cte_sql,
        exp.DateAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.DateDiff: lambda self, e: f"DATEDIFF({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.DateStrToDate: lambda self, e: self.sql(e, "this"),
        exp.FileFormat: _fileformat_sql,
        exp.If: _if_sql,
        exp.ILike: _no_ilike_sql,
        exp.Join: _unnest_to_explode_sql,
        exp.JSONPath: lambda self, e: f"GET_JSON_OBJECT({self.sql(e, 'this')}, {self.sql(e, 'path')})",
        exp.Quantile: lambda self, e: f"PERCENTILE({self.sql(e, 'this')}, {self.sql(e, 'quantile')})",
        exp.SetAgg: lambda self, e: f"COLLECT_SET({self.sql(e, 'this')})",
        exp.StrPosition: lambda self, e: f"LOCATE({csv(self.sql(e, 'substr'), self.sql(e, 'this'), self.sql(e, 'position'))})",
        exp.StrToTime: _str_to_time,
        exp.StrToUnix: _str_to_unix,
        exp.StructExtract: _struct_extract_sql,
        exp.TimeStrToDate: lambda self, e: f"TO_DATE({self.sql(e, 'this')})",
        exp.TimeStrToTime: lambda self, e: self.sql(e, "this"),
        exp.TimeStrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')})",
        exp.TimeToStr: _time_to_str,
        exp.TimeToTimeStr: lambda self, e: self.sql(e, "this"),
        exp.TimeToUnix: _time_to_unix,
        exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.TsOrDsToDateStr: lambda self, e: f"TO_DATE({self.sql(e, 'this')})",
        exp.TsOrDsToDate: lambda self, e: f"TO_DATE({self.sql(e, 'this')})",
        exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({csv(self.sql(e, 'this'), Hive._time_format(self, e))})",
        exp.UnixToTime: _unix_to_time,
        exp.UnixToTimeStr: _unix_to_time,
    }


Hive.functions = {
    "APPROX_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
    "COLLECT_LIST": exp.ArrayAgg.from_arg_list,
    "DATE_ADD": lambda args: exp.TsOrDsAdd(
        this=list_get(args, 0),
        expression=list_get(args, 1),
        unit=exp.Literal.string("DAY"),
    ),
    "DATEDIFF": lambda args: exp.DateDiff(
        this=exp.DateStrToDate(this=list_get(args, 0)),
        expression=exp.DateStrToDate(this=list_get(args, 1)),
    ),
    "DATE_SUB": lambda args: exp.TsOrDsAdd(
        this=list_get(args, 0),
        expression=exp.Mul(
            this=list_get(args, 1),
            expression=exp.Literal.number(-1),
        ),
        unit=exp.Literal.string("DAY"),
    ),
    "DATE_FORMAT": _format_time(exp.TimeToStr, Hive),
    "DAY": lambda args: exp.Day(this=exp.TsOrDsToDate(this=list_get(args, 0))),
    "FROM_UNIXTIME": _format_time(exp.UnixToStr, Hive, Hive.TIME_FORMAT),
    "GET_JSON_OBJECT": exp.JSONPath.from_arg_list,
    "LOCATE": lambda args: exp.StrPosition(
        this=list_get(args, 1), substr=list_get(args, 0), position=list_get(args, 2)
    ),
    "LOG": (
        lambda args: exp.Log.from_arg_list(args)
        if len(args) > 1
        else exp.Ln.from_arg_list(args)
    ),
    "MAP": Hive._parse_map,
    "MONTH": lambda args: exp.Month(this=exp.TsOrDsToDate.from_arg_list(args)),
    "PERCENTILE": exp.Quantile.from_arg_list,
    "COLLECT_SET": exp.SetAgg.from_arg_list,
    "SIZE": exp.ArraySize.from_arg_list,
    "TO_DATE": exp.TsOrDsToDateStr.from_arg_list,
    "UNIX_TIMESTAMP": _format_time(exp.StrToUnix, Hive, Hive.TIME_FORMAT),
}


class MySQL(Dialect):
    identifier = "`"

    time_mapping = MYSQL_TIME_MAPPING

    transforms = {
        exp.ILike: _no_ilike_sql,
        exp.TableSample: _no_tablesample_sql,
    }


class StarRocks(MySQL):
    type_mapping = {
        exp.DataType.Type.TEXT: "STRING",
        exp.DataType.Type.TIMESTAMP: "DATETIME",
        exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
    }


class Postgres(Dialect):
    type_mapping = {
        exp.DataType.Type.TINYINT: "SMALLINT",
        exp.DataType.Type.FLOAT: "REAL",
        exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
        exp.DataType.Type.BINARY: "BYTEA",
    }

    transforms = {
        exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
        exp.TableSample: _no_tablesample_sql,
    }

    functions = {"TO_TIMESTAMP": exp.StrToTime.from_arg_list}


class Presto(Dialect):
    index_offset = 1
    TIME_FORMAT = "'%Y-%m-%d %H:%i:%S'"

    def _approx_distinct_sql(self, expression):
        accuracy = expression.args.get("accuracy")
        accuracy = ", " + self.sql(accuracy) if accuracy else ""
        return f"APPROX_DISTINCT({self.sql(expression, 'this')}{accuracy})"

    def _concat_ws_sql(self, expression):
        sep, *args = expression.args["expressions"]
        sep = self.sql(sep)
        if len(args) > 1:
            return f"ARRAY_JOIN(ARRAY[{csv(*(self.sql(e) for e in args))}], {sep})"
        return f"ARRAY_JOIN({self.sql(args[0])}, {sep})"

    def _datatype_sql(self, expression):
        sql = self.datatype_sql(expression)
        if expression.this == exp.DataType.Type.TIMESTAMPTZ:
            sql = f"{sql} WITH TIME ZONE"
        return sql

    def _fileformat_sql(self, expression):
        file_format = self.sql(expression, "this").replace(self.quote, "")
        if file_format:
            return f"WITH (FORMAT = '{file_format}')"
        return ""

    def _date_parse_sql(self, expression):
        return f"DATE_PARSE({self.sql(expression, 'this')}, '%Y-%m-%d %H:%i:%s')"

    def _initcap_sql(self, expression):
        regex = "(\w)(\w*)"  # pylint: disable=anomalous-backslash-in-string
        return f"REGEXP_REPLACE({self.sql(expression, 'this')}, '{regex}', x -> UPPER(x[1]) || LOWER(x[2]))"

    def _quantile_sql(self, expression):
        self.unsupported("Presto does not support exact quantiles")
        return f"APPROX_PERCENTILE({self.sql(expression, 'this')}, {self.sql(expression, 'quantile')})"

    def _str_position_sql(self, expression):
        this = self.sql(expression, "this")
        substr = self.sql(expression, "substr")
        position = self.sql(expression, "position")
        if position:
            return f"STRPOS(SUBSTR({this}, {position}), {substr}) + {position} - 1"
        return f"STRPOS({this}, {substr})"

    def _ts_or_ds_to_date_str_sql(self, expression):
        this = self.sql(expression, "this")
        return f"DATE_FORMAT(DATE_PARSE(SUBSTR({this}, 1, 10), '%Y-%m-%d'), '%Y-%m-%d')"

    def _ts_or_ds_to_date_sql(self, expression):
        this = self.sql(expression, "this")
        return f"DATE_PARSE(SUBSTR({this}, 1, 10), '%Y-%m-%d')"

    def _ts_or_ds_add_sql(self, expression):
        this = self.sql(expression, "this")
        e = self.sql(expression, "expression")
        unit = self.sql(expression, "unit") or "'day'"
        return f"DATE_FORMAT(DATE_ADD({unit}, {e}, DATE_PARSE(SUBSTR({this}, 1, 10), '%Y-%m-%d')), '%Y-%m-%d')"

    time_mapping = MYSQL_TIME_MAPPING

    type_mapping = {
        exp.DataType.Type.INT: "INTEGER",
        exp.DataType.Type.FLOAT: "REAL",
        exp.DataType.Type.BINARY: "VARBINARY",
        exp.DataType.Type.TEXT: "VARCHAR",
        exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
    }

    transforms = {
        exp.ApproxDistinct: _approx_distinct_sql,
        exp.Array: lambda self, e: f"ARRAY[{self.expressions(e, flat=True)}]",
        exp.ArrayContains: lambda self, e: f"CONTAINS({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.ArraySize: lambda self, e: f"CARDINALITY({self.sql(e, 'this')})",
        exp.BitwiseAnd: lambda self, e: f"BITWISE_AND({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.BitwiseLeftShift: lambda self, e: f"BITWISE_ARITHMETIC_SHIFT_LEFT({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.BitwiseNot: lambda self, e: f"BITWISE_NOT({self.sql(e, 'this')})",
        exp.BitwiseOr: lambda self, e: f"BITWISE_OR({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.BitwiseRightShift: lambda self, e: f"BITWISE_ARITHMETIC_SHIFT_RIGHT({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.BitwiseXor: lambda self, e: f"BITWISE_XOR({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.ConcatWs: _concat_ws_sql,
        exp.DataType: _datatype_sql,
        exp.DateAdd: lambda self, e: f"""DATE_ADD({self.sql(e, 'unit') or "'day'"}, {self.sql(e, 'expression')}, {self.sql(e, 'this')})""",
        exp.DateDiff: lambda self, e: f"""DATE_DIFF({self.sql(e, 'unit') or "'day'"}, {self.sql(e, 'expression')}, {self.sql(e, 'this')})""",
        exp.DateStrToDate: lambda self, e: f"DATE_PARSE({self.sql(e, 'this')}, '%Y-%m-%d')",
        exp.FileFormat: _fileformat_sql,
        exp.If: _if_sql,
        exp.ILike: _no_ilike_sql,
        exp.Initcap: _initcap_sql,
        exp.JSONPath: lambda self, e: f"JSON_EXTRACT_SCALAR({self.sql(e, 'this')}, {self.sql(e, 'path')})",
        exp.Lateral: _explode_to_unnest_sql,
        exp.Quantile: _quantile_sql,
        exp.RegexLike: lambda self, e: f"REGEXP_LIKE({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.StrPosition: _str_position_sql,
        exp.StrToTime: lambda self, e: f"DATE_PARSE({self.sql(e, 'this')}, {self.format_time(e)})",
        exp.StrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {self.format_time(e)}))",
        exp.StructExtract: _struct_extract_sql,
        exp.TableSample: _no_tablesample_sql,
        exp.TimeStrToDate: _date_parse_sql,
        exp.TimeStrToTime: _date_parse_sql,
        exp.TimeStrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {Presto.TIME_FORMAT}))",
        exp.TimeToStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.format_time(e)})",
        exp.TimeToTimeStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {Presto.TIME_FORMAT})",
        exp.TimeToUnix: lambda self, e: f"TO_UNIXTIME({self.sql(e, 'this')})",
        exp.TsOrDsAdd: _ts_or_ds_add_sql,
        exp.TsOrDsToDateStr: _ts_or_ds_to_date_str_sql,
        exp.TsOrDsToDate: _ts_or_ds_to_date_sql,
        exp.UnixToStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), {self.format_time(e)})",
        exp.UnixToTime: lambda self, e: f"FROM_UNIXTIME({self.sql(e, 'this')})",
        exp.UnixToTimeStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), {Presto.TIME_FORMAT})",
    }


Presto.functions = {
    "APPROX_DISTINCT": exp.ApproxDistinct.from_arg_list,
    "CARDINALITY": exp.ArraySize.from_arg_list,
    "CONTAINS": exp.ArrayContains.from_arg_list,
    "DATE_ADD": lambda args: exp.DateAdd(
        this=list_get(args, 2),
        expression=list_get(args, 1),
        unit=list_get(args, 0),
    ),
    "DATE_DIFF": lambda args: exp.DateDiff(
        this=list_get(args, 2),
        expression=list_get(args, 1),
        unit=list_get(args, 0),
    ),
    "DATE_FORMAT": _format_time(exp.TimeToStr, Presto),
    "DATE_PARSE": _format_time(exp.StrToTime, Presto),
    "FROM_UNIXTIME": exp.UnixToTime.from_arg_list,
    "JSON_EXTRACT": exp.JSONPath.from_arg_list,
    "JSON_EXTRACT_SCALAR": exp.JSONPath.from_arg_list,
    "REGEXP_LIKE": exp.RegexLike.from_arg_list,
    "STRPOS": exp.StrPosition.from_arg_list,
    "TO_UNIXTIME": exp.TimeToUnix.from_arg_list,
}


class Spark(Hive):
    def _create_sql(self, e):
        kind = e.args.get("kind")
        temporary = e.args.get("temporary")

        if kind.upper() == "TABLE" and temporary is True:
            return f"CREATE TEMPORARY VIEW {self.sql(e, 'this')} AS {self.sql(e, 'expression')}"
        return self.create_sql(e)

    type_mapping = {
        **Hive.type_mapping,
        exp.DataType.Type.TINYINT: "BYTE",
        exp.DataType.Type.SMALLINT: "SHORT",
        exp.DataType.Type.BIGINT: "LONG",
        exp.DataType.Type.BINARY: "ARRAY[BYTE]",
    }

    transforms = {
        **Hive.transforms,
        exp.Hint: lambda self, e: f" /*+ {self.expressions(e).strip()} */",
        exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
        exp.Create: _create_sql,
    }


Spark.functions = {
    **Hive.functions,
    "TO_UNIX_TIMESTAMP": exp.StrToUnix.from_arg_list,
    "LEFT": lambda args: exp.Substring(
        this=list_get(args, 0),
        start=exp.Literal.number(1),
        length=list_get(args, 1),
    ),
    "RIGHT": lambda args: exp.Substring(
        this=list_get(args, 0),
        start=exp.Sub(
            this=exp.Length(this=list_get(args, 0)),
            expression=exp.Add(
                this=list_get(args, 1), expression=exp.Literal.number(1)
            ),
        ),
        length=list_get(args, 1),
    ),
}


class SQLite(Dialect):
    type_mapping = {
        exp.DataType.Type.BOOLEAN: "INTEGER",
        exp.DataType.Type.TINYINT: "INTEGER",
        exp.DataType.Type.SMALLINT: "INTEGER",
        exp.DataType.Type.INT: "INTEGER",
        exp.DataType.Type.BIGINT: "INTEGER",
        exp.DataType.Type.FLOAT: "REAL",
        exp.DataType.Type.DOUBLE: "REAL",
        exp.DataType.Type.DECIMAL: "REAL",
        exp.DataType.Type.CHAR: "TEXT",
        exp.DataType.Type.VARCHAR: "TEXT",
        exp.DataType.Type.BINARY: "BLOB",
    }

    transforms = {
        exp.TableSample: _no_tablesample_sql,
    }


class Trino(Presto):
    pass


for d in Dialect.classes.values():
    d.time_trie = new_trie(d.time_mapping)
    d.inverse_time_mapping = {v: k for k, v in d.time_mapping.items()}
    d.inverse_time_trie = new_trie(d.inverse_time_mapping)
