from __future__ import annotations
import typing as t
from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    rename_func,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

# (FuncType, Multiplier)
DATE_DELTA_INTERVAL = {
    "YEAR": ("ADD_MONTHS", 12),
    "MONTH": ("ADD_MONTHS", 1),
    "QUARTER": ("ADD_MONTHS", 3),
    "WEEK": ("DATE_ADD", 7),
    "DAY": ("DATE_ADD", 1),
}

# TRANSFORMS.pop(exp.DateTrunc)
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

def _to_date_sql(self: generator.Generator, expression: exp.TsOrDsToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    # if time_format and time_format not in (Doris.TIME_FORMAT, Doris.DATE_FORMAT):
    #     return f"TO_DATE({this}, {time_format})"
    return f"TO_DATE({this})"

def _str_to_date_sql(self: generator.Generator, expression: exp.StrToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format not in (Doris.TIME_FORMAT, Doris.DATE_FORMAT):
        this = f"FROM_UNIXTIME(UNIX_TIMESTAMP({this}, {time_format}))"
    return f"CAST({this} AS DATE)"


def _str_to_time_sql(self: generator.Generator, expression: exp.StrToTime) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format not in (Doris.TIME_FORMAT, Doris.DATE_FORMAT):
        this = f"FROM_UNIXTIME(UNIX_TIMESTAMP({this}, {time_format}))"
    return f"CAST({this} AS TIMESTAMP)"

def _str_to_unix_sql(self: generator.Generator, expression: exp.StrToUnix) -> str:
    return self.func("UNIX_TIMESTAMP", expression.this, _time_format(self, expression))

def _time_format(
    self: generator.Generator, expression: exp.UnixToStr | exp.StrToUnix
) -> t.Optional[str]:
    time_format = self.format_time(expression)
    if time_format == Doris.TIME_FORMAT:
        return None
    return time_format

def var_map_sql(
    self, expression: exp.Map | exp.VarMap, map_func_name: str = "ARRAY_MAP"
) -> str:
    keys = expression.args["keys"]
    values = expression.args["values"]

    if not isinstance(keys, exp.Array) or not isinstance(values, exp.Array):
        self.unsupported("Cannot convert array columns into map.")
        return self.func(map_func_name, keys, values)

    args = []
    for key, value in zip(keys.expressions, values.expressions):
        args.append(self.sql(key))
        args.append(self.sql(value))

    return self.func(*args,map_func_name)

class Doris(MySQL):

    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    # https://prestodb.io/docs/current/functions/datetime.html#mysql-date-functions
    TIME_MAPPING = {
        "%M": "%B",
        "%m": "%%-M",
        "%c": "%-m",
        "%e": "%-d",
        "%h": "%I",
        "%i": "%M",
        "%s": "%S",
        "%S": "%S",
        "%u": "%W",
        "%k": "%-H",
        "%l": "%-I",
        "%W": "%a",
        "%Y": "%Y",
        "%d": "%%-d",
        "%H": "%%-H",
        "%s": "%%-S",
        "%": "%%",
    }
    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "DATE_TRUNC": lambda args: exp.TimestampTrunc(
                this=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "SYSDATE": TokenType.CURRENT_TIMESTAMP,
        }

    class Generator(MySQL.Generator):
        CAST_MAPPING = {
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.UBIGINT: "UNSIGNED",
            exp.DataType.Type.VARCHAR: "VARCHAR",
            exp.DataType.Type.BINARY: "STRING",
            exp.DataType.Type.BIT: "BOOLEAN",
            exp.DataType.Type.DATETIME64: "DATETIME",
            exp.DataType.Type.ENUM: "STRING",
            exp.DataType.Type.IMAGE: "UNSUPPORTED",
            exp.DataType.Type.INT128: "LARGEINT",
            exp.DataType.Type.INT256: "STRING",
            exp.DataType.Type.UINT128: "STRING",
            exp.DataType.Type.JSONB: "JSON",
            exp.DataType.Type.LONGTEXT: "STRING",
            exp.DataType.Type.MONEY: "DECIMAL",
        }

        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
        }

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.DateDiff: rename_func("DATEDIFF"),
            exp.RegexpLike: rename_func("REGEXP"),
            exp.Coalesce: rename_func("NVL"),
            exp.CurrentTimestamp: lambda self, e: "NOW()",
            # exp.CurrentTime: lambda self, e: "NOW()",
            exp.TimeToStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.format_time(e)})",
            # exp.StrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.StrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", exp.Literal.string(e.text("unit")), e.this
            ),
            exp.TimeStrToDate: rename_func("STR_TO_DATE"),
            # exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, _time_format(self, e)
            ),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.SetAgg: rename_func("COLLECT_SET"),
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})", # Only for day level
            exp.TsOrDsToDate: _to_date_sql,
            exp.StrToDate: _str_to_date_sql,
            exp.StrToTime: _str_to_time_sql,
            exp.Map: rename_func("ARRAY_MAP"),
            exp.VarMap: var_map_sql,
            exp.RegexpSplit: rename_func("SPLIT_BY_STRING"),
            exp.Split: rename_func("SPLIT_BY_STRING"),
            exp.Quantile: rename_func("PERCENTILE"),
            exp.ApproxQuantile: rename_func("PERCENTILE_APPROX"),
            # exp.StrToUnix: _str_to_unix_sql,

        }
        # TRANSFORMS.pop(exp.Map)



