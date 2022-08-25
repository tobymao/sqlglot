from sqlglot import exp
from sqlglot.dialects.dialect import rename_func, no_ilike_sql
from sqlglot.dialects.hive import Hive, HiveMap
from sqlglot.helper import list_get


def _create_sql(self, e):
    kind = e.args.get("kind")
    temporary = e.args.get("temporary")

    if kind.upper() == "TABLE" and temporary is True:
        return f"CREATE TEMPORARY VIEW {self.sql(e, 'this')} AS {self.sql(e, 'expression')}"
    return self.create_sql(e)


def _map_sql(self, expression):
    keys = self.sql(expression.args["keys"])
    values = self.sql(expression.args["values"])
    return f"MAP_FROM_ARRAYS({keys}, {values})"


def _str_to_date(self, expression):
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format == Hive.date_format:
        return f"TO_DATE({this})"
    return f"TO_DATE({this}, {time_format})"


def _unix_to_time(self, expression):
    scale = expression.args.get("scale")
    timestamp = self.sql(expression, "this")
    if scale is None:
        return f"FROM_UNIXTIME({timestamp})"
    if scale == exp.UnixToTime.SECONDS:
        return f"TIMESTAMP_SECONDS({timestamp})"
    if scale == exp.UnixToTime.MILLIS:
        return f"TIMESTAMP_MILLIS({timestamp})"
    if scale == exp.UnixToTime.MICROS:
        return f"TIMESTAMP_MICROS({timestamp})"

    raise ValueError("Improper scale for timestamp")


class Spark(Hive):
    class Parser(Hive.Parser):
        FUNCTIONS = {
            **Hive.Parser.FUNCTIONS,
            "MAP_FROM_ARRAYS": exp.Map.from_arg_list,
            "TO_UNIX_TIMESTAMP": exp.StrToUnix.from_arg_list,
            "LEFT": lambda args: exp.Substring(
                this=list_get(args, 0),
                start=exp.Literal.number(1),
                length=list_get(args, 1),
            ),
            "SHIFTLEFT": lambda args: exp.BitwiseLeftShift(
                this=list_get(args, 0),
                expression=list_get(args, 1),
            ),
            "SHIFTRIGHT": lambda args: exp.BitwiseRightShift(
                this=list_get(args, 0),
                expression=list_get(args, 1),
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

    class Generator(Hive.Generator):
        TYPE_MAPPING = {
            **Hive.Generator.TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "BYTE",
            exp.DataType.Type.SMALLINT: "SHORT",
            exp.DataType.Type.BIGINT: "LONG",
        }

        TRANSFORMS = {
            **{
                k: v
                for k, v in Hive.Generator.TRANSFORMS.items()
                if k not in {exp.ArraySort}
            },
            exp.ArraySum: lambda self, e: f"AGGREGATE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.BitwiseLeftShift: rename_func("SHIFTLEFT"),
            exp.BitwiseRightShift: rename_func("SHIFTRIGHT"),
            exp.Hint: lambda self, e: f" /*+ {self.expressions(e).strip()} */",
            exp.ILike: no_ilike_sql,
            exp.StrToDate: _str_to_date,
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.UnixToTime: _unix_to_time,
            exp.Create: _create_sql,
            exp.Map: _map_sql,
            exp.Reduce: rename_func("AGGREGATE"),
            exp.StructKwarg: lambda self, e: f"{self.sql(e, 'this')}: {self.sql(e, 'expression')}",
            HiveMap: _map_sql,
        }
