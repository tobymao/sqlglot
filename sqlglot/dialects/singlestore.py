from sqlglot.dialects.dialect import build_formatted_time
from sqlglot.dialects.mysql import MySQL
import typing as t
from sqlglot import exp, Dialect
from sqlglot.helper import seq_get


class SingleStore(MySQL):
    SUPPORTS_ORDER_BY_ALL = True

    TIME_MAPPING: t.Dict[str, str] = {
        "D": "%u",  # Day of week (1-7)
        "DD": "%d",  # day of month (01-31)
        "DY": "%a",  # abbreviated name of day
        "HH": "%I",  # Hour of day (01-12)
        "HH12": "%I",  # alias for HH
        "HH24": "%H",  # Hour of day (00-23)
        "MI": "%M",  # Minute (00-59)
        "MM": "%m",  # Month (01-12; January = 01)
        "MON": "%b",  # Abbreviated name of month
        "MONTH": "%B",  # Name of month
        "SS": "%S",  # Second (00-59)
        "RR": "%y",  # 15
        "YY": "%y",  # 15
        "YYYY": "%Y",  # 2015
        "FF6": "%f",  # only 6 digits are supported in python formats
    }

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "TO_DATE": build_formatted_time(exp.StrToDate, "singlestore"),
            "TO_TIMESTAMP": build_formatted_time(exp.StrToTime, "singlestore"),
            "TO_CHAR": build_formatted_time(exp.TimeToStr, "singlestore"),
            "STR_TO_DATE": build_formatted_time(exp.StrToDate, "mysql"),
            "DATE_FORMAT": build_formatted_time(exp.TimeToStr, "mysql"),
            # The first argument is converted to TIME(6)
            # This is needed because exp.TimeToStr is converted to DATE_FORMAT
            # which interprets the first argument as DATETIME and fails to parse
            # string literals like '12:05:47' without a date part.
            "TIME_FORMAT": lambda args: exp.TimeToStr(
                this=exp.Cast(
                    this=seq_get(args, 0),
                    to=exp.DataType.build(
                        exp.DataType.Type.TIME,
                        expressions=[exp.DataTypeParam(this=exp.Literal.number(6))],
                    ),
                ),
                format=Dialect["mysql"].format_time(seq_get(args, 1)),
            ),
        }

    class Generator(MySQL.Generator):
        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.StrToDate: lambda self, e: self.func(
                "STR_TO_DATE",
                e.this,
                self.format_time(
                    e,
                    inverse_time_mapping=Dialect["mysql"].INVERSE_TIME_MAPPING,
                    inverse_time_trie=Dialect["mysql"].INVERSE_TIME_TRIE,
                ),
            ),
            exp.TimeToStr: lambda self, e: self.func(
                "DATE_FORMAT",
                e.this,
                self.format_time(
                    e,
                    inverse_time_mapping=Dialect["mysql"].INVERSE_TIME_MAPPING,
                    inverse_time_trie=Dialect["mysql"].INVERSE_TIME_TRIE,
                ),
            ),
            exp.StrToTime: lambda self, e: self.func(
                "STR_TO_DATE",
                e.this,
                self.format_time(
                    e,
                    inverse_time_mapping=Dialect["mysql"].INVERSE_TIME_MAPPING,
                    inverse_time_trie=Dialect["mysql"].INVERSE_TIME_TRIE,
                ),
            ),
        }
