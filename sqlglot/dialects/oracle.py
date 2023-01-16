from __future__ import annotations

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import Dialect, no_ilike_sql, rename_func, trim_sql
from sqlglot.helper import csv
from sqlglot.tokens import TokenType


def _limit_sql(self, expression):
    return self.fetch_sql(exp.Fetch(direction="FIRST", count=expression.expression))


class Oracle(Dialect):
    # https://docs.oracle.com/database/121/SQLRF/sql_elements004.htm#SQLRF00212
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    time_mapping = {
        "AM": "%p",  # Meridian indicator with or without periods
        "A.M.": "%p",  # Meridian indicator with or without periods
        "PM": "%p",  # Meridian indicator with or without periods
        "P.M.": "%p",  # Meridian indicator with or without periods
        "D": "%u",  # Day of week (1-7)
        "DAY": "%A",  # name of day
        "DD": "%d",  # day of month (1-31)
        "DDD": "%j",  # day of year (1-366)
        "DY": "%a",  # abbreviated name of day
        "HH": "%I",  # Hour of day (1-12)
        "HH12": "%I",  # alias for HH
        "HH24": "%H",  # Hour of day (0-23)
        "IW": "%V",  # Calendar week of year (1-52 or 1-53), as defined by the ISO 8601 standard
        "MI": "%M",  # Minute (0-59)
        "MM": "%m",  # Month (01-12; January = 01)
        "MON": "%b",  # Abbreviated name of month
        "MONTH": "%B",  # Name of month
        "SS": "%S",  # Second (0-59)
        "WW": "%W",  # Week of year (1-53)
        "YY": "%y",  # 15
        "YYYY": "%Y",  # 2015
    }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "DECODE": exp.Matches.from_arg_list,
        }

    class Generator(generator.Generator):
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.TINYINT: "NUMBER",
            exp.DataType.Type.SMALLINT: "NUMBER",
            exp.DataType.Type.INT: "NUMBER",
            exp.DataType.Type.BIGINT: "NUMBER",
            exp.DataType.Type.DECIMAL: "NUMBER",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.VARCHAR: "VARCHAR2",
            exp.DataType.Type.NVARCHAR: "NVARCHAR2",
            exp.DataType.Type.TEXT: "CLOB",
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.VARBINARY: "BLOB",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            **transforms.UNALIAS_GROUP,  # type: ignore
            exp.ILike: no_ilike_sql,
            exp.Limit: _limit_sql,
            exp.Trim: trim_sql,
            exp.Matches: rename_func("DECODE"),
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToStr: lambda self, e: f"TO_CHAR({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.UnixToTime: lambda self, e: f"TO_DATE('1970-01-01','YYYY-MM-DD') + ({self.sql(e, 'this')} / 86400)",
        }

        def query_modifiers(self, expression, *sqls):
            return csv(
                *sqls,
                *[self.sql(sql) for sql in expression.args.get("laterals", [])],
                *[self.sql(sql) for sql in expression.args.get("joins", [])],
                self.sql(expression, "where"),
                self.sql(expression, "group"),
                self.sql(expression, "having"),
                self.sql(expression, "qualify"),
                self.sql(expression, "window"),
                self.sql(expression, "distribute"),
                self.sql(expression, "sort"),
                self.sql(expression, "cluster"),
                self.sql(expression, "order"),
                self.sql(expression, "offset"),  # offset before limit in oracle
                self.sql(expression, "limit"),
                sep="",
            )

        def offset_sql(self, expression):
            return f"{super().offset_sql(expression)} ROWS"

        def table_sql(self, expression):
            return super().table_sql(expression, sep=" ")

    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "MINUS": TokenType.EXCEPT,
            "START": TokenType.BEGIN,
            "TOP": TokenType.TOP,
            "VARCHAR2": TokenType.VARCHAR,
            "NVARCHAR2": TokenType.NVARCHAR,
        }
