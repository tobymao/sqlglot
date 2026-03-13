from __future__ import annotations

import typing as t

from sqlglot import exp, generator, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    no_ilike_sql,
    rename_func,
    strposition_sql,
    to_number_with_nls_param,
    trim_sql,
)
from sqlglot.parsers.oracle import OracleParser
from sqlglot.tokens import TokenType


def _trim_sql(self: Oracle.Generator, expression: exp.Trim) -> str:
    position = expression.args.get("position")

    if position and position.upper() in ("LEADING", "TRAILING"):
        return self.trim_sql(expression)

    return trim_sql(self, expression)


class Oracle(Dialect):
    ALIAS_POST_TABLESAMPLE = True
    LOCKING_READS_SUPPORTED = True
    TABLESAMPLE_SIZE_IS_PERCENT = True
    NULL_ORDERING = "nulls_are_large"
    ON_CONDITION_EMPTY_BEFORE_ERROR = False
    ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = False
    DISABLES_ALIAS_REF_EXPANSION = True

    # See section 8: https://docs.oracle.com/cd/A97630_01/server.920/a96540/sql_elements9a.htm
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    # https://docs.oracle.com/database/121/SQLRF/sql_elements004.htm#SQLRF00212
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    TIME_MAPPING = {
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
        "FF6": "%f",  # only 6 digits are supported in python formats
    }

    PSEUDOCOLUMNS = {"ROWNUM", "ROWID", "OBJECT_ID", "OBJECT_VALUE", "LEVEL"}

    def can_quote(self, identifier: exp.Identifier, identify: str | bool = "safe") -> bool:
        # Disable quoting for pseudocolumns as it may break queries e.g
        # `WHERE "ROWNUM" = ...` does not work but `WHERE ROWNUM = ...` does
        return (
            identifier.quoted or not isinstance(identifier.parent, exp.Pseudocolumn)
        ) and super().can_quote(identifier, identify=identify)

    class Tokenizer(tokens.Tokenizer):
        VAR_SINGLE_TOKENS = {"@", "$", "#"}

        UNICODE_STRINGS = [
            (prefix + q, q)
            for q in t.cast(t.List[str], tokens.Tokenizer.QUOTES)
            for prefix in ("U", "u")
        ]

        NESTED_COMMENTS = False

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "(+)": TokenType.JOIN_MARKER,
            "BINARY_DOUBLE": TokenType.DOUBLE,
            "BINARY_FLOAT": TokenType.FLOAT,
            "BULK COLLECT INTO": TokenType.BULK_COLLECT_INTO,
            "COLUMNS": TokenType.COLUMN,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "MINUS": TokenType.EXCEPT,
            "NVARCHAR2": TokenType.NVARCHAR,
            "ORDER SIBLINGS BY": TokenType.ORDER_SIBLINGS_BY,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "START": TokenType.BEGIN,
            "TOP": TokenType.TOP,
            "VARCHAR2": TokenType.VARCHAR,
            "SYSTIMESTAMP": TokenType.SYSTIMESTAMP,
        }

    Parser = OracleParser

    class Generator(generator.Generator):
        LOCKING_READS_SUPPORTED = True
        JOIN_HINTS = False
        TABLE_HINTS = False
        DATA_TYPE_SPECIFIERS_ALLOWED = True
        ALTER_TABLE_INCLUDE_COLUMN_KEYWORD = False
        LIMIT_FETCH = "FETCH"
        TABLESAMPLE_KEYWORDS = "SAMPLE"
        LAST_DAY_SUPPORTS_DATE_PART = False
        SUPPORTS_SELECT_INTO = True
        TZ_TO_WITH_TIME_ZONE = True
        SUPPORTS_WINDOW_EXCLUDE = True
        QUERY_HINT_SEP = " "
        SUPPORTS_DECODE_CASE = True

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DType.TINYINT: "SMALLINT",
            exp.DType.SMALLINT: "SMALLINT",
            exp.DType.INT: "INT",
            exp.DType.BIGINT: "INT",
            exp.DType.DECIMAL: "NUMBER",
            exp.DType.DOUBLE: "DOUBLE PRECISION",
            exp.DType.VARCHAR: "VARCHAR2",
            exp.DType.NVARCHAR: "NVARCHAR2",
            exp.DType.NCHAR: "NCHAR",
            exp.DType.TEXT: "CLOB",
            exp.DType.TIMETZ: "TIME",
            exp.DType.TIMESTAMPNTZ: "TIMESTAMP",
            exp.DType.TIMESTAMPTZ: "TIMESTAMP",
            exp.DType.BINARY: "BLOB",
            exp.DType.VARBINARY: "BLOB",
            exp.DType.ROWVERSION: "BLOB",
        }
        TYPE_MAPPING.pop(exp.DType.BLOB)

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.DateStrToDate: lambda self, e: self.func(
                "TO_DATE", e.this, exp.Literal.string("YYYY-MM-DD")
            ),
            exp.DateTrunc: lambda self, e: self.func("TRUNC", e.this, e.unit),
            exp.EuclideanDistance: rename_func("L2_DISTANCE"),
            exp.ILike: no_ilike_sql,
            exp.LogicalOr: rename_func("MAX"),
            exp.LogicalAnd: rename_func("MIN"),
            exp.Mod: rename_func("MOD"),
            exp.Rand: rename_func("DBMS_RANDOM.VALUE"),
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_distinct_on,
                    transforms.eliminate_qualify,
                ]
            ),
            exp.StrPosition: lambda self, e: strposition_sql(
                self, e, func_name="INSTR", supports_position=True, supports_occurrence=True
            ),
            exp.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
            exp.StrToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.Subquery: lambda self, e: self.subquery_sql(e, sep=" "),
            exp.Substring: rename_func("SUBSTR"),
            exp.Table: lambda self, e: self.table_sql(e, sep=" "),
            exp.TableSample: lambda self, e: self.tablesample_sql(e),
            exp.TemporaryProperty: lambda _, e: f"{e.name or 'GLOBAL'} TEMPORARY",
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.ToNumber: to_number_with_nls_param,
            exp.Trim: _trim_sql,
            exp.Unicode: lambda self, e: f"ASCII(UNISTR({self.sql(e.this)}))",
            exp.UnixToTime: lambda self, e: (
                f"TO_DATE('1970-01-01', 'YYYY-MM-DD') + ({self.sql(e, 'this')} / 86400)"
            ),
            exp.UtcTimestamp: rename_func("UTC_TIMESTAMP"),
            exp.UtcTime: rename_func("UTC_TIME"),
            exp.Systimestamp: lambda self, e: "SYSTIMESTAMP",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def currenttimestamp_sql(self, expression: exp.CurrentTimestamp) -> str:
            if expression.args.get("sysdate"):
                return "SYSDATE"

            this = expression.this
            return self.func("CURRENT_TIMESTAMP", this) if this else "CURRENT_TIMESTAMP"

        def offset_sql(self, expression: exp.Offset) -> str:
            return f"{super().offset_sql(expression)} ROWS"

        def add_column_sql(self, expression: exp.Expr) -> str:
            return f"ADD {self.sql(expression)}"

        def queryoption_sql(self, expression: exp.QueryOption) -> str:
            option = self.sql(expression, "this")
            value = self.sql(expression, "expression")
            value = f" CONSTRAINT {value}" if value else ""

            return f"{option}{value}"

        def coalesce_sql(self, expression: exp.Coalesce) -> str:
            func_name = "NVL" if expression.args.get("is_nvl") else "COALESCE"
            return rename_func(func_name)(self, expression)

        def into_sql(self, expression: exp.Into) -> str:
            into = "INTO" if not expression.args.get("bulk_collect") else "BULK COLLECT INTO"
            if expression.this:
                return f"{self.seg(into)} {self.sql(expression, 'this')}"

            return f"{self.seg(into)} {self.expressions(expression)}"

        def hint_sql(self, expression: exp.Hint) -> str:
            expressions = []

            for expression in expression.expressions:
                if isinstance(expression, exp.Anonymous):
                    formatted_args = self.format_args(*expression.expressions, sep=" ")
                    expressions.append(f"{self.sql(expression, 'this')}({formatted_args})")
                else:
                    expressions.append(self.sql(expression))

            return f" /*+ {self.expressions(sqls=expressions, sep=self.QUERY_HINT_SEP).strip()} */"

        def isascii_sql(self, expression: exp.IsAscii) -> str:
            return f"NVL(REGEXP_LIKE({self.sql(expression.this)}, '^[' || CHR(1) || '-' || CHR(127) || ']*$'), TRUE)"

        def interval_sql(self, expression: exp.Interval) -> str:
            return f"{'INTERVAL ' if isinstance(expression.this, exp.Literal) else ''}{self.sql(expression, 'this')} {self.sql(expression, 'unit')}"

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            param_constraint = expression.find(exp.InOutColumnConstraint)
            if param_constraint:
                sep = f" {self.sql(param_constraint)} "
                param_constraint.pop()
            return super().columndef_sql(expression, sep)
