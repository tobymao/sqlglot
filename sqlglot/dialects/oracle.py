from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import Dialect, no_ilike_sql, rename_func, trim_sql
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _parse_xml_table(self: parser.Parser) -> exp.XMLTable:
    this = self._parse_string()

    passing = None
    columns = None

    if self._match_text_seq("PASSING"):
        # The BY VALUE keywords are optional and are provided for semantic clarity
        self._match_text_seq("BY", "VALUE")
        passing = self._parse_csv(self._parse_column)

    by_ref = self._match_text_seq("RETURNING", "SEQUENCE", "BY", "REF")

    if self._match_text_seq("COLUMNS"):
        columns = self._parse_csv(lambda: self._parse_column_def(self._parse_field(any_token=True)))

    return self.expression(exp.XMLTable, this=this, passing=passing, columns=columns, by_ref=by_ref)


class Oracle(Dialect):
    ALIAS_POST_TABLESAMPLE = True

    # https://docs.oracle.com/database/121/SQLRF/sql_elements004.htm#SQLRF00212
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    TIME_MAPPING = {
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
        WINDOW_BEFORE_PAREN_TOKENS = {TokenType.OVER, TokenType.KEEP}

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
        }

        FUNCTION_PARSERS: t.Dict[str, t.Callable] = {
            **parser.Parser.FUNCTION_PARSERS,
            "XMLTABLE": _parse_xml_table,
        }

        TYPE_LITERAL_PARSERS = {
            exp.DataType.Type.DATE: lambda self, this, _: self.expression(
                exp.DateStrToDate, this=this
            )
        }

        def _parse_column(self) -> t.Optional[exp.Expression]:
            column = super()._parse_column()
            if column:
                column.set("join_mark", self._match(TokenType.JOIN_MARKER))
            return column

        def _parse_hint(self) -> t.Optional[exp.Hint]:
            if self._match(TokenType.HINT):
                start = self._curr
                while self._curr and not self._match_pair(TokenType.STAR, TokenType.SLASH):
                    self._advance()

                if not self._curr:
                    self.raise_error("Expected */ after HINT")

                end = self._tokens[self._index - 3]
                return exp.Hint(expressions=[self._find_sql(start, end)])

            return None

    class Generator(generator.Generator):
        LOCKING_READS_SUPPORTED = True
        JOIN_HINTS = False
        TABLE_HINTS = False
        COLUMN_JOIN_MARKS_SUPPORTED = True

        LIMIT_FETCH = "FETCH"

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "NUMBER",
            exp.DataType.Type.SMALLINT: "NUMBER",
            exp.DataType.Type.INT: "NUMBER",
            exp.DataType.Type.BIGINT: "NUMBER",
            exp.DataType.Type.DECIMAL: "NUMBER",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.VARCHAR: "VARCHAR2",
            exp.DataType.Type.NVARCHAR: "NVARCHAR2",
            exp.DataType.Type.NCHAR: "NCHAR",
            exp.DataType.Type.TEXT: "CLOB",
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.VARBINARY: "BLOB",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.DateStrToDate: lambda self, e: self.func(
                "TO_DATE", e.this, exp.Literal.string("YYYY-MM-DD")
            ),
            exp.Group: transforms.preprocess([transforms.unalias_group]),
            exp.ILike: no_ilike_sql,
            exp.Coalesce: rename_func("NVL"),
            exp.Select: transforms.preprocess([transforms.eliminate_distinct_on]),
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.Subquery: lambda self, e: self.subquery_sql(e, sep=" "),
            exp.Substring: rename_func("SUBSTR"),
            exp.Table: lambda self, e: self.table_sql(e, sep=" "),
            exp.TableSample: lambda self, e: self.tablesample_sql(e, sep=" "),
            exp.TimeToStr: lambda self, e: f"TO_CHAR({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.Trim: trim_sql,
            exp.UnixToTime: lambda self, e: f"TO_DATE('1970-01-01','YYYY-MM-DD') + ({self.sql(e, 'this')} / 86400)",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def offset_sql(self, expression: exp.Offset) -> str:
            return f"{super().offset_sql(expression)} ROWS"

        def xmltable_sql(self, expression: exp.XMLTable) -> str:
            this = self.sql(expression, "this")
            passing = self.expressions(expression, key="passing")
            passing = f"{self.sep()}PASSING{self.seg(passing)}" if passing else ""
            columns = self.expressions(expression, key="columns")
            columns = f"{self.sep()}COLUMNS{self.seg(columns)}" if columns else ""
            by_ref = (
                f"{self.sep()}RETURNING SEQUENCE BY REF" if expression.args.get("by_ref") else ""
            )
            return f"XMLTABLE({self.sep('')}{self.indent(this + passing + by_ref + columns)}{self.seg(')', sep='')}"

    class Tokenizer(tokens.Tokenizer):
        VAR_SINGLE_TOKENS = {"@"}

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "(+)": TokenType.JOIN_MARKER,
            "BINARY_DOUBLE": TokenType.DOUBLE,
            "BINARY_FLOAT": TokenType.FLOAT,
            "COLUMNS": TokenType.COLUMN,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "MINUS": TokenType.EXCEPT,
            "NVARCHAR2": TokenType.NVARCHAR,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "START": TokenType.BEGIN,
            "TOP": TokenType.TOP,
            "VARCHAR2": TokenType.VARCHAR,
        }
