from __future__ import annotations

import typing as t
from sqlglot import exp, generator, parser, tokens

from sqlglot.dialects.dialect import Dialect

from sqlglot.tokens import Tokenizer, TokenType

from sqlglot.helper import seq_get


from sqlglot.dialects.dialect import (
    build_date_delta_with_interval,
    build_formatted_time,
    NormalizationStrategy,
    date_add_sql,
)


class DB2(Dialect):
    DATE_FORMAT = "'yyyy-MM-dd'"
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    TIME_MAPPING = {
        "AM": "%p",
        "A.M.": "%p",
        "PM": "%p",
        "P.M.": "%p",
        "D": "%u",
        "DAY": "%A",
        "DD": "%d",
        "DDD": "%j",
        "DY": "%a",
        "HH": "%I",
        "HH12": "%I",
        "HH24": "%H",
        "IW": "%V",
        "MI": "%M",
        "MM": "%m",
        "MON": "%b",
        "MONTH": "%B",
        "SS": "%S",
        "WW": "%W",
        "YY": "%y",
        "YYYY": "%Y",
        "FF": "%f",
        "FF1": "%f",
        "FF2": "%f",
        "FF3": "%f",
        "FF4": "%f",
        "FF5": "%f",
        "FF6": "%f",
    }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]

        # Associates certain meaningful words with tokens that capture their intent
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "(+)": TokenType.JOIN_MARKER,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
            "CURRENT DATE": TokenType.CURRENT_DATE,
            "CURRENT TIME": TokenType.CURRENT_TIME,
            "CURRENT TIMESTAMP": TokenType.CURRENT_TIMESTAMP,
            "ORDER BY": TokenType.ORDER_BY,
        }

    class Parser(parser.Parser):
        ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = False
        WINDOW_BEFORE_PAREN_TOKENS = {TokenType.OVER, TokenType.KEEP}
        VALUES_FOLLOWED_BY_PAREN = False
        ALIAS_POST_TABLESAMPLE = True
        STRING_ALIASES = True

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "DATE_SUB": build_date_delta_with_interval(exp.DateSub),
            "TO_CHAR": _build_timetostr_or_tochar,
            "DATE_ADD": build_date_delta_with_interval(exp.DateAdd),
            "DATE": lambda args: exp.TsOrDsToDate(this=seq_get(args, 0)),
            "CHAR": lambda self: self._parse_chr(),
            "GROUP_CONCAT": lambda self: self._parse_group_concat(),
            # https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
            "VALUES": lambda self: self.expression(
                exp.Anonymous, this="VALUES", expressions=[self._parse_id_var()]
            ),
            "TO_DATE": build_formatted_time(exp.StrToDate, "db2"),
            "DAY": lambda args: exp.Day(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
            "TO_DAYS": lambda args: exp.paren(
                exp.DateDiff(
                    this=exp.TsOrDsToDate(this=seq_get(args, 0)),
                    expression=exp.TsOrDsToDate(this=exp.Literal.string("0000-01-01")),
                    unit=exp.var("DAY"),
                )
                + 1
            ),
        }

        FUNCTION_PARSERS: t.Dict[str, t.Callable] = {
            **parser.Parser.FUNCTION_PARSERS,
            "JSON_ARRAY": lambda self: self._parse_json_array(
                exp.JSONArray,
                expressions=self._parse_csv(lambda: self._parse_format_json(self._parse_bitwise())),
            ),
            "JSON_ARRAYAGG": lambda self: self._parse_json_array(
                exp.JSONArrayAgg,
                this=self._parse_format_json(self._parse_bitwise()),
                order=self._parse_order(),
            ),
            "JSON_OBJECT": lambda self: self.elf._parse_json_object(
                exp.JSONObject,
                this=self._parse_format_json(self._parse_bitwise()),
            ),
            "XMLTABLE": lambda self: self._parse_xml_table(),
        }


    class Generator(generator.Generator):
        SINGLE_STRING_INTERVAL = True
        RENAME_TABLE_WITH_DB = False
        LOCKING_READS_SUPPORTED = True
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        PARAMETER_TOKEN = "?"
        TABLESAMPLE_SIZE_IS_ROWS = True
        TABLESAMPLE_SEED_KEYWORD = "REPEATABLE"
        SUPPORTS_SELECT_INTO = True
        JSON_TYPE_REQUIRED_FOR_EXTRACTION = False
        SUPPORTS_UNLOGGED_TABLES = False
        LIKE_PROPERTY_INSIDE_SCHEMA = True
        MULTI_ARG_DISTINCT = True
        CAN_IMPLEMENT_ARRAY_ANY = False
        COPY_HAS_INTO_KEYWORD = False

        LIMIT_FETCH = "FETCH"
        TABLESAMPLE_KEYWORDS = "SAMPLE"
        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.StrToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.Array: lambda self, e: f"[{self.expressions(e)}]",
            exp.DateSub: date_add_sql("SUB"),
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
        }

        # Specifies how AST nodes representing data types should be converted into SQL
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.VARCHAR: "VARCHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.TEXT: "CLOB",
            exp.DataType.Type.TIMETZ: "TIME",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.VARBINARY: "BLOB",
            exp.DataType.Type.ROWVERSION: "ROWID",
            exp.DataType.Type.XML: "XML",
        }

        def schemacommentproperty_sql(self, expression: exp.SchemaCommentProperty) -> str:
            self.unsupported("Table comments are not supported in the CREATE statement for DB2")
            return ""

        def commentcolumnconstraint_sql(self, expression: exp.CommentColumnConstraint) -> str:
            self.unsupported("Column comments are not supported in the CREATE statement for DB2")
            return ""

        def currenttimestamp_sql(self, expression: exp.CurrentTimestamp) -> str:
            this = expression.this
            return self.func("CURRENT_TIMESTAMP", this) if this else "CURRENT_TIMESTAMP"

        def add_column_sql(self, expression: exp.Alter) -> str:
            actions = self.expressions(expression, key="actions", flat=True)
            if len(expression.args.get("actions", [])) > 1:
                return f"ADD ({actions})"
            return f"ADD {actions}"
