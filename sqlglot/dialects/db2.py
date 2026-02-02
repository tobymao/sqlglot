from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    max_or_greatest,
    min_or_least,
    rename_func,
    strposition_sql,
    trim_sql,
    no_ilike_sql,
    no_pivot_sql,
    no_trycast_sql,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def _date_add_sql(
    kind: str,
) -> t.Callable[[DB2.Generator, exp.DateAdd | exp.DateSub], str]:
    def func(self: DB2.Generator, expression: exp.DateAdd | exp.DateSub) -> str:
        this = self.sql(expression, "this")
        unit = expression.args.get("unit")
        value = self._simplify_unless_literal(expression.expression)

        if not isinstance(value, exp.Literal):
            self.unsupported("Cannot add non literal")

        value_sql = self.sql(value)
        unit_sql = self.sql(unit) if unit else "DAY"

        return f"{this} {kind} {value_sql} {unit_sql}"

    return func


class DB2(Dialect):
    # DB2 is case-insensitive by default for unquoted identifiers
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE
    
    # DB2 supports NULL ordering
    NULL_ORDERING = "nulls_are_large"
    
    # DB2 specific settings
    TYPED_DIVISION = True
    SAFE_DIVISION = True
    
    # Time format mappings for DB2
    # https://www.ibm.com/docs/en/db2/11.5?topic=functions-timestamp-format
    TIME_MAPPING = {
        "YYYY": "%Y",
        "YY": "%y",
        "MM": "%m",
        "DD": "%d",
        "HH": "%H",
        "HH12": "%I",
        "HH24": "%H",
        "MI": "%M",
        "SS": "%S",
        "FF": "%f",
        "FF3": "%f",
        "FF6": "%f",
        "MON": "%b",
        "MONTH": "%B",
        "DY": "%a",
        "DAY": "%A",
    }

    class Tokenizer(tokens.Tokenizer):
        # DB2 uses @ for variables
        VAR_SINGLE_TOKENS = {"@"}

        # DB2 specific keywords
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "CHAR": TokenType.CHAR,
            "CLOB": TokenType.TEXT,
            "DBCLOB": TokenType.TEXT,
            "DECFLOAT": TokenType.DECIMAL,
            "GRAPHIC": TokenType.NCHAR,
            "VARGRAPHIC": TokenType.NVARCHAR,
            "SMALLINT": TokenType.SMALLINT,
            "INTEGER": TokenType.INT,
            "BIGINT": TokenType.BIGINT,
            "REAL": TokenType.FLOAT,
            "DOUBLE": TokenType.DOUBLE,
            "DECIMAL": TokenType.DECIMAL,
            "NUMERIC": TokenType.DECIMAL,
            "VARCHAR": TokenType.VARCHAR,
            "TIMESTAMP": TokenType.TIMESTAMP,
            "TIMESTMP": TokenType.TIMESTAMP,
            "SYSIBM": TokenType.SCHEMA,
            "SYSFUN": TokenType.SCHEMA,
            "SYSTOOLS": TokenType.SCHEMA,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "CHAR": exp.Cast.from_arg_list,
            "DAYOFWEEK": lambda args: exp.Extract(
                this=exp.var("DAYOFWEEK"), expression=seq_get(args, 0)
            ),
            "DAYOFYEAR": lambda args: exp.Extract(
                this=exp.var("DAYOFYEAR"), expression=seq_get(args, 0)
            ),
            "DAYS": lambda args: exp.DateDiff(
                this=seq_get(args, 0), expression=exp.Literal.string("1970-01-01"), unit=exp.var("DAY")
            ),
            "MIDNIGHT_SECONDS": lambda args: exp.Anonymous(
                this="MIDNIGHT_SECONDS", expressions=args
            ),
            "POSSTR": lambda args: exp.StrPosition(
                this=seq_get(args, 0), substr=seq_get(args, 1)
            ),
            "VARCHAR_FORMAT": lambda args: exp.TimeToStr(
                this=seq_get(args, 0), format=seq_get(args, 1)
            ),
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "CAST": lambda self: self._parse_cast(self.STRICT_CAST),
        }

    class Generator(generator.Generator):
        LIMIT_FETCH = "FETCH"
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        NVL2_SUPPORTED = False
        LAST_DAY_SUPPORTS_DATE_PART = False
        
        # DB2 uses CONCAT operator
        CONCAT_COALESCE = True

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.BOOLEAN: "SMALLINT",
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.VARBINARY: "BLOB",
            exp.DataType.Type.TEXT: "CLOB",
            exp.DataType.Type.NCHAR: "GRAPHIC",
            exp.DataType.Type.NVARCHAR: "VARGRAPHIC",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ArgMax: rename_func("MAX"),
            exp.ArgMin: rename_func("MIN"),
            exp.DateAdd: _date_add_sql("+"),
            exp.DateSub: _date_add_sql("-"),
            exp.DateDiff: lambda self, e: f"{self.func('DAYS', e.this)} - {self.func('DAYS', e.expression)}",
            exp.CurrentDate: lambda self, e: "CURRENT DATE",
            exp.CurrentTimestamp: lambda self, e: "CURRENT TIMESTAMP",
            exp.ILike: no_ilike_sql,
            exp.Max: max_or_greatest,
            exp.Min: min_or_least,
            exp.Pivot: no_pivot_sql,
            exp.Select: transforms.preprocess([transforms.eliminate_distinct_on]),
            exp.StrPosition: lambda self, e: self.func("POSSTR", e.this, e.args.get("substr")),
            exp.TimeToStr: lambda self, e: self.func("VARCHAR_FORMAT", e.this, self.format_time(e)),
            exp.TryCast: no_trycast_sql,
            exp.Trim: trim_sql,
        }

        def offset_sql(self, expression: exp.Offset) -> str:
            # DB2 uses OFFSET ... ROWS syntax
            return f"{super().offset_sql(expression)} ROWS"

        def fetch_sql(self, expression: exp.Fetch) -> str:
            # DB2 uses FETCH FIRST ... ROWS ONLY syntax
            count = expression.args.get("count")
            if count:
                return f" FETCH FIRST {self.sql(count)} ROWS ONLY"
            return " FETCH FIRST ROW ONLY"

        def currenttimestamp_sql(self, expression: exp.CurrentTimestamp) -> str:
            return "CURRENT TIMESTAMP"

        def currentdate_sql(self, expression: exp.CurrentDate) -> str:
            return "CURRENT DATE"

        def boolean_sql(self, expression: exp.Boolean) -> str:
            # DB2 doesn't have native boolean, use 0/1
            return "1" if expression.this else "0"

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Handle DB2 specific data types
            if expression.this == exp.DataType.Type.DECIMAL:
                precision = expression.args.get("expressions")
                if precision:
                    return f"DECIMAL({self.expressions(expression, flat=True)})"
                return "DECIMAL(31, 0)"
            
            return super().datatype_sql(expression)

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            # DB2 uses different CAST syntax for some types
            to_type = expression.to
            
            if to_type.this == exp.DataType.Type.CHAR:
                # DB2 CHAR casting
                return f"CHAR({self.sql(expression.this)})"
            
            return super().cast_sql(expression, safe_prefix=safe_prefix)

        def extract_sql(self, expression: exp.Extract) -> str:
            this = self.sql(expression, "this")
            expression_sql = self.sql(expression, "expression")
            
            # DB2 uses different function names for some extracts
            if this.upper() == "DAYOFWEEK":
                return f"DAYOFWEEK({expression_sql})"
            elif this.upper() == "DAYOFYEAR":
                return f"DAYOFYEAR({expression_sql})"
            
            return f"EXTRACT({this} FROM {expression_sql})"

        def concat_sql(self, expression: exp.Concat) -> str:
            # DB2 uses CONCAT function or || operator
            expressions = expression.expressions
            if len(expressions) == 2:
                return f"{self.sql(expressions[0])} || {self.sql(expressions[1])}"
            
            # For multiple arguments, use nested CONCAT or multiple ||
            return " || ".join(self.sql(e) for e in expressions)

