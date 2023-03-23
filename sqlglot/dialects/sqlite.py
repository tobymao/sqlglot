from __future__ import annotations

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    arrow_json_extract_scalar_sql,
    arrow_json_extract_sql,
    count_if_to_sum,
    no_ilike_sql,
    no_tablesample_sql,
    no_trycast_sql,
    rename_func,
)
from sqlglot.tokens import TokenType


def _date_add_sql(self, expression):
    modifier = expression.expression
    modifier = expression.name if modifier.is_string else self.sql(modifier)
    unit = expression.args.get("unit")
    modifier = f"'{modifier} {unit.name}'" if unit else f"'{modifier}'"
    return self.func("DATE", expression.this, modifier)


class SQLite(Dialect):
    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', ("[", "]"), "`"]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", ""), ("0X", "")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "EDITDIST3": exp.Levenshtein.from_arg_list,
        }

    class Generator(generator.Generator):
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,  # type: ignore
            exp.DataType.Type.BOOLEAN: "INTEGER",
            exp.DataType.Type.TINYINT: "INTEGER",
            exp.DataType.Type.SMALLINT: "INTEGER",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.BIGINT: "INTEGER",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.DOUBLE: "REAL",
            exp.DataType.Type.DECIMAL: "REAL",
            exp.DataType.Type.CHAR: "TEXT",
            exp.DataType.Type.NCHAR: "TEXT",
            exp.DataType.Type.VARCHAR: "TEXT",
            exp.DataType.Type.NVARCHAR: "TEXT",
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.VARBINARY: "BLOB",
        }

        TOKEN_MAPPING = {
            TokenType.AUTO_INCREMENT: "AUTOINCREMENT",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            **transforms.ELIMINATE_QUALIFY,  # type: ignore
            exp.CountIf: count_if_to_sum,
            exp.CurrentDate: lambda *_: "CURRENT_DATE",
            exp.CurrentTime: lambda *_: "CURRENT_TIME",
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.DateAdd: _date_add_sql,
            exp.DateStrToDate: lambda self, e: self.sql(e, "this"),
            exp.ILike: no_ilike_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONBExtract: arrow_json_extract_sql,
            exp.JSONBExtractScalar: arrow_json_extract_scalar_sql,
            exp.Levenshtein: rename_func("EDITDIST3"),
            exp.LogicalOr: rename_func("MAX"),
            exp.LogicalAnd: rename_func("MIN"),
            exp.TableSample: no_tablesample_sql,
            exp.TimeStrToTime: lambda self, e: self.sql(e, "this"),
            exp.TryCast: no_trycast_sql,
        }

        LIMIT_FETCH = "LIMIT"

        def cast_sql(self, expression: exp.Cast) -> str:
            if expression.to.this == exp.DataType.Type.DATE:
                return self.func("DATE", expression.this)

            return super().cast_sql(expression)

        def datediff_sql(self, expression: exp.DateDiff) -> str:
            unit = expression.args.get("unit")
            unit = unit.name.upper() if unit else "DAY"

            sql = f"(JULIANDAY({self.sql(expression, 'this')}) - JULIANDAY({self.sql(expression, 'expression')}))"

            if unit == "MONTH":
                sql = f"{sql} / 30.0"
            elif unit == "YEAR":
                sql = f"{sql} / 365.0"
            elif unit == "HOUR":
                sql = f"{sql} * 24.0"
            elif unit == "MINUTE":
                sql = f"{sql} * 1440.0"
            elif unit == "SECOND":
                sql = f"{sql} * 86400.0"
            elif unit == "MILLISECOND":
                sql = f"{sql} * 86400000.0"
            elif unit == "MICROSECOND":
                sql = f"{sql} * 86400000000.0"
            elif unit == "NANOSECOND":
                sql = f"{sql} * 8640000000000.0"
            else:
                self.unsupported("DATEDIFF unsupported for '{unit}'.")

            return f"CAST({sql} AS INTEGER)"

        # https://www.sqlite.org/lang_aggfunc.html#group_concat
        def groupconcat_sql(self, expression):
            this = expression.this
            distinct = expression.find(exp.Distinct)
            if distinct:
                this = distinct.expressions[0]
                distinct = "DISTINCT "

            if isinstance(expression.this, exp.Order):
                self.unsupported("SQLite GROUP_CONCAT doesn't support ORDER BY.")
                if expression.this.this and not distinct:
                    this = expression.this.this

            separator = expression.args.get("separator")
            return f"GROUP_CONCAT({distinct or ''}{self.format_args(this, separator)})"

        def least_sql(self, expression: exp.Least) -> str:
            if len(expression.expressions) > 1:
                return rename_func("MIN")(self, expression)

            return self.expressions(expression)

        def transaction_sql(self, expression: exp.Transaction) -> str:
            this = expression.this
            this = f" {this}" if this else ""
            return f"BEGIN{this} TRANSACTION"
