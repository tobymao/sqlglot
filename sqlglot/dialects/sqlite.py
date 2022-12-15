from __future__ import annotations

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    arrow_json_extract_scalar_sql,
    arrow_json_extract_sql,
    no_ilike_sql,
    no_tablesample_sql,
    no_trycast_sql,
    rename_func,
)
from sqlglot.tokens import TokenType


# https://www.sqlite.org/lang_aggfunc.html#group_concat
def _group_concat_sql(self, expression):
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


class SQLite(Dialect):
    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', ("[", "]"), "`"]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", ""), ("0X", "")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "AUTOINCREMENT": TokenType.AUTO_INCREMENT,
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
            exp.ILike: no_ilike_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONBExtract: arrow_json_extract_sql,
            exp.JSONBExtractScalar: arrow_json_extract_scalar_sql,
            exp.Levenshtein: rename_func("EDITDIST3"),
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
            exp.GroupConcat: _group_concat_sql,
        }

        def transaction_sql(self, expression):
            this = expression.this
            this = f" {this}" if this else ""
            return f"BEGIN{this} TRANSACTION"
