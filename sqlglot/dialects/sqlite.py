from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    arrow_json_extract_sql,
    arrow_json_extract_scalar_sql,
    no_tablesample_sql,
    no_trycast_sql,
    rename_func,
    no_ilike_sql,
)
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class SQLite(Dialect):
    identifiers = ['"', ("[", "]"), "`"]

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "AUTOINCREMENT": TokenType.AUTO_INCREMENT,
        }

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "EDITDIST3": exp.Levenshtein.from_arg_list,
        }

    class Generator(Generator):
        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
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
        }

        TOKEN_MAPPING = {
            TokenType.AUTO_INCREMENT: "AUTOINCREMENT",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.ILike: no_ilike_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONBExtract: arrow_json_extract_sql,
            exp.JSONBExtractScalar: arrow_json_extract_scalar_sql,
            exp.Levenshtein: rename_func("EDITDIST3"),
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
        }
