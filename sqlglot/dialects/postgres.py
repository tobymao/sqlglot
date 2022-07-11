from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, no_tablesample_sql, no_trycast_sql
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class Postgres(Dialect):
    strict_cast = False

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "UUID": TokenType.UUID,
        }

    class Parser(Parser):
        FUNCTIONS = {**Parser.FUNCTIONS, "TO_TIMESTAMP": exp.StrToTime.from_arg_list}

    class Generator(Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.BINARY: "BYTEA",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
        }
