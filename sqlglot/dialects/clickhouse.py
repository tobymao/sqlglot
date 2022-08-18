from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class ClickHouse(Dialect):
    normalize_functions = None

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "NULLABLE": TokenType.NULLABLE,
            "FINAL": TokenType.FINAL,
            "INT8": TokenType.TINYINT,
            "INT16": TokenType.SMALLINT,
            "INT32": TokenType.INT,
            "INT64": TokenType.BIGINT,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
        }

    class Parser(Parser):
        def _parse_table(self, schema=False):
            this = super()._parse_table(schema)

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

    class Generator(Generator):
        STRUCT_DELIMITER = ("(", ")")

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
        }
