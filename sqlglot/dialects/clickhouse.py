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
            "FINAL": TokenType.FINAL,
        }

    class Parser(Parser):
        def _parse_table(self, schema=False):
            this = super()._parse_table(schema)

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

    class Generator(Generator):
        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
        }
