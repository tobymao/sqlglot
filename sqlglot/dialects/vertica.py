from sqlglot import exp, generator, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Tokenizer, TokenType


class Vertica(Dialect):
    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "REFRESH": TokenType.REFRESH,
            "TEMP": TokenType.TEMPORARY,
        }

    class Generator(generator.Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.MONEY: "MONEY",
            exp.DataType.Type.VARBINARY: "LONGVARBINARY",
            exp.DataType.Type.INTERVAL: "INTERVAL DAY TO SECOND",
            exp.DataType.Type.INTERVAL: "INTERVAL YEAR TO MONTH",
        }
