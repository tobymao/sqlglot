from sqlglot import exp, generator, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Tokenizer, TokenType


class Vertica(Dialect):
    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "MONEY": TokenType.MONEY,
            "LONGVARBINARY": TokenType.VARBINARY,
            "INTERVAL DAY TO SECOND": TokenType.INTERVAL,
        }

    class Generator(generator.Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.MONEY: "MONEY",
            exp.DataType.Type.VARBINARY: "LONGVARBINARY",
            exp.DataType.Type.INTERVAL: "INTERVAL DAY TO SECOND",
        }
