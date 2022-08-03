from sqlglot.dialects.dialect import Dialect
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class Snowflake(Dialect):
    class Parser(Parser):
        COLUMN_OPERATORS = Parser.COLUMN_OPERATORS | {TokenType.COLON}

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "QUALIFY": TokenType.QUALIFY,
            "DOUBLE PRECISION": TokenType.DOUBLE,
        }
