from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Tokenizer, TokenType


class Snowflake(Dialect):
    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "QUALIFY": TokenType.QUALIFY,
            "DOUBLE PRECISION": TokenType.DOUBLE,
        }
