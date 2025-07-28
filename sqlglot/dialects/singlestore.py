from sqlglot import TokenType
from sqlglot.dialects.mysql import MySQL


class SingleStore(MySQL):
    SUPPORTS_ORDER_BY_ALL = True

    class Tokenizer(MySQL.Tokenizer):
        BYTE_STRINGS = [("e'", "'"), ("E'", "'")]

        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
            "BSON": TokenType.JSONB,
            "GEOGRAPHYPOINT": TokenType.GEOGRAPHY,
            ":>": TokenType.COLON_GT,
            "!:>": TokenType.NCOLON_GT,
            "::$": TokenType.DCOLONDOLLAR,
            "::%": TokenType.DCOLONPERCENT,
        }
