from sqlglot import TokenType
from sqlglot.dialects.mysql import MySQL
from sqlglot.generators.singlestore import SingleStoreGenerator
from sqlglot.parsers.singlestore import SingleStoreParser, cast_to_time6


class SingleStore(MySQL):
    SUPPORTS_ORDER_BY_ALL = True

    MYSQL_INVERSE_TIME_MAPPING = MySQL.INVERSE_TIME_MAPPING
    MYSQL_INVERSE_TIME_TRIE = MySQL.INVERSE_TIME_TRIE
    CAST_TO_TIME6 = staticmethod(cast_to_time6)

    TIME_MAPPING: dict[str, str] = {
        "D": "%u",  # Day of week (1-7)
        "DD": "%d",  # day of month (01-31)
        "DY": "%a",  # abbreviated name of day
        "HH": "%I",  # Hour of day (01-12)
        "HH12": "%I",  # alias for HH
        "HH24": "%H",  # Hour of day (00-23)
        "MI": "%M",  # Minute (00-59)
        "MM": "%m",  # Month (01-12; January = 01)
        "MON": "%b",  # Abbreviated name of month
        "MONTH": "%B",  # Name of month
        "SS": "%S",  # Second (00-59)
        "RR": "%y",  # 15
        "YY": "%y",  # 15
        "YYYY": "%Y",  # 2015
        "FF6": "%f",  # only 6 digits are supported in python formats
    }

    VECTOR_TYPE_ALIASES = {
        "I8": "TINYINT",
        "I16": "SMALLINT",
        "I32": "INT",
        "I64": "BIGINT",
        "F32": "FLOAT",
        "F64": "DOUBLE",
    }

    INVERSE_VECTOR_TYPE_ALIASES = {v: k for k, v in VECTOR_TYPE_ALIASES.items()}

    class Tokenizer(MySQL.Tokenizer):
        BYTE_STRINGS = [("e'", "'"), ("E'", "'")]

        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
            "BSON": TokenType.JSONB,
            "GEOGRAPHYPOINT": TokenType.GEOGRAPHYPOINT,
            "TIMESTAMP": TokenType.TIMESTAMP,
            "UTC_DATE": TokenType.UTC_DATE,
            "UTC_TIME": TokenType.UTC_TIME,
            "UTC_TIMESTAMP": TokenType.UTC_TIMESTAMP,
            ":>": TokenType.COLON_GT,
            "!:>": TokenType.NCOLON_GT,
            "::$": TokenType.DCOLONDOLLAR,
            "::%": TokenType.DCOLONPERCENT,
            "::?": TokenType.DCOLONQMARK,
            "RECORD": TokenType.STRUCT,
        }

    Parser = SingleStoreParser

    Generator = SingleStoreGenerator
