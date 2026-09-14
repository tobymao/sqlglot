from sqlglot import Dialect, tokens, parser, generator
from sqlglot.dialects.dialect import NormalizationStrategy


class DB2(Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    SUPPORTS_SEMI_ANTI_JOIN = False
    TYPED_DIVISION = True
    SAFE_DIVISION = True

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', ("[", "]"), "`"]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", ""), ("0X", "")]

        KEYWORDS = tokens.Tokenizer.KEYWORDS.copy()
        KEYWORDS.pop("/*+")

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
        }
        STRING_ALIASES = True

    class Generator(generator.Generator):
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        NVL2_SUPPORTED = False
        JSON_PATH_BRACKETED_KEY_SUPPORTED = False
        SUPPORTS_CREATE_TABLE_LIKE = False
        SUPPORTS_TABLE_ALIAS_COLUMNS = False
        SUPPORTS_TO_NUMBER = False
        SUPPORTS_WINDOW_EXCLUDE = True
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        SUPPORTS_MEDIAN = False
        JSON_KEY_VALUE_PAIR_SEP = ","
        LIMIT_FETCH = "LIMIT"


        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
        }


        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
        }


        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
        }