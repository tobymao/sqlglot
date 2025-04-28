from sqlglot import Dialect, generator, Tokenizer, TokenType, tokens
from sqlglot.dialects.dialect import NormalizationStrategy
import typing as t
from sqlglot import exp


class SingleStore(Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE
    IDENTIFIERS_CAN_START_WITH_DIGIT = True
    DPIPE_IS_STRING_CONCAT = False
    SUPPORTS_USER_DEFINED_TYPES = False
    SUPPORTS_SEMI_ANTI_JOIN = False
    SAFE_DIVISION = True
    TIME_FORMAT = "'%Y-%m-%d %T'"

    TIME_MAPPING: t.Dict[str, str] = {
        "%Y": "%Y",
        "%y": "%-y",
        "%j": "%j",
        "%b": "%b",
        "%M": "%B",
        "%m": "%m",
        "%c": "%-m",
        "%d": "%d",
        "%e": "%-d",
        "%H": "%H",
        "%h": "%I",
        "%I": "%I",
        "%k": "%-H",
        "%l": "%-I",
        "%i": "%M",
        "%S": "%S",
        "%s": "%S",
        "%f": "%f",
        "%p": "%p",
        "%r": "%H:%i:%S %p",
        "%T": "%H:%i:%S",
        "%U": "%U",
        "%u": "%W",
        "%W": "%A",
        "%w": "%w",
        "%a": "%a",
        "%%": "%%"
    }

    FORCE_EARLY_ALIAS_REF_EXPANSION = True
    SUPPORTS_ORDER_BY_ALL = True
    PROMOTE_TO_INFERRED_DATETIME_TYPE = True

    CREATABLE_KIND_MAPPING: dict[str, str] = {
        "DATABASE": "SCHEMA"
    }

    class Tokenizer(tokens.Tokenizer):
        BIT_STRINGS = [("b'", "'"), ("B'", "'"), ("0b", "")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", "")]
        IDENTIFIERS = ['"', '`']
        QUOTES = ["'", '"']
        STRING_ESCAPES = ["'", '"', "\\"]
        COMMENTS = ["--", "#", ("/*", "*/")]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "@@": TokenType.SESSION_PARAMETER,
            "YEAR": TokenType.YEAR,
            "BSON": TokenType.JSONB,
            "GEOGRAPHYPOINT": TokenType.GEOGRAPHY,
            "IGNORE": TokenType.IGNORE,
            "KEY": TokenType.KEY,
            "START": TokenType.BEGIN
        }

        COMMANDS = {*tokens.Tokenizer.COMMANDS, TokenType.REPLACE} - {TokenType.SHOW}

    # TODO: implement
    # class Parser(parser.Parser):

    class Generator(generator.Generator):
        LOCKING_READS_SUPPORTED = True
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        MATCHED_BY_SOURCE = False
        INTERVAL_ALLOWS_PLURAL_FORM = False
        LIMIT_FETCH = "LIMIT"
        LIMIT_ONLY_LITERALS = True
        JOIN_HINTS = False
        DUPLICATE_KEY_UPDATE_WITH_SET = False
        NVL2_SUPPORTED = False
        VALUES_AS_TABLE = False
        LAST_DAY_SUPPORTS_DATE_PART = False
        PAD_FILL_PATTERN_IS_REQUIRED = True
        SUPPORTS_TABLE_ALIAS_COLUMNS = False
        SUPPORTED_JSON_PATH_PARTS = set(exp.JSONPathKey)
        SET_OP_MODIFIERS = False
        TRY_SUPPORTED = False
        SUPPORTS_UESCAPE = False
        WITH_PROPERTIES_PREFIX = " "
        SUPPORTS_CONVERT_TIMEZONE = True
        SUPPORTS_UNIX_SECONDS = True
        PARSE_JSON_NAME: t.Optional[str] = "TO_JSON"
