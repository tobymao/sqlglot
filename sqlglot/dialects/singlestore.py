from sqlglot import Dialect, tokens, parser, generator, jsonpath
from sqlglot.dialects.dialect import NormalizationStrategy
import typing as t


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

    # TODO: implement
    # class Tokenizer(tokens.Tokenizer):

    # TODO: implement
    # class Parser(parser.Parser):

    # TODO: implement
    # class Generator(generator.Generator):
