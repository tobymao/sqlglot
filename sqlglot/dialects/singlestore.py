from sqlglot import Dialect
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
    FORCE_EARLY_ALIAS_REF_EXPANSION = True
    SUPPORTS_ORDER_BY_ALL = True
    PROMOTE_TO_INFERRED_DATETIME_TYPE = True
