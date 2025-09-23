from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy


# https://solr.apache.org/guide/solr/latest/query-guide/sql-query.html


class Solr(Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    DPIPE_IS_STRING_CONCAT = False
    SUPPORTS_SEMI_ANTI_JOIN = False

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'"]
        IDENTIFIERS = ["`"]
