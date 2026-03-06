from sqlglot import exp, parser, tokens
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.tokens import TokenType


# https://solr.apache.org/guide/solr/latest/query-guide/sql-query.html


class Solr(Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    DPIPE_IS_STRING_CONCAT = False
    SUPPORTS_SEMI_ANTI_JOIN = False

    class Parser(parser.Parser):
        DISJUNCTION = {
            **parser.Parser.DISJUNCTION,
            TokenType.DPIPE: exp.Or,
        }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'"]
        IDENTIFIERS = ["`"]
