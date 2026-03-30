from sqlglot import tokens
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.generators.solr import SolrGenerator
from sqlglot.parsers.solr import SolrParser


# https://solr.apache.org/guide/solr/latest/query-guide/sql-query.html


class Solr(Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    DPIPE_IS_STRING_CONCAT = False

    Generator = SolrGenerator

    Parser = SolrParser

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'"]
        IDENTIFIERS = ["`"]
