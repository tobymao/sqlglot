from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.tokens import TokenType


class SolrParser(parser.Parser):
    DISJUNCTION = {
        **parser.Parser.DISJUNCTION,
        TokenType.DPIPE: exp.Or,
    }
