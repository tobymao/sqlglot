from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.tokens import TokenType


class SolrParser(parser.Parser):
    TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS | {
        TokenType.ANTI,
        TokenType.SEMI,
    }

    DISJUNCTION = {
        **parser.Parser.DISJUNCTION,
        TokenType.DPIPE: exp.Or,
    }
