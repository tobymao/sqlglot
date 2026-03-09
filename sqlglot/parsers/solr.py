from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.helper import mypyc_attr
from sqlglot.tokens import TokenType


@mypyc_attr(allow_interpreted_subclasses=True)
class SolrParser(parser.Parser):
    DISJUNCTION = {
        **parser.Parser.DISJUNCTION,
        TokenType.DPIPE: exp.Or,
    }
