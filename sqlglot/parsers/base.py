from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.tokens import TokenType


class BaseParser(parser.Parser):
    NO_PAREN_FUNCTIONS = {
        **parser.Parser.NO_PAREN_FUNCTIONS,
        TokenType.LOCALTIME: exp.Localtime,
        TokenType.LOCALTIMESTAMP: exp.Localtimestamp,
        TokenType.CURRENT_CATALOG: exp.CurrentCatalog,
        TokenType.SESSION_USER: exp.SessionUser,
    }

    ID_VAR_TOKENS = parser.Parser.ID_VAR_TOKENS - {TokenType.STRAIGHT_JOIN}
    TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS - {TokenType.STRAIGHT_JOIN}
