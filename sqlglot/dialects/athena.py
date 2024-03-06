from __future__ import annotations

from sqlglot.dialects.trino import Trino
from sqlglot.tokens import TokenType


class Athena(Trino):
    class Parser(Trino.Parser):
        STATEMENT_PARSERS = {
            **Trino.Parser.STATEMENT_PARSERS,
            TokenType.USING: lambda self: self._parse_as_command(self._prev),
        }
