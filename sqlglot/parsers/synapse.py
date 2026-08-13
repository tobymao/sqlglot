from __future__ import annotations

from sqlglot import exp
from sqlglot.parsers.tsql import TSQLParser
from sqlglot.tokens import TokenType


class SynapseParser(TSQLParser):
    FUNCTION_PARSERS = {
        **TSQLParser.FUNCTION_PARSERS,
        "OPENROWSET": lambda self: self._parse_openrowset(),
    }

    def _parse_openrowset(self) -> exp.OpenRowset:
        # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-openrowset
        if not self._match_text_seq("BULK"):
            self.raise_error("Expected BULK in OPENROWSET")

        bulk = self._parse_string()
        if bulk is None:
            self.raise_error("Expected a BULK path in OPENROWSET")

        if not self._match(TokenType.COMMA) or not self._match_text_seq("FORMAT"):
            self.raise_error("Expected FORMAT in OPENROWSET")

        if not self._match(TokenType.EQ):
            self.raise_error("Expected = after FORMAT in OPENROWSET")

        format_ = self._parse_string()
        if format_ is None:
            self.raise_error("Expected a FORMAT value in OPENROWSET")

        return self.expression(exp.OpenRowset(bulk=bulk, format=format_))
