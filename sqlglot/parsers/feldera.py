from __future__ import annotations

from sqlglot import exp
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokens import Token


class FelderaParser(PostgresParser):
    CONSTRAINT_PARSERS = {
        **PostgresParser.CONSTRAINT_PARSERS,
        "INTERNED": lambda self: self.expression(exp.InternedColumnConstraint()),
        "LATENESS": lambda self: self.expression(
            exp.LatenessColumnConstraint(this=self._parse_disjunction())
        ),
    }

    SCHEMA_UNNAMED_CONSTRAINTS = {
        *PostgresParser.SCHEMA_UNNAMED_CONSTRAINTS,
        "INTERNED",
        "LATENESS",
    }

    def _parse_statement(self) -> exp.Expr | None:
        if self._curr and self._curr.text.upper() in {"DECLARE", "LATENESS", "REMOVE"}:
            start = self._curr
            self._advance()
            return self._parse_feldera_command(start)

        return super()._parse_statement()

    def _parse_feldera_command(self, start: Token) -> exp.Command:
        while self._curr:
            self._advance()

        text = self._find_sql(start, self._prev)
        size = len(start.text)
        return self.expression(exp.Command(this=text[:size], expression=text[size:]))