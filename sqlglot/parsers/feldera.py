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
        if self._curr and self._curr.text.upper() == "REMOVE":
            self._advance()
            return self._parse_remove()

        if self._curr and self._curr.text.upper() == "LATENESS":
            self._advance()
            return self._parse_lateness()

        if self._curr and self._curr.text.upper() == "DECLARE":
            start = self._curr
            self._advance()
            return self._parse_feldera_command(start)

        return super()._parse_statement()

    def _parse_lateness(self) -> exp.Lateness:
        return self.expression(
            exp.Lateness(this=self._parse_column(), expression=self._parse_disjunction())
        )

    def _parse_remove(self) -> exp.Remove:
        self._match_text_seq("FROM")
        return self.expression(
            exp.Remove(
                this=self._parse_table(schema=True),
                expression=self._parse_derived_table_values(),
            )
        )

    def _parse_feldera_command(self, start: Token) -> exp.Command:
        while self._curr:
            self._advance()

        text = self._find_sql(start, self._prev)
        size = len(start.text)
        return self.expression(exp.Command(this=text[:size], expression=text[size:]))