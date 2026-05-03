from __future__ import annotations

from sqlglot import exp
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokens import Token, TokenType


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
            if self._curr and self._curr.token_type == TokenType.RECURSIVE:
                return self._parse_declare_recursive_view(start)
            return self._parse_feldera_command(start)

        return super()._parse_statement()

    def _parse_create(self) -> exp.Create | exp.Command:
        if self._curr and self._curr.text.upper() in {"LINEAR", "AGGREGATE"}:
            return self._parse_create_aggregate(self._prev)

        return super()._parse_create()

    def _parse_create_aggregate(self, start: Token) -> exp.Create | exp.Command:
        linear = self._match_text_seq("LINEAR")
        if not self._match_text_seq("AGGREGATE"):
            return self._parse_as_command(start)

        exists = self._parse_exists(not_=True)
        this = self._parse_user_defined_function()
        returns = self._match_text_seq("RETURNS") and self._parse_returns()

        if not this or not returns:
            return self._parse_as_command(start)

        properties: list[exp.Expression] = [returns]
        if linear:
            properties.insert(0, self.expression(exp.LinearProperty()))

        return self.expression(
            exp.Create(
                this=this,
                kind="AGGREGATE",
                exists=exists,
                properties=exp.Properties(expressions=properties),
            )
        )

    def _parse_declare_recursive_view(
        self, start: Token
    ) -> exp.DeclareRecursiveView | exp.Command:
        if not self._match(TokenType.RECURSIVE) or not self._match(TokenType.VIEW):
            return self._parse_feldera_command(start)

        this = self._parse_table_parts(schema=True)
        if not this or not self._match(TokenType.ALIAS):
            return self._parse_feldera_command(start)

        expression = self._parse_ddl_select()
        if not expression:
            return self._parse_feldera_command(start)

        return self.expression(exp.DeclareRecursiveView(this=this, expression=expression))

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