from __future__ import annotations

from sqlglot import exp
from sqlglot.parsers.tsql import TSQLParser
from sqlglot.tokens import TokenType


class SynapseParser(TSQLParser):
    # Synapse permits OUTPUT as an unquoted table alias, including on
    # OPENROWSET sources. T-SQL tokenizes it as RETURNING for DML parsing.
    ID_VAR_TOKENS = TSQLParser.ID_VAR_TOKENS | {TokenType.RETURNING}

    PROPERTY_PARSERS = {
        **TSQLParser.PROPERTY_PARSERS,
        "FORMAT_OPTIONS": lambda self: self._parse_format_options_property(),
    }

    FUNCTION_PARSERS = {
        **TSQLParser.FUNCTION_PARSERS,
        "OPENROWSET": lambda self: self._parse_openrowset(),
        "TRY_PARSE": lambda self: self._parse_try_parse(),
    }

    def _parse_create(self) -> exp.Create | exp.Command:
        if self._curr and self._curr.text.upper() == "EXTERNAL":
            self._advance()
            return self._parse_external_create()

        return super()._parse_create()

    def _parse_format_options_property(self) -> exp.FormatOptionsProperty:
        equals = self._match(TokenType.EQ)
        if not self._match(TokenType.L_PAREN):
            self.raise_error("Expected ( after FORMAT_OPTIONS")

        expressions = self._parse_csv(self._parse_key_value_property)
        self._match_r_paren()
        return self.expression(exp.FormatOptionsProperty(expressions=expressions, equals=equals))

    def _parse_try_parse(self) -> exp.TryParse:
        this = self._parse_assignment()
        if not self._match(TokenType.ALIAS):
            self.raise_error("Expected AS after TRY_PARSE expression")

        to = self._parse_types(with_collation=True)
        culture = self._match_text_seq("USING") and self._parse_bitwise()
        return self.expression(exp.TryParse(this=this, to=to, culture=culture))

    def _parse_external_create(self) -> exp.Create | exp.Command:
        start = self._prev

        if self._match_text_seq("FILE", "FORMAT"):
            kind = "FILE FORMAT"
            table = self._parse_table_parts()
        elif self._match_text_seq("DATA", "SOURCE"):
            kind = "DATA SOURCE"
            table = self._parse_table_parts()
        elif self._match_text_seq("TABLE"):
            kind = "TABLE"
            table = self._parse_table_parts(schema=True)
        else:
            return self._parse_as_command(start)

        if not table:
            return self._parse_as_command(start)

        if kind == "TABLE":
            table = self._parse_schema(this=table)

        exists = self._parse_exists(not_=True)
        if not self._match(TokenType.WITH):
            return self._parse_as_command(start)

        properties = self._parse_wrapped_properties()
        external = self.expression(exp.ExternalProperty())
        properties = self.expression(
            exp.Properties(expressions=[external, *(properties or [])])
        )

        if self._curr and not self._match_set(
            (TokenType.R_PAREN, TokenType.COMMA, TokenType.SEMICOLON, TokenType.END),
            advance=False,
        ):
            return self._parse_as_command(start)

        return self.expression(
            exp.Create(
                this=table,
                kind=kind,
                exists=exists,
                properties=properties,
            )
        )

    def _parse_openrowset(self) -> exp.OpenRowset:
        if not self._match_text_seq("BULK"):
            self.raise_error("Expected BULK in OPENROWSET")

        bulk_parenthesized = self._match(TokenType.L_PAREN)
        if bulk_parenthesized:
            bulk = self._parse_csv(self._parse_bitwise)
            self._match_r_paren()
        else:
            bulk = [self._parse_bitwise()]

        if not bulk or any(value is None for value in bulk):
            self.raise_error("Expected a BULK path in OPENROWSET")

        properties: list[exp.Expr] = []
        while self._match(TokenType.COMMA):
            if self._curr and self._curr.text.upper() == "FORMAT_OPTIONS":
                self._advance()
                properties.append(self._parse_format_options_property())
            else:
                property_ = self._parse_key_value_property()
                if not property_:
                    self.raise_error("Expected an OPENROWSET option")
                properties.append(property_)

        self._openrowset_pending_schema = True
        return self.expression(
            exp.OpenRowset(
                bulk=bulk,
                properties=properties,
                bulk_parenthesized=bulk_parenthesized,
            )
        )

    def _parse_table_hints(self) -> list[exp.Expr] | None:
        if (
            getattr(self, "_openrowset_pending_schema", False)
            and self._curr
            and self._curr.text.upper() == "WITH"
            and self._next
            and self._next.token_type == TokenType.L_PAREN
        ):
            self._openrowset_pending_schema = False
            return None

        self._openrowset_pending_schema = False
        return super()._parse_table_hints()

    def _parse_table(self, *args, **kwargs) -> exp.Expr | None:
        table = super()._parse_table(*args, **kwargs)
        if not isinstance(table, exp.Table) or not isinstance(table.this, exp.OpenRowset):
            return table

        openrowset = table.this
        if self._match(TokenType.WITH):
            schema = self._parse_schema()
            if schema:
                openrowset.set("schema", schema)

            if not table.args.get("alias"):
                alias = self._parse_table_alias(
                    alias_tokens=kwargs.get("alias_tokens") or self.TABLE_ALIAS_TOKENS
                )
                if alias:
                    table.set("alias", alias)

            if kwargs.get("joins"):
                for join in self._parse_joins(alias_tokens=kwargs.get("alias_tokens")):
                    table.append("joins", join)

        return table
