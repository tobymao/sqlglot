from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType


class PRQL(Dialect):
    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ["`"]
        QUOTES = ["'", '"']

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "=": TokenType.ALIAS,
            "'": TokenType.QUOTE,
            '"': TokenType.QUOTE,
            "`": TokenType.IDENTIFIER,
            "#": TokenType.COMMENT,
        }

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
        }

    class Parser(parser.Parser):
        TRANSFORM_PARSERS = {
            "DERIVE": lambda self, query: self._parse_selection(query),
            "SELECT": lambda self, query: self._parse_selection(query, append=False),
        }

        def _parse_statement(self) -> t.Optional[exp.Expression]:
            expression = self._parse_expression()
            expression = expression if expression else self._parse_query()
            return expression

        def _parse_query(
            self,
        ) -> t.Optional[exp.Query]:
            from_ = self._parse_from()

            if not from_:
                return None

            query = exp.select("*").from_(from_, copy=False)

            while self._match_texts(self.TRANSFORM_PARSERS):
                query = self.TRANSFORM_PARSERS[self._prev.text.upper()](self, query)

            return query

        def _parse_selection(self, query: exp.Query, append: bool = True) -> exp.Query:
            if self._match(TokenType.L_BRACE):
                selects = self._parse_csv(self._parse_expression)

                if not self._match(TokenType.R_BRACE, expression=query):
                    self.raise_error("Expecting ]")
            else:
                expression = self._parse_expression()
                selects = [expression] if expression else []

            projections = {
                select.alias_or_name: select.this if isinstance(select, exp.Alias) else select
                for select in query.selects
            }

            selects = [
                select.transform(
                    lambda s: (projections[s.name].copy() if s.name in projections else s)
                    if isinstance(s, exp.Column)
                    else s,
                    copy=False,
                )
                for select in selects
            ]

            return query.select(*selects, append=append, copy=False)

        def _parse_expression(self) -> t.Optional[exp.Expression]:
            if self._next and self._next.token_type == TokenType.ALIAS:
                alias = self._parse_id_var(True)
                self._match(TokenType.ALIAS)
                return self.expression(exp.Alias, this=self._parse_conjunction(), alias=alias)
            return self._parse_conjunction()

        def _parse_table(
            self,
            schema: bool = False,
            joins: bool = False,
            alias_tokens: t.Optional[t.Collection[TokenType]] = None,
            parse_bracket: bool = False,
            is_db_reference: bool = False,
        ) -> t.Optional[exp.Expression]:
            return self._parse_table_parts()

        def _parse_from(
            self, joins: bool = False, skip_from_token: bool = False
        ) -> t.Optional[exp.From]:
            if not skip_from_token and not self._match(TokenType.FROM):
                return None

            return self.expression(
                exp.From, comments=self._prev_comments, this=self._parse_table(joins=joins)
            )

    class Generator(generator.Generator):
        pass
