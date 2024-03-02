from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect


class PRQL(Dialect):
    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ["`"]

    class Parser(parser.Parser):
        def _parse_statement(self) -> t.Optional[exp.Expression]:
            expression = self._parse_expression()
            expression = expression if expression else self._parse_select()
            return expression

        def _parse_select(
            self,
            nested: bool = False,
            table: bool = False,
            parse_subquery_alias: bool = True,
            parse_set_operation: bool = True,
        ) -> t.Optional[exp.Expression]:
            this: t.Optional[exp.Expression] = self._parse_from()

            if this:
                this = exp.select("*").from_(this, copy=False)

            return this

    class Generator(generator.Generator):
        pass
