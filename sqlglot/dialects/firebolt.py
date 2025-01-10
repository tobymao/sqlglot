import typing as t

from sqlglot import exp, generator, parser
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType


class Firebolt(Dialect):
    class Parser(parser.Parser):
        UNARY_PARSERS = {
            **parser.Parser.UNARY_PARSERS,
            TokenType.NOT: lambda self: self.expression(exp.Not, this=self._parse_unary()),
        }

        def _negate_range(
            self, this: t.Optional[exp.Expression] = None
        ) -> t.Optional[exp.Expression]:
            if not this:
                return this

            return self.expression(exp.Not, this=self.expression(exp.Paren, this=this))

    class Generator(generator.Generator):
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.VARBINARY: "BYTEA",
        }

        def not_sql(self, expression: exp.Not) -> str:
            # Firebolt requires negated to be wrapped in parentheses, since NOT has higher
            # precedence than IN, IS, BETWEEN, etc.
            if isinstance(expression.this, exp.In):
                return f"NOT ({self.sql(expression, 'this')})"

            return super().not_sql(expression)
