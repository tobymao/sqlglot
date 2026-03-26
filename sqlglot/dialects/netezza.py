from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.postgres import PostgresGenerator
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokens import TokenType


class Netezza(Postgres):
    class Parser(PostgresParser):
        PROPERTY_PARSERS = {
            **PostgresParser.PROPERTY_PARSERS,
            "DISTRIBUTE": lambda self: self._parse_distribute_on(),
            "ORGANIZE": lambda self: self._parse_organize_on(),
        }

        def _parse_distribute_on(self) -> exp.DistributeOnProperty:
            self._match(TokenType.ON)

            if self._match_text_seq("RANDOM"):
                return self.expression(exp.DistributeOnProperty(this=exp.var("RANDOM")))

            is_hash = self._match_text_seq("HASH")
            return self.expression(
                exp.DistributeOnProperty(
                    this=exp.var("HASH") if is_hash else None,
                    expressions=self._parse_wrapped_csv(self._parse_id_var),
                )
            )

        def _parse_organize_on(self) -> exp.OrganizeOnProperty:
            self._match(TokenType.ON)

            if self._match_text_seq("NONE"):
                return self.expression(exp.OrganizeOnProperty(none=True))

            return self.expression(
                exp.OrganizeOnProperty(expressions=self._parse_wrapped_csv(self._parse_id_var))
            )

    class Generator(PostgresGenerator):
        PROPERTIES_LOCATION = {
            **PostgresGenerator.PROPERTIES_LOCATION,
            exp.DistributeOnProperty: exp.Properties.Location.POST_SCHEMA,
            exp.OrganizeOnProperty: exp.Properties.Location.POST_SCHEMA,
        }

        def distributeonproperty_sql(self, expression: exp.DistributeOnProperty) -> str:
            kind = expression.args.get("this")
            if kind and kind.name.upper() == "RANDOM":
                return "DISTRIBUTE ON RANDOM"

            expressions = self.expressions(expression, flat=True)
            if kind and kind.name.upper() == "HASH":
                return f"DISTRIBUTE ON HASH({expressions})"

            return f"DISTRIBUTE ON ({expressions})"

        def organizeonproperty_sql(self, expression: exp.OrganizeOnProperty) -> str:
            if expression.args.get("none"):
                return "ORGANIZE ON NONE"

            return f"ORGANIZE ON ({self.expressions(expression, flat=True)})"
