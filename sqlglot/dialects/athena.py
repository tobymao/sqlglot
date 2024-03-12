from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.trino import Trino
from sqlglot.tokens import TokenType


class Athena(Trino):
    class Parser(Trino.Parser):
        STATEMENT_PARSERS = {
            **Trino.Parser.STATEMENT_PARSERS,
            TokenType.USING: lambda self: self._parse_as_command(self._prev),
        }

    class Generator(Trino.Generator):
        PROPERTIES_LOCATION = {
            **Trino.Generator.PROPERTIES_LOCATION,
            exp.LocationProperty: exp.Properties.Location.POST_SCHEMA,
        }

        TYPE_MAPPING = {
            **Trino.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
        }

        TRANSFORMS = {
            **Trino.Generator.TRANSFORMS,
            exp.FileFormatProperty: lambda self, e: f"'FORMAT'={self.sql(e, 'this')}",
        }

        def property_sql(self, expression: exp.Property) -> str:
            return (
                f"{self.property_name(expression, string_key=True)}={self.sql(expression, 'value')}"
            )

        def with_properties(self, properties: exp.Properties) -> str:
            return self.properties(properties, prefix=self.seg("TBLPROPERTIES"))
