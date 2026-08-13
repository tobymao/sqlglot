from __future__ import annotations

from sqlglot import exp
from sqlglot.generators.tsql import TSQLGenerator


class SynapseGenerator(TSQLGenerator):
    PROPERTIES_LOCATION = {
        **TSQLGenerator.PROPERTIES_LOCATION,
        exp.LocationProperty: exp.Properties.Location.POST_WITH,
    }

    TRANSFORMS = {
        **TSQLGenerator.TRANSFORMS,
        exp.LocationProperty: lambda self, expression: (
            f"LOCATION={self.sql(expression, 'this')}"
        ),
        exp.OpenRowset: lambda self, expression: self.openrowset_sql(expression),
        exp.TryParse: lambda self, expression: self.tryparse_sql(expression),
    }

    def formatoptionsproperty_sql(self, expression: exp.FormatOptionsProperty) -> str:
        equals = " =" if expression.args.get("equals") else ""
        return f"FORMAT_OPTIONS{equals} {self.wrap(self.expressions(expression))}"

    def openrowset_sql(self, expression: exp.OpenRowset) -> str:
        bulk = self.expressions(
            expression, key="bulk", flat=not expression.args.get("bulk_parenthesized")
        )
        if expression.args.get("bulk_parenthesized"):
            bulk = self.wrap(bulk)

        properties = self.expressions(expression, key="properties", sep=", ", flat=True)
        if properties:
            properties = f", {properties}"

        sql = f"OPENROWSET(BULK {bulk}{properties})"
        schema = expression.args.get("schema")
        if schema:
            sql = f"{sql} WITH {self.sql(schema)}"
        return sql

    def tryparse_sql(self, expression: exp.TryParse) -> str:
        sql = f"TRY_PARSE({self.sql(expression, 'this')} AS {self.sql(expression, 'to')}"
        culture = expression.args.get("culture")
        if culture:
            sql = f"{sql} USING {self.sql(culture)}"
        return f"{sql})"
