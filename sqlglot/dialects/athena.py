from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.trino import Trino
from sqlglot.tokens import TokenType


class Athena(Trino):
    class Tokenizer(Trino.Tokenizer):
        IDENTIFIERS = ['"', "`"]
        KEYWORDS = {
            **Trino.Tokenizer.KEYWORDS,
            "UNLOAD": TokenType.COMMAND,
        }

    class Parser(Trino.Parser):
        STATEMENT_PARSERS = {
            **Trino.Parser.STATEMENT_PARSERS,
            TokenType.USING: lambda self: self._parse_as_command(self._prev),
        }

    class Generator(Trino.Generator):
        WITH_PROPERTIES_PREFIX = "TBLPROPERTIES"

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

        def generate(self, expression: exp.Expression, copy: bool = True) -> str:
            if isinstance(expression, exp.DDL) or isinstance(expression, exp.Drop):
                # Athena DDL uses backticks for quoting, unlike Athena DML which uses double quotes
                # ...unless the DDL is CREATE VIEW, then it uses DML quoting, I guess because the view is based on a SELECT query
                # ref: https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html
                # ref: https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html#table-names-that-include-numbers
                if not (isinstance(expression, exp.Create) and expression.kind == "VIEW"):
                    self._identifier_start = "`"
                    self._identifier_end = "`"

            try:
                return super().generate(expression, copy)
            finally:
                self._identifier_start = self.dialect.IDENTIFIER_START
                self._identifier_end = self.dialect.IDENTIFIER_END

        def property_sql(self, expression: exp.Property) -> str:
            return (
                f"{self.property_name(expression, string_key=True)}={self.sql(expression, 'value')}"
            )
