from __future__ import annotations

"""
Netezza dialect support.

This initial implementation inherits from Postgres, which Netezza is largely
compatible with. We can extend Tokenizer/Parser/Generator as Netezza-specific
behavior is identified.
"""

from sqlglot import exp
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.parser import build_coalesce
from sqlglot.tokens import TokenType
from sqlglot.dialects.postgres import Postgres


class Netezza(Postgres):
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    class Tokenizer(Postgres.Tokenizer):
        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "BYTEINT": TokenType.TINYINT,
        }
    class Parser(Postgres.Parser):
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,
            "NVL": lambda args: build_coalesce(args, is_nvl=True),
        }
        PROPERTY_PARSERS = {
            **Postgres.Parser.PROPERTY_PARSERS,
            "DISTRIBUTE": lambda self: self._parse_netezza_distribute_on(),
            "ORGANIZE": lambda self: self._parse_netezza_organize_on(),
            "USING": lambda self: self._parse_netezza_external_using(),
        }

        def _parse_netezza_distribute_on(self) -> exp.DistributedByProperty:
            self._match_text_seq("ON")

            if self._match_text_seq("RANDOM"):
                return self.expression(exp.DistributedByProperty, kind="RANDOM")

            expressions = self._parse_wrapped_id_vars()
            return self.expression(exp.DistributedByProperty, expressions=expressions, kind="HASH")

        def _parse_netezza_organize_on(self) -> exp.Expression:
            self._match_text_seq("ON")
            expressions = self._parse_wrapped_id_vars()
            return self.expression(exp.OrganizeOnProperty, expressions=expressions)

        def _parse_netezza_external_using(self) -> exp.Expression:
            # Parse: USING (key value[, key = value ...])
            self._match(TokenType.L_PAREN)

            props: list[exp.Expression] = []
            while self._curr and not self._match(TokenType.R_PAREN, advance=False):
                # key can be an identifier/var or dotted identifier
                key = self._parse_column() or self._parse_var(any_token=True)
                if isinstance(key, exp.Column):
                    key = key.to_dot() if len(key.parts) > 1 else exp.var(key.name)
                if not key:
                    break

                # Optional '=' between key and value
                self._match(TokenType.EQ)
                self._match(TokenType.ALIAS)

                # Value can be string/number/identifier
                value = (
                    self._parse_string()
                    or self._parse_number()
                    or self._parse_unquoted_field()
                    or self._parse_var(any_token=True)
                )

                props.append(self.expression(exp.Property, this=key, value=value))
                if not self._match(TokenType.COMMA):
                    break

            self._match(TokenType.R_PAREN)
            return self.expression(exp.UsingOptionsProperty, expressions=props)

    class Generator(Postgres.Generator):
        SUPPORTS_DECODE_CASE = True
        NVL2_SUPPORTED = False

        PROPERTIES_LOCATION = {
            **Postgres.Generator.PROPERTIES_LOCATION,
            exp.OrganizeOnProperty: exp.Properties.Location.POST_SCHEMA,
            exp.UsingOptionsProperty: exp.Properties.Location.POST_SCHEMA,
        }

        def distributedbyproperty_sql(self, expression: exp.DistributedByProperty) -> str:
            kind = (self.sql(expression, "kind") or "").upper()
            if kind == "RANDOM":
                return "DISTRIBUTE ON RANDOM"

            expressions = self.expressions(expression, flat=True)
            return f"DISTRIBUTE ON ({expressions})" if expressions else ""

        def organizeonproperty_sql(self, expression: exp.OrganizeOnProperty) -> str:
            expressions = self.expressions(expression, flat=True)
            return f"ORGANIZE ON ({expressions})"

        def usingoptionsproperty_sql(self, expression: exp.UsingOptionsProperty) -> str:
            parts = []
            for p in expression.expressions or []:
                if isinstance(p, exp.Property):
                    name = self.property_name(p)
                    value = self.sql(p, "value")
                    parts.append(f"{name} {value}")
                else:
                    parts.append(self.sql(p))
            inner = ", ".join(parts)
            return f"USING ({inner})"

        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "BYTEINT",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.TIMETZ: "TIME",
            exp.DataType.Type.JSON: "VARCHAR",
        }

        def coalesce_sql(self, expression: exp.Coalesce) -> str:
            func_name = "NVL" if expression.args.get("is_nvl") else "COALESCE"
            return self.func(func_name, expression.this, *expression.expressions)

        def decodecase_sql(self, expression: exp.DecodeCase) -> str:
            # Netezza supports DECODE with multiple search/result pairs
            return self.func("DECODE", *expression.expressions)

        def index_sql(self, expression: exp.Index) -> str:
            # Netezza does not support indexes; suppress emission
            self.unsupported("Indexes are not supported in Netezza")
            return ""

        def create_sql(self, expression: exp.Create) -> str:
            # Intercept INDEX and avoid emitting unsupported DDL
            kind = expression.kind
            if kind == "INDEX":
                self.unsupported("CREATE INDEX is not supported in Netezza")
                return ""
            return super().create_sql(expression)
