from __future__ import annotations


from sqlglot import exp
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokens import TokenType


class RisingWaveParser(PostgresParser):
    WRAPPED_TRANSFORM_COLUMN_CONSTRAINT = False

    PROPERTY_PARSERS = {
        **PostgresParser.PROPERTY_PARSERS,
        "ENCODE": lambda self: self._parse_encode_property(),
        "INCLUDE": lambda self: self._parse_include_property(),
        "KEY": lambda self: self._parse_encode_property(key=True),
    }

    CONSTRAINT_PARSERS = {
        **PostgresParser.CONSTRAINT_PARSERS,
        "WATERMARK": lambda self: self.expression(
            exp.WatermarkColumnConstraint(
                this=self._match(TokenType.FOR) and self._parse_column(),
                expression=self._match(TokenType.ALIAS) and self._parse_disjunction(),
            )
        ),
    }

    SCHEMA_UNNAMED_CONSTRAINTS = {
        *PostgresParser.SCHEMA_UNNAMED_CONSTRAINTS,
        "WATERMARK",
    }

    def _parse_table_hints(self) -> list[exp.Expr] | None:
        # There is no hint in risingwave.
        # Do nothing here to avoid WITH keywords conflict in CREATE SINK statement.
        return None

    def _parse_include_property(self) -> exp.Expr | None:
        header: exp.Expr | None = None
        coldef: exp.Expr | None = None

        this = self._parse_var_or_string()

        if not self._match(TokenType.ALIAS):
            header = self._parse_field()
            if header:
                coldef = self.expression(exp.ColumnDef(this=header, kind=self._parse_types()))

        self._match(TokenType.ALIAS)
        alias = self._parse_id_var(tokens=self.ALIAS_TOKENS)

        return self.expression(exp.IncludeProperty(this=this, alias=alias, column_def=coldef))

    def _parse_encode_property(self, key: bool | None = None) -> exp.EncodeProperty:
        self._match_text_seq("ENCODE")
        this = self._parse_var_or_string()

        if self._match(TokenType.L_PAREN, advance=False):
            properties = self.expression(
                exp.Properties(expressions=self._parse_wrapped_properties())
            )
        else:
            properties = None

        return self.expression(exp.EncodeProperty(this=this, properties=properties, key=key))
