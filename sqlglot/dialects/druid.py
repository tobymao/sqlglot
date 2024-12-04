from sqlglot import exp, generator, parser
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType


class TemporalFloor(exp.Func):
    """
    Druid-specific FLOOR function with a temporal unit.

    Example: FLOOR(my_date TO DAY)
    """

    arg_types = {"unit": True, "this": True}

    @property
    def unit(self) -> exp.Expression:
        return self.args["unit"]


class TemporalCeil(exp.Func):
    """
    Druid-specific CEIL function with a temporal unit.

    Example: CEIL(my_date TO DAY)
    """

    arg_types = {"unit": True, "this": True}

    @property
    def unit(self) -> exp.Expression:
        return self.args["unit"]


class Druid(Dialect):
    class Parser(parser.Parser):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            self.FUNCTIONS["FLOOR"] = self._parse_temporal_floor
            self.FUNCTIONS["CEIL"] = self._parse_temporal_ceil

        def _parse_temporal_floor(self, args, dialect):
            """
            Custom handler for FLOOR with 'TO' syntax.

            See https://druid.apache.org/docs/latest/querying/sql-functions#floor-date-and-time.
            """
            if len(args) < 1:
                self.raise_error("FLOOR requires at least one argument")

            this = args[0]
            decimals = args[1] if len(args) > 1 else None
            unit = None

            # Check for the 'TO' token and handle it
            if (
                self._curr
                and self._curr.token_type == TokenType.VAR
                and self._curr.text.upper() == "TO"
            ):
                self._advance()  # Consume the 'TO' token
                unit = self._parse_expression()
                return TemporalFloor(this=this, unit=unit)

            return exp.Floor(this=this, decimals=decimals)

        def _parse_temporal_ceil(self, args, dialect):
            """
            Custom handler for CEIL with 'TO' syntax.

            See https://druid.apache.org/docs/latest/querying/sql-functions#ceil-date-and-time.
            """
            if len(args) < 1:
                self.raise_error("CEIL requires at least one argument")

            this = args[0]
            decimals = args[1] if len(args) > 1 else None
            unit = None

            # Check for the 'TO' token and handle it
            if (
                self._curr
                and self._curr.token_type == TokenType.VAR
                and self._curr.text.upper() == "TO"
            ):
                self._advance()  # Consume the 'TO' token
                unit = self._parse_expression()
                return TemporalCeil(this=this, unit=unit)

            return exp.Ceil(this=this, decimals=decimals)

    class Generator(generator.Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.INT: "LONG",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.ARRAY: "ARRAY",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            TemporalFloor: lambda self, e: f"FLOOR({e.this} TO {e.unit})",
            TemporalCeil: lambda self, e: f"CEIL({e.this} TO {e.unit})",
        }
