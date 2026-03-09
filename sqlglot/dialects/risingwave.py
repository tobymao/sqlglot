from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generator import Generator
from sqlglot.parsers.risingwave import Parser as RisingWaveParser
from sqlglot.tokens import TokenType


class RisingWave(Postgres):
    REQUIRES_PARENTHESIZED_STRUCT_ACCESS = True
    SUPPORTS_STRUCT_STAR_EXPANSION = True

    class Tokenizer(Postgres.Tokenizer):
        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "SINK": TokenType.SINK,
            "SOURCE": TokenType.SOURCE,
        }

    Parser = RisingWaveParser

    class Generator(Postgres.Generator):
        LOCKING_READS_SUPPORTED = False
        SUPPORTS_BETWEEN_FLAGS = False

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.FileFormatProperty: lambda self, e: f"FORMAT {self.sql(e, 'this')}",
        }

        PROPERTIES_LOCATION = {
            **Postgres.Generator.PROPERTIES_LOCATION,
            exp.FileFormatProperty: exp.Properties.Location.POST_EXPRESSION,
        }

        EXPRESSION_PRECEDES_PROPERTIES_CREATABLES = {"SINK"}

        def computedcolumnconstraint_sql(self, expression: exp.ComputedColumnConstraint) -> str:
            return Generator.computedcolumnconstraint_sql(self, expression)

        def datatype_sql(self, expression: exp.DataType) -> str:
            if expression.is_type(exp.DType.MAP) and len(expression.expressions) == 2:
                key_type, value_type = expression.expressions
                return f"MAP({self.sql(key_type)}, {self.sql(value_type)})"

            return super().datatype_sql(expression)
