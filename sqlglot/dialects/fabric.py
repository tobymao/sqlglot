from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.dialects.tsql import TSQL
from sqlglot.tokens import TokenType


class Fabric(TSQL):
    """
    Microsoft Fabric Data Warehouse dialect that inherits from T-SQL.

    Microsoft Fabric is a cloud-based analytics platform that provides a unified
    data warehouse experience. While it shares much of T-SQL's syntax, it has
    specific differences and limitations that this dialect addresses.

    Key differences from T-SQL:
    - Case-sensitive identifiers (unlike T-SQL which is case-insensitive)
    - Limited data type support with mappings to supported alternatives
    - Temporal types (DATETIME2, DATETIMEOFFSET, TIME) limited to 6 digits precision
    - Certain legacy types (MONEY, SMALLMONEY, etc.) are not supported
    - Unicode types (NCHAR, NVARCHAR) are mapped to non-unicode equivalents

    References:
    - Data Types: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
    - T-SQL Surface Area: https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area
    """

    # Fabric is case-sensitive unlike T-SQL which is case-insensitive
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    class Tokenizer(TSQL.Tokenizer):
        # Override T-SQL tokenizer to handle TIMESTAMP differently
        # In T-SQL, TIMESTAMP is a synonym for ROWVERSION, but in Fabric we want it to be a datetime type
        # Also add UTINYINT keyword mapping since T-SQL doesn't have it
        KEYWORDS = {
            **TSQL.Tokenizer.KEYWORDS,
            "TIMESTAMP": TokenType.TIMESTAMP,
            "UTINYINT": TokenType.UTINYINT,
        }

    class Generator(TSQL.Generator):
        # Fabric-specific type mappings - override T-SQL types that aren't supported
        # Reference: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
        TYPE_MAPPING = {
            **TSQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.DATETIME: "DATETIME2",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.IMAGE: "VARBINARY",
            exp.DataType.Type.INT: "INT",
            exp.DataType.Type.JSON: "VARCHAR",
            exp.DataType.Type.MONEY: "DECIMAL",
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.ROWVERSION: "ROWVERSION",
            exp.DataType.Type.SMALLDATETIME: "DATETIME2",
            exp.DataType.Type.SMALLMONEY: "DECIMAL",
            exp.DataType.Type.TIMESTAMP: "DATETIME2",
            exp.DataType.Type.TIMESTAMPNTZ: "DATETIME2",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIMEOFFSET",
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.UTINYINT: "SMALLINT",
            exp.DataType.Type.UUID: "VARBINARY(MAX)",
            exp.DataType.Type.XML: "VARCHAR",
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Check if this is a temporal type that needs precision handling. Fabric limits temporal
            # types to max 6 digits precision. When no precision is specified, we default to 6 digits.
            if (
                expression.is_type(*exp.DataType.TEMPORAL_TYPES)
                and expression.this != exp.DataType.Type.DATE
            ):
                # Get the current precision (first expression if it exists)
                precision_param = expression.find(exp.DataTypeParam)
                target_precision = 6

                if precision_param and precision_param.this.is_int:
                    # Cap precision at 6
                    current_precision = precision_param.this.to_py()
                    target_precision = min(current_precision, 6)
                else:
                    # If precision exists but is not an integer, default to 6
                    target_precision = 6

                # Create a new expression with the target precision
                expression = exp.DataType(
                    this=expression.this,
                    expressions=[exp.DataTypeParam(this=exp.Literal.number(target_precision))],
                )

            return super().datatype_sql(expression)

        def unixtotime_sql(self, expression: exp.UnixToTime) -> str:
            scale = expression.args.get("scale")
            timestamp = expression.this

            if scale not in (None, exp.UnixToTime.SECONDS):
                self.unsupported(f"UnixToTime scale {scale} is not supported by Fabric")
                return ""

            # Convert unix timestamp (seconds) to microseconds and round to avoid decimals
            microseconds = timestamp * exp.Literal.number("1e6")
            rounded = exp.func("round", microseconds, 0)
            rounded_ms_as_bigint = exp.cast(rounded, exp.DataType.Type.BIGINT)

            # Create the base datetime as '1970-01-01' cast to DATETIME2(6)
            epoch_start = exp.cast("'1970-01-01'", "datetime2(6)", dialect="fabric")

            dateadd = exp.DateAdd(
                this=epoch_start,
                expression=rounded_ms_as_bigint,
                unit=exp.Literal.string("MICROSECONDS"),
            )
            return self.sql(dateadd)
