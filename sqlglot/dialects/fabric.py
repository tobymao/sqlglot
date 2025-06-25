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
            "TIMESTAMP": TokenType.TIMESTAMP,  # Override T-SQL's mapping of TIMESTAMP to ROWVERSION
            "UTINYINT": TokenType.UTINYINT,  # Add UTINYINT keyword that T-SQL is missing
        }

    class Generator(TSQL.Generator):
        # Fabric-specific type mappings - override T-SQL types that aren't supported
        # Reference: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
        TYPE_MAPPING = {
            exp.DataType.Type.BOOLEAN: "BIT",
            exp.DataType.Type.DATETIME: "DATETIME2(6)",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.DOUBLE: "FLOAT",
            exp.DataType.Type.IMAGE: "VARBINARY",
            exp.DataType.Type.INT: "INT",
            exp.DataType.Type.JSON: "VARCHAR",
            exp.DataType.Type.MONEY: "DECIMAL",
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.ROWVERSION: "ROWVERSION",
            exp.DataType.Type.SMALLDATETIME: "DATETIME2(6)",
            exp.DataType.Type.SMALLMONEY: "DECIMAL",
            exp.DataType.Type.TEXT: "VARCHAR(MAX)",
            exp.DataType.Type.TIMESTAMP: "DATETIME2(6)",
            exp.DataType.Type.TIMESTAMPNTZ: "DATETIME2(6)",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIMEOFFSET(6)",
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.UTINYINT: "SMALLINT",
            exp.DataType.Type.UUID: "VARBINARY(MAX)",
            exp.DataType.Type.VARIANT: "SQL_VARIANT",
            exp.DataType.Type.XML: "VARCHAR",
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Override datatype generation to handle Fabric-specific precision limitations.

            Fabric limits temporal types (TIME, DATETIME2, DATETIMEOFFSET) to max 6 digits precision.
            When no precision is specified, we default to 6 digits.
            """
            # First apply the parent type mapping to get the correct base type
            result = super().datatype_sql(expression)

            # Check if this is a temporal type that needs precision handling
            if expression.is_type(
                exp.DataType.Type.TIME,
                exp.DataType.Type.DATETIME2,
                exp.DataType.Type.TIMESTAMPTZ,  # Maps to DATETIMEOFFSET in Fabric
                exp.DataType.Type.TIMESTAMP,  # Maps to DATETIME2 in Fabric
                exp.DataType.Type.TIMESTAMPNTZ,  # Maps to DATETIME2 in Fabric
                exp.DataType.Type.DATETIME,  # Maps to DATETIME2 in Fabric
                exp.DataType.Type.SMALLDATETIME,  # Maps to DATETIME2 in Fabric
            ):
                # Get the current precision (first expression if it exists)
                precision_param = expression.find(exp.DataTypeParam)
                target_precision = 6  # Default precision

                if precision_param and precision_param.this.is_int:
                    # Cap precision at 6
                    current_precision = precision_param.this.to_py()
                    target_precision = min(current_precision, 6)

                # Extract the base type from the result and add precision
                if "(" in result:
                    # Type already has parameters, extract just the type name
                    base_type = result.split("(")[0]
                else:
                    base_type = result

                return f"{base_type}({target_precision})"

            return result
