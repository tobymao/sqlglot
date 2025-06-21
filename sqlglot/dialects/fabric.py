from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.dialects.tsql import TSQL


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

    class Generator(TSQL.Generator):
        # Fabric-specific type mappings - override T-SQL types that aren't supported
        # Reference: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
        TYPE_MAPPING = {
            **TSQL.Generator.TYPE_MAPPING,
            # Fabric doesn't support these types, map to alternatives
            exp.DataType.Type.MONEY: "DECIMAL",
            exp.DataType.Type.SMALLMONEY: "DECIMAL",
            exp.DataType.Type.DATETIME: "DATETIME2(6)",
            exp.DataType.Type.SMALLDATETIME: "DATETIME2(6)",
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.TEXT: "VARCHAR(MAX)",
            exp.DataType.Type.IMAGE: "VARBINARY",
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.UTINYINT: "SMALLINT",  # T-SQL parses TINYINT as UTINYINT
            exp.DataType.Type.JSON: "VARCHAR",
            exp.DataType.Type.XML: "VARCHAR",
            exp.DataType.Type.UUID: "VARBINARY(MAX)",  # UNIQUEIDENTIFIER has limitations in Fabric
            # Override T-SQL mappings that use different names in Fabric
            exp.DataType.Type.DECIMAL: "DECIMAL",  # T-SQL uses NUMERIC
            exp.DataType.Type.DOUBLE: "FLOAT",
            exp.DataType.Type.INT: "INT",  # T-SQL uses INTEGER
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Override datatype generation to handle Fabric-specific precision limitations.

            Fabric limits temporal types (TIME, DATETIME2, DATETIMEOFFSET) to max 6 digits precision.
            When no precision is specified, we default to 6 digits.
            """
            if expression.is_type(
                exp.DataType.Type.TIME,
                exp.DataType.Type.DATETIME2,
                exp.DataType.Type.TIMESTAMPTZ,  # DATETIMEOFFSET in Fabric
            ):
                # Get the current precision (first expression if it exists)
                precision = expression.find(exp.DataTypeParam)

                # Determine the target precision
                if precision is None:
                    # No precision specified, default to 6
                    target_precision = 6
                elif precision.this.is_int:
                    # Cap precision at 6
                    current_precision = precision.this.to_py()
                    target_precision = min(current_precision, 6)

                # Create a new expression with the target precision
                new_expression = exp.DataType(
                    this=expression.this,
                    expressions=[exp.DataTypeParam(this=exp.Literal.number(target_precision))],
                )

                return super().datatype_sql(new_expression)

            return super().datatype_sql(expression)
