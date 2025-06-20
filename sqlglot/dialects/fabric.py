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
            exp.DataType.Type.DATETIME: "DATETIME2",
            exp.DataType.Type.SMALLDATETIME: "DATETIME2",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME2",
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.TEXT: "VARCHAR",
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
