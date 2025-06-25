from __future__ import annotations
from sqlglot import exp, generator
from sqlglot.dialects.dialect import Dialect


class Exasol(Dialect):
    class Generator(generator.Generator):
        # https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm#StringDataType
        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "VARCHAR",
            exp.DataType.Type.LONGBLOB: "VARCHAR",
            exp.DataType.Type.LONGTEXT: "VARCHAR",
            exp.DataType.Type.MEDIUMBLOB: "VARCHAR",
            exp.DataType.Type.MEDIUMTEXT: "VARCHAR",
            exp.DataType.Type.TINYBLOB: "VARCHAR",
            exp.DataType.Type.TINYTEXT: "VARCHAR",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.VARBINARY: "VARCHAR",
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            # https://docs.exasol.com/db/latest/sql_references/data_types/datatypealiases.htm
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.MEDIUMINT: "INT",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP WITH LOCAL TIME ZONE",
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Override datatype generation to align with Exasol's support for temporal types.

            Exasol supports a fixed default precision for TIMESTAMP WITH LOCAL TIME ZONE and does not allow specifying a custom precision.
            This method strips any precision argument from TIMESTAMPLTZ types to ensure compatibility with Exasol's syntax.
            """
            if expression.is_type(exp.DataType.Type.TIMESTAMPLTZ):
                return super().datatype_sql(
                    exp.DataType(
                        this=expression.this,
                        expressions=[],
                    )
                )

            return super().datatype_sql(expression)
