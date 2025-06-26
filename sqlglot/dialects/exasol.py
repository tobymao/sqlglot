from __future__ import annotations
from sqlglot import exp, generator
from sqlglot.dialects.dialect import Dialect, rename_func


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

        # https://docs.exasol.com/db/latest/sql_references/data_types/datatypealiases.htm
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.MEDIUMINT: "INT",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Exasol supports a fixed default precision of 3 for TIMESTAMP WITH LOCAL TIME ZONE
            # and does not allow specifying a different custom precision
            if expression.is_type(exp.DataType.Type.TIMESTAMPLTZ):
                return "TIMESTAMP WITH LOCAL TIME ZONE"

            return super().datatype_sql(expression)

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/mod.htm
            exp.Mod: rename_func("MOD"),
        }
