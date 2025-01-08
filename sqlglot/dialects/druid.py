from sqlglot import exp, generator
from sqlglot.dialects.dialect import Dialect


class Druid(Dialect):
    class Generator(generator.Generator):
        # https://druid.apache.org/docs/latest/querying/sql-data-types/
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.CHAR: "STRING",
            exp.DataType.Type.NCHAR: "STRING",
            exp.DataType.Type.NVARCHAR: "STRING",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.VARCHAR: "STRING",
            exp.DataType.Type.UUID: "STRING",
            exp.DataType.Type.DECIMAL: "DOUBLE",
            exp.DataType.Type.BOOLEAN: "LONG",
            exp.DataType.Type.TINYINT: "LONG",
            exp.DataType.Type.SMALLINT: "LONG",
            exp.DataType.Type.INT: "LONG",
            exp.DataType.Type.BIGINT: "LONG",
        }
