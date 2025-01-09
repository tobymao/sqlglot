from sqlglot import exp, generator
from sqlglot.dialects.dialect import Dialect


class Druid(Dialect):
    class Generator(generator.Generator):
        # https://druid.apache.org/docs/latest/querying/sql-data-types/
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.NCHAR: "STRING",
            exp.DataType.Type.NVARCHAR: "STRING",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.UUID: "STRING",
        }
