from sqlglot import exp, generator
from sqlglot.dialects.dialect import Dialect


class Druid(Dialect):
    class Generator(generator.Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.INT: "LONG",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.ARRAY: "ARRAY",
        }
