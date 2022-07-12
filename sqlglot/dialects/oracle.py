from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator


class Oracle(Dialect):
    class Generator(Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.TINYINT: "NUMBER",
            exp.DataType.Type.SMALLINT: "NUMBER",
            exp.DataType.Type.INT: "NUMBER",
            exp.DataType.Type.BIGINT: "NUMBER",
            exp.DataType.Type.DECIMAL: "NUMBER",
            exp.DataType.Type.VARCHAR: "VARCHAR2",
        }
