from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    rename_func,
    no_tablesample_sql,
    no_trycast_sql,
)
from sqlglot.generator import Generator


class SQLite(Dialect):
    class Generator(Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.BOOLEAN: "INTEGER",
            exp.DataType.Type.TINYINT: "INTEGER",
            exp.DataType.Type.SMALLINT: "INTEGER",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.BIGINT: "INTEGER",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.DOUBLE: "REAL",
            exp.DataType.Type.DECIMAL: "REAL",
            exp.DataType.Type.CHAR: "TEXT",
            exp.DataType.Type.VARCHAR: "TEXT",
            exp.DataType.Type.BINARY: "BLOB",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.Levenshtein: rename_func("EDITDIST3"),
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
        }
