from sqlglot import exp, generator
from sqlglot.dialects.dialect import rename_func, Dialect
from sqlglot.parsers.druid import DruidParser


class Druid(Dialect):
    Parser = DruidParser

    class Generator(generator.Generator):
        # https://druid.apache.org/docs/latest/querying/sql-data-types/
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DType.NCHAR: "STRING",
            exp.DType.NVARCHAR: "STRING",
            exp.DType.TEXT: "STRING",
            exp.DType.UUID: "STRING",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.Mod: rename_func("MOD"),
            exp.Array: lambda self, e: f"ARRAY[{self.expressions(e)}]",
        }
