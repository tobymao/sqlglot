from sqlglot import exp
from sqlglot.dialects.mysql import MySQL


class StarRocks(MySQL):
    class Generator(MySQL.Generator):
        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
        }
