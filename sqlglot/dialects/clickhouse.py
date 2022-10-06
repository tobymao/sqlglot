from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, inline_array_sql, var_map_sql
from sqlglot.generator import Generator
from sqlglot.helper import csv
from sqlglot.parser import Parser, parse_var_map
from sqlglot.tokens import Tokenizer, TokenType


def _lower_func(sql):
    index = sql.index("(")
    return sql[:index].lower() + sql[index:]


class ClickHouse(Dialect):
    normalize_functions = None
    null_ordering = "nulls_are_last"

    class Tokenizer(Tokenizer):
        IDENTIFIERS = ['"', "`"]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "NULLABLE": TokenType.NULLABLE,
            "FINAL": TokenType.FINAL,
            "DATETIME64": TokenType.DATETIME,
            "INT8": TokenType.TINYINT,
            "INT16": TokenType.SMALLINT,
            "INT32": TokenType.INT,
            "INT64": TokenType.BIGINT,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
        }

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "MAP": parse_var_map,
        }

        def _parse_table(self, schema=False):
            this = super()._parse_table(schema)

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

    class Generator(Generator):
        STRUCT_DELIMITER = ("(", ")")

        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.NULLABLE: "Nullable",
            exp.DataType.Type.DATETIME: "DateTime64",
            exp.DataType.Type.MAP: "Map",
            exp.DataType.Type.ARRAY: "Array",
            exp.DataType.Type.STRUCT: "Tuple",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.Array: inline_array_sql,
            exp.StrPosition: lambda self, e: f"position({csv(self.sql(e, 'this'), self.sql(e, 'substr'), self.sql(e, 'position'))})",
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
            exp.Map: lambda self, e: _lower_func(var_map_sql(self, e)),
            exp.VarMap: lambda self, e: _lower_func(var_map_sql(self, e)),
        }

        EXPLICIT_UNION = True
