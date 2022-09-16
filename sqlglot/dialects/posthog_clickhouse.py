from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, inline_array_sql
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class PosthogClickHouse(Dialect):
    normalize_functions = None
    null_ordering = "nulls_are_last"

    class Tokenizer(Tokenizer):
        IDENTIFIERS = ['"', "`"]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "NULLABLE": TokenType.NULLABLE,
            "FINAL": TokenType.FINAL,
            "INT8": TokenType.TINYINT,
            "INT16": TokenType.SMALLINT,
            "INT32": TokenType.INT,
            "INT64": TokenType.BIGINT,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
        }

    class Parser(Parser):

        FUNCTIONS = {"PROPERTY": exp.PosthogProperty.from_arg_list}

        def _parse_table(self, schema=False):
            this = super()._parse_table(schema)

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

        # def _parse_where(self):
        #     this = super()._parse_where()
        #     if this:
        #         return and_(this, condition("team_id = 2"))

        #     return self.expression(exp.Where, this=condition("team_id = 2"))

    class Generator(Generator):
        STRUCT_DELIMITER = ("(", ")")

        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.NULLABLE: "Nullable",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.Array: inline_array_sql,
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
            exp.PosthogProperty: lambda self, e: f"JSONExtractRaw(properties, {self.sql(e, 'this')})",
        }
