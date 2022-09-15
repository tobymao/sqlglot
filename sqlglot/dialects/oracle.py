from sqlglot import exp, transforms
from sqlglot.dialects.dialect import Dialect, no_ilike_sql
from sqlglot.generator import Generator
from sqlglot.helper import csv
from sqlglot.tokens import Tokenizer, TokenType


def _limit_sql(self, expression):
    return self.fetch_sql(exp.Fetch(direction="FIRST", count=expression.expression))


class Oracle(Dialect):
    class Generator(Generator):
        TYPE_MAPPING = {
            **Generator.TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "NUMBER",
            exp.DataType.Type.SMALLINT: "NUMBER",
            exp.DataType.Type.INT: "NUMBER",
            exp.DataType.Type.BIGINT: "NUMBER",
            exp.DataType.Type.DECIMAL: "NUMBER",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.VARCHAR: "VARCHAR2",
            exp.DataType.Type.NVARCHAR: "NVARCHAR2",
            exp.DataType.Type.TEXT: "CLOB",
            exp.DataType.Type.BINARY: "BLOB",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            **transforms.UNALIAS_GROUP,
            exp.ILike: no_ilike_sql,
            exp.Limit: _limit_sql,
        }

        def query_modifiers(self, expression, *sqls):
            return csv(
                *sqls,
                *[self.sql(sql) for sql in expression.args.get("laterals", [])],
                *[self.sql(sql) for sql in expression.args.get("joins", [])],
                self.sql(expression, "where"),
                self.sql(expression, "group"),
                self.sql(expression, "having"),
                self.sql(expression, "qualify"),
                self.sql(expression, "window"),
                self.sql(expression, "distribute"),
                self.sql(expression, "sort"),
                self.sql(expression, "cluster"),
                self.sql(expression, "order"),
                self.sql(expression, "offset"),  # offset before limit in oracle
                self.sql(expression, "limit"),
                sep="",
            )

        def alias_sql(self, expression):
            if isinstance(expression.this, exp.Table):
                to_sql = self.sql(expression, "alias")
                # oracle does not allow "AS" between table and alias
                to_sql = f" {to_sql}" if to_sql else ""
                return f"{self.sql(expression, 'this')}{to_sql}"
            return super().alias_sql(expression)

        def offset_sql(self, expression):
            return f"{super().offset_sql(expression)} ROWS"

    class Tokenizer(Tokenizer):
        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "TOP": TokenType.TOP,
            "VARCHAR2": TokenType.VARCHAR,
            "NVARCHAR2": TokenType.NVARCHAR,
        }
