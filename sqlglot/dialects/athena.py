from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.trino import Trino
from sqlglot.dialects.hive import Hive
from sqlglot.tokens import TokenType


def _generate_as_hive(expression: exp.Expression) -> bool:
    if isinstance(expression, exp.Create):
        if expression.kind == "TABLE":
            properties: t.Optional[exp.Properties] = expression.args.get("properties")
            if properties and properties.find(exp.ExternalProperty):
                return True  # CREATE EXTERNAL TABLE is Hive

            if not isinstance(expression.expression, exp.Select):
                return True  # any CREATE TABLE other than CREATE TABLE AS SELECT is Hive
        else:
            return expression.kind != "VIEW"  # CREATE VIEW is never Hive but CREATE SCHEMA etc is

    elif isinstance(expression, exp.Alter) or isinstance(expression, exp.Drop):
        return True  # all ALTER and DROP statements are Hive

    return False


class Athena(Trino):
    """
    Over the years, it looks like AWS has taken various execution engines, bolted on AWS-specific modifications and then
    built the Athena service around them.

    Thus, Athena is not simply hosted Trino, it's more like a router that routes SQL queries to an execution engine depending
    on the query type.

    As at 2024-09-10, assuming your Athena workgroup is configured to use "Athena engine version 3", the following engines exist:

    Hive:
     - Accepts mostly the same syntax as Hadoop / Hive
     - Uses backticks to quote identifiers
     - Has a distinctive DDL syntax (around things like setting table properties, storage locations etc) that is different from Trino
     - Used for *most* DDL, with some exceptions that get routed to the Trino engine instead:
        - CREATE [EXTERNAL] TABLE (without AS SELECT)
        - ALTER
        - DROP

    Trino:
      - Uses double quotes to quote identifiers
      - Used for DDL operations that involve SELECT queries, eg:
        - CREATE VIEW
        - CREATE TABLE... AS SELECT
      - Used for DML operations
        - SELECT, INSERT, UPDATE, DELETE, MERGE

    The SQLGlot Athena dialect tries to identify which engine a query would be routed to and then uses the parser / generator for that engine
    rather than trying to create a universal syntax that can handle both types.
    """

    class Tokenizer(Trino.Tokenizer):
        """
        The Tokenizer is flexible enough to tokenize queries across both the Hive and Trino engines
        """

        IDENTIFIERS = ['"', "`"]
        KEYWORDS = {
            **Hive.Tokenizer.KEYWORDS,
            **Trino.Tokenizer.KEYWORDS,
            "UNLOAD": TokenType.COMMAND,
        }

    class Parser(Trino.Parser):
        """
        Parse queries for the Athena Trino execution engine
        """

        STATEMENT_PARSERS = {
            **Trino.Parser.STATEMENT_PARSERS,
            TokenType.USING: lambda self: self._parse_as_command(self._prev),
        }

    class Generator(Trino.Generator):
        """
        Generate queries for the Athena Trino execution engine
        """

        TYPE_MAPPING = {
            **Trino.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
        }

        TRANSFORMS = {
            **Trino.Generator.TRANSFORMS,
            exp.FileFormatProperty: lambda self, e: f"'FORMAT'={self.sql(e, 'this')}",
        }

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            hive_kwargs = {**kwargs, "dialect": "hive"}

            self._hive_generator = Hive.Generator(*args, **hive_kwargs)

        def generate(self, expression: exp.Expression, copy: bool = True) -> str:
            if _generate_as_hive(expression):
                return self._hive_generator.generate(expression, copy)

            return super().generate(expression, copy)
