from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects import Dialect, Hive, Trino
from sqlglot.tokens import TokenType, Token


class Athena(Dialect):
    """
    Over the years, it looks like AWS has taken various execution engines, bolted on AWS-specific
    modifications and then built the Athena service around them.

    Thus, Athena is not simply hosted Trino, it's more like a router that routes SQL queries to an
    execution engine depending on the query type.

    As at 2024-09-10, assuming your Athena workgroup is configured to use "Athena engine version 3",
    the following engines exist:

    Hive:
     - Accepts mostly the same syntax as Hadoop / Hive
     - Uses backticks to quote identifiers
     - Has a distinctive DDL syntax (around things like setting table properties, storage locations etc)
       that is different from Trino
     - Used for *most* DDL, with some exceptions that get routed to the Trino engine instead:
        - CREATE [EXTERNAL] TABLE (without AS SELECT)
        - ALTER
        - DROP

    Trino:
      - Uses double quotes to quote identifiers
      - Used for DDL operations that involve SELECT queries, eg:
        - CREATE VIEW / DROP VIEW
        - CREATE TABLE... AS SELECT
      - Used for DML operations
        - SELECT, INSERT, UPDATE, DELETE, MERGE

    The SQLGlot Athena dialect tries to identify which engine a query would be routed to and then uses the
    tokenizer / parser / generator for that engine. This is unfortunately necessary, as there are certain
    incompatibilities between the engines' dialects and thus can't be handled by a single, unifying dialect.

    References:
    - https://docs.aws.amazon.com/athena/latest/ug/ddl-reference.html
    - https://docs.aws.amazon.com/athena/latest/ug/dml-queries-functions-operators.html
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._hive = Hive(**kwargs)
        self._trino = Trino(**kwargs)

    def tokenize(self, sql: str, **opts) -> t.List[Token]:
        opts["hive"] = self._hive
        opts["trino"] = self._trino
        return super().tokenize(sql, **opts)

    def parse(self, sql: str, **opts) -> t.List[t.Optional[exp.Expression]]:
        opts["hive"] = self._hive
        opts["trino"] = self._trino
        return super().parse(sql, **opts)

    def parse_into(
        self, expression_type: exp.IntoType, sql: str, **opts
    ) -> t.List[t.Optional[exp.Expression]]:
        opts["hive"] = self._hive
        opts["trino"] = self._trino
        return super().parse_into(expression_type, sql, **opts)

    def generate(self, expression: exp.Expression, copy: bool = True, **opts) -> str:
        opts["hive"] = self._hive
        opts["trino"] = self._trino
        return super().generate(expression, copy=copy, **opts)

    # This Tokenizer consumes a combination of HiveQL and Trino SQL and then processes the tokens
    # to disambiguate which dialect needs to be actually used in order to tokenize correctly.
    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = Trino.Tokenizer.IDENTIFIERS + Hive.Tokenizer.IDENTIFIERS
        STRING_ESCAPES = Trino.Tokenizer.STRING_ESCAPES + Hive.Tokenizer.STRING_ESCAPES
        HEX_STRINGS = Trino.Tokenizer.HEX_STRINGS + Hive.Tokenizer.HEX_STRINGS
        UNICODE_STRINGS = Trino.Tokenizer.UNICODE_STRINGS + Hive.Tokenizer.UNICODE_STRINGS

        NUMERIC_LITERALS = {
            **Trino.Tokenizer.NUMERIC_LITERALS,
            **Hive.Tokenizer.NUMERIC_LITERALS,
        }

        KEYWORDS = {
            **Hive.Tokenizer.KEYWORDS,
            **Trino.Tokenizer.KEYWORDS,
            "UNLOAD": TokenType.COMMAND,
        }

        def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
            hive = kwargs.pop("hive", None) or Hive()
            trino = kwargs.pop("trino", None) or Trino()

            super().__init__(*args, **kwargs)

            self._hive_tokenizer = hive.tokenizer(*args, **{**kwargs, "dialect": hive})
            self._trino_tokenizer = _TrinoTokenizer(*args, **{**kwargs, "dialect": trino})

        def tokenize(self, sql: str) -> t.List[Token]:
            tokens = super().tokenize(sql)

            if _tokenize_as_hive(tokens):
                return [Token(TokenType.HIVE_TOKEN_STREAM, "")] + self._hive_tokenizer.tokenize(sql)

            return self._trino_tokenizer.tokenize(sql)

    class Parser(parser.Parser):
        def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
            hive = kwargs.pop("hive", None) or Hive()
            trino = kwargs.pop("trino", None) or Trino()

            super().__init__(*args, **kwargs)

            self._hive_parser = hive.parser(*args, **{**kwargs, "dialect": hive})
            self._trino_parser = _TrinoParser(*args, **{**kwargs, "dialect": trino})

        def parse(
            self, raw_tokens: t.List[Token], sql: t.Optional[str] = None
        ) -> t.List[t.Optional[exp.Expression]]:
            if raw_tokens and raw_tokens[0].token_type == TokenType.HIVE_TOKEN_STREAM:
                return self._hive_parser.parse(raw_tokens[1:], sql)

            return self._trino_parser.parse(raw_tokens, sql)

        def parse_into(
            self,
            expression_types: exp.IntoType,
            raw_tokens: t.List[Token],
            sql: t.Optional[str] = None,
        ) -> t.List[t.Optional[exp.Expression]]:
            if raw_tokens and raw_tokens[0].token_type == TokenType.HIVE_TOKEN_STREAM:
                return self._hive_parser.parse_into(expression_types, raw_tokens[1:], sql)

            return self._trino_parser.parse_into(expression_types, raw_tokens, sql)

    class Generator(generator.Generator):
        def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
            hive = kwargs.pop("hive", None) or Hive()
            trino = kwargs.pop("trino", None) or Trino()

            super().__init__(*args, **kwargs)

            self._hive_generator = _HiveGenerator(*args, **{**kwargs, "dialect": hive})
            self._trino_generator = _TrinoGenerator(*args, **{**kwargs, "dialect": trino})

        def generate(self, expression: exp.Expression, copy: bool = True) -> str:
            if _generate_as_hive(expression):
                generator = self._hive_generator
            else:
                generator = self._trino_generator

            return generator.generate(expression, copy=copy)


def _tokenize_as_hive(tokens: t.List[Token]) -> bool:
    if len(tokens) < 2:
        return False

    first, second, *rest = tokens

    first_type = first.token_type
    first_text = first.text.upper()
    second_type = second.token_type
    second_text = second.text.upper()

    if first_type in (TokenType.DESCRIBE, TokenType.SHOW) or first_text == "MSCK REPAIR":
        return True

    if first_type in (TokenType.ALTER, TokenType.CREATE, TokenType.DROP):
        if second_text in ("DATABASE", "EXTERNAL", "SCHEMA"):
            return True
        if second_type == TokenType.VIEW:
            return False

        return all(t.token_type != TokenType.SELECT for t in rest)

    return False


def _generate_as_hive(expression: exp.Expression) -> bool:
    if isinstance(expression, exp.Create):
        if expression.kind == "TABLE":
            properties = expression.args.get("properties")

            # CREATE EXTERNAL TABLE is Hive
            if properties and properties.find(exp.ExternalProperty):
                return True

            # Any CREATE TABLE other than CREATE TABLE ... AS <query> is Hive
            if not isinstance(expression.expression, exp.Query):
                return True
        else:
            # CREATE VIEW is Trino, but CREATE SCHEMA, CREATE DATABASE, etc, is Hive
            return expression.kind != "VIEW"
    elif isinstance(expression, (exp.Alter, exp.Drop, exp.Describe, exp.Show)):
        if isinstance(expression, exp.Drop) and expression.kind == "VIEW":
            # DROP VIEW is Trino, because CREATE VIEW is as well
            return False

        # Everything else, e.g., ALTER statements, is Hive
        return True

    return False


def _is_iceberg_table(properties: exp.Properties) -> bool:
    for p in properties.expressions:
        if isinstance(p, exp.Property) and p.name == "table_type":
            return p.text("value").lower() == "iceberg"

    return False


def _location_property_sql(self: Athena.Generator, e: exp.LocationProperty):
    # If table_type='iceberg', the LocationProperty is called 'location'
    # Otherwise, it's called 'external_location'
    # ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html

    prop_name = "external_location"

    if isinstance(e.parent, exp.Properties):
        if _is_iceberg_table(e.parent):
            prop_name = "location"

    return f"{prop_name}={self.sql(e, 'this')}"


def _partitioned_by_property_sql(self: Athena.Generator, e: exp.PartitionedByProperty) -> str:
    # If table_type='iceberg' then the table property for partitioning is called 'partitioning'
    # If table_type='hive' it's called 'partitioned_by'
    # ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties

    prop_name = "partitioned_by"

    if isinstance(e.parent, exp.Properties):
        if _is_iceberg_table(e.parent):
            prop_name = "partitioning"

    return f"{prop_name}={self.sql(e, 'this')}"


# Athena extensions to Hive's generator
class _HiveGenerator(Hive.Generator):
    def alter_sql(self, expression: exp.Alter) -> str:
        # Package any ALTER TABLE ADD actions into a Schema object, so it gets generated as
        # `ALTER TABLE .. ADD COLUMNS(...)`, instead of `ALTER TABLE ... ADD COLUMN`, which
        # is invalid syntax on Athena
        if isinstance(expression, exp.Alter) and expression.kind == "TABLE":
            if expression.actions and isinstance(expression.actions[0], exp.ColumnDef):
                new_actions = exp.Schema(expressions=expression.actions)
                expression.set("actions", [new_actions])

        return super().alter_sql(expression)


# Athena extensions to Trino's tokenizer
class _TrinoTokenizer(Trino.Tokenizer):
    KEYWORDS = {
        **Trino.Tokenizer.KEYWORDS,
        "UNLOAD": TokenType.COMMAND,
    }


# Athena extensions to Trino's parser
class _TrinoParser(Trino.Parser):
    STATEMENT_PARSERS = {
        **Trino.Parser.STATEMENT_PARSERS,
        TokenType.USING: lambda self: self._parse_as_command(self._prev),
    }


# Athena extensions to Trino's generator
class _TrinoGenerator(Trino.Generator):
    PROPERTIES_LOCATION = {
        **Trino.Generator.PROPERTIES_LOCATION,
        exp.LocationProperty: exp.Properties.Location.POST_WITH,
    }

    TRANSFORMS = {
        **Trino.Generator.TRANSFORMS,
        exp.PartitionedByProperty: _partitioned_by_property_sql,
        exp.LocationProperty: _location_property_sql,
    }
