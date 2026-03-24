from __future__ import annotations

import typing as t

from sqlglot import exp, tokens
from sqlglot.dialects import Dialect, Hive, Trino
from sqlglot.generators.athena import AthenaGenerator
from sqlglot.parsers.athena import AthenaParser
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

    def parse(self, sql: str, **opts) -> t.List[t.Optional[exp.Expr]]:
        opts["hive"] = self._hive
        opts["trino"] = self._trino
        return super().parse(sql, **opts)

    def parse_into(
        self, expression_type: exp.IntoType, sql: str, **opts
    ) -> t.List[t.Optional[exp.Expr]]:
        opts["hive"] = self._hive
        opts["trino"] = self._trino
        return super().parse_into(expression_type, sql, **opts)

    def generate(self, expression: exp.Expr, copy: bool = True, **opts) -> str:
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

    Parser = AthenaParser

    Generator = AthenaGenerator


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


# Athena extensions to Trino's tokenizer
class _TrinoTokenizer(Trino.Tokenizer):
    KEYWORDS = {
        **Trino.Tokenizer.KEYWORDS,
        "UNLOAD": TokenType.COMMAND,
    }
