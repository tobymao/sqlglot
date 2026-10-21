from __future__ import annotations


from sqlglot import tokens
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import TokenError
from sqlglot.generators.athena import AthenaGenerator
from sqlglot.parsers.athena import AthenaParser
from sqlglot.tokens import TokenType, Token
from sqlglot.dialects.trino import Trino
from sqlglot.dialects.hive import Hive


class Athena(Trino):
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

        def __init__(self, dialect: DialectType = None) -> None:
            super().__init__(dialect=dialect)

            self._hive_tokenizer = Hive().tokenizer()
            self._trino_tokenizer = _TrinoTokenizer(Trino())

        def tokenize(self, sql: str) -> list[Token]:
            # Only statements starting with these can be routed to Hive, so the rest (DML) need a
            # single pass with Trino's rules instead of the combined pass followed by Trino's
            try:
                trino_tokens: list[Token] | None = self._trino_tokenizer.tokenize(sql)
            except TokenError:
                trino_tokens = None

            if trino_tokens is not None and not _may_be_hive(trino_tokens):
                return trino_tokens

            try:
                tokens = super().tokenize(sql)
            except TokenError:
                # Combined rules can reject literals valid in only one engine.
                for tokenizer, hive in (
                    (self._trino_tokenizer, False),
                    (self._hive_tokenizer, True),
                ):
                    try:
                        tokens = tokenizer.tokenize(sql)
                    except TokenError:
                        continue

                    if _tokenize_as_hive(tokens) == hive:
                        return [Token(TokenType.HIVE_TOKEN_STREAM, "")] + tokens if hive else tokens
                raise

            if _tokenize_as_hive(tokens):
                return [Token(TokenType.HIVE_TOKEN_STREAM, "")] + self._hive_tokenizer.tokenize(sql)

            return trino_tokens if trino_tokens is not None else self._trino_tokenizer.tokenize(sql)

    Parser = AthenaParser

    Generator = AthenaGenerator


_MAYBE_HIVE_TOKENS = {
    TokenType.ALTER,
    TokenType.CREATE,
    TokenType.DESCRIBE,
    TokenType.DROP,
    TokenType.SHOW,
}


def _may_be_hive(tokens: list[Token]) -> bool:
    return bool(tokens) and (
        tokens[0].token_type in _MAYBE_HIVE_TOKENS or tokens[0].text.upper() == "MSCK"
    )


def _tokenize_as_hive(tokens: list[Token]) -> bool:
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
