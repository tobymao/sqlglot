from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.errors import ErrorLevel
from sqlglot.parser import Parser
from sqlglot.parsers.trino import TrinoParser
from sqlglot.tokens import TokenType, Token

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.dialects.hive import Hive
    from sqlglot.dialects.trino import Trino


class AthenaTrinoParser(TrinoParser):
    STATEMENT_PARSERS = {
        **TrinoParser.STATEMENT_PARSERS,
        TokenType.USING: lambda self: self._parse_as_command(self._prev),
    }


class AthenaParser(Parser):
    def __init__(
        self,
        error_level: t.Optional[ErrorLevel] = None,
        error_message_context: int = 100,
        max_errors: int = 3,
        dialect: DialectType = None,
        hive: t.Optional[Hive] = None,
        trino: t.Optional[Trino] = None,
    ) -> None:
        from sqlglot.dialects.hive import Hive
        from sqlglot.dialects.trino import Trino

        hive = hive or Hive()
        trino = trino or Trino()

        super().__init__(
            error_level=error_level,
            error_message_context=error_message_context,
            max_errors=max_errors,
            dialect=dialect,
        )

        self._hive_parser = hive.parser(
            error_level=error_level,
            error_message_context=error_message_context,
            max_errors=max_errors,
        )
        self._trino_parser = AthenaTrinoParser(
            error_level=error_level,
            error_message_context=error_message_context,
            max_errors=max_errors,
            dialect=trino,
        )

    def parse(self, raw_tokens: list[Token], sql: str) -> list[t.Optional[exp.Expr]]:
        if raw_tokens and raw_tokens[0].token_type == TokenType.HIVE_TOKEN_STREAM:
            return self._hive_parser.parse(raw_tokens[1:], sql)

        return self._trino_parser.parse(raw_tokens, sql)

    def parse_into(
        self,
        expression_types: exp.IntoType,
        raw_tokens: list[Token],
        sql: t.Optional[str] = None,
    ) -> list[t.Optional[exp.Expr]]:
        if raw_tokens and raw_tokens[0].token_type == TokenType.HIVE_TOKEN_STREAM:
            return self._hive_parser.parse_into(expression_types, raw_tokens[1:], sql)

        return self._trino_parser.parse_into(expression_types, raw_tokens, sql)
