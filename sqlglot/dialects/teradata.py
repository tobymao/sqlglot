from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType


class Teradata(Dialect):
    class Parser(parser.Parser):
        def _parse_create_table_index(self):
            unique = self._match_text_seq("UNIQUE")
            primary = self._match_text_seq("PRIMARY")
            self._match(TokenType.INDEX)
            index = self._parse_id_var()
            return self.expression(
                exp.Index,
                this=index,
                columns=self.expression(
                    exp.Tuple, expressions=self._parse_wrapped_csv(self._parse_column)
                ),
                unique=unique,
                primary=primary,
            )
