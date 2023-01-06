from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType


class Teradata(Dialect):
    class Parser(parser.Parser):
        def _parse_create_table_index(self):
            unique = self._match_texts("UNIQUE")
            no_primary = self._match_text_seq("NO", "PRIMARY")
            self._match_text_seq("PRIMARY")
            self._match(TokenType.INDEX)
            index = self._parse_id_var()
            return self.expression(
                exp.CreateTableIndex,
                this=index,
                table=self.expression(exp.Table, this=self._parse_id_var()),
                columns=self._parse_expression(),
                unique=unique,
                no_primary=no_primary,
            )
