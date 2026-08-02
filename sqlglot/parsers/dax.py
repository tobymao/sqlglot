from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.tokens import TokenType


class DAXParser(parser.Parser):
    CONJUNCTION = {
        **parser.Parser.CONJUNCTION,
        TokenType.DAMP: exp.And,
    }

    DISJUNCTION = {
        **parser.Parser.DISJUNCTION,
        TokenType.DPIPE: exp.Or,
    }

    def _parse_statement(self) -> exp.Expr | None:
        if not self._match_text_seq("EVALUATE"):
            return None

        query = self._parse_table_expression()

        if query and self._match(TokenType.ORDER_BY):
            order = exp.Order(expressions=self._parse_csv(self._parse_ordered))
            query = query.order_by(self.expression(order), copy=False)

        return query

    def _parse_table_expression(self) -> exp.Query | None:
        if self._match_pair(TokenType.FILTER, TokenType.L_PAREN):
            query = self._parse_table_expression()
            self._match(TokenType.COMMA)
            where = self._parse_disjunction()
            self._match_r_paren()
            return query.where(where, copy=False) if query else None

        table = self._parse_table_parts()
        return exp.select("*").from_(table, copy=False) if table else None

    def _parse_column(self) -> exp.Expr | None:
        this = super()._parse_column()

        # Combine adjacent identifiers into a column reference: Table[Column]
        if isinstance(this, exp.Column) and (column := self._parse_identifier()):
            this = self.expression(exp.Column(this=column, table=this.this))

        return this
