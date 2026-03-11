from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _select_all(table: exp.Expr) -> t.Optional[exp.Select]:
    return exp.select("*").from_(table, copy=False) if table else None


def _resolve_projection(s: exp.Expr, projections: t.Dict[str, exp.Expr]) -> exp.Expr:
    if isinstance(s, exp.Column) and s.name in projections:
        return projections[s.name].copy()
    return s


class PRQLParser(parser.Parser):
    CONJUNCTION = {
        **parser.Parser.CONJUNCTION,
        TokenType.DAMP: exp.And,
    }

    DISJUNCTION = {
        **parser.Parser.DISJUNCTION,
        TokenType.DPIPE: exp.Or,
    }

    TRANSFORM_PARSERS = {
        "DERIVE": lambda self, query: self._parse_selection(query),
        "SELECT": lambda self, query: self._parse_selection(query, append=False),
        "TAKE": lambda self, query: self._parse_take(query),
        "FILTER": lambda self, query: query.where(self._parse_disjunction()),
        "APPEND": lambda self, query: query.union(
            _select_all(self._parse_table()), distinct=False, copy=False
        ),
        "REMOVE": lambda self, query: query.except_(
            _select_all(self._parse_table()), distinct=False, copy=False
        ),
        "INTERSECT": lambda self, query: query.intersect(
            _select_all(self._parse_table()), distinct=False, copy=False
        ),
        "SORT": lambda self, query: self._parse_order_by(query),
        "AGGREGATE": lambda self, query: self._parse_selection(
            query, parse_method=self._parse_aggregate, append=False
        ),
    }

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "AVERAGE": exp.Avg.from_arg_list,
        "SUM": lambda args: exp.func("COALESCE", exp.Sum(this=seq_get(args, 0)), 0),
    }

    def _parse_equality(self) -> t.Optional[exp.Expr]:
        eq = self._parse_tokens(self._parse_comparison, self.EQUALITY)
        if not isinstance(eq, (exp.EQ, exp.NEQ)):
            return eq

        # https://prql-lang.org/book/reference/spec/null.html
        if isinstance(eq.expression, exp.Null):
            is_exp = exp.Is(this=eq.this, expression=eq.expression)
            return is_exp if isinstance(eq, exp.EQ) else exp.Not(this=is_exp)
        if isinstance(eq.this, exp.Null):
            is_exp = exp.Is(this=eq.expression, expression=eq.this)
            return is_exp if isinstance(eq, exp.EQ) else exp.Not(this=is_exp)
        return eq

    def _parse_statement(self) -> t.Optional[exp.Expr]:
        expression = self._parse_expression()
        expression = expression if expression else self._parse_query()
        return expression

    def _parse_query(self) -> t.Optional[exp.Query]:
        from_ = self._parse_from()

        if not from_:
            return None

        query: exp.Query = exp.select("*").from_(from_, copy=False)

        while self._match_texts(self.TRANSFORM_PARSERS):
            query = self.TRANSFORM_PARSERS[self._prev.text.upper()](self, query)

        return query

    def _parse_selection(
        self,
        query: exp.Query,
        parse_method: t.Optional[t.Callable] = None,
        append: bool = True,
    ) -> exp.Query:
        parse_method = parse_method if parse_method else self._parse_expression
        if self._match(TokenType.L_BRACE):
            selects = self._parse_csv(parse_method)

            if not self._match(TokenType.R_BRACE, expression=query):
                self.raise_error("Expecting }")
        else:
            expression = parse_method()
            selects = [expression] if expression else []

        projections = {
            select.alias_or_name: select.this if isinstance(select, exp.Alias) else select
            for select in query.selects
        }

        resolved = [
            select.transform(_resolve_projection, projections=projections, copy=False)
            for select in selects
        ]

        return query.select(*resolved, append=append, copy=False)

    def _parse_take(self, query: exp.Query) -> t.Optional[exp.Query]:
        num = self._parse_number()  # TODO: TAKE for ranges a..b
        return query.limit(num) if num else None

    def _parse_ordered(
        self, parse_method: t.Optional[t.Callable] = None
    ) -> t.Optional[exp.Ordered]:
        asc = self._match(TokenType.PLUS)
        desc = self._match(TokenType.DASH) or (asc and False)
        term = term = super()._parse_ordered(parse_method=parse_method)
        if term and desc:
            term.set("desc", True)
            term.set("nulls_first", False)
        return term

    def _parse_order_by(self, query: exp.Select) -> t.Optional[exp.Query]:
        l_brace = self._match(TokenType.L_BRACE)
        expressions = self._parse_csv(self._parse_ordered)
        if l_brace and not self._match(TokenType.R_BRACE):
            self.raise_error("Expecting }")
        return query.order_by(self.expression(exp.Order(expressions=expressions)), copy=False)

    def _parse_aggregate(self) -> t.Optional[exp.Expr]:
        alias = None
        if self._next and self._next.token_type == TokenType.ALIAS:
            alias = self._parse_id_var(any_token=True)
            self._match(TokenType.ALIAS)

        name = self._curr and self._curr.text.upper()
        func_builder = self.FUNCTIONS.get(name)
        if func_builder:
            self._advance()
            args = self._parse_column()
            func = func_builder([args])
        else:
            self.raise_error(f"Unsupported aggregation function {name}")
        if alias:
            return self.expression(exp.Alias(this=func, alias=alias))
        return func

    def _parse_expression(self) -> t.Optional[exp.Expr]:
        if self._next and self._next.token_type == TokenType.ALIAS:
            alias = self._parse_id_var(True)
            self._match(TokenType.ALIAS)
            return self.expression(exp.Alias(this=self._parse_assignment(), alias=alias))
        return self._parse_assignment()

    def _parse_table(
        self,
        schema: bool = False,
        joins: bool = False,
        alias_tokens: t.Optional[t.Collection[TokenType]] = None,
        parse_bracket: bool = False,
        is_db_reference: bool = False,
        parse_partition: bool = False,
        consume_pipe: bool = False,
    ) -> t.Optional[exp.Expr]:
        return self._parse_table_parts()

    def _parse_from(
        self,
        joins: bool = False,
        skip_from_token: bool = False,
        consume_pipe: bool = False,
    ) -> t.Optional[exp.From]:
        if not skip_from_token and not self._match(TokenType.FROM):
            return None

        comments = self._prev_comments
        return self.expression(
            exp.From(this=self._parse_table(joins=joins)),
            comments=comments,
        )
