from __future__ import annotations

import typing as t
from collections import defaultdict

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.helper import csv
from sqlglot.tokens import TokenType


class Vertica(Postgres):
    class Tokenizer(Postgres.Tokenizer):
        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "MINUS": TokenType.EXCEPT,
        }
        TOKENS_PRECEDING_HINT = Postgres.Tokenizer.TOKENS_PRECEDING_HINT | {
            TokenType.WITH,
            TokenType.GROUP_BY,
        }

    class Parser(Postgres.Parser):
        def _parse_hint(self) -> t.Optional[exp.Hint]:
            hint = super()._parse_hint()
            if hint:
                return hint

            comments = self._prev_comments
            if not comments:
                return None

            for i, comment in enumerate(comments):
                comment = comment.strip()
                if not comment.startswith("+"):
                    continue

                comments.pop(i)
                return exp.maybe_parse(comment[1:], into=exp.Hint, dialect=self.dialect)

            return None

        def _parse_table_alias(
            self, alias_tokens: t.Optional[t.Collection[TokenType]] = None
        ) -> t.Optional[exp.TableAlias]:
            if self._curr and self._curr.text.upper() in {"MATCH", "TIMESERIES"}:
                return None

            return super()._parse_table_alias(alias_tokens=alias_tokens)

        def _parse_statement(self) -> t.Optional[exp.Expression]:
            if (
                self._curr
                and self._curr.text.upper() == "AT"
                and self._next
                and self._next.text.upper() in {"EPOCH", "TIME"}
            ):
                return self._parse_select()

            return super()._parse_statement()

        def _parse_historical_data(self) -> t.Optional[exp.HistoricalData]:
            index = self._index

            if not self._match_text_seq("AT"):
                return None

            if self._match_text_seq("EPOCH"):
                kind = "EPOCH"
                if self._curr and self._curr.text.upper() == "LATEST":
                    self._advance()
                    expression = exp.var("LATEST")
                else:
                    expression = self._parse_bitwise()
            elif self._match_text_seq("TIME"):
                kind = "TIME"
                expression = self._parse_bitwise()
            else:
                self._retreat(index)
                return None

            if not expression:
                self._retreat(index)
                return None

            return self.expression(
                exp.HistoricalData,
                this=exp.var("AT"),
                kind=exp.var(kind),
                expression=expression,
            )

        def _parse_limit(
            self,
            this: t.Optional[exp.Expression] = None,
            top: bool = False,
            skip_limit_token: bool = False,
        ) -> t.Optional[exp.Expression]:
            limit = super()._parse_limit(this=this, top=top, skip_limit_token=skip_limit_token)

            if isinstance(limit, exp.Limit) and self._match(TokenType.OVER):
                if isinstance(limit.expression, exp.Column) and limit.expression.name.upper() == "ALL":
                    self.raise_error("LIMIT ALL cannot be used with OVER in Vertica")
                self._match_l_paren()
                limit.set("partition_by", self._parse_partition_by())
                limit.set("order", self._parse_order())
                self._match_r_paren()

            return limit

        @staticmethod
        def _rightmost_set_operand(expression: exp.Expression) -> exp.Expression:
            while isinstance(expression, exp.SetOperation) and expression.expression:
                expression = expression.expression
            return expression

        def _reassociate_set_operations(
            self, expression: exp.SetOperation
        ) -> t.Optional[exp.Expression]:
            operands: t.List[t.Optional[exp.Expression]] = []
            operators: t.List[exp.SetOperation] = []

            def collect(node: t.Optional[exp.Expression]) -> None:
                if isinstance(node, exp.SetOperation):
                    collect(node.this)
                    operators.append(node)
                    operands.append(node.expression)
                else:
                    operands.append(node)

            collect(expression)

            if not operators or len(operands) != len(operators) + 1:
                return expression

            reduced_operands: t.List[t.Optional[exp.Expression]] = [operands[0]]
            reduced_operators: t.List[exp.SetOperation] = []

            for operator, right in zip(operators, operands[1:]):
                if isinstance(operator, exp.Intersect):
                    left = reduced_operands.pop()
                    operator.set("this", left)
                    operator.set("expression", right)
                    reduced_operands.append(operator)
                else:
                    reduced_operators.append(operator)
                    reduced_operands.append(right)

            result = reduced_operands[0]
            for operator, right in zip(reduced_operators, reduced_operands[1:]):
                operator.set("this", result)
                operator.set("expression", right)
                result = operator

            return result

        def parse_set_operation(
            self, this: t.Optional[exp.Expression], consume_pipe: bool = False
        ) -> t.Optional[exp.Expression]:
            setop = super().parse_set_operation(this, consume_pipe=consume_pipe)

            if isinstance(setop, (exp.Intersect, exp.Except)) and setop.args.get("distinct") is False:
                self.raise_error(f"{setop.key.upper()} ALL is not supported in Vertica")

            return setop

        def _parse_set_operations(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
            while this:
                setop = self.parse_set_operation(this)
                if not setop:
                    break
                this = setop

            if isinstance(this, exp.SetOperation):
                this = self._reassociate_set_operations(this)

            if isinstance(this, exp.SetOperation) and self.MODIFIERS_ATTACHED_TO_SET_OP:
                rightmost_expression = self._rightmost_set_operand(this)

                for arg in self.SET_OP_MODIFIERS:
                    expr = rightmost_expression.args.get(arg)
                    if expr:
                        this.set(arg, expr.pop())

            return this

        def _parse_with(self, skip_with_token: bool = False) -> t.Optional[exp.With]:
            if not skip_with_token and not self._match(TokenType.WITH):
                return None

            comments = self._prev_comments
            hint = self._parse_hint()
            recursive = self._match(TokenType.RECURSIVE)

            last_comments = None
            expressions = []
            while True:
                cte = self._parse_cte()
                if isinstance(cte, exp.CTE):
                    expressions.append(cte)
                    if last_comments:
                        cte.add_comments(last_comments)

                if not self._match(TokenType.COMMA) and not self._match(TokenType.WITH):
                    break
                else:
                    self._match(TokenType.WITH)

                last_comments = self._prev_comments

            return self.expression(
                exp.With,
                comments=comments,
                hint=hint,
                expressions=expressions,
                recursive=recursive,
                search=self._parse_recursive_with_search(),
            )

        def _parse_group(self, skip_group_by_token: bool = False) -> t.Optional[exp.Group]:
            if not skip_group_by_token and not self._match(TokenType.GROUP_BY):
                return None
            comments = self._prev_comments
            hint = self._parse_hint()

            elements: t.Dict[str, t.Any] = defaultdict(list)

            if self._match(TokenType.ALL):
                elements["all"] = True
            elif self._match(TokenType.DISTINCT):
                elements["all"] = False

            if self._match_set(self.QUERY_MODIFIER_TOKENS, advance=False):
                return self.expression(exp.Group, comments=comments, hint=hint, **elements)

            while True:
                index = self._index

                elements["expressions"].extend(
                    self._parse_csv(
                        lambda: None
                        if self._match_set((TokenType.CUBE, TokenType.ROLLUP), advance=False)
                        else self._parse_disjunction()
                    )
                )

                before_with_index = self._index
                with_prefix = self._match(TokenType.WITH)

                if cube_or_rollup := self._parse_cube_or_rollup(with_prefix=with_prefix):
                    key = "rollup" if isinstance(cube_or_rollup, exp.Rollup) else "cube"
                    elements[key].append(cube_or_rollup)
                elif grouping_sets := self._parse_grouping_sets():
                    elements["grouping_sets"].append(grouping_sets)
                elif self._match_text_seq("TOTALS"):
                    elements["totals"] = True

                if before_with_index <= self._index <= before_with_index + 1:
                    self._retreat(before_with_index)
                    break

                if index == self._index:
                    break

            return self.expression(exp.Group, comments=comments, hint=hint, **elements)

        def _parse_match_clause(self) -> t.Optional[exp.MatchRecognize]:
            if not self._match_text_seq("MATCH"):
                return None

            self._match_l_paren()

            partition_by = self._parse_partition_by()
            order = self._parse_order()
            if not order:
                self.raise_error("Expecting ORDER BY")

            if not self._match_text_seq("DEFINE"):
                self.raise_error("Expecting DEFINE")

            define = self._parse_csv(self._parse_name_as_expression)

            if not self._match_text_seq("PATTERN"):
                self.raise_error("Expecting PATTERN")

            pattern_name = self._parse_id_var(any_token=False)
            self._match_text_seq("AS")
            self._match_l_paren()

            if not self._curr:
                self.raise_error("Expecting )", self._curr)

            paren = 1
            start = self._curr

            while self._curr and paren > 0:
                if self._curr.token_type == TokenType.L_PAREN:
                    paren += 1
                if self._curr.token_type == TokenType.R_PAREN:
                    paren -= 1

                end = self._prev
                self._advance()

            if paren > 0:
                self.raise_error("Expecting )", self._curr)

            pattern_expr = exp.var(self._find_sql(start, end))
            pattern = (
                exp.alias_(pattern_expr, pattern_name, copy=False) if pattern_name else pattern_expr
            )

            if self._match_text_seq("ROWS", "MATCH", "ALL", "EVENTS"):
                rows = exp.var("ROWS MATCH ALL EVENTS")
            elif self._match_text_seq("ROWS", "MATCH", "FIRST", "EVENT"):
                rows = exp.var("ROWS MATCH FIRST EVENT")
            else:
                rows = None

            self._match_r_paren()

            return self.expression(
                exp.MatchRecognize,
                partition_by=partition_by,
                order=order,
                define=define,
                pattern=pattern,
                rows=rows,
            )

        def _parse_timeseries(self) -> t.Optional[exp.Timeseries]:
            if not self._match_text_seq("TIMESERIES"):
                return None

            this = self._parse_id_var(any_token=False)
            if not this:
                self.raise_error("Expecting slice time alias")

            self._match(TokenType.ALIAS)
            expression = self._parse_bitwise()
            if not expression:
                self.raise_error("Expecting interval expression")

            if not self._match(TokenType.OVER):
                self.raise_error("Expecting OVER")

            self._match_l_paren()
            partition_by = self._parse_partition_by()
            order = self._parse_order()
            if not order:
                self.raise_error("Expecting ORDER BY")
            self._match_r_paren()

            return self.expression(
                exp.Timeseries,
                this=this,
                expression=expression,
                partition_by=partition_by,
                order=order,
            )

        def _parse_query_modifiers(self, this):
            if isinstance(this, self.MODIFIABLES):
                for join in self._parse_joins():
                    this.append("joins", join)
                for lateral in iter(self._parse_lateral, None):
                    this.append("laterals", lateral)

                while True:
                    modifier_token = self._curr

                    if self._match_text_seq("TIMESERIES", advance=False):
                        key = "timeseries"
                        expression = self._parse_timeseries()
                    elif self._match_text_seq("MATCH", advance=False):
                        key = "match"
                        expression = self._parse_match_clause()
                    elif self._match_set(self.QUERY_MODIFIER_PARSERS, advance=False):
                        parser = self.QUERY_MODIFIER_PARSERS[modifier_token.token_type]
                        key, expression = parser(self)
                    else:
                        break

                    if not expression:
                        break

                    if this.args.get(key):
                        self.raise_error(
                            f"Found multiple '{key.upper()}' clauses",
                            token=modifier_token,
                        )

                    this.set(key, expression)
                    if key == "limit":
                        offset = expression.args.get("offset")
                        expression.set("offset", None)

                        if offset:
                            offset = exp.Offset(expression=offset)
                            this.set("offset", offset)

                            limit_by_expressions = expression.expressions
                            expression.set("expressions", None)
                            offset.set("expressions", limit_by_expressions)

            if self.SUPPORTS_IMPLICIT_UNNEST and this and this.args.get("from_"):
                this = self._implicit_unnests_to_explicit(this)

            return this

        def _parse_select(
            self,
            nested: bool = False,
            table: bool = False,
            parse_subquery_alias: bool = True,
            parse_set_operation: bool = True,
            consume_pipe: bool = True,
            from_: t.Optional[exp.From] = None,
        ) -> t.Optional[exp.Expression]:
            historical_data = self._parse_historical_data()

            query = super()._parse_select(
                nested=nested,
                table=table,
                parse_subquery_alias=parse_subquery_alias,
                parse_set_operation=parse_set_operation,
                consume_pipe=consume_pipe,
                from_=from_,
            )

            if historical_data and query and "when" in query.arg_types:
                query.set("when", historical_data)

            return query

    class Generator(Postgres.Generator):
        QUERY_HINTS = True
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False

        def historicaldata_sql(self, expression: exp.HistoricalData) -> str:
            if expression.text("this").upper() == "AT":
                kind = expression.text("kind").upper()
                if kind in {"EPOCH", "TIME"}:
                    return f"AT {kind} {self.sql(expression, 'expression')}"

            return super().historicaldata_sql(expression)

        def select_sql(self, expression: exp.Select) -> str:
            sql = super().select_sql(expression)
            when = self.sql(expression, "when")
            return f"{when} {sql}" if when else sql

        def set_operations(self, expression: exp.SetOperation) -> str:
            sql = super().set_operations(expression)
            when = self.sql(expression, "when")
            return f"{when} {sql}" if when else sql

        def with_sql(self, expression: exp.With) -> str:
            sql = self.expressions(expression, flat=True)
            hint = self.sql(expression, "hint")
            recursive = (
                "RECURSIVE "
                if self.CTE_RECURSIVE_KEYWORD_REQUIRED and expression.args.get("recursive")
                else ""
            )
            search = self.sql(expression, "search")
            search = f" {search}" if search else ""

            return f"WITH{hint} {recursive}{sql}{search}"

        def group_sql(self, expression: exp.Group) -> str:
            group_by_all = expression.args.get("all")
            if group_by_all is True:
                modifier = " ALL"
            elif group_by_all is False:
                modifier = " DISTINCT"
            else:
                modifier = ""

            hint = self.sql(expression, "hint")
            group_by = self.op_expressions(f"GROUP BY{modifier}{hint}", expression)

            grouping_sets = self.expressions(expression, key="grouping_sets")
            cube = self.expressions(expression, key="cube")
            rollup = self.expressions(expression, key="rollup")

            groupings = csv(
                self.seg(grouping_sets) if grouping_sets else "",
                self.seg(cube) if cube else "",
                self.seg(rollup) if rollup else "",
                self.seg("WITH TOTALS") if expression.args.get("totals") else "",
                sep=self.GROUPINGS_SEP,
            )

            if (
                expression.expressions
                and groupings
                and groupings.strip() not in ("WITH CUBE", "WITH ROLLUP")
            ):
                group_by = f"{group_by}{self.GROUPINGS_SEP}"

            return f"{group_by}{groupings}"

        def limit_sql(self, expression: exp.Limit, top: bool = False) -> str:
            sql = super().limit_sql(expression, top=top)
            partition = self.expressions(expression, key="partition_by", flat=True)
            partition = f"PARTITION BY {partition}" if partition else ""
            order = self.sql(expression, "order").strip()

            if partition or order:
                if isinstance(expression.expression, exp.Column) and expression.expression.name.upper() == "ALL":
                    self.unsupported("LIMIT ALL cannot be used with OVER in Vertica")
                over = " ".join(part for part in (partition, order) if part)
                sql = f"{sql} OVER ({over})"

            return sql

        def timeseries_sql(self, expression: exp.Timeseries) -> str:
            partition = self.expressions(expression, key="partition_by", flat=True)
            partition = f"PARTITION BY {partition} " if partition else ""
            order = self.sql(expression, "order").strip()
            return (
                f" TIMESERIES {self.sql(expression, 'this')} AS {self.sql(expression, 'expression')} "
                f"OVER ({partition}{order})"
            )

        def matchrecognize_sql(self, expression: exp.MatchRecognize) -> str:
            partition = self.expressions(expression, key="partition_by", flat=True)
            partition = f"PARTITION BY {partition}" if partition else ""
            order = self.sql(expression, "order").strip()

            definition_sqls = [
                f"{self.sql(definition, 'alias')} AS {self.sql(definition, 'this')}"
                for definition in expression.args.get("define", [])
            ]
            define = self.expressions(sqls=definition_sqls)
            define = f"DEFINE {define}" if define else ""

            pattern_expr = expression.args.get("pattern")
            if isinstance(pattern_expr, exp.Alias):
                pattern_name = self.sql(pattern_expr, "alias")
                pattern_body = self.sql(pattern_expr, "this")
                pattern = f"PATTERN {pattern_name} AS ({pattern_body})"
            elif pattern_expr:
                pattern = f"PATTERN ({self.sql(pattern_expr)})"
            else:
                pattern = ""

            rows = self.sql(expression, "rows")
            body = " ".join(part for part in (partition, order, define, pattern, rows) if part)
            return f" MATCH ({body})"

        def query_modifiers(self, expression: exp.Expression, *sqls: str) -> str:
            limit = expression.args.get("limit")

            if self.LIMIT_FETCH == "LIMIT" and isinstance(limit, exp.Fetch):
                limit = exp.Limit(expression=exp.maybe_copy(limit.args.get("count")))
            elif self.LIMIT_FETCH == "FETCH" and isinstance(limit, exp.Limit):
                limit = exp.Fetch(direction="FIRST", count=exp.maybe_copy(limit.expression))

            return csv(
                *sqls,
                *[self.sql(join) for join in expression.args.get("joins") or []],
                *[self.sql(lateral) for lateral in expression.args.get("laterals") or []],
                self.sql(expression, "prewhere"),
                self.sql(expression, "where"),
                self.sql(expression, "timeseries"),
                self.sql(expression, "connect"),
                self.sql(expression, "group"),
                self.sql(expression, "having"),
                *[gen(self, expression) for gen in self.AFTER_HAVING_MODIFIER_TRANSFORMS.values()],
                self.sql(expression, "match"),
                self.sql(expression, "order"),
                *self.offset_limit_modifiers(expression, isinstance(limit, exp.Fetch), limit),
                *self.after_limit_modifiers(expression),
                self.options_modifier(expression),
                self.for_modifiers(expression),
                sep="",
            )
