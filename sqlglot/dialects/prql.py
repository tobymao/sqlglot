from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType
from functools import reduce
from enum import Enum


def _select_all(table: exp.Expression) -> t.Optional[exp.Select]:
    return exp.select("*").from_(table, copy=False) if table else None


def _select_sql(self: PRQL.Generator, expression: exp.Select) -> str:
    """
    Performs a select transform.
    """

    all_expressions = expression.args.get("expressions", [])

    # Empty `expressions` edge-case.
    if not all_expressions: return f""

    all_partitions = _partition_select(self, all_expressions)

    # Remove default.
    all_partitions.pop(PRQL.Generator.SelectionPartition.DEFAULT, None)
    ret_val = "".join(
        PRQL.Generator.SelectionPartition._partitions_to_str_list(
            self,
            all_partitions
        )
    )
    ret_val = (
        f"{self.sql(expression, 'from')}"
        f"{self.sql(expression, 'order')}"
        f"{self.sql(expression, 'limit')}"
        f"{self.sql(expression, 'where')}"
        f"{ret_val}"
    )

    return ret_val


def _partition_select(self: PRQL.Generator, expressions: list[exp.Expression], group_by: t.Optional[exp.Group] = None) -> dict[PRQL.Generator.SelectionPartition, list[exp.Expression]]:
    """
    Partitions `expressions` into a `dict` of `PRQL.Generator.SelectionPartition`
    groups.
    Parameters:
        expressions: A `list` of `exp.Expression` that will be partitioned via
            `PRQL.Generator.SelectionPartition`.
    """

    _PartitionName: t.TypeAlias = PRQL.Generator.SelectionPartition
    _Crit: t.TypeAlias = t.Callable[[exp.Expression], bool]
    _Crits: t.TypeAlias = dict[_PartitionName, _Crit]
    _RetDict: t.TypeAlias = dict[_PartitionName, list[exp.Expression]]

    def _partition_via_crit_high_low(items: list[exp.Expression], crits: _Crits) -> _RetDict:
        """
        Partitions `items` via the values of `crits`.
        Parameters:
            items: A `list` of `exp.Expression` to partition via `crits`.
            crits: A `dict` of `_PartitionName` to partition, in order, matched
                via their `_Crit`.
        Returns:
            A `dict` of `_PartitionName` to `list[exp.Expression]`. A
                `_PartitionName.DEFAULT` is added as a catch-all for any
                unmatched `items`.
        """

        def _partition_last_list(all_items_dict: _RetDict, cur_crit_tuple: tuple[_PartitionName, _Crit]) -> _RetDict:
            """
            A reduction function that partitions the last element of
            `all_items_dict` via the `_Crit` of `cur_crit_tuple` and returns
            `all_items_dict` w/ the new partition and `_PartitionName` appended
            as the penultimate element. Any items that failed the `_Crit` remain
            as the last element, under their previous `_PartitionName`.
            """

            (default_partition, cur_list) = all_items_dict.popitem()

            (cur_partition_name, cur_crit) = cur_crit_tuple

            # Values.
            new_partition = self._partition_list_via_filter( \
                cur_list, \
                cur_crit, \
            )
            # Guarantee `True` first.
            new_partition = dict(sorted(new_partition.items(), reverse=True))
            new_partition_vals = new_partition.values()

            # Keys.
            new_partition_keys = [cur_partition_name, default_partition]

            zipped_new_partitions = zip(new_partition_keys, new_partition_vals, strict=True)

            # Append partitions.
            return all_items_dict | {k:v for (k, v) in zipped_new_partitions}

        crits_tuples = [(k, v) for (k, v) in crits.items()]

        # Create the default entry for unmatched elements of `items`.
        preprocessed_items = {_PartitionName.DEFAULT: items}

        return reduce(_partition_last_list, crits_tuples, preprocessed_items)

    # Check for any `exp.Star()`
    contains_star = bool([elem for elem in expressions if elem.is_star])

    criteria: _Crits = {
        # TODO: Implement for: _PartitionName.AGGREGATE
        _PartitionName.DERIVE: lambda e: contains_star and not e.is_star,
        _PartitionName.SELECT: lambda e : True and not e.is_star,
    }
    partitions = _partition_via_crit_high_low(expressions, criteria)

    return partitions

def _tuple_if_necessary(self: PRQL.Generator, func_name: t.Optional[str], expressions: list[exp.Expression], seperator: t.Tuple[str, str] = ("{", "}"), force_tuple: t.Optional[bool] = None) -> str:
    """
    Concatenates expressions, creating a tuple iff PRQL requires it. Optionally,
    prepends a function name. Always returns the empty `str` if `expressions`
    is empty.
    Parameters:
        func_name: The name to prepend to this tuple. Will have a leading and
            trailing space.
        expressions: The list of expression that may be inserted into a tuple.
            Always returns empty `str` if empty.
        seperator: The pre and post tuple parenthesis type.
        force_tuple: If the tuple should be forced on, `True`, or off, `False`.
            `None` for decision based on PRQL tuple crit. Empty `expressions`
            still returns the empty `str`.
    """

    def _form_tuple(inside: str):
        """
        Forms a tuple around `inside`.
        """

        return f"{seperator[0]}{inside}{seperator[1]}"

    prepend_str = f" {func_name} " if func_name is not None else ""

    num_expressions = len(expressions)
    joined_expressions = ", ".join([f"{self.sql(e)}" for e in expressions])

    tupled_expressions: str; pass # Prevent accidental assignment.
    if force_tuple is None:
        match num_expressions:
            case 0: return f""
            case 1:
                # TODO: Check if parenthesis are required.
                tupled_expressions = f"{_form_tuple(joined_expressions)}"
            case _: tupled_expressions = f"{_form_tuple(joined_expressions)}"
    elif force_tuple is True:
        match num_expressions:
            case 0: return f""
            case _: tupled_expressions = f"{_form_tuple(joined_expressions)}"
    else: # `force_tuple` is False.
        match num_expressions:
            case 0: return f""
            case _: tupled_expressions = f"{joined_expressions}"

    return f"{prepend_str}{tupled_expressions}"


class PRQL(Dialect):
    DPIPE_IS_STRING_CONCAT = False

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ["`"]
        QUOTES = ["'", '"']

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "=": TokenType.ALIAS,
            "'": TokenType.QUOTE,
            '"': TokenType.QUOTE,
            "`": TokenType.IDENTIFIER,
            "#": TokenType.COMMENT,
        }

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
        }

    class Parser(parser.Parser):
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
            "FILTER": lambda self, query: query.where(self._parse_assignment()),
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

        def _parse_equality(self) -> t.Optional[exp.Expression]:
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

        def _parse_statement(self) -> t.Optional[exp.Expression]:
            expression = self._parse_expression()
            expression = expression if expression else self._parse_query()
            return expression

        def _parse_query(self) -> t.Optional[exp.Query]:
            from_ = self._parse_from()

            if not from_:
                return None

            query = exp.select("*").from_(from_, copy=False)

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

            selects = [
                select.transform(
                    lambda s: (projections[s.name].copy() if s.name in projections else s)
                    if isinstance(s, exp.Column)
                    else s,
                    copy=False,
                )
                for select in selects
            ]

            return query.select(*selects, append=append, copy=False)

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
            return query.order_by(self.expression(exp.Order, expressions=expressions), copy=False)

        def _parse_aggregate(self) -> t.Optional[exp.Expression]:
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
                return self.expression(exp.Alias, this=func, alias=alias)
            return func

        def _parse_expression(self) -> t.Optional[exp.Expression]:
            if self._next and self._next.token_type == TokenType.ALIAS:
                alias = self._parse_id_var(True)
                self._match(TokenType.ALIAS)
                return self.expression(exp.Alias, this=self._parse_assignment(), alias=alias)
            return self._parse_assignment()

        def _parse_table(
            self,
            schema: bool = False,
            joins: bool = False,
            alias_tokens: t.Optional[t.Collection[TokenType]] = None,
            parse_bracket: bool = False,
            is_db_reference: bool = False,
            parse_partition: bool = False,
        ) -> t.Optional[exp.Expression]:
            return self._parse_table_parts()

        def _parse_from(
            self, joins: bool = False, skip_from_token: bool = False
        ) -> t.Optional[exp.From]:
            if not skip_from_token and not self._match(TokenType.FROM):
                return None

            return self.expression(
                exp.From, comments=self._prev_comments, this=self._parse_table(joins=joins)
            )
        
    class Generator(generator.Generator):

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.From: lambda self, e: f" from {e.this}",
            exp.Limit: lambda self, e: f" take {self.sql(e, 'expression')}",
            exp.Select: _select_sql,
            exp.Where: lambda self, e: f" filter {self.sql(e, 'this')}", # TODO: Fix: Move to func & add parenthesis for args >= 2.
        }


        _T_ListElement = t.TypeVar("_T_ListElement")
        @staticmethod
        def _partition_list_via_filter(in_list: list[_T_ListElement], criteria: t.Callable[[_T_ListElement], bool]) \
            -> dict[bool, list[_T_ListElement]]:
            """
            Partitions a `list` into two lists: a `True` `list`, and a `False`
            `list`. Both lists contain all their respective elements in input
            order.
            Parameters:
                in_list: The input `list`, that will be partitioned.
                criteria: A unary function that will determine if each element
                    goes in the `True` or `False` output `list`.
            
            Returns:
                A `dict` containing the two output lists. Either/both may be
                    empty.
            """

            true_list = [elem for elem in in_list if criteria(elem)]
            false_list = [elem for elem in in_list if not criteria(elem)]

            return {
                True: true_list,
                False: false_list,
            }
        

        class SelectionPartition(Enum):
            DEFAULT = 0
            SELECT = 1
            DERIVE = 2
            AGGREGATE = 3

            def __str__(self):
                _Self = PRQL.Generator.SelectionPartition

                match self:
                    case _Self.DEFAULT: return ""
                    case _Self.SELECT: return "select"
                    case _Self.DERIVE: return "derive"
                    case _Self.AGGREGATE: return "aggregate"
                    case _: raise ValueError(f"Unsupported {_Self}: {self}")

            @staticmethod
            def _partitions_to_str_list(generator: PRQL.Generator, partitions: dict[PRQL.Generator.SelectionPartition, list[exp.Expression]]) -> list[str]:
                """
                Constructs a `list[str]` from `partitions`.
                Parameters:
                    generator: The `PRQL.Generator` for transforming `exp.Expression`.
                    partitions {key: val}: Each elem of `partitions` corresponds
                        to its respective elem to be returned. Each returned
                        elem is constructed via
                        `_tuple_if_necessary(generator, str(key), val)`.
                """

                return [_tuple_if_necessary(generator, str(k), v) for (k, v) in partitions.items()]  
