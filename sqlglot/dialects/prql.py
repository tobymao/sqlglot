from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    rename_func,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType
from sqlglot.transforms import (
    preprocess,
)
from functools import reduce
from enum import Enum
from sqlglot.parser import logger as logger # TODO: Remove unnecessary imports.


def _select_all(table: exp.Expression) -> t.Optional[exp.Select]:
    return exp.select("*").from_(table, copy=False) if table else None


def _alias_sql(self: PRQL.Generator, expression: exp.Alias) -> str:
    """
    Performs an alias transform. If the `this` attribute is a `Window`,
    passes itself to `_window_sql()`.
    """

    alias_expression = expression.args.get("alias", None)
    this_expression = expression.this

    if alias_expression is None:
        return f"{this_expression}"

    # `Window` expressions handle their aliases.
    if isinstance(this_expression, exp.Window):
        return f"{_window_sql(self, this_expression, expression)}"

    return f"{self.sql(alias_expression)} = {self.sql(this_expression)}"

def _coalesce_sql(self: PRQL.Generator, expression: exp.Coalesce) -> str:
    """
    Performs a coalesce transform.
    """
    
    def _coalesce_expressions_generator() -> t.Generator[exp.Expression, None, None]:
        """
        Iterates through all expressions of `expression`, including
        `expression.this`.
        """
        
        # Prepend `expressions.this`.
        yield expression.this
        return (cur_col for cur_col in expression.expressions)

    expressions_strs = [self.sql(cur_col) for cur_col in _coalesce_expressions_generator()]
    
    joined_expressions = " ?? ".join(expressions_strs)

    return f"{joined_expressions or self.sql(expression.this)}"

def _group_sql(self: PRQL.Generator, expression: exp.Group) -> str:
    """
    Performs a group transform, using attribute `expressions` to group attribute
    `grouping_sets`.
    """

    cols = expression.args.get("expressions", [])
    cols = _tuple_if_necessary(self, "group", cols)

    grouping_sets = expression.args.get("grouping_sets", [])
    grouping_sets = _tuple_if_necessary(self, "", grouping_sets, ("(", ")"), True)
    # Remove the extra space from `""` as a function name in `_tuple_if_necessary()`.
    grouping_sets = _remove_preceding_space(grouping_sets)

    # `None` for `cols` or `grouping_sets` will produce a weird result.
    return f"{cols}{grouping_sets}"

def _join_sql(self: PRQL.Generator, expression: exp.Join) -> str:
    """
    Performs a join transform.
    """

    # Defaults to inner join (defualt join type in PRQL also).
    join_side = f"{expression.args.get('side', '')}".lower()
    relation_to_join = f"{self.sql(expression, 'this')}"
    join_condition = self._get_optional_arg_sql(expression, 'on')

    return (
        f" join"
        f"{f' side:{join_side}' if join_side else ''}"
        f" {relation_to_join}"
        f"{f' ({join_condition})' if join_condition else ''}"
    )

def _list_of_joins_sql(self: PRQL.Generator, expressions: list[exp.Join]) -> str:
    """
    Transforms a sequence of joins.
    """

    # Returns "" if `expressions` is empty.
    return f"{''.join(_join_sql(self, e) for e in expressions)}" # TODO: Implement w/ multiple levels of joins.

def _not_sql(self: PRQL.Generator, expression: exp.Not) -> str:
    """
    Performs a not transform. Attempts to sink not gate to leaf nodes via De
    Morgan's Rule. Removes double not gate.
    """

    modified_arg = expression.this.copy()

    def _modify_arg(type: t.Type[exp.Binary], recursive: t.Callable[[exp.Expression], exp.Expression] = (lambda item: item), base_arg: exp.Binary = modified_arg) -> exp.Expression:
        """
        Constructs a binary operator of `type` from `base_arg` with `recursive`
        applied to it's components.
        """
        
        return type( \
            this=recursive(base_arg.this), \
            expression=recursive(base_arg.expression))
    
    match type(modified_arg):
        # De Morgan's Rule.
        case exp.And: modified_arg = _modify_arg(exp.Or, lambda item: exp.Not(this=item))
        case exp.EQ: modified_arg = _modify_arg(exp.NEQ)
        case exp.Is: modified_arg = _modify_arg(exp.NEQ)
        case exp.NEQ: modified_arg = _modify_arg(exp.EQ)
        # Remove double `Not`.
        case exp.Not: modified_arg = modified_arg.this
        # De Morgan's Rule.
        case exp.Or: modified_arg = _modify_arg(exp.And, lambda item: exp.Not(this=item))
        case _: return _unary_op_factory("not")(self, expression)
    
    return f"{self.sql(modified_arg)}"

def _select_sql(self: PRQL.Generator, expression: exp.Select) -> str:
    """
    Performs a select transform.
    """
    
    all_expressions = expression.args.get("expressions", [])
    
    # Empty `expressions` edge-case.
    if not all_expressions: return f""

    group_by_expressions: t.Optional[list[exp.Expression]] = \
        self._optional_arg_chain(expression, ["group", "expressions"])
    # Remove group cols from `all_expressions`.
    if group_by_expressions:
        group_by_expressions_set = set(group_by_expressions)
        all_expressions = [e for e in all_expressions if e not in group_by_expressions_set]

    all_partitions = _partition_select(self, all_expressions)

    join_expressions = expression.args.get("joins", "")

    # Remove default.
    all_partitions.pop(PRQL.Generator.SelectionPartition.DEFAULT, None)
    ret_val = "".join(
        PRQL.Generator.SelectionPartition._partitions_to_str_list(
            self,
            all_partitions
        )
    )
    ret_val = (
        f"{self.sql(expression, 'order')}"
        f"{self.sql(expression, 'limit')}"
        f"{self.sql(expression, 'where')}"
        f"{ret_val}"
        f"{_list_of_joins_sql(self, join_expressions)}"
    )

    if group_by_expressions:
        # `exp.Group` attribute `grouping_sets` expects a list.
        group_exp = exp.Group(
            expressions=group_by_expressions,
            grouping_sets=[_remove_preceding_space(ret_val)],
        )
        ret_val = self.sql(group_exp)

    ret_val = f"{self.sql(expression, 'from')}{ret_val}"

    return ret_val

def _order_sql(self: PRQL.Generator, expression: exp.Order) -> str:
    """
    Performs an order transform.
    """

    return f"{_tuple_if_necessary(self, 'sort', expression.expressions)}"

def _ordered_sql(self: PRQL.Generator, expression: exp.Ordered) -> str:
    """
    Performs an ordered transform.
    """
    
    # May not contain attribute `desc`, if is ascending.
    is_desc = expression.args.get("desc", False)

    return (
        f"{'-' if is_desc else ''}"
        f"{self.sql(expression, 'this')}"
    )

def _window_sql(self: PRQL.Generator, expression: exp.Window, alias: t.Optional[exp.Alias] = None) -> str:
    """
    Performs a window transform. May be passed an alias, which will be used.
    """
    
    # logger.warning(f"{expression.__repr__()}")
    # TODO: Finish implementation.

    order_by = expression.args.get("order", "")
    order_by = self.sql(order_by)

    window_spec = expression.args.get("spec", "")
    window_spec = self.sql(window_spec)

    window_str = f"{self.sql(expression.this)}"
    window_str = f"{f'window {window_spec} ({window_str})' if window_spec else window_str}"
    window_str = f"{order_by} {window_str})"

    # If in 'group', nest self within an `exp.Group`.
    if partition_by := expression.args.get("partition_by", []):
        group_by = exp.Group(expressions=partition_by, grouping_sets=[window_str])
        return self.sql(group_by)
    
    return window_str

    # -------------------------------------
    # --------------- OLD CODE ------------ # TODO: Remove.
    # -------------------------------------
    
    """
    # TODO: Remove.
    # group_by = self._to_sql_for_each(expression.args.get("partition_by", []))
    # group_by = _tuple_if_necessary(group_by)

    # If in 'group', nest self within an `exp.Group`.
    if group_by := expression.args.get("partition_by", []):
        new_window = expression.copy()
        new_window.args.pop("partition_by")

        # Nest window in alias, if necessary.
        if alias is not None:
            new_alias = alias.copy()
            new_alias.args["this"] = new_window
            new_window = new_alias

        return self.sql(
            exp.Group(expressions=group_by, grouping_sets=[new_window])
        )

    window_rows = None
    window_range = None # TODO: Implement.
    window_expands = False
    window_rolling = None
    
    window_func_has_args = window_rows or window_range or window_expands or window_rolling
    
    window_pipeline = expression.this
    if alias is not None: # Add alias.
        new_alias = alias.copy()
        new_alias.args["this"] = window_pipeline
        window_pipeline = new_alias
        window_pipeline = self.sql(window_pipeline)
        window_pipeline = f"derive {window_pipeline}"
    else:
        window_pipeline = self.sql(window_pipeline)

    # Trailing space appears before 'window'.
    sort_by = f"{self.sql(expression, 'order')} ".removeprefix(' ')
    window_spec = self.sql(expression, "window_spec") # TODO: Use.

    # TODO: Implement.
    # return f"--{expression.__repr__}--"

    # Implicit window.
    if not window_func_has_args:
        return f"{window_pipeline}"

    all_fields = {
        "rows": window_rows,
        "range": window_range,
        "expanding": window_expands,
        "rolling": window_rolling,
        None: f"({window_pipeline})", # No field name.
    }
    # Convert to tuple list.
    all_fields = [(key, item) for key, item in all_fields.items()]
    all_fields = _concat_optional_fields(all_fields, name_delimeter=":")
    
    return f"{sort_by}window {all_fields}"
    """

def _concat_optional_fields(fields: list[t.Tuple[t.Optional[str], t.Optional[str]]], name_delimeter: str = "", delimeter: str = " ") -> str:
    """
    Concatenates fields, skipping `value` that are `None. Optionally, fields
    have a `name`.

    Parameters:
        fields [(name, value)]: A `list` of `(name, value)` pairs. Duplicate names
            allowed. A field with `None` `value` is skipped entirely. A field
            with `None` `name` will use only the `value`, with no `name_delimeter`.
        name_delimeter: The seperator between a `name`, iff it exists, and its
            `value`.
        delimeter: The seperator between `fields`.
    """
    
    def _get_optionally_named_field(name: t.Optional[str], field: str) -> str:
        """
        Creates a `str` for a `field` that optionally has a `name`.
        """

        return f"{field if name is None else f'{name}{name_delimeter}{field}'}"
    
    field_strs = [f"{_get_optionally_named_field(field_name, field)}" for (field_name, field) in fields if field]
    return f"{delimeter.join(field_strs)}"

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
        _PartitionName.AGGREGATE: lambda e: self._is_agg_func(e),
        _PartitionName.DERIVE: lambda e: contains_star and not e.is_star,
        _PartitionName.SELECT: lambda e : True and not e.is_star,
    }
    partitions = _partition_via_crit_high_low(expressions, criteria)

    return partitions

def _remove_preceding_space(str_to_modify: str) -> str:
    """
    Returns a `str` w/ the preceding space removed, if it exists, o/w the
    origonal `str`.
    """

    if str_to_modify.startswith(" "):
        return str_to_modify[1:]
    
    return str_to_modify

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
    joined_expressions = ", ".join(self._to_sql_for_each(expressions))

    tupled_expressions: str; pass # Prevent accidental assignment.
    if force_tuple is None:
        match num_expressions:
            case 0: return f""
            case 1:
                first_exp = expressions[0]
                if _requires_parenthesis(first_exp):
                    tupled_expressions = f"{_form_tuple(joined_expressions)}"
                else:
                    tupled_expressions = f"{joined_expressions}"
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

def _requires_parenthesis(expression: exp.Expression) -> bool:
    """
    Determines if `expression` needs parenthesis in PRQL.
    """

    def _is_PRQL_operator(exp_type: type[exp.Expression]) -> bool:
        """
        Determines if `type(expression)` is a valid PRQL operator, besides a
        funciton call.
        """

        # https://prql-lang.org/book/reference/syntax/operators.html#operator-precedence
        match exp_type:
            case exp.Paren: return True
            case exp.Identifier: return True
            case exp.Not: return True
            case exp.RangeN: return True
            case exp.Pow: return True
            case exp.Mul: return True
            case exp.Div: return True
            case exp.Mod: return True
            case exp.Add: return True
            case exp.Sub: return True
            case exp.EQ: return True
            case exp.NEQ: return True
            case exp.LTE: return True
            case exp.GTE: return True
            case exp.LT: return True
            case exp.GT: return True
            case exp.Coalesce: return True
            case exp.And: return True
            case exp.Or: return True
            case _: return False

    exp_type = type(expression)
    
    match exp_type:
        case exp.Column: return False
        case exp.Identifier: return False
        case exp.Literal: return False
        case exp.Ordered: return _requires_parenthesis(expression.this)

    if _is_PRQL_operator(exp_type):
        match exp_type:
            case c if issubclass(c, exp.Binary):
                return _requires_parenthesis(expression.left) \
                    or _requires_parenthesis(expression.right)
            case c if issubclass(c, exp.Unary):
                return _requires_parenthesis(expression.this)
            case _: return True
    
    return True

def _union_op_factory(operator: str) -> t.Callable[[generator.Generator, exp.Expression], str]:
    """
    Constructs a function that takes a `generator.Generator` and an
    `exp.Expression` and transforms it into a union of type `operator`.
    """
    
    def _union_op_func(self, expression: exp.Expression) -> str:
        left_arg = self.sql(expression.left)

        # Copy right side & make necessary edits.
        right_copy = expression.right.copy()
        right_cols = self.sql(right_copy.args.pop('from'), 'this')
        right_arg = self.sql(right_copy)

        return f"{left_arg} {operator} {right_cols} {right_arg}"
    
    return _union_op_func

def _binary_op_factory(operator: str) -> t.Callable[[generator.Generator, exp.Expression], str]:
    """
    Constructs a function that takes a `generator.Generator` and an
    `exp.Expression` and transforms it into a binary function of type `operator`.
    """

    def _binary_op_func(self, expression: exp.Expression) -> str:
        arg0 = self.sql(expression, 'this')
        arg1 = self.sql(expression, 'expression')

        return f"{arg0} {operator} {arg1}" # No preceding space.

    return _binary_op_func

def _unary_op_factory(operator: str) -> t.Callable[[generator.Generator, exp.Expression], str]:
    """
    Constructs a function that takes a `generator.Generator` and an
    `exp.Expression` and transforms it into a unary function of type `operator`.
    """
    
    def _unary_op_func(self, expression: exp.Expression) -> str:
        # No preceding space.
        return f"{operator} {self.sql(expression, 'this')}"
    
    return _unary_op_func


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
            exp.Alias: _alias_sql,
            exp.And: _binary_op_factory("&&"),
            exp.Avg: _unary_op_factory("average"),
            exp.Coalesce: _coalesce_sql,
            # exp.Comment: , # TODO: Implement: No multiline comments: https://prql-lang.org/book/reference/syntax/comments.html
            exp.Count: _unary_op_factory("count"),
            exp.EQ: _binary_op_factory("=="),
            exp.Except: _union_op_factory("remove"),
            exp.From: lambda self, e: f" from {e.this}",
            exp.Group: _group_sql,
            exp.Intersect: _union_op_factory("intersect"),
            exp.Is: _binary_op_factory("=="), # TODO: Fix?
            exp.Join: _join_sql,
            exp.Limit: lambda self, e: f" take {self.sql(e, 'expression')}",
            exp.Max: _unary_op_factory("max"),
            exp.Min: _unary_op_factory("min"),
            exp.NEQ: _binary_op_factory("!="),
            exp.Not: _not_sql,
            exp.Null: lambda self, e: f"null", # TODO: Fix null eval, diff than sql: https://prql-lang.org/book/reference/spec/null.html
            exp.Or: _binary_op_factory("||"),
            exp.Order: _order_sql,
            exp.Ordered: _ordered_sql,
            exp.Paren: lambda self, e: f"({self.sql(e, 'this')})",
            # exp.Partition: lambda self, e: f"partition", # TODO: Implement.
            # exp.Query: lambda self, e: f"{e.From} {e.Where} {e.Select}", # TODO: Test.
            #exp.RawString: , # TODO: Implement: Can have ' or ", any odd num: https://prql-lang.org/book/reference/syntax/strings.html
            # exp.RenameColumn: lambda self, e: f"{self.sql(e, 'this')}{self.sql(e, 'to')}{self.sql(e, 'exists')}", # TODO: Test.
            exp.Select: _select_sql,
            #exp.Sort: lambda self, e: f" sort {self.sql(e, 'expressions')}", # TODO: Test.
            exp.Star: lambda self, e: "*",
            exp.Stddev: _unary_op_factory("stddev"),
            exp.Sum: _unary_op_factory("sum"),
            #exp.Tuple: lambda self, e: f"", # TODO: Implement: `expression` is optional.
            exp.Union: _union_op_factory("append"),
            exp.Where: lambda self, e: f" filter {self.sql(e, 'this')}", # TODO: Fix: Move to func & add parenthesis for args >= 2.
            # exp.Where: lambda self, e: f" filter {_tuple_if_necessary(self, None, [e.this])}",
            exp.Window: _window_sql,
            exp.WindowSpec: lambda self, e: f"{e}", # TODO: Implement.
            # TODO: Add more transforms.
        }
        
        # def _join_condition_insert_this_that_self_equality_sql(self, expression: exp.Join) -> str:
        #     # TODO: Implement, maybe as special equality transform.
        #
        #     return self._get_optional_arg_sql(expression, 'on')

        def _get_optional_arg_sql(self, expression: exp.Expression, key: str) -> str:
            """
            Transforms the expression with the given key, if it exists, o/w
            returns the empty `str`.
            """
            
            return f"{self.sql(expression, key) if key in expression.args else ''}"

        def _optional_arg_chain(self, expression: exp.Expression, keys: list[str]) -> t.Optional[exp.Expression | t.Any]:
            """
            Recursively retrieves the current attribute of `keys` from
            `expression`, returning the final result. If any key doesn't exist,
            returns `None`.
            """

            def _optional_expression_retrieve_attribute(e: t.Optional[exp.Expression], attribute_name: str) -> t.Optional[exp.Expression]:
                """
                Retrieves the attribute `attribute_name` of `e` iff `e` is not
                `None` and `e` contains `attribute_name`, o/w returns `None`.
                """
                
                return None if e is None else e.args.get(attribute_name, None)

            return reduce( \
                _optional_expression_retrieve_attribute, \
                keys, \
                expression \
            )

        def _is_agg_func(self, expression: exp.Expression) -> bool:
            """
            Determines if `expression` is an aggregation function.
            `exp.Coalesce` aggregate if any components do.
            """

            def _agg_reduce_or(expression_list: list[exp.Expression]) -> bool:
                """
                Determines if at least one `expression` in `expression_list` is
                an aggregation function.
                """

                return reduce(
                    lambda acc, item: acc or self._is_agg_func(item), \
                    expression_list, \
                    False)

            match type(expression):
                case exp.Alias:
                    return self._is_agg_func(expression.this)
                case exp.Coalesce:
                    # True if any `expression` is True.
                    return self._is_agg_func(expression.this) or _agg_reduce_or(expression.expressions)
                case exp.Avg | exp.Count | exp.Max | exp.Min | exp.Stddev | exp.Sum:
                    return True
                case _:
                    return False

        def _to_sql_for_each(self, items: list[exp.Expression], concat_with: t.Optional[str] = None) -> list[str] | str:
            """
            Runs `self.sql(item)` on all elements of `items`. Concats iff
            `concat_with` isn't `None`.

            Parameters:
                items [item]: Items to convert to sql via `self.sql(item)`.
                concat_with: A `str` to concat the converted `items`. Iff `None`,
                    output remains a `list`.

            Returns:
                A `str` of the concat converted `items`, or a `list` of
                    converted `items`, iff `concat_with` is `None`.
            """
            
            items_stringified = [f"{self.sql(item)}" for item in items]
            
            return items_stringified if concat_with is None else f"{concat_with.join(items_stringified)}"

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
            # GROUP = 4 # TODO: Remove.
            # JOIN = 5

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
