"""
## Expressions

Every AST node in SQLGlot is represented by a subclass of `Expression`.

This module contains the implementation of all supported `Expression` types. Additionally,
it exposes a number of helper functions, which are mainly used to programmatically build
SQL expressions, such as `sqlglot.expressions.select`.

----
"""

from __future__ import annotations

import datetime
import math
import numbers
import re
import typing as t
from collections import deque
from copy import deepcopy
from enum import auto

from sqlglot._typing import E
from sqlglot.errors import ParseError
from sqlglot.helper import (
    AutoName,
    camel_to_snake_case,
    ensure_collection,
    ensure_list,
    seq_get,
    subclasses,
)
from sqlglot.tokens import Token

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


class _Expression(type):
    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)

        # When an Expression class is created, its key is automatically set to be
        # the lowercase version of the class' name.
        klass.key = clsname.lower()

        # This is so that docstrings are not inherited in pdoc
        klass.__doc__ = klass.__doc__ or ""

        return klass


class Expression(metaclass=_Expression):
    """
    The base class for all expressions in a syntax tree. Each Expression encapsulates any necessary
    context, such as its child expressions, their names (arg keys), and whether a given child expression
    is optional or not.

    Attributes:
        key: a unique key for each class in the Expression hierarchy. This is useful for hashing
            and representing expressions as strings.
        arg_types: determines what arguments (child nodes) are supported by an expression. It
            maps arg keys to booleans that indicate whether the corresponding args are optional.
        parent: a reference to the parent expression (or None, in case of root expressions).
        arg_key: the arg key an expression is associated with, i.e. the name its parent expression
            uses to refer to it.
        comments: a list of comments that are associated with a given expression. This is used in
            order to preserve comments when transpiling SQL code.
        _type: the `sqlglot.expressions.DataType` type of an expression. This is inferred by the
            optimizer, in order to enable some transformations that require type information.

    Example:
        >>> class Foo(Expression):
        ...     arg_types = {"this": True, "expression": False}

        The above definition informs us that Foo is an Expression that requires an argument called
        "this" and may also optionally receive an argument called "expression".

    Args:
        args: a mapping used for retrieving the arguments of an expression, given their arg keys.
    """

    key = "expression"
    arg_types = {"this": True}
    __slots__ = ("args", "parent", "arg_key", "comments", "_type", "_meta", "_hash")

    def __init__(self, **args: t.Any):
        self.args: t.Dict[str, t.Any] = args
        self.parent: t.Optional[Expression] = None
        self.arg_key: t.Optional[str] = None
        self.comments: t.Optional[t.List[str]] = None
        self._type: t.Optional[DataType] = None
        self._meta: t.Optional[t.Dict[str, t.Any]] = None
        self._hash: t.Optional[int] = None

        for arg_key, value in self.args.items():
            self._set_parent(arg_key, value)

    def __eq__(self, other) -> bool:
        return type(self) is type(other) and hash(self) == hash(other)

    @property
    def hashable_args(self) -> t.Any:
        return frozenset(
            (k, tuple(_norm_arg(a) for a in v) if type(v) is list else _norm_arg(v))
            for k, v in self.args.items()
            if not (v is None or v is False or (type(v) is list and not v))
        )

    def __hash__(self) -> int:
        if self._hash is not None:
            return self._hash

        return hash((self.__class__, self.hashable_args))

    @property
    def this(self):
        """
        Retrieves the argument with key "this".
        """
        return self.args.get("this")

    @property
    def expression(self):
        """
        Retrieves the argument with key "expression".
        """
        return self.args.get("expression")

    @property
    def expressions(self):
        """
        Retrieves the argument with key "expressions".
        """
        return self.args.get("expressions") or []

    def text(self, key) -> str:
        """
        Returns a textual representation of the argument corresponding to "key". This can only be used
        for args that are strings or leaf Expression instances, such as identifiers and literals.
        """
        field = self.args.get(key)
        if isinstance(field, str):
            return field
        if isinstance(field, (Identifier, Literal, Var)):
            return field.this
        if isinstance(field, (Star, Null)):
            return field.name
        return ""

    @property
    def is_string(self) -> bool:
        """
        Checks whether a Literal expression is a string.
        """
        return isinstance(self, Literal) and self.args["is_string"]

    @property
    def is_number(self) -> bool:
        """
        Checks whether a Literal expression is a number.
        """
        return isinstance(self, Literal) and not self.args["is_string"]

    @property
    def is_int(self) -> bool:
        """
        Checks whether a Literal expression is an integer.
        """
        if self.is_number:
            try:
                int(self.name)
                return True
            except ValueError:
                pass
        return False

    @property
    def is_star(self) -> bool:
        """Checks whether an expression is a star."""
        return isinstance(self, Star) or (isinstance(self, Column) and isinstance(self.this, Star))

    @property
    def alias(self) -> str:
        """
        Returns the alias of the expression, or an empty string if it's not aliased.
        """
        if isinstance(self.args.get("alias"), TableAlias):
            return self.args["alias"].name
        return self.text("alias")

    @property
    def name(self) -> str:
        return self.text("this")

    @property
    def alias_or_name(self) -> str:
        return self.alias or self.name

    @property
    def output_name(self) -> str:
        """
        Name of the output column if this expression is a selection.

        If the Expression has no output name, an empty string is returned.

        Example:
            >>> from sqlglot import parse_one
            >>> parse_one("SELECT a").expressions[0].output_name
            'a'
            >>> parse_one("SELECT b AS c").expressions[0].output_name
            'c'
            >>> parse_one("SELECT 1 + 2").expressions[0].output_name
            ''
        """
        return ""

    @property
    def type(self) -> t.Optional[DataType]:
        return self._type

    @type.setter
    def type(self, dtype: t.Optional[DataType | DataType.Type | str]) -> None:
        if dtype and not isinstance(dtype, DataType):
            dtype = DataType.build(dtype)
        self._type = dtype  # type: ignore

    @property
    def meta(self) -> t.Dict[str, t.Any]:
        if self._meta is None:
            self._meta = {}
        return self._meta

    def __deepcopy__(self, memo):
        copy = self.__class__(**deepcopy(self.args))
        if self.comments is not None:
            copy.comments = deepcopy(self.comments)

        if self._type is not None:
            copy._type = self._type.copy()

        if self._meta is not None:
            copy._meta = deepcopy(self._meta)

        return copy

    def copy(self):
        """
        Returns a deep copy of the expression.
        """
        new = deepcopy(self)
        new.parent = self.parent
        return new

    def add_comments(self, comments: t.Optional[t.List[str]]) -> None:
        if self.comments is None:
            self.comments = []
        if comments:
            self.comments.extend(comments)

    def append(self, arg_key: str, value: t.Any) -> None:
        """
        Appends value to arg_key if it's a list or sets it as a new list.

        Args:
            arg_key (str): name of the list expression arg
            value (Any): value to append to the list
        """
        if not isinstance(self.args.get(arg_key), list):
            self.args[arg_key] = []
        self.args[arg_key].append(value)
        self._set_parent(arg_key, value)

    def set(self, arg_key: str, value: t.Any) -> None:
        """
        Sets arg_key to value.

        Args:
            arg_key: name of the expression arg.
            value: value to set the arg to.
        """
        if value is None:
            self.args.pop(arg_key, None)
            return

        self.args[arg_key] = value
        self._set_parent(arg_key, value)

    def _set_parent(self, arg_key: str, value: t.Any) -> None:
        if hasattr(value, "parent"):
            value.parent = self
            value.arg_key = arg_key
        elif type(value) is list:
            for v in value:
                if hasattr(v, "parent"):
                    v.parent = self
                    v.arg_key = arg_key

    @property
    def depth(self) -> int:
        """
        Returns the depth of this tree.
        """
        if self.parent:
            return self.parent.depth + 1
        return 0

    def iter_expressions(self) -> t.Iterator[t.Tuple[str, Expression]]:
        """Yields the key and expression for all arguments, exploding list args."""
        for k, vs in self.args.items():
            if type(vs) is list:
                for v in vs:
                    if hasattr(v, "parent"):
                        yield k, v
            else:
                if hasattr(vs, "parent"):
                    yield k, vs

    def find(self, *expression_types: t.Type[E], bfs: bool = True) -> t.Optional[E]:
        """
        Returns the first node in this tree which matches at least one of
        the specified types.

        Args:
            expression_types: the expression type(s) to match.
            bfs: whether to search the AST using the BFS algorithm (DFS is used if false).

        Returns:
            The node which matches the criteria or None if no such node was found.
        """
        return next(self.find_all(*expression_types, bfs=bfs), None)

    def find_all(self, *expression_types: t.Type[E], bfs: bool = True) -> t.Iterator[E]:
        """
        Returns a generator object which visits all nodes in this tree and only
        yields those that match at least one of the specified expression types.

        Args:
            expression_types: the expression type(s) to match.
            bfs: whether to search the AST using the BFS algorithm (DFS is used if false).

        Returns:
            The generator object.
        """
        for expression, *_ in self.walk(bfs=bfs):
            if isinstance(expression, expression_types):
                yield expression

    def find_ancestor(self, *expression_types: t.Type[E]) -> t.Optional[E]:
        """
        Returns a nearest parent matching expression_types.

        Args:
            expression_types: the expression type(s) to match.

        Returns:
            The parent node.
        """
        ancestor = self.parent
        while ancestor and not isinstance(ancestor, expression_types):
            ancestor = ancestor.parent
        return t.cast(E, ancestor)

    @property
    def parent_select(self) -> t.Optional[Select]:
        """
        Returns the parent select statement.
        """
        return self.find_ancestor(Select)

    @property
    def same_parent(self) -> bool:
        """Returns if the parent is the same class as itself."""
        return type(self.parent) is self.__class__

    def root(self) -> Expression:
        """
        Returns the root expression of this tree.
        """
        expression = self
        while expression.parent:
            expression = expression.parent
        return expression

    def walk(self, bfs=True, prune=None):
        """
        Returns a generator object which visits all nodes in this tree.

        Args:
            bfs (bool): if set to True the BFS traversal order will be applied,
                otherwise the DFS traversal will be used instead.
            prune ((node, parent, arg_key) -> bool): callable that returns True if
                the generator should stop traversing this branch of the tree.

        Returns:
            the generator object.
        """
        if bfs:
            yield from self.bfs(prune=prune)
        else:
            yield from self.dfs(prune=prune)

    def dfs(self, parent=None, key=None, prune=None):
        """
        Returns a generator object which visits all nodes in this tree in
        the DFS (Depth-first) order.

        Returns:
            The generator object.
        """
        parent = parent or self.parent
        yield self, parent, key
        if prune and prune(self, parent, key):
            return

        for k, v in self.iter_expressions():
            yield from v.dfs(self, k, prune)

    def bfs(self, prune=None):
        """
        Returns a generator object which visits all nodes in this tree in
        the BFS (Breadth-first) order.

        Returns:
            The generator object.
        """
        queue = deque([(self, self.parent, None)])

        while queue:
            item, parent, key = queue.popleft()

            yield item, parent, key
            if prune and prune(item, parent, key):
                continue

            for k, v in item.iter_expressions():
                queue.append((v, item, k))

    def unnest(self):
        """
        Returns the first non parenthesis child or self.
        """
        expression = self
        while type(expression) is Paren:
            expression = expression.this
        return expression

    def unalias(self):
        """
        Returns the inner expression if this is an Alias.
        """
        if isinstance(self, Alias):
            return self.this
        return self

    def unnest_operands(self):
        """
        Returns unnested operands as a tuple.
        """
        return tuple(arg.unnest() for _, arg in self.iter_expressions())

    def flatten(self, unnest=True):
        """
        Returns a generator which yields child nodes who's parents are the same class.

        A AND B AND C -> [A, B, C]
        """
        for node, _, _ in self.dfs(prune=lambda n, p, *_: p and not type(n) is self.__class__):
            if not type(node) is self.__class__:
                yield node.unnest() if unnest else node

    def __str__(self) -> str:
        return self.sql()

    def __repr__(self) -> str:
        return self._to_s()

    def sql(self, dialect: DialectType = None, **opts) -> str:
        """
        Returns SQL string representation of this tree.

        Args:
            dialect: the dialect of the output SQL string (eg. "spark", "hive", "presto", "mysql").
            opts: other `sqlglot.generator.Generator` options.

        Returns:
            The SQL string.
        """
        from sqlglot.dialects import Dialect

        return Dialect.get_or_raise(dialect)().generate(self, **opts)

    def _to_s(self, hide_missing: bool = True, level: int = 0) -> str:
        indent = "" if not level else "\n"
        indent += "".join(["  "] * level)
        left = f"({self.key.upper()} "

        args: t.Dict[str, t.Any] = {
            k: ", ".join(
                v._to_s(hide_missing=hide_missing, level=level + 1)
                if hasattr(v, "_to_s")
                else str(v)
                for v in ensure_list(vs)
                if v is not None
            )
            for k, vs in self.args.items()
        }
        args["comments"] = self.comments
        args["type"] = self.type
        args = {k: v for k, v in args.items() if v or not hide_missing}

        right = ", ".join(f"{k}: {v}" for k, v in args.items())
        right += ")"

        return indent + left + right

    def transform(self, fun, *args, copy=True, **kwargs):
        """
        Recursively visits all tree nodes (excluding already transformed ones)
        and applies the given transformation function to each node.

        Args:
            fun (function): a function which takes a node as an argument and returns a
                new transformed node or the same node without modifications. If the function
                returns None, then the corresponding node will be removed from the syntax tree.
            copy (bool): if set to True a new tree instance is constructed, otherwise the tree is
                modified in place.

        Returns:
            The transformed tree.
        """
        node = self.copy() if copy else self
        new_node = fun(node, *args, **kwargs)

        if new_node is None or not isinstance(new_node, Expression):
            return new_node
        if new_node is not node:
            new_node.parent = node.parent
            return new_node

        replace_children(new_node, lambda child: child.transform(fun, *args, copy=False, **kwargs))
        return new_node

    @t.overload
    def replace(self, expression: E) -> E:
        ...

    @t.overload
    def replace(self, expression: None) -> None:
        ...

    def replace(self, expression):
        """
        Swap out this expression with a new expression.

        For example::

            >>> tree = Select().select("x").from_("tbl")
            >>> tree.find(Column).replace(Column(this="y"))
            (COLUMN this: y)
            >>> tree.sql()
            'SELECT y FROM tbl'

        Args:
            expression: new node

        Returns:
            The new expression or expressions.
        """
        if not self.parent:
            return expression

        parent = self.parent
        self.parent = None

        replace_children(parent, lambda child: expression if child is self else child)
        return expression

    def pop(self: E) -> E:
        """
        Remove this expression from its AST.

        Returns:
            The popped expression.
        """
        self.replace(None)
        return self

    def assert_is(self, type_: t.Type[E]) -> E:
        """
        Assert that this `Expression` is an instance of `type_`.

        If it is NOT an instance of `type_`, this raises an assertion error.
        Otherwise, this returns this expression.

        Examples:
            This is useful for type security in chained expressions:

            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT x from y").assert_is(Select).select("z").sql()
            'SELECT x, z FROM y'
        """
        assert isinstance(self, type_)
        return self

    def error_messages(self, args: t.Optional[t.Sequence] = None) -> t.List[str]:
        """
        Checks if this expression is valid (e.g. all mandatory args are set).

        Args:
            args: a sequence of values that were used to instantiate a Func expression. This is used
                to check that the provided arguments don't exceed the function argument limit.

        Returns:
            A list of error messages for all possible errors that were found.
        """
        errors: t.List[str] = []

        for k in self.args:
            if k not in self.arg_types:
                errors.append(f"Unexpected keyword: '{k}' for {self.__class__}")
        for k, mandatory in self.arg_types.items():
            v = self.args.get(k)
            if mandatory and (v is None or (isinstance(v, list) and not v)):
                errors.append(f"Required keyword: '{k}' missing for {self.__class__}")

        if (
            args
            and isinstance(self, Func)
            and len(args) > len(self.arg_types)
            and not self.is_var_len_args
        ):
            errors.append(
                f"The number of provided arguments ({len(args)}) is greater than "
                f"the maximum number of supported arguments ({len(self.arg_types)})"
            )

        return errors

    def dump(self):
        """
        Dump this Expression to a JSON-serializable dict.
        """
        from sqlglot.serde import dump

        return dump(self)

    @classmethod
    def load(cls, obj):
        """
        Load a dict (as returned by `Expression.dump`) into an Expression instance.
        """
        from sqlglot.serde import load

        return load(obj)


IntoType = t.Union[
    str,
    t.Type[Expression],
    t.Collection[t.Union[str, t.Type[Expression]]],
]
ExpOrStr = t.Union[str, Expression]


class Condition(Expression):
    def and_(
        self,
        *expressions: t.Optional[ExpOrStr],
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Condition:
        """
        AND this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").and_("y=1").sql()
            'x = 1 AND y = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect: the dialect used to parse the input expression.
            copy: whether or not to copy the involved expressions (only applies to Expressions).
            opts: other options to use to parse the input expressions.

        Returns:
            The new And condition.
        """
        return and_(self, *expressions, dialect=dialect, copy=copy, **opts)

    def or_(
        self,
        *expressions: t.Optional[ExpOrStr],
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Condition:
        """
        OR this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").or_("y=1").sql()
            'x = 1 OR y = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect: the dialect used to parse the input expression.
            copy: whether or not to copy the involved expressions (only applies to Expressions).
            opts: other options to use to parse the input expressions.

        Returns:
            The new Or condition.
        """
        return or_(self, *expressions, dialect=dialect, copy=copy, **opts)

    def not_(self, copy: bool = True):
        """
        Wrap this condition with NOT.

        Example:
            >>> condition("x=1").not_().sql()
            'NOT x = 1'

        Args:
            copy: whether or not to copy this object.

        Returns:
            The new Not instance.
        """
        return not_(self, copy=copy)

    def as_(
        self,
        alias: str | Identifier,
        quoted: t.Optional[bool] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Alias:
        return alias_(self, alias, quoted=quoted, dialect=dialect, copy=copy, **opts)

    def _binop(self, klass: t.Type[E], other: t.Any, reverse: bool = False) -> E:
        this = self.copy()
        other = convert(other, copy=True)
        if not isinstance(this, klass) and not isinstance(other, klass):
            this = _wrap(this, Binary)
            other = _wrap(other, Binary)
        if reverse:
            return klass(this=other, expression=this)
        return klass(this=this, expression=other)

    def __getitem__(self, other: ExpOrStr | t.Tuple[ExpOrStr]):
        return Bracket(
            this=self.copy(), expressions=[convert(e, copy=True) for e in ensure_list(other)]
        )

    def isin(
        self,
        *expressions: t.Any,
        query: t.Optional[ExpOrStr] = None,
        unnest: t.Optional[ExpOrStr] | t.Collection[ExpOrStr] = None,
        copy: bool = True,
        **opts,
    ) -> In:
        return In(
            this=maybe_copy(self, copy),
            expressions=[convert(e, copy=copy) for e in expressions],
            query=maybe_parse(query, copy=copy, **opts) if query else None,
            unnest=Unnest(
                expressions=[
                    maybe_parse(t.cast(ExpOrStr, e), copy=copy, **opts) for e in ensure_list(unnest)
                ]
            )
            if unnest
            else None,
        )

    def between(self, low: t.Any, high: t.Any, copy: bool = True, **opts) -> Between:
        return Between(
            this=maybe_copy(self, copy),
            low=convert(low, copy=copy, **opts),
            high=convert(high, copy=copy, **opts),
        )

    def is_(self, other: ExpOrStr) -> Is:
        return self._binop(Is, other)

    def like(self, other: ExpOrStr) -> Like:
        return self._binop(Like, other)

    def ilike(self, other: ExpOrStr) -> ILike:
        return self._binop(ILike, other)

    def eq(self, other: t.Any) -> EQ:
        return self._binop(EQ, other)

    def neq(self, other: t.Any) -> NEQ:
        return self._binop(NEQ, other)

    def rlike(self, other: ExpOrStr) -> RegexpLike:
        return self._binop(RegexpLike, other)

    def __lt__(self, other: t.Any) -> LT:
        return self._binop(LT, other)

    def __le__(self, other: t.Any) -> LTE:
        return self._binop(LTE, other)

    def __gt__(self, other: t.Any) -> GT:
        return self._binop(GT, other)

    def __ge__(self, other: t.Any) -> GTE:
        return self._binop(GTE, other)

    def __add__(self, other: t.Any) -> Add:
        return self._binop(Add, other)

    def __radd__(self, other: t.Any) -> Add:
        return self._binop(Add, other, reverse=True)

    def __sub__(self, other: t.Any) -> Sub:
        return self._binop(Sub, other)

    def __rsub__(self, other: t.Any) -> Sub:
        return self._binop(Sub, other, reverse=True)

    def __mul__(self, other: t.Any) -> Mul:
        return self._binop(Mul, other)

    def __rmul__(self, other: t.Any) -> Mul:
        return self._binop(Mul, other, reverse=True)

    def __truediv__(self, other: t.Any) -> Div:
        return self._binop(Div, other)

    def __rtruediv__(self, other: t.Any) -> Div:
        return self._binop(Div, other, reverse=True)

    def __floordiv__(self, other: t.Any) -> IntDiv:
        return self._binop(IntDiv, other)

    def __rfloordiv__(self, other: t.Any) -> IntDiv:
        return self._binop(IntDiv, other, reverse=True)

    def __mod__(self, other: t.Any) -> Mod:
        return self._binop(Mod, other)

    def __rmod__(self, other: t.Any) -> Mod:
        return self._binop(Mod, other, reverse=True)

    def __pow__(self, other: t.Any) -> Pow:
        return self._binop(Pow, other)

    def __rpow__(self, other: t.Any) -> Pow:
        return self._binop(Pow, other, reverse=True)

    def __and__(self, other: t.Any) -> And:
        return self._binop(And, other)

    def __rand__(self, other: t.Any) -> And:
        return self._binop(And, other, reverse=True)

    def __or__(self, other: t.Any) -> Or:
        return self._binop(Or, other)

    def __ror__(self, other: t.Any) -> Or:
        return self._binop(Or, other, reverse=True)

    def __neg__(self) -> Neg:
        return Neg(this=_wrap(self.copy(), Binary))

    def __invert__(self) -> Not:
        return not_(self.copy())


class Predicate(Condition):
    """Relationships like x = y, x > 1, x >= y."""


class DerivedTable(Expression):
    @property
    def alias_column_names(self) -> t.List[str]:
        table_alias = self.args.get("alias")
        if not table_alias:
            return []
        return [c.name for c in table_alias.args.get("columns") or []]

    @property
    def selects(self) -> t.List[Expression]:
        return self.this.selects if isinstance(self.this, Subqueryable) else []

    @property
    def named_selects(self) -> t.List[str]:
        return [select.output_name for select in self.selects]


class Unionable(Expression):
    def union(
        self, expression: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
    ) -> Unionable:
        """
        Builds a UNION expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").union("SELECT * FROM bla").sql()
            'SELECT * FROM foo UNION SELECT * FROM bla'

        Args:
            expression: the SQL code string.
                If an `Expression` instance is passed, it will be used as-is.
            distinct: set the DISTINCT flag if and only if this is true.
            dialect: the dialect used to parse the input expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Union expression.
        """
        return union(left=self, right=expression, distinct=distinct, dialect=dialect, **opts)

    def intersect(
        self, expression: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
    ) -> Unionable:
        """
        Builds an INTERSECT expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").intersect("SELECT * FROM bla").sql()
            'SELECT * FROM foo INTERSECT SELECT * FROM bla'

        Args:
            expression: the SQL code string.
                If an `Expression` instance is passed, it will be used as-is.
            distinct: set the DISTINCT flag if and only if this is true.
            dialect: the dialect used to parse the input expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Intersect expression.
        """
        return intersect(left=self, right=expression, distinct=distinct, dialect=dialect, **opts)

    def except_(
        self, expression: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
    ) -> Unionable:
        """
        Builds an EXCEPT expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").except_("SELECT * FROM bla").sql()
            'SELECT * FROM foo EXCEPT SELECT * FROM bla'

        Args:
            expression: the SQL code string.
                If an `Expression` instance is passed, it will be used as-is.
            distinct: set the DISTINCT flag if and only if this is true.
            dialect: the dialect used to parse the input expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Except expression.
        """
        return except_(left=self, right=expression, distinct=distinct, dialect=dialect, **opts)


class UDTF(DerivedTable, Unionable):
    @property
    def selects(self) -> t.List[Expression]:
        alias = self.args.get("alias")
        return alias.columns if alias else []


class Cache(Expression):
    arg_types = {
        "with": False,
        "this": True,
        "lazy": False,
        "options": False,
        "expression": False,
    }


class Uncache(Expression):
    arg_types = {"this": True, "exists": False}


class Create(Expression):
    arg_types = {
        "with": False,
        "this": True,
        "kind": True,
        "expression": False,
        "exists": False,
        "properties": False,
        "replace": False,
        "unique": False,
        "indexes": False,
        "no_schema_binding": False,
        "begin": False,
        "clone": False,
    }


# https://docs.snowflake.com/en/sql-reference/sql/create-clone
class Clone(Expression):
    arg_types = {
        "this": True,
        "when": False,
        "kind": False,
        "expression": False,
    }


class Describe(Expression):
    arg_types = {"this": True, "kind": False}


class Pragma(Expression):
    pass


class Set(Expression):
    arg_types = {"expressions": False, "unset": False, "tag": False}


class SetItem(Expression):
    arg_types = {
        "this": False,
        "expressions": False,
        "kind": False,
        "collate": False,  # MySQL SET NAMES statement
        "global": False,
    }


class Show(Expression):
    arg_types = {
        "this": True,
        "target": False,
        "offset": False,
        "limit": False,
        "like": False,
        "where": False,
        "db": False,
        "full": False,
        "mutex": False,
        "query": False,
        "channel": False,
        "global": False,
        "log": False,
        "position": False,
        "types": False,
    }


class UserDefinedFunction(Expression):
    arg_types = {"this": True, "expressions": False, "wrapped": False}


class CharacterSet(Expression):
    arg_types = {"this": True, "default": False}


class With(Expression):
    arg_types = {"expressions": True, "recursive": False}

    @property
    def recursive(self) -> bool:
        return bool(self.args.get("recursive"))


class WithinGroup(Expression):
    arg_types = {"this": True, "expression": False}


class CTE(DerivedTable):
    arg_types = {"this": True, "alias": True}


class TableAlias(Expression):
    arg_types = {"this": False, "columns": False}

    @property
    def columns(self):
        return self.args.get("columns") or []


class BitString(Condition):
    pass


class HexString(Condition):
    pass


class ByteString(Condition):
    pass


class RawString(Condition):
    pass


class Column(Condition):
    arg_types = {"this": True, "table": False, "db": False, "catalog": False, "join_mark": False}

    @property
    def table(self) -> str:
        return self.text("table")

    @property
    def db(self) -> str:
        return self.text("db")

    @property
    def catalog(self) -> str:
        return self.text("catalog")

    @property
    def output_name(self) -> str:
        return self.name

    @property
    def parts(self) -> t.List[Identifier]:
        """Return the parts of a column in order catalog, db, table, name."""
        return [
            t.cast(Identifier, self.args[part])
            for part in ("catalog", "db", "table", "this")
            if self.args.get(part)
        ]

    def to_dot(self) -> Dot:
        """Converts the column into a dot expression."""
        parts = self.parts
        parent = self.parent

        while parent:
            if isinstance(parent, Dot):
                parts.append(parent.expression)
            parent = parent.parent

        return Dot.build(parts)


class ColumnPosition(Expression):
    arg_types = {"this": False, "position": True}


class ColumnDef(Expression):
    arg_types = {
        "this": True,
        "kind": False,
        "constraints": False,
        "exists": False,
        "position": False,
    }

    @property
    def constraints(self) -> t.List[ColumnConstraint]:
        return self.args.get("constraints") or []


class AlterColumn(Expression):
    arg_types = {
        "this": True,
        "dtype": False,
        "collate": False,
        "using": False,
        "default": False,
        "drop": False,
    }


class RenameTable(Expression):
    pass


class Comment(Expression):
    arg_types = {"this": True, "kind": True, "expression": True, "exists": False}


# https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#mergetree-table-ttl
class MergeTreeTTLAction(Expression):
    arg_types = {
        "this": True,
        "delete": False,
        "recompress": False,
        "to_disk": False,
        "to_volume": False,
    }


# https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#mergetree-table-ttl
class MergeTreeTTL(Expression):
    arg_types = {
        "expressions": True,
        "where": False,
        "group": False,
        "aggregates": False,
    }


# https://dev.mysql.com/doc/refman/8.0/en/create-table.html
class IndexConstraintOption(Expression):
    arg_types = {
        "key_block_size": False,
        "using": False,
        "parser": False,
        "comment": False,
        "visible": False,
        "engine_attr": False,
        "secondary_engine_attr": False,
    }


class ColumnConstraint(Expression):
    arg_types = {"this": False, "kind": True}

    @property
    def kind(self) -> ColumnConstraintKind:
        return self.args["kind"]


class ColumnConstraintKind(Expression):
    pass


class AutoIncrementColumnConstraint(ColumnConstraintKind):
    pass


class CaseSpecificColumnConstraint(ColumnConstraintKind):
    arg_types = {"not_": True}


class CharacterSetColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True}


class CheckColumnConstraint(ColumnConstraintKind):
    pass


class CollateColumnConstraint(ColumnConstraintKind):
    pass


class CommentColumnConstraint(ColumnConstraintKind):
    pass


class CompressColumnConstraint(ColumnConstraintKind):
    pass


class DateFormatColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True}


class DefaultColumnConstraint(ColumnConstraintKind):
    pass


class EncodeColumnConstraint(ColumnConstraintKind):
    pass


class GeneratedAsIdentityColumnConstraint(ColumnConstraintKind):
    # this: True -> ALWAYS, this: False -> BY DEFAULT
    arg_types = {
        "this": False,
        "expression": False,
        "on_null": False,
        "start": False,
        "increment": False,
        "minvalue": False,
        "maxvalue": False,
        "cycle": False,
    }


# https://dev.mysql.com/doc/refman/8.0/en/create-table.html
class IndexColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": False, "schema": True, "kind": False, "type": False, "options": False}


class InlineLengthColumnConstraint(ColumnConstraintKind):
    pass


class NotNullColumnConstraint(ColumnConstraintKind):
    arg_types = {"allow_null": False}


# https://dev.mysql.com/doc/refman/5.7/en/timestamp-initialization.html
class OnUpdateColumnConstraint(ColumnConstraintKind):
    pass


class PrimaryKeyColumnConstraint(ColumnConstraintKind):
    arg_types = {"desc": False}


class TitleColumnConstraint(ColumnConstraintKind):
    pass


class UniqueColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": False}


class UppercaseColumnConstraint(ColumnConstraintKind):
    arg_types: t.Dict[str, t.Any] = {}


class PathColumnConstraint(ColumnConstraintKind):
    pass


class Constraint(Expression):
    arg_types = {"this": True, "expressions": True}


class Delete(Expression):
    arg_types = {
        "with": False,
        "this": False,
        "using": False,
        "where": False,
        "returning": False,
        "limit": False,
        "tables": False,  # Multiple-Table Syntax (MySQL)
    }

    def delete(
        self,
        table: ExpOrStr,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Delete:
        """
        Create a DELETE expression or replace the table on an existing DELETE expression.

        Example:
            >>> delete("tbl").sql()
            'DELETE FROM tbl'

        Args:
            table: the table from which to delete.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Delete: the modified expression.
        """
        return _apply_builder(
            expression=table,
            instance=self,
            arg="this",
            dialect=dialect,
            into=Table,
            copy=copy,
            **opts,
        )

    def where(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Delete:
        """
        Append to or set the WHERE expressions.

        Example:
            >>> delete("tbl").where("x = 'a' OR x < 'b'").sql()
            "DELETE FROM tbl WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Delete: the modified expression.
        """
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="where",
            append=append,
            into=Where,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def returning(
        self,
        expression: ExpOrStr,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Delete:
        """
        Set the RETURNING expression. Not supported by all dialects.

        Example:
            >>> delete("tbl").returning("*", dialect="postgres").sql()
            'DELETE FROM tbl RETURNING *'

        Args:
            expression: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Delete: the modified expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="returning",
            prefix="RETURNING",
            dialect=dialect,
            copy=copy,
            into=Returning,
            **opts,
        )


class Drop(Expression):
    arg_types = {
        "this": False,
        "kind": False,
        "exists": False,
        "temporary": False,
        "materialized": False,
        "cascade": False,
        "constraints": False,
        "purge": False,
    }


class Filter(Expression):
    arg_types = {"this": True, "expression": True}


class Check(Expression):
    pass


class Directory(Expression):
    # https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-dml-insert-overwrite-directory-hive.html
    arg_types = {"this": True, "local": False, "row_format": False}


class ForeignKey(Expression):
    arg_types = {
        "expressions": True,
        "reference": False,
        "delete": False,
        "update": False,
    }


class PrimaryKey(Expression):
    arg_types = {"expressions": True, "options": False}


# https://www.postgresql.org/docs/9.1/sql-selectinto.html
# https://docs.aws.amazon.com/redshift/latest/dg/r_SELECT_INTO.html#r_SELECT_INTO-examples
class Into(Expression):
    arg_types = {"this": True, "temporary": False, "unlogged": False}


class From(Expression):
    @property
    def name(self) -> str:
        return self.this.name

    @property
    def alias_or_name(self) -> str:
        return self.this.alias_or_name


class Having(Expression):
    pass


class Hint(Expression):
    arg_types = {"expressions": True}


class JoinHint(Expression):
    arg_types = {"this": True, "expressions": True}


class Identifier(Expression):
    arg_types = {"this": True, "quoted": False, "global": False, "temporary": False}

    @property
    def quoted(self) -> bool:
        return bool(self.args.get("quoted"))

    @property
    def hashable_args(self) -> t.Any:
        return (self.this, self.quoted)

    @property
    def output_name(self) -> str:
        return self.name


class Index(Expression):
    arg_types = {
        "this": False,
        "table": False,
        "using": False,
        "where": False,
        "columns": False,
        "unique": False,
        "primary": False,
        "amp": False,  # teradata
        "partition_by": False,  # teradata
    }


class Insert(Expression):
    arg_types = {
        "with": False,
        "this": True,
        "expression": False,
        "conflict": False,
        "returning": False,
        "overwrite": False,
        "exists": False,
        "partition": False,
        "alternative": False,
        "where": False,
        "ignore": False,
    }

    def with_(
        self,
        alias: ExpOrStr,
        as_: ExpOrStr,
        recursive: t.Optional[bool] = None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Insert:
        """
        Append to or set the common table expressions.

        Example:
            >>> insert("SELECT x FROM cte", "t").with_("cte", as_="SELECT * FROM tbl").sql()
            'WITH cte AS (SELECT * FROM tbl) INSERT INTO t SELECT x FROM cte'

        Args:
            alias: the SQL code string to parse as the table name.
                If an `Expression` instance is passed, this is used as-is.
            as_: the SQL code string to parse as the table expression.
                If an `Expression` instance is passed, it will be used as-is.
            recursive: set the RECURSIVE part of the expression. Defaults to `False`.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_cte_builder(
            self, alias, as_, recursive=recursive, append=append, dialect=dialect, copy=copy, **opts
        )


class OnConflict(Expression):
    arg_types = {
        "duplicate": False,
        "expressions": False,
        "nothing": False,
        "key": False,
        "constraint": False,
    }


class Returning(Expression):
    arg_types = {"expressions": True, "into": False}


# https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html
class Introducer(Expression):
    arg_types = {"this": True, "expression": True}


# national char, like n'utf8'
class National(Expression):
    pass


class LoadData(Expression):
    arg_types = {
        "this": True,
        "local": False,
        "overwrite": False,
        "inpath": True,
        "partition": False,
        "input_format": False,
        "serde": False,
    }


class Partition(Expression):
    arg_types = {"expressions": True}


class Fetch(Expression):
    arg_types = {
        "direction": False,
        "count": False,
        "percent": False,
        "with_ties": False,
    }


class Group(Expression):
    arg_types = {
        "expressions": False,
        "grouping_sets": False,
        "cube": False,
        "rollup": False,
        "totals": False,
        "all": False,
    }


class Lambda(Expression):
    arg_types = {"this": True, "expressions": True}


class Limit(Expression):
    arg_types = {"this": False, "expression": True, "offset": False}


class Literal(Condition):
    arg_types = {"this": True, "is_string": True}

    @property
    def hashable_args(self) -> t.Any:
        return (self.this, self.args.get("is_string"))

    @classmethod
    def number(cls, number) -> Literal:
        return cls(this=str(number), is_string=False)

    @classmethod
    def string(cls, string) -> Literal:
        return cls(this=str(string), is_string=True)

    @property
    def output_name(self) -> str:
        return self.name


class Join(Expression):
    arg_types = {
        "this": True,
        "on": False,
        "side": False,
        "kind": False,
        "using": False,
        "method": False,
        "global": False,
        "hint": False,
    }

    @property
    def method(self) -> str:
        return self.text("method").upper()

    @property
    def kind(self) -> str:
        return self.text("kind").upper()

    @property
    def side(self) -> str:
        return self.text("side").upper()

    @property
    def hint(self) -> str:
        return self.text("hint").upper()

    @property
    def alias_or_name(self) -> str:
        return self.this.alias_or_name

    def on(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Join:
        """
        Append to or set the ON expressions.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("JOIN x", into=Join).on("y = 1").sql()
            'JOIN x ON y = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Join expression.
        """
        join = _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="on",
            append=append,
            dialect=dialect,
            copy=copy,
            **opts,
        )

        if join.kind == "CROSS":
            join.set("kind", None)

        return join

    def using(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Join:
        """
        Append to or set the USING expressions.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("JOIN x", into=Join).using("foo", "bla").sql()
            'JOIN x USING (foo, bla)'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append: if `True`, concatenate the new expressions to the existing "using" list.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Join expression.
        """
        join = _apply_list_builder(
            *expressions,
            instance=self,
            arg="using",
            append=append,
            dialect=dialect,
            copy=copy,
            **opts,
        )

        if join.kind == "CROSS":
            join.set("kind", None)

        return join


class Lateral(UDTF):
    arg_types = {"this": True, "view": False, "outer": False, "alias": False}


class MatchRecognize(Expression):
    arg_types = {
        "partition_by": False,
        "order": False,
        "measures": False,
        "rows": False,
        "after": False,
        "pattern": False,
        "define": False,
        "alias": False,
    }


# Clickhouse FROM FINAL modifier
# https://clickhouse.com/docs/en/sql-reference/statements/select/from/#final-modifier
class Final(Expression):
    pass


class Offset(Expression):
    arg_types = {"this": False, "expression": True}


class Order(Expression):
    arg_types = {"this": False, "expressions": True}


# hive specific sorts
# https://cwiki.apache.org/confluence/display/Hive/LanguageManual+SortBy
class Cluster(Order):
    pass


class Distribute(Order):
    pass


class Sort(Order):
    pass


class Ordered(Expression):
    arg_types = {"this": True, "desc": True, "nulls_first": True}


class Property(Expression):
    arg_types = {"this": True, "value": True}


class AlgorithmProperty(Property):
    arg_types = {"this": True}


class AutoIncrementProperty(Property):
    arg_types = {"this": True}


class BlockCompressionProperty(Property):
    arg_types = {"autotemp": False, "always": False, "default": True, "manual": True, "never": True}


class CharacterSetProperty(Property):
    arg_types = {"this": True, "default": True}


class ChecksumProperty(Property):
    arg_types = {"on": False, "default": False}


class CollateProperty(Property):
    arg_types = {"this": True}


class CopyGrantsProperty(Property):
    arg_types = {}


class DataBlocksizeProperty(Property):
    arg_types = {
        "size": False,
        "units": False,
        "minimum": False,
        "maximum": False,
        "default": False,
    }


class DefinerProperty(Property):
    arg_types = {"this": True}


class DistKeyProperty(Property):
    arg_types = {"this": True}


class DistStyleProperty(Property):
    arg_types = {"this": True}


class EngineProperty(Property):
    arg_types = {"this": True}


class ToTableProperty(Property):
    arg_types = {"this": True}


class ExecuteAsProperty(Property):
    arg_types = {"this": True}


class ExternalProperty(Property):
    arg_types = {"this": False}


class FallbackProperty(Property):
    arg_types = {"no": True, "protection": False}


class FileFormatProperty(Property):
    arg_types = {"this": True}


class FreespaceProperty(Property):
    arg_types = {"this": True, "percent": False}


class InputOutputFormat(Expression):
    arg_types = {"input_format": False, "output_format": False}


class IsolatedLoadingProperty(Property):
    arg_types = {
        "no": True,
        "concurrent": True,
        "for_all": True,
        "for_insert": True,
        "for_none": True,
    }


class JournalProperty(Property):
    arg_types = {
        "no": False,
        "dual": False,
        "before": False,
        "local": False,
        "after": False,
    }


class LanguageProperty(Property):
    arg_types = {"this": True}


# spark ddl
class ClusteredByProperty(Property):
    arg_types = {"expressions": True, "sorted_by": False, "buckets": True}


class DictProperty(Property):
    arg_types = {"this": True, "kind": True, "settings": False}


class DictSubProperty(Property):
    pass


class DictRange(Property):
    arg_types = {"this": True, "min": True, "max": True}


# Clickhouse CREATE ... ON CLUSTER modifier
# https://clickhouse.com/docs/en/sql-reference/distributed-ddl
class OnCluster(Property):
    arg_types = {"this": True}


class LikeProperty(Property):
    arg_types = {"this": True, "expressions": False}


class LocationProperty(Property):
    arg_types = {"this": True}


class LockingProperty(Property):
    arg_types = {
        "this": False,
        "kind": True,
        "for_or_in": True,
        "lock_type": True,
        "override": False,
    }


class LogProperty(Property):
    arg_types = {"no": True}


class MaterializedProperty(Property):
    arg_types = {"this": False}


class MergeBlockRatioProperty(Property):
    arg_types = {"this": False, "no": False, "default": False, "percent": False}


class NoPrimaryIndexProperty(Property):
    arg_types = {}


class OnCommitProperty(Property):
    arg_type = {"delete": False}


class PartitionedByProperty(Property):
    arg_types = {"this": True}


class ReturnsProperty(Property):
    arg_types = {"this": True, "is_table": False, "table": False}


class RowFormatProperty(Property):
    arg_types = {"this": True}


class RowFormatDelimitedProperty(Property):
    # https://cwiki.apache.org/confluence/display/hive/languagemanual+dml
    arg_types = {
        "fields": False,
        "escaped": False,
        "collection_items": False,
        "map_keys": False,
        "lines": False,
        "null": False,
        "serde": False,
    }


class RowFormatSerdeProperty(Property):
    arg_types = {"this": True, "serde_properties": False}


# https://spark.apache.org/docs/3.1.2/sql-ref-syntax-qry-select-transform.html
class QueryTransform(Expression):
    arg_types = {
        "expressions": True,
        "command_script": True,
        "schema": False,
        "row_format_before": False,
        "record_writer": False,
        "row_format_after": False,
        "record_reader": False,
    }


class SchemaCommentProperty(Property):
    arg_types = {"this": True}


class SerdeProperties(Property):
    arg_types = {"expressions": True}


class SetProperty(Property):
    arg_types = {"multi": True}


class SettingsProperty(Property):
    arg_types = {"expressions": True}


class SortKeyProperty(Property):
    arg_types = {"this": True, "compound": False}


class SqlSecurityProperty(Property):
    arg_types = {"definer": True}


class StabilityProperty(Property):
    arg_types = {"this": True}


class TemporaryProperty(Property):
    arg_types = {}


class TransientProperty(Property):
    arg_types = {"this": False}


class VolatileProperty(Property):
    arg_types = {"this": False}


class WithDataProperty(Property):
    arg_types = {"no": True, "statistics": False}


class WithJournalTableProperty(Property):
    arg_types = {"this": True}


class Properties(Expression):
    arg_types = {"expressions": True}

    NAME_TO_PROPERTY = {
        "ALGORITHM": AlgorithmProperty,
        "AUTO_INCREMENT": AutoIncrementProperty,
        "CHARACTER SET": CharacterSetProperty,
        "CLUSTERED_BY": ClusteredByProperty,
        "COLLATE": CollateProperty,
        "COMMENT": SchemaCommentProperty,
        "DEFINER": DefinerProperty,
        "DISTKEY": DistKeyProperty,
        "DISTSTYLE": DistStyleProperty,
        "ENGINE": EngineProperty,
        "EXECUTE AS": ExecuteAsProperty,
        "FORMAT": FileFormatProperty,
        "LANGUAGE": LanguageProperty,
        "LOCATION": LocationProperty,
        "PARTITIONED_BY": PartitionedByProperty,
        "RETURNS": ReturnsProperty,
        "ROW_FORMAT": RowFormatProperty,
        "SORTKEY": SortKeyProperty,
    }

    PROPERTY_TO_NAME = {v: k for k, v in NAME_TO_PROPERTY.items()}

    # CREATE property locations
    # Form: schema specified
    #   create [POST_CREATE]
    #     table a [POST_NAME]
    #     (b int) [POST_SCHEMA]
    #     with ([POST_WITH])
    #     index (b) [POST_INDEX]
    #
    # Form: alias selection
    #   create [POST_CREATE]
    #     table a [POST_NAME]
    #     as [POST_ALIAS] (select * from b) [POST_EXPRESSION]
    #     index (c) [POST_INDEX]
    class Location(AutoName):
        POST_CREATE = auto()
        POST_NAME = auto()
        POST_SCHEMA = auto()
        POST_WITH = auto()
        POST_ALIAS = auto()
        POST_EXPRESSION = auto()
        POST_INDEX = auto()
        UNSUPPORTED = auto()

    @classmethod
    def from_dict(cls, properties_dict: t.Dict) -> Properties:
        expressions = []
        for key, value in properties_dict.items():
            property_cls = cls.NAME_TO_PROPERTY.get(key.upper())
            if property_cls:
                expressions.append(property_cls(this=convert(value)))
            else:
                expressions.append(Property(this=Literal.string(key), value=convert(value)))

        return cls(expressions=expressions)


class Qualify(Expression):
    pass


# https://www.ibm.com/docs/en/ias?topic=procedures-return-statement-in-sql
class Return(Expression):
    pass


class Reference(Expression):
    arg_types = {"this": True, "expressions": False, "options": False}


class Tuple(Expression):
    arg_types = {"expressions": False}

    def isin(
        self,
        *expressions: t.Any,
        query: t.Optional[ExpOrStr] = None,
        unnest: t.Optional[ExpOrStr] | t.Collection[ExpOrStr] = None,
        copy: bool = True,
        **opts,
    ) -> In:
        return In(
            this=maybe_copy(self, copy),
            expressions=[convert(e, copy=copy) for e in expressions],
            query=maybe_parse(query, copy=copy, **opts) if query else None,
            unnest=Unnest(
                expressions=[
                    maybe_parse(t.cast(ExpOrStr, e), copy=copy, **opts) for e in ensure_list(unnest)
                ]
            )
            if unnest
            else None,
        )


class Subqueryable(Unionable):
    def subquery(self, alias: t.Optional[ExpOrStr] = None, copy: bool = True) -> Subquery:
        """
        Convert this expression to an aliased expression that can be used as a Subquery.

        Example:
            >>> subquery = Select().select("x").from_("tbl").subquery()
            >>> Select().select("x").from_(subquery).sql()
            'SELECT x FROM (SELECT x FROM tbl)'

        Args:
            alias (str | Identifier): an optional alias for the subquery
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Alias: the subquery
        """
        instance = maybe_copy(self, copy)
        if not isinstance(alias, Expression):
            alias = TableAlias(this=to_identifier(alias)) if alias else None

        return Subquery(this=instance, alias=alias)

    def limit(
        self, expression: ExpOrStr | int, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Select:
        raise NotImplementedError

    @property
    def ctes(self):
        with_ = self.args.get("with")
        if not with_:
            return []
        return with_.expressions

    @property
    def selects(self) -> t.List[Expression]:
        raise NotImplementedError("Subqueryable objects must implement `selects`")

    @property
    def named_selects(self) -> t.List[str]:
        raise NotImplementedError("Subqueryable objects must implement `named_selects`")

    def with_(
        self,
        alias: ExpOrStr,
        as_: ExpOrStr,
        recursive: t.Optional[bool] = None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Subqueryable:
        """
        Append to or set the common table expressions.

        Example:
            >>> Select().with_("tbl2", as_="SELECT * FROM tbl").select("x").from_("tbl2").sql()
            'WITH tbl2 AS (SELECT * FROM tbl) SELECT x FROM tbl2'

        Args:
            alias: the SQL code string to parse as the table name.
                If an `Expression` instance is passed, this is used as-is.
            as_: the SQL code string to parse as the table expression.
                If an `Expression` instance is passed, it will be used as-is.
            recursive: set the RECURSIVE part of the expression. Defaults to `False`.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_cte_builder(
            self, alias, as_, recursive=recursive, append=append, dialect=dialect, copy=copy, **opts
        )


QUERY_MODIFIERS = {
    "match": False,
    "laterals": False,
    "joins": False,
    "pivots": False,
    "where": False,
    "group": False,
    "having": False,
    "qualify": False,
    "windows": False,
    "distribute": False,
    "sort": False,
    "cluster": False,
    "order": False,
    "limit": False,
    "offset": False,
    "locks": False,
    "sample": False,
    "settings": False,
    "format": False,
}


# https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16
class WithTableHint(Expression):
    arg_types = {"expressions": True}


# https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
class IndexTableHint(Expression):
    arg_types = {"this": True, "expressions": False, "target": False}


class Table(Expression):
    arg_types = {
        "this": True,
        "alias": False,
        "db": False,
        "catalog": False,
        "laterals": False,
        "joins": False,
        "pivots": False,
        "hints": False,
        "system_time": False,
    }

    @property
    def name(self) -> str:
        if isinstance(self.this, Func):
            return ""
        return self.this.name

    @property
    def db(self) -> str:
        return self.text("db")

    @property
    def catalog(self) -> str:
        return self.text("catalog")

    @property
    def selects(self) -> t.List[Expression]:
        return []

    @property
    def named_selects(self) -> t.List[str]:
        return []

    @property
    def parts(self) -> t.List[Identifier]:
        """Return the parts of a table in order catalog, db, table."""
        parts: t.List[Identifier] = []

        for arg in ("catalog", "db", "this"):
            part = self.args.get(arg)

            if isinstance(part, Identifier):
                parts.append(part)
            elif isinstance(part, Dot):
                parts.extend(part.flatten())

        return parts


# See the TSQL "Querying data in a system-versioned temporal table" page
class SystemTime(Expression):
    arg_types = {
        "this": False,
        "expression": False,
        "kind": True,
    }


class Union(Subqueryable):
    arg_types = {
        "with": False,
        "this": True,
        "expression": True,
        "distinct": False,
        **QUERY_MODIFIERS,
    }

    def limit(
        self, expression: ExpOrStr | int, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Select:
        """
        Set the LIMIT expression.

        Example:
            >>> select("1").union(select("1")).limit(1).sql()
            'SELECT * FROM (SELECT 1 UNION SELECT 1) AS _l_0 LIMIT 1'

        Args:
            expression: the SQL code string to parse.
                This can also be an integer.
                If a `Limit` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Limit`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The limited subqueryable.
        """
        return (
            select("*")
            .from_(self.subquery(alias="_l_0", copy=copy))
            .limit(expression, dialect=dialect, copy=False, **opts)
        )

    def select(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Union:
        """Append to or set the SELECT of the union recursively.

        Example:
            >>> from sqlglot import parse_one
            >>> parse_one("select a from x union select a from y union select a from z").select("b").sql()
            'SELECT a, b FROM x UNION SELECT a, b FROM y UNION SELECT a, b FROM z'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Union: the modified expression.
        """
        this = self.copy() if copy else self
        this.this.unnest().select(*expressions, append=append, dialect=dialect, copy=False, **opts)
        this.expression.unnest().select(
            *expressions, append=append, dialect=dialect, copy=False, **opts
        )
        return this

    @property
    def named_selects(self) -> t.List[str]:
        return self.this.unnest().named_selects

    @property
    def is_star(self) -> bool:
        return self.this.is_star or self.expression.is_star

    @property
    def selects(self) -> t.List[Expression]:
        return self.this.unnest().selects

    @property
    def left(self):
        return self.this

    @property
    def right(self):
        return self.expression


class Except(Union):
    pass


class Intersect(Union):
    pass


class Unnest(UDTF):
    arg_types = {
        "expressions": True,
        "ordinality": False,
        "alias": False,
        "offset": False,
    }


class Update(Expression):
    arg_types = {
        "with": False,
        "this": False,
        "expressions": True,
        "from": False,
        "where": False,
        "returning": False,
        "limit": False,
    }


class Values(UDTF):
    arg_types = {
        "expressions": True,
        "ordinality": False,
        "alias": False,
    }


class Var(Expression):
    pass


class Schema(Expression):
    arg_types = {"this": False, "expressions": False}


# https://dev.mysql.com/doc/refman/8.0/en/select.html
# https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SELECT.html
class Lock(Expression):
    arg_types = {"update": True, "expressions": False, "wait": False}


class Select(Subqueryable):
    arg_types = {
        "with": False,
        "kind": False,
        "expressions": False,
        "hint": False,
        "distinct": False,
        "into": False,
        "from": False,
        **QUERY_MODIFIERS,
    }

    def from_(
        self, expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Select:
        """
        Set the FROM expression.

        Example:
            >>> Select().from_("tbl").select("x").sql()
            'SELECT x FROM tbl'

        Args:
            expression : the SQL code strings to parse.
                If a `From` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `From`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="from",
            into=From,
            prefix="FROM",
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def group_by(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Set the GROUP BY expression.

        Example:
            >>> Select().from_("tbl").select("x", "COUNT(1)").group_by("x").sql()
            'SELECT x, COUNT(1) FROM tbl GROUP BY x'

        Args:
            *expressions: the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Group`.
                If nothing is passed in then a group by is not applied to the expression
            append: if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Group` expression into a single expression.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        if not expressions:
            return self if not copy else self.copy()

        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="group",
            append=append,
            copy=copy,
            prefix="GROUP BY",
            into=Group,
            dialect=dialect,
            **opts,
        )

    def order_by(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Set the ORDER BY expression.

        Example:
            >>> Select().from_("tbl").select("x").order_by("x DESC").sql()
            'SELECT x FROM tbl ORDER BY x DESC'

        Args:
            *expressions: the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Order`.
            append: if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="order",
            append=append,
            copy=copy,
            prefix="ORDER BY",
            into=Order,
            dialect=dialect,
            **opts,
        )

    def sort_by(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Set the SORT BY expression.

        Example:
            >>> Select().from_("tbl").select("x").sort_by("x DESC").sql(dialect="hive")
            'SELECT x FROM tbl SORT BY x DESC'

        Args:
            *expressions: the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `SORT`.
            append: if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="sort",
            append=append,
            copy=copy,
            prefix="SORT BY",
            into=Sort,
            dialect=dialect,
            **opts,
        )

    def cluster_by(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Set the CLUSTER BY expression.

        Example:
            >>> Select().from_("tbl").select("x").cluster_by("x DESC").sql(dialect="hive")
            'SELECT x FROM tbl CLUSTER BY x DESC'

        Args:
            *expressions: the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Cluster`.
            append: if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="cluster",
            append=append,
            copy=copy,
            prefix="CLUSTER BY",
            into=Cluster,
            dialect=dialect,
            **opts,
        )

    def limit(
        self, expression: ExpOrStr | int, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Select:
        """
        Set the LIMIT expression.

        Example:
            >>> Select().from_("tbl").select("x").limit(10).sql()
            'SELECT x FROM tbl LIMIT 10'

        Args:
            expression: the SQL code string to parse.
                This can also be an integer.
                If a `Limit` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Limit`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="limit",
            into=Limit,
            prefix="LIMIT",
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def offset(
        self, expression: ExpOrStr | int, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Select:
        """
        Set the OFFSET expression.

        Example:
            >>> Select().from_("tbl").select("x").offset(10).sql()
            'SELECT x FROM tbl OFFSET 10'

        Args:
            expression: the SQL code string to parse.
                This can also be an integer.
                If a `Offset` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Offset`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="offset",
            into=Offset,
            prefix="OFFSET",
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def select(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Append to or set the SELECT expressions.

        Example:
            >>> Select().select("x", "y").sql()
            'SELECT x, y'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_list_builder(
            *expressions,
            instance=self,
            arg="expressions",
            append=append,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def lateral(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Append to or set the LATERAL expressions.

        Example:
            >>> Select().select("x").lateral("OUTER explode(y) tbl2 AS z").from_("tbl").sql()
            'SELECT x FROM tbl LATERAL VIEW OUTER EXPLODE(y) tbl2 AS z'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_list_builder(
            *expressions,
            instance=self,
            arg="laterals",
            append=append,
            into=Lateral,
            prefix="LATERAL VIEW",
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def join(
        self,
        expression: ExpOrStr,
        on: t.Optional[ExpOrStr] = None,
        using: t.Optional[ExpOrStr | t.Collection[ExpOrStr]] = None,
        append: bool = True,
        join_type: t.Optional[str] = None,
        join_alias: t.Optional[Identifier | str] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Append to or set the JOIN expressions.

        Example:
            >>> Select().select("*").from_("tbl").join("tbl2", on="tbl1.y = tbl2.y").sql()
            'SELECT * FROM tbl JOIN tbl2 ON tbl1.y = tbl2.y'

            >>> Select().select("1").from_("a").join("b", using=["x", "y", "z"]).sql()
            'SELECT 1 FROM a JOIN b USING (x, y, z)'

            Use `join_type` to change the type of join:

            >>> Select().select("*").from_("tbl").join("tbl2", on="tbl1.y = tbl2.y", join_type="left outer").sql()
            'SELECT * FROM tbl LEFT OUTER JOIN tbl2 ON tbl1.y = tbl2.y'

        Args:
            expression: the SQL code string to parse.
                If an `Expression` instance is passed, it will be used as-is.
            on: optionally specify the join "on" criteria as a SQL string.
                If an `Expression` instance is passed, it will be used as-is.
            using: optionally specify the join "using" criteria as a SQL string.
                If an `Expression` instance is passed, it will be used as-is.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            join_type: if set, alter the parsed join type.
            join_alias: an optional alias for the joined source.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
        """
        parse_args: t.Dict[str, t.Any] = {"dialect": dialect, **opts}

        try:
            expression = maybe_parse(expression, into=Join, prefix="JOIN", **parse_args)
        except ParseError:
            expression = maybe_parse(expression, into=(Join, Expression), **parse_args)

        join = expression if isinstance(expression, Join) else Join(this=expression)

        if isinstance(join.this, Select):
            join.this.replace(join.this.subquery())

        if join_type:
            method: t.Optional[Token]
            side: t.Optional[Token]
            kind: t.Optional[Token]

            method, side, kind = maybe_parse(join_type, into="JOIN_TYPE", **parse_args)  # type: ignore

            if method:
                join.set("method", method.text)
            if side:
                join.set("side", side.text)
            if kind:
                join.set("kind", kind.text)

        if on:
            on = and_(*ensure_list(on), dialect=dialect, copy=copy, **opts)
            join.set("on", on)

        if using:
            join = _apply_list_builder(
                *ensure_list(using),
                instance=join,
                arg="using",
                append=append,
                copy=copy,
                into=Identifier,
                **opts,
            )

        if join_alias:
            join.set("this", alias_(join.this, join_alias, table=True))

        return _apply_list_builder(
            join,
            instance=self,
            arg="joins",
            append=append,
            copy=copy,
            **opts,
        )

    def where(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Append to or set the WHERE expressions.

        Example:
            >>> Select().select("x").from_("tbl").where("x = 'a' OR x < 'b'").sql()
            "SELECT x FROM tbl WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
        """
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="where",
            append=append,
            into=Where,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def having(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Append to or set the HAVING expressions.

        Example:
            >>> Select().select("x", "COUNT(y)").from_("tbl").group_by("x").having("COUNT(y) > 3").sql()
            'SELECT x, COUNT(y) FROM tbl GROUP BY x HAVING COUNT(y) > 3'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="having",
            append=append,
            into=Having,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def window(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        return _apply_list_builder(
            *expressions,
            instance=self,
            arg="windows",
            append=append,
            into=Window,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def qualify(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="qualify",
            append=append,
            into=Qualify,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def distinct(
        self, *ons: t.Optional[ExpOrStr], distinct: bool = True, copy: bool = True
    ) -> Select:
        """
        Set the OFFSET expression.

        Example:
            >>> Select().from_("tbl").select("x").distinct().sql()
            'SELECT DISTINCT x FROM tbl'

        Args:
            ons: the expressions to distinct on
            distinct: whether the Select should be distinct
            copy: if `False`, modify this expression instance in-place.

        Returns:
            Select: the modified expression.
        """
        instance = maybe_copy(self, copy)
        on = Tuple(expressions=[maybe_parse(on, copy=copy) for on in ons if on]) if ons else None
        instance.set("distinct", Distinct(on=on) if distinct else None)
        return instance

    def ctas(
        self,
        table: ExpOrStr,
        properties: t.Optional[t.Dict] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Create:
        """
        Convert this expression to a CREATE TABLE AS statement.

        Example:
            >>> Select().select("*").from_("tbl").ctas("x").sql()
            'CREATE TABLE x AS SELECT * FROM tbl'

        Args:
            table: the SQL code string to parse as the table name.
                If another `Expression` instance is passed, it will be used as-is.
            properties: an optional mapping of table properties
            dialect: the dialect used to parse the input table.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input table.

        Returns:
            The new Create expression.
        """
        instance = maybe_copy(self, copy)
        table_expression = maybe_parse(
            table,
            into=Table,
            dialect=dialect,
            **opts,
        )
        properties_expression = None
        if properties:
            properties_expression = Properties.from_dict(properties)

        return Create(
            this=table_expression,
            kind="table",
            expression=instance,
            properties=properties_expression,
        )

    def lock(self, update: bool = True, copy: bool = True) -> Select:
        """
        Set the locking read mode for this expression.

        Examples:
            >>> Select().select("x").from_("tbl").where("x = 'a'").lock().sql("mysql")
            "SELECT x FROM tbl WHERE x = 'a' FOR UPDATE"

            >>> Select().select("x").from_("tbl").where("x = 'a'").lock(update=False).sql("mysql")
            "SELECT x FROM tbl WHERE x = 'a' FOR SHARE"

        Args:
            update: if `True`, the locking type will be `FOR UPDATE`, else it will be `FOR SHARE`.
            copy: if `False`, modify this expression instance in-place.

        Returns:
            The modified expression.
        """
        inst = maybe_copy(self, copy)
        inst.set("locks", [Lock(update=update)])

        return inst

    def hint(self, *hints: ExpOrStr, dialect: DialectType = None, copy: bool = True) -> Select:
        """
        Set hints for this expression.

        Examples:
            >>> Select().select("x").from_("tbl").hint("BROADCAST(y)").sql(dialect="spark")
            'SELECT /*+ BROADCAST(y) */ x FROM tbl'

        Args:
            hints: The SQL code strings to parse as the hints.
                If an `Expression` instance is passed, it will be used as-is.
            dialect: The dialect used to parse the hints.
            copy: If `False`, modify this expression instance in-place.

        Returns:
            The modified expression.
        """
        inst = maybe_copy(self, copy)
        inst.set(
            "hint", Hint(expressions=[maybe_parse(h, copy=copy, dialect=dialect) for h in hints])
        )

        return inst

    @property
    def named_selects(self) -> t.List[str]:
        return [e.output_name for e in self.expressions if e.alias_or_name]

    @property
    def is_star(self) -> bool:
        return any(expression.is_star for expression in self.expressions)

    @property
    def selects(self) -> t.List[Expression]:
        return self.expressions


class Subquery(DerivedTable, Unionable):
    arg_types = {
        "this": True,
        "alias": False,
        "with": False,
        **QUERY_MODIFIERS,
    }

    def unnest(self):
        """
        Returns the first non subquery.
        """
        expression = self
        while isinstance(expression, Subquery):
            expression = expression.this
        return expression

    @property
    def is_star(self) -> bool:
        return self.this.is_star

    @property
    def output_name(self) -> str:
        return self.alias


class TableSample(Expression):
    arg_types = {
        "this": False,
        "method": False,
        "bucket_numerator": False,
        "bucket_denominator": False,
        "bucket_field": False,
        "percent": False,
        "rows": False,
        "size": False,
        "seed": False,
        "kind": False,
    }


class Tag(Expression):
    """Tags are used for generating arbitrary sql like SELECT <span>x</span>."""

    arg_types = {
        "this": False,
        "prefix": False,
        "postfix": False,
    }


# Represents both the standard SQL PIVOT operator and DuckDB's "simplified" PIVOT syntax
# https://duckdb.org/docs/sql/statements/pivot
class Pivot(Expression):
    arg_types = {
        "this": False,
        "alias": False,
        "expressions": True,
        "field": False,
        "unpivot": False,
        "using": False,
        "group": False,
        "columns": False,
    }


class Window(Expression):
    arg_types = {
        "this": True,
        "partition_by": False,
        "order": False,
        "spec": False,
        "alias": False,
        "over": False,
        "first": False,
    }


class WindowSpec(Expression):
    arg_types = {
        "kind": False,
        "start": False,
        "start_side": False,
        "end": False,
        "end_side": False,
    }


class Where(Expression):
    pass


class Star(Expression):
    arg_types = {"except": False, "replace": False}

    @property
    def name(self) -> str:
        return "*"

    @property
    def output_name(self) -> str:
        return self.name


class Parameter(Condition):
    arg_types = {"this": True, "wrapped": False}


class SessionParameter(Condition):
    arg_types = {"this": True, "kind": False}


class Placeholder(Condition):
    arg_types = {"this": False, "kind": False}


class Null(Condition):
    arg_types: t.Dict[str, t.Any] = {}

    @property
    def name(self) -> str:
        return "NULL"


class Boolean(Condition):
    pass


class DataTypeSize(Expression):
    arg_types = {"this": True, "expression": False}


class DataType(Expression):
    arg_types = {
        "this": True,
        "expressions": False,
        "nested": False,
        "values": False,
        "prefix": False,
    }

    class Type(AutoName):
        ARRAY = auto()
        BIGDECIMAL = auto()
        BIGINT = auto()
        BIGSERIAL = auto()
        BINARY = auto()
        BIT = auto()
        BOOLEAN = auto()
        CHAR = auto()
        DATE = auto()
        DATETIME = auto()
        DATETIME64 = auto()
        ENUM = auto()
        INT4RANGE = auto()
        INT4MULTIRANGE = auto()
        INT8RANGE = auto()
        INT8MULTIRANGE = auto()
        NUMRANGE = auto()
        NUMMULTIRANGE = auto()
        TSRANGE = auto()
        TSMULTIRANGE = auto()
        TSTZRANGE = auto()
        TSTZMULTIRANGE = auto()
        DATERANGE = auto()
        DATEMULTIRANGE = auto()
        DECIMAL = auto()
        DOUBLE = auto()
        FLOAT = auto()
        GEOGRAPHY = auto()
        GEOMETRY = auto()
        HLLSKETCH = auto()
        HSTORE = auto()
        IMAGE = auto()
        INET = auto()
        IPADDRESS = auto()
        IPPREFIX = auto()
        INT = auto()
        INT128 = auto()
        INT256 = auto()
        INTERVAL = auto()
        JSON = auto()
        JSONB = auto()
        LONGBLOB = auto()
        LONGTEXT = auto()
        MAP = auto()
        MEDIUMBLOB = auto()
        MEDIUMTEXT = auto()
        MONEY = auto()
        NCHAR = auto()
        NULL = auto()
        NULLABLE = auto()
        NVARCHAR = auto()
        OBJECT = auto()
        ROWVERSION = auto()
        SERIAL = auto()
        SET = auto()
        SMALLINT = auto()
        SMALLMONEY = auto()
        SMALLSERIAL = auto()
        STRUCT = auto()
        SUPER = auto()
        TEXT = auto()
        TIME = auto()
        TIMESTAMP = auto()
        TIMESTAMPTZ = auto()
        TIMESTAMPLTZ = auto()
        TINYINT = auto()
        UBIGINT = auto()
        UINT = auto()
        USMALLINT = auto()
        UTINYINT = auto()
        UNKNOWN = auto()  # Sentinel value, useful for type annotation
        UINT128 = auto()
        UINT256 = auto()
        UNIQUEIDENTIFIER = auto()
        USERDEFINED = "USER-DEFINED"
        UUID = auto()
        VARBINARY = auto()
        VARCHAR = auto()
        VARIANT = auto()
        XML = auto()

    TEXT_TYPES = {
        Type.CHAR,
        Type.NCHAR,
        Type.VARCHAR,
        Type.NVARCHAR,
        Type.TEXT,
    }

    INTEGER_TYPES = {
        Type.INT,
        Type.TINYINT,
        Type.SMALLINT,
        Type.BIGINT,
        Type.INT128,
        Type.INT256,
    }

    FLOAT_TYPES = {
        Type.FLOAT,
        Type.DOUBLE,
    }

    NUMERIC_TYPES = {*INTEGER_TYPES, *FLOAT_TYPES}

    TEMPORAL_TYPES = {
        Type.TIME,
        Type.TIMESTAMP,
        Type.TIMESTAMPTZ,
        Type.TIMESTAMPLTZ,
        Type.DATE,
        Type.DATETIME,
        Type.DATETIME64,
    }

    META_TYPES = {"UNKNOWN", "NULL"}

    @classmethod
    def build(
        cls, dtype: str | DataType | DataType.Type, dialect: DialectType = None, **kwargs
    ) -> DataType:
        from sqlglot import parse_one

        if isinstance(dtype, str):
            upper = dtype.upper()
            if upper in DataType.META_TYPES:
                data_type_exp: t.Optional[Expression] = DataType(this=DataType.Type[upper])
            else:
                data_type_exp = parse_one(dtype, read=dialect, into=DataType)

            if data_type_exp is None:
                raise ValueError(f"Unparsable data type value: {dtype}")
        elif isinstance(dtype, DataType.Type):
            data_type_exp = DataType(this=dtype)
        elif isinstance(dtype, DataType):
            return dtype
        else:
            raise ValueError(f"Invalid data type: {type(dtype)}. Expected str or DataType.Type")

        return DataType(**{**data_type_exp.args, **kwargs})

    def is_type(self, *dtypes: str | DataType | DataType.Type) -> bool:
        return any(self.this == DataType.build(dtype).this for dtype in dtypes)


# https://www.postgresql.org/docs/15/datatype-pseudo.html
class PseudoType(Expression):
    pass


# WHERE x <OP> EXISTS|ALL|ANY|SOME(SELECT ...)
class SubqueryPredicate(Predicate):
    pass


class All(SubqueryPredicate):
    pass


class Any(SubqueryPredicate):
    pass


class Exists(SubqueryPredicate):
    pass


# Commands to interact with the databases or engines. For most of the command
# expressions we parse whatever comes after the command's name as a string.
class Command(Expression):
    arg_types = {"this": True, "expression": False}


class Transaction(Expression):
    arg_types = {"this": False, "modes": False, "mark": False}


class Commit(Expression):
    arg_types = {"chain": False, "this": False, "durability": False}


class Rollback(Expression):
    arg_types = {"savepoint": False, "this": False}


class AlterTable(Expression):
    arg_types = {"this": True, "actions": True, "exists": False}


class AddConstraint(Expression):
    arg_types = {"this": False, "expression": False, "enforced": False}


class DropPartition(Expression):
    arg_types = {"expressions": True, "exists": False}


# Binary expressions like (ADD a b)
class Binary(Condition):
    arg_types = {"this": True, "expression": True}

    @property
    def left(self):
        return self.this

    @property
    def right(self):
        return self.expression


class Add(Binary):
    pass


class Connector(Binary):
    pass


class And(Connector):
    pass


class Or(Connector):
    pass


class BitwiseAnd(Binary):
    pass


class BitwiseLeftShift(Binary):
    pass


class BitwiseOr(Binary):
    pass


class BitwiseRightShift(Binary):
    pass


class BitwiseXor(Binary):
    pass


class Div(Binary):
    pass


class Overlaps(Binary):
    pass


class Dot(Binary):
    @property
    def name(self) -> str:
        return self.expression.name

    @property
    def output_name(self) -> str:
        return self.name

    @classmethod
    def build(self, expressions: t.Sequence[Expression]) -> Dot:
        """Build a Dot object with a sequence of expressions."""
        if len(expressions) < 2:
            raise ValueError(f"Dot requires >= 2 expressions.")

        a, b, *expressions = expressions
        dot = Dot(this=a, expression=b)

        for expression in expressions:
            dot = Dot(this=dot, expression=expression)

        return dot


class DPipe(Binary):
    pass


class SafeDPipe(DPipe):
    pass


class EQ(Binary, Predicate):
    pass


class NullSafeEQ(Binary, Predicate):
    pass


class NullSafeNEQ(Binary, Predicate):
    pass


class Distance(Binary):
    pass


class Escape(Binary):
    pass


class Glob(Binary, Predicate):
    pass


class GT(Binary, Predicate):
    pass


class GTE(Binary, Predicate):
    pass


class ILike(Binary, Predicate):
    pass


class ILikeAny(Binary, Predicate):
    pass


class IntDiv(Binary):
    pass


class Is(Binary, Predicate):
    pass


class Kwarg(Binary):
    """Kwarg in special functions like func(kwarg => y)."""


class Like(Binary, Predicate):
    pass


class LikeAny(Binary, Predicate):
    pass


class LT(Binary, Predicate):
    pass


class LTE(Binary, Predicate):
    pass


class Mod(Binary):
    pass


class Mul(Binary):
    pass


class NEQ(Binary, Predicate):
    pass


class SimilarTo(Binary, Predicate):
    pass


class Slice(Binary):
    arg_types = {"this": False, "expression": False}


class Sub(Binary):
    pass


class ArrayOverlaps(Binary):
    pass


# Unary Expressions
# (NOT a)
class Unary(Condition):
    pass


class BitwiseNot(Unary):
    pass


class Not(Unary):
    pass


class Paren(Unary):
    arg_types = {"this": True, "with": False}

    @property
    def output_name(self) -> str:
        return self.this.name


class Neg(Unary):
    pass


class Alias(Expression):
    arg_types = {"this": True, "alias": False}

    @property
    def output_name(self) -> str:
        return self.alias


class Aliases(Expression):
    arg_types = {"this": True, "expressions": True}

    @property
    def aliases(self):
        return self.expressions


class AtTimeZone(Expression):
    arg_types = {"this": True, "zone": True}


class Between(Predicate):
    arg_types = {"this": True, "low": True, "high": True}


class Bracket(Condition):
    arg_types = {"this": True, "expressions": True}


class SafeBracket(Bracket):
    """Represents array lookup where OOB index yields NULL instead of causing a failure."""


class Distinct(Expression):
    arg_types = {"expressions": False, "on": False}


class In(Predicate):
    arg_types = {
        "this": True,
        "expressions": False,
        "query": False,
        "unnest": False,
        "field": False,
        "is_global": False,
    }


class TimeUnit(Expression):
    """Automatically converts unit arg into a var."""

    arg_types = {"unit": False}

    def __init__(self, **args):
        unit = args.get("unit")
        if isinstance(unit, (Column, Literal)):
            args["unit"] = Var(this=unit.name)
        elif isinstance(unit, Week):
            unit.set("this", Var(this=unit.this.name))

        super().__init__(**args)


class Interval(TimeUnit):
    arg_types = {"this": False, "unit": False}

    @property
    def unit(self) -> t.Optional[Var]:
        return self.args.get("unit")


class IgnoreNulls(Expression):
    pass


class RespectNulls(Expression):
    pass


# Functions
class Func(Condition):
    """
    The base class for all function expressions.

    Attributes:
        is_var_len_args (bool): if set to True the last argument defined in arg_types will be
            treated as a variable length argument and the argument's value will be stored as a list.
        _sql_names (list): determines the SQL name (1st item in the list) and aliases (subsequent items)
            for this function expression. These values are used to map this node to a name during parsing
            as well as to provide the function's name during SQL string generation. By default the SQL
            name is set to the expression's class name transformed to snake case.
    """

    is_var_len_args = False

    @classmethod
    def from_arg_list(cls, args):
        if cls.is_var_len_args:
            all_arg_keys = list(cls.arg_types)
            # If this function supports variable length argument treat the last argument as such.
            non_var_len_arg_keys = all_arg_keys[:-1] if cls.is_var_len_args else all_arg_keys
            num_non_var = len(non_var_len_arg_keys)

            args_dict = {arg_key: arg for arg, arg_key in zip(args, non_var_len_arg_keys)}
            args_dict[all_arg_keys[-1]] = args[num_non_var:]
        else:
            args_dict = {arg_key: arg for arg, arg_key in zip(args, cls.arg_types)}

        return cls(**args_dict)

    @classmethod
    def sql_names(cls):
        if cls is Func:
            raise NotImplementedError(
                "SQL name is only supported by concrete function implementations"
            )
        if "_sql_names" not in cls.__dict__:
            cls._sql_names = [camel_to_snake_case(cls.__name__)]
        return cls._sql_names

    @classmethod
    def sql_name(cls):
        return cls.sql_names()[0]

    @classmethod
    def default_parser_mappings(cls):
        return {name: cls.from_arg_list for name in cls.sql_names()}


class AggFunc(Func):
    pass


class ParameterizedAgg(AggFunc):
    arg_types = {"this": True, "expressions": True, "params": True}


class Abs(Func):
    pass


# https://spark.apache.org/docs/latest/api/sql/index.html#transform
class Transform(Func):
    arg_types = {"this": True, "expression": True}


class Anonymous(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


# https://docs.snowflake.com/en/sql-reference/functions/hll
# https://docs.aws.amazon.com/redshift/latest/dg/r_HLL_function.html
class Hll(AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ApproxDistinct(AggFunc):
    arg_types = {"this": True, "accuracy": False}
    _sql_names = ["APPROX_DISTINCT", "APPROX_COUNT_DISTINCT"]


class Array(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


# https://docs.snowflake.com/en/sql-reference/functions/to_char
class ToChar(Func):
    arg_types = {"this": True, "format": False}


class GenerateSeries(Func):
    arg_types = {"start": True, "end": True, "step": False}


class ArrayAgg(AggFunc):
    pass


class ArrayAll(Func):
    arg_types = {"this": True, "expression": True}


class ArrayAny(Func):
    arg_types = {"this": True, "expression": True}


class ArrayConcat(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ArrayContains(Binary, Func):
    pass


class ArrayContained(Binary):
    pass


class ArrayFilter(Func):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["FILTER", "ARRAY_FILTER"]


class ArrayJoin(Func):
    arg_types = {"this": True, "expression": True, "null": False}


class ArraySize(Func):
    arg_types = {"this": True, "expression": False}


class ArraySort(Func):
    arg_types = {"this": True, "expression": False}


class ArraySum(Func):
    pass


class ArrayUnionAgg(AggFunc):
    pass


class Avg(AggFunc):
    pass


class AnyValue(AggFunc):
    arg_types = {"this": True, "having": False, "max": False}


class Case(Func):
    arg_types = {"this": False, "ifs": True, "default": False}

    def when(self, condition: ExpOrStr, then: ExpOrStr, copy: bool = True, **opts) -> Case:
        instance = maybe_copy(self, copy)
        instance.append(
            "ifs",
            If(
                this=maybe_parse(condition, copy=copy, **opts),
                true=maybe_parse(then, copy=copy, **opts),
            ),
        )
        return instance

    def else_(self, condition: ExpOrStr, copy: bool = True, **opts) -> Case:
        instance = maybe_copy(self, copy)
        instance.set("default", maybe_parse(condition, copy=copy, **opts))
        return instance


class Cast(Func):
    arg_types = {"this": True, "to": True, "format": False}

    @property
    def name(self) -> str:
        return self.this.name

    @property
    def to(self) -> DataType:
        return self.args["to"]

    @property
    def output_name(self) -> str:
        return self.name

    def is_type(self, *dtypes: str | DataType | DataType.Type) -> bool:
        return self.to.is_type(*dtypes)


class CastToStrType(Func):
    arg_types = {"this": True, "expression": True}


class Collate(Binary):
    pass


class TryCast(Cast):
    pass


class Ceil(Func):
    arg_types = {"this": True, "decimals": False}
    _sql_names = ["CEIL", "CEILING"]


class Coalesce(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True
    _sql_names = ["COALESCE", "IFNULL", "NVL"]


class Concat(Func):
    arg_types = {"expressions": True}
    is_var_len_args = True


class SafeConcat(Concat):
    pass


class ConcatWs(Concat):
    _sql_names = ["CONCAT_WS"]


class Count(AggFunc):
    arg_types = {"this": False, "expressions": False}
    is_var_len_args = True


class CountIf(AggFunc):
    pass


class CurrentDate(Func):
    arg_types = {"this": False}


class CurrentDatetime(Func):
    arg_types = {"this": False}


class CurrentTime(Func):
    arg_types = {"this": False}


class CurrentTimestamp(Func):
    arg_types = {"this": False}


class CurrentUser(Func):
    arg_types = {"this": False}


class DateAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateDiff(Func, TimeUnit):
    _sql_names = ["DATEDIFF", "DATE_DIFF"]
    arg_types = {"this": True, "expression": True, "unit": False}


class DateTrunc(Func):
    arg_types = {"unit": True, "this": True, "zone": False}


class DatetimeAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


class DayOfWeek(Func):
    _sql_names = ["DAY_OF_WEEK", "DAYOFWEEK"]


class DayOfMonth(Func):
    _sql_names = ["DAY_OF_MONTH", "DAYOFMONTH"]


class DayOfYear(Func):
    _sql_names = ["DAY_OF_YEAR", "DAYOFYEAR"]


class WeekOfYear(Func):
    _sql_names = ["WEEK_OF_YEAR", "WEEKOFYEAR"]


class MonthsBetween(Func):
    arg_types = {"this": True, "expression": True, "roundoff": False}


class LastDateOfMonth(Func):
    pass


class Extract(Func):
    arg_types = {"this": True, "expression": True}


class TimestampAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


class TimeAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


class DateFromParts(Func):
    _sql_names = ["DATEFROMPARTS"]
    arg_types = {"year": True, "month": True, "day": True}


class DateStrToDate(Func):
    pass


class DateToDateStr(Func):
    pass


class DateToDi(Func):
    pass


# https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date
class Date(Func):
    arg_types = {"this": True, "zone": False}


class Day(Func):
    pass


class Decode(Func):
    arg_types = {"this": True, "charset": True, "replace": False}


class DiToDate(Func):
    pass


class Encode(Func):
    arg_types = {"this": True, "charset": True}


class Exp(Func):
    pass


class Explode(Func):
    pass


class Floor(Func):
    arg_types = {"this": True, "decimals": False}


class FromBase64(Func):
    pass


class ToBase64(Func):
    pass


class Greatest(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class GroupConcat(Func):
    arg_types = {"this": True, "separator": False}


class Hex(Func):
    pass


class Xor(Connector, Func):
    arg_types = {"this": False, "expression": False, "expressions": False}


class If(Func):
    arg_types = {"this": True, "true": True, "false": False}


class Initcap(Func):
    arg_types = {"this": True, "expression": False}


class IsNan(Func):
    _sql_names = ["IS_NAN", "ISNAN"]


class JSONKeyValue(Expression):
    arg_types = {"this": True, "expression": True}


class JSONObject(Func):
    arg_types = {
        "expressions": False,
        "null_handling": False,
        "unique_keys": False,
        "return_type": False,
        "format_json": False,
        "encoding": False,
    }


class OpenJSONColumnDef(Expression):
    arg_types = {"this": True, "kind": True, "path": False, "as_json": False}


class OpenJSON(Func):
    arg_types = {"this": True, "path": False, "expressions": False}


class JSONBContains(Binary):
    _sql_names = ["JSONB_CONTAINS"]


class JSONExtract(Binary, Func):
    _sql_names = ["JSON_EXTRACT"]


class JSONExtractScalar(JSONExtract):
    _sql_names = ["JSON_EXTRACT_SCALAR"]


class JSONBExtract(JSONExtract):
    _sql_names = ["JSONB_EXTRACT"]


class JSONBExtractScalar(JSONExtract):
    _sql_names = ["JSONB_EXTRACT_SCALAR"]


class JSONFormat(Func):
    arg_types = {"this": False, "options": False}
    _sql_names = ["JSON_FORMAT"]


# https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#operator_member-of
class JSONArrayContains(Binary, Predicate, Func):
    _sql_names = ["JSON_ARRAY_CONTAINS"]


class Least(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Left(Func):
    arg_types = {"this": True, "expression": True}


class Right(Func):
    arg_types = {"this": True, "expression": True}


class Length(Func):
    _sql_names = ["LENGTH", "LEN"]


class Levenshtein(Func):
    arg_types = {
        "this": True,
        "expression": False,
        "ins_cost": False,
        "del_cost": False,
        "sub_cost": False,
    }


class Ln(Func):
    pass


class Log(Func):
    arg_types = {"this": True, "expression": False}


class Log2(Func):
    pass


class Log10(Func):
    pass


class LogicalOr(AggFunc):
    _sql_names = ["LOGICAL_OR", "BOOL_OR", "BOOLOR_AGG"]


class LogicalAnd(AggFunc):
    _sql_names = ["LOGICAL_AND", "BOOL_AND", "BOOLAND_AGG"]


class Lower(Func):
    _sql_names = ["LOWER", "LCASE"]


class Map(Func):
    arg_types = {"keys": False, "values": False}


class MapFromEntries(Func):
    pass


class StarMap(Func):
    pass


class VarMap(Func):
    arg_types = {"keys": True, "values": True}
    is_var_len_args = True

    @property
    def keys(self) -> t.List[Expression]:
        return self.args["keys"].expressions

    @property
    def values(self) -> t.List[Expression]:
        return self.args["values"].expressions


# https://dev.mysql.com/doc/refman/8.0/en/fulltext-search.html
class MatchAgainst(Func):
    arg_types = {"this": True, "expressions": True, "modifier": False}


class Max(AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class MD5(Func):
    _sql_names = ["MD5"]


# Represents the variant of the MD5 function that returns a binary value
class MD5Digest(Func):
    _sql_names = ["MD5_DIGEST"]


class Min(AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Month(Func):
    pass


class Nvl2(Func):
    arg_types = {"this": True, "true": True, "false": False}


class Posexplode(Func):
    pass


class Pow(Binary, Func):
    _sql_names = ["POWER", "POW"]


class PercentileCont(AggFunc):
    arg_types = {"this": True, "expression": False}


class PercentileDisc(AggFunc):
    arg_types = {"this": True, "expression": False}


class Quantile(AggFunc):
    arg_types = {"this": True, "quantile": True}


class ApproxQuantile(Quantile):
    arg_types = {"this": True, "quantile": True, "accuracy": False, "weight": False}


class RangeN(Func):
    arg_types = {"this": True, "expressions": True, "each": False}


class ReadCSV(Func):
    _sql_names = ["READ_CSV"]
    is_var_len_args = True
    arg_types = {"this": True, "expressions": False}


class Reduce(Func):
    arg_types = {"this": True, "initial": True, "merge": True, "finish": False}


class RegexpExtract(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "occurrence": False,
        "parameters": False,
        "group": False,
    }


class RegexpReplace(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "replacement": True,
        "position": False,
        "occurrence": False,
        "parameters": False,
    }


class RegexpLike(Binary, Func):
    arg_types = {"this": True, "expression": True, "flag": False}


class RegexpILike(Func):
    arg_types = {"this": True, "expression": True, "flag": False}


# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.split.html
# limit is the number of times a pattern is applied
class RegexpSplit(Func):
    arg_types = {"this": True, "expression": True, "limit": False}


class Repeat(Func):
    arg_types = {"this": True, "times": True}


class Round(Func):
    arg_types = {"this": True, "decimals": False}


class RowNumber(Func):
    arg_types: t.Dict[str, t.Any] = {}


class SafeDivide(Func):
    arg_types = {"this": True, "expression": True}


class SetAgg(AggFunc):
    pass


class SHA(Func):
    _sql_names = ["SHA", "SHA1"]


class SHA2(Func):
    _sql_names = ["SHA2"]
    arg_types = {"this": True, "length": False}


class SortArray(Func):
    arg_types = {"this": True, "asc": False}


class Split(Func):
    arg_types = {"this": True, "expression": True, "limit": False}


# Start may be omitted in the case of postgres
# https://www.postgresql.org/docs/9.1/functions-string.html @ Table 9-6
class Substring(Func):
    arg_types = {"this": True, "start": False, "length": False}


class StandardHash(Func):
    arg_types = {"this": True, "expression": False}


class StartsWith(Func):
    _sql_names = ["STARTS_WITH", "STARTSWITH"]
    arg_types = {"this": True, "expression": True}


class StrPosition(Func):
    arg_types = {
        "this": True,
        "substr": True,
        "position": False,
        "instance": False,
    }


class StrToDate(Func):
    arg_types = {"this": True, "format": True}


class StrToTime(Func):
    arg_types = {"this": True, "format": True, "zone": False}


# Spark allows unix_timestamp()
# https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.unix_timestamp.html
class StrToUnix(Func):
    arg_types = {"this": False, "format": False}


class NumberToStr(Func):
    arg_types = {"this": True, "format": True}


class FromBase(Func):
    arg_types = {"this": True, "expression": True}


class Struct(Func):
    arg_types = {"expressions": True}
    is_var_len_args = True


class StructExtract(Func):
    arg_types = {"this": True, "expression": True}


class Sum(AggFunc):
    pass


class Sqrt(Func):
    pass


class Stddev(AggFunc):
    pass


class StddevPop(AggFunc):
    pass


class StddevSamp(AggFunc):
    pass


class TimeToStr(Func):
    arg_types = {"this": True, "format": True}


class TimeToTimeStr(Func):
    pass


class TimeToUnix(Func):
    pass


class TimeStrToDate(Func):
    pass


class TimeStrToTime(Func):
    pass


class TimeStrToUnix(Func):
    pass


class Trim(Func):
    arg_types = {
        "this": True,
        "expression": False,
        "position": False,
        "collation": False,
    }


class TsOrDsAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TsOrDsToDateStr(Func):
    pass


class TsOrDsToDate(Func):
    arg_types = {"this": True, "format": False}


class TsOrDiToDi(Func):
    pass


class Unhex(Func):
    pass


class UnixToStr(Func):
    arg_types = {"this": True, "format": False}


# https://prestodb.io/docs/current/functions/datetime.html
# presto has weird zone/hours/minutes
class UnixToTime(Func):
    arg_types = {"this": True, "scale": False, "zone": False, "hours": False, "minutes": False}

    SECONDS = Literal.string("seconds")
    MILLIS = Literal.string("millis")
    MICROS = Literal.string("micros")


class UnixToTimeStr(Func):
    pass


class Upper(Func):
    _sql_names = ["UPPER", "UCASE"]


class Variance(AggFunc):
    _sql_names = ["VARIANCE", "VARIANCE_SAMP", "VAR_SAMP"]


class VariancePop(AggFunc):
    _sql_names = ["VARIANCE_POP", "VAR_POP"]


class Week(Func):
    arg_types = {"this": True, "mode": False}


class XMLTable(Func):
    arg_types = {"this": True, "passing": False, "columns": False, "by_ref": False}


class Year(Func):
    pass


class Use(Expression):
    arg_types = {"this": True, "kind": False}


class Merge(Expression):
    arg_types = {"this": True, "using": True, "on": True, "expressions": True}


class When(Func):
    arg_types = {"matched": True, "source": False, "condition": False, "then": True}


# https://docs.oracle.com/javadb/10.8.3.0/ref/rrefsqljnextvaluefor.html
# https://learn.microsoft.com/en-us/sql/t-sql/functions/next-value-for-transact-sql?view=sql-server-ver16
class NextValueFor(Func):
    arg_types = {"this": True, "order": False}


def _norm_arg(arg):
    return arg.lower() if type(arg) is str else arg


ALL_FUNCTIONS = subclasses(__name__, Func, (AggFunc, Anonymous, Func))


# Helpers
@t.overload
def maybe_parse(
    sql_or_expression: ExpOrStr,
    *,
    into: t.Type[E],
    dialect: DialectType = None,
    prefix: t.Optional[str] = None,
    copy: bool = False,
    **opts,
) -> E:
    ...


@t.overload
def maybe_parse(
    sql_or_expression: str | E,
    *,
    into: t.Optional[IntoType] = None,
    dialect: DialectType = None,
    prefix: t.Optional[str] = None,
    copy: bool = False,
    **opts,
) -> E:
    ...


def maybe_parse(
    sql_or_expression: ExpOrStr,
    *,
    into: t.Optional[IntoType] = None,
    dialect: DialectType = None,
    prefix: t.Optional[str] = None,
    copy: bool = False,
    **opts,
) -> Expression:
    """Gracefully handle a possible string or expression.

    Example:
        >>> maybe_parse("1")
        (LITERAL this: 1, is_string: False)
        >>> maybe_parse(to_identifier("x"))
        (IDENTIFIER this: x, quoted: False)

    Args:
        sql_or_expression: the SQL code string or an expression
        into: the SQLGlot Expression to parse into
        dialect: the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        prefix: a string to prefix the sql with before it gets parsed
            (automatically includes a space)
        copy: whether or not to copy the expression.
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Expression: the parsed or given expression.
    """
    if isinstance(sql_or_expression, Expression):
        if copy:
            return sql_or_expression.copy()
        return sql_or_expression

    if sql_or_expression is None:
        raise ParseError(f"SQL cannot be None")

    import sqlglot

    sql = str(sql_or_expression)
    if prefix:
        sql = f"{prefix} {sql}"

    return sqlglot.parse_one(sql, read=dialect, into=into, **opts)


def maybe_copy(instance: E, copy: bool = True) -> E:
    return instance.copy() if copy else instance


def _is_wrong_expression(expression, into):
    return isinstance(expression, Expression) and not isinstance(expression, into)


def _apply_builder(
    expression,
    instance,
    arg,
    copy=True,
    prefix=None,
    into=None,
    dialect=None,
    **opts,
):
    if _is_wrong_expression(expression, into):
        expression = into(this=expression)
    instance = maybe_copy(instance, copy)
    expression = maybe_parse(
        sql_or_expression=expression,
        prefix=prefix,
        into=into,
        dialect=dialect,
        **opts,
    )
    instance.set(arg, expression)
    return instance


def _apply_child_list_builder(
    *expressions,
    instance,
    arg,
    append=True,
    copy=True,
    prefix=None,
    into=None,
    dialect=None,
    properties=None,
    **opts,
):
    instance = maybe_copy(instance, copy)
    parsed = []
    for expression in expressions:
        if expression is not None:
            if _is_wrong_expression(expression, into):
                expression = into(expressions=[expression])

            expression = maybe_parse(
                expression,
                into=into,
                dialect=dialect,
                prefix=prefix,
                **opts,
            )
            parsed.extend(expression.expressions)

    existing = instance.args.get(arg)
    if append and existing:
        parsed = existing.expressions + parsed

    child = into(expressions=parsed)
    for k, v in (properties or {}).items():
        child.set(k, v)
    instance.set(arg, child)

    return instance


def _apply_list_builder(
    *expressions,
    instance,
    arg,
    append=True,
    copy=True,
    prefix=None,
    into=None,
    dialect=None,
    **opts,
):
    inst = maybe_copy(instance, copy)

    expressions = [
        maybe_parse(
            sql_or_expression=expression,
            into=into,
            prefix=prefix,
            dialect=dialect,
            **opts,
        )
        for expression in expressions
        if expression is not None
    ]

    existing_expressions = inst.args.get(arg)
    if append and existing_expressions:
        expressions = existing_expressions + expressions

    inst.set(arg, expressions)
    return inst


def _apply_conjunction_builder(
    *expressions,
    instance,
    arg,
    into=None,
    append=True,
    copy=True,
    dialect=None,
    **opts,
):
    expressions = [exp for exp in expressions if exp is not None and exp != ""]
    if not expressions:
        return instance

    inst = maybe_copy(instance, copy)

    existing = inst.args.get(arg)
    if append and existing is not None:
        expressions = [existing.this if into else existing] + list(expressions)

    node = and_(*expressions, dialect=dialect, copy=copy, **opts)

    inst.set(arg, into(this=node) if into else node)
    return inst


def _apply_cte_builder(
    instance: E,
    alias: ExpOrStr,
    as_: ExpOrStr,
    recursive: t.Optional[bool] = None,
    append: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> E:
    alias_expression = maybe_parse(alias, dialect=dialect, into=TableAlias, **opts)
    as_expression = maybe_parse(as_, dialect=dialect, **opts)
    cte = CTE(this=as_expression, alias=alias_expression)
    return _apply_child_list_builder(
        cte,
        instance=instance,
        arg="with",
        append=append,
        copy=copy,
        into=With,
        properties={"recursive": recursive or False},
    )


def _combine(
    expressions: t.Sequence[t.Optional[ExpOrStr]],
    operator: t.Type[Connector],
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Expression:
    conditions = [
        condition(expression, dialect=dialect, copy=copy, **opts)
        for expression in expressions
        if expression is not None
    ]

    this, *rest = conditions
    if rest:
        this = _wrap(this, Connector)
    for expression in rest:
        this = operator(this=this, expression=_wrap(expression, Connector))

    return this


def _wrap(expression: E, kind: t.Type[Expression]) -> E | Paren:
    return Paren(this=expression) if isinstance(expression, kind) else expression


def union(
    left: ExpOrStr, right: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
) -> Union:
    """
    Initializes a syntax tree from one UNION expression.

    Example:
        >>> union("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo UNION SELECT * FROM bla'

    Args:
        left: the SQL code string corresponding to the left-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        right: the SQL code string corresponding to the right-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        distinct: set the DISTINCT flag if and only if this is true.
        dialect: the dialect used to parse the input expression.
        opts: other options to use to parse the input expressions.

    Returns:
        The new Union instance.
    """
    left = maybe_parse(sql_or_expression=left, dialect=dialect, **opts)
    right = maybe_parse(sql_or_expression=right, dialect=dialect, **opts)

    return Union(this=left, expression=right, distinct=distinct)


def intersect(
    left: ExpOrStr, right: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
) -> Intersect:
    """
    Initializes a syntax tree from one INTERSECT expression.

    Example:
        >>> intersect("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo INTERSECT SELECT * FROM bla'

    Args:
        left: the SQL code string corresponding to the left-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        right: the SQL code string corresponding to the right-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        distinct: set the DISTINCT flag if and only if this is true.
        dialect: the dialect used to parse the input expression.
        opts: other options to use to parse the input expressions.

    Returns:
        The new Intersect instance.
    """
    left = maybe_parse(sql_or_expression=left, dialect=dialect, **opts)
    right = maybe_parse(sql_or_expression=right, dialect=dialect, **opts)

    return Intersect(this=left, expression=right, distinct=distinct)


def except_(
    left: ExpOrStr, right: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
) -> Except:
    """
    Initializes a syntax tree from one EXCEPT expression.

    Example:
        >>> except_("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo EXCEPT SELECT * FROM bla'

    Args:
        left: the SQL code string corresponding to the left-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        right: the SQL code string corresponding to the right-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        distinct: set the DISTINCT flag if and only if this is true.
        dialect: the dialect used to parse the input expression.
        opts: other options to use to parse the input expressions.

    Returns:
        The new Except instance.
    """
    left = maybe_parse(sql_or_expression=left, dialect=dialect, **opts)
    right = maybe_parse(sql_or_expression=right, dialect=dialect, **opts)

    return Except(this=left, expression=right, distinct=distinct)


def select(*expressions: ExpOrStr, dialect: DialectType = None, **opts) -> Select:
    """
    Initializes a syntax tree from one or multiple SELECT expressions.

    Example:
        >>> select("col1", "col2").from_("tbl").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expressions: the SQL code string to parse as the expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().select(*expressions, dialect=dialect, **opts)


def from_(expression: ExpOrStr, dialect: DialectType = None, **opts) -> Select:
    """
    Initializes a syntax tree from a FROM expression.

    Example:
        >>> from_("tbl").select("col1", "col2").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expression: the SQL code string to parse as the FROM expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().from_(expression, dialect=dialect, **opts)


def update(
    table: str | Table,
    properties: dict,
    where: t.Optional[ExpOrStr] = None,
    from_: t.Optional[ExpOrStr] = None,
    dialect: DialectType = None,
    **opts,
) -> Update:
    """
    Creates an update statement.

    Example:
        >>> update("my_table", {"x": 1, "y": "2", "z": None}, from_="baz", where="id > 1").sql()
        "UPDATE my_table SET x = 1, y = '2', z = NULL FROM baz WHERE id > 1"

    Args:
        *properties: dictionary of properties to set which are
            auto converted to sql objects eg None -> NULL
        where: sql conditional parsed into a WHERE statement
        from_: sql statement parsed into a FROM statement
        dialect: the dialect used to parse the input expressions.
        **opts: other options to use to parse the input expressions.

    Returns:
        Update: the syntax tree for the UPDATE statement.
    """
    update_expr = Update(this=maybe_parse(table, into=Table, dialect=dialect))
    update_expr.set(
        "expressions",
        [
            EQ(this=maybe_parse(k, dialect=dialect, **opts), expression=convert(v))
            for k, v in properties.items()
        ],
    )
    if from_:
        update_expr.set(
            "from",
            maybe_parse(from_, into=From, dialect=dialect, prefix="FROM", **opts),
        )
    if isinstance(where, Condition):
        where = Where(this=where)
    if where:
        update_expr.set(
            "where",
            maybe_parse(where, into=Where, dialect=dialect, prefix="WHERE", **opts),
        )
    return update_expr


def delete(
    table: ExpOrStr,
    where: t.Optional[ExpOrStr] = None,
    returning: t.Optional[ExpOrStr] = None,
    dialect: DialectType = None,
    **opts,
) -> Delete:
    """
    Builds a delete statement.

    Example:
        >>> delete("my_table", where="id > 1").sql()
        'DELETE FROM my_table WHERE id > 1'

    Args:
        where: sql conditional parsed into a WHERE statement
        returning: sql conditional parsed into a RETURNING statement
        dialect: the dialect used to parse the input expressions.
        **opts: other options to use to parse the input expressions.

    Returns:
        Delete: the syntax tree for the DELETE statement.
    """
    delete_expr = Delete().delete(table, dialect=dialect, copy=False, **opts)
    if where:
        delete_expr = delete_expr.where(where, dialect=dialect, copy=False, **opts)
    if returning:
        delete_expr = delete_expr.returning(returning, dialect=dialect, copy=False, **opts)
    return delete_expr


def insert(
    expression: ExpOrStr,
    into: ExpOrStr,
    columns: t.Optional[t.Sequence[ExpOrStr]] = None,
    overwrite: t.Optional[bool] = None,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Insert:
    """
    Builds an INSERT statement.

    Example:
        >>> insert("VALUES (1, 2, 3)", "tbl").sql()
        'INSERT INTO tbl VALUES (1, 2, 3)'

    Args:
        expression: the sql string or expression of the INSERT statement
        into: the tbl to insert data to.
        columns: optionally the table's column names.
        overwrite: whether to INSERT OVERWRITE or not.
        dialect: the dialect used to parse the input expressions.
        copy: whether or not to copy the expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Insert: the syntax tree for the INSERT statement.
    """
    expr = maybe_parse(expression, dialect=dialect, copy=copy, **opts)
    this: Table | Schema = maybe_parse(into, into=Table, dialect=dialect, copy=copy, **opts)

    if columns:
        this = _apply_list_builder(
            *columns,
            instance=Schema(this=this),
            arg="expressions",
            into=Identifier,
            copy=False,
            dialect=dialect,
            **opts,
        )

    return Insert(this=this, expression=expr, overwrite=overwrite)


def condition(
    expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts
) -> Condition:
    """
    Initialize a logical condition expression.

    Example:
        >>> condition("x=1").sql()
        'x = 1'

        This is helpful for composing larger logical syntax trees:
        >>> where = condition("x=1")
        >>> where = where.and_("y=1")
        >>> Select().from_("tbl").select("*").where(where).sql()
        'SELECT * FROM tbl WHERE x = 1 AND y = 1'

    Args:
        *expression: the SQL code string to parse.
            If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        copy: Whether or not to copy `expression` (only applies to expressions).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        The new Condition instance
    """
    return maybe_parse(
        expression,
        into=Condition,
        dialect=dialect,
        copy=copy,
        **opts,
    )


def and_(
    *expressions: t.Optional[ExpOrStr], dialect: DialectType = None, copy: bool = True, **opts
) -> Condition:
    """
    Combine multiple conditions with an AND logical operator.

    Example:
        >>> and_("x=1", and_("y=1", "z=1")).sql()
        'x = 1 AND (y = 1 AND z = 1)'

    Args:
        *expressions: the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether or not to copy `expressions` (only applies to Expressions).
        **opts: other options to use to parse the input expressions.

    Returns:
        And: the new condition
    """
    return t.cast(Condition, _combine(expressions, And, dialect, copy=copy, **opts))


def or_(
    *expressions: t.Optional[ExpOrStr], dialect: DialectType = None, copy: bool = True, **opts
) -> Condition:
    """
    Combine multiple conditions with an OR logical operator.

    Example:
        >>> or_("x=1", or_("y=1", "z=1")).sql()
        'x = 1 OR (y = 1 OR z = 1)'

    Args:
        *expressions: the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether or not to copy `expressions` (only applies to Expressions).
        **opts: other options to use to parse the input expressions.

    Returns:
        Or: the new condition
    """
    return t.cast(Condition, _combine(expressions, Or, dialect, copy=copy, **opts))


def not_(expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts) -> Not:
    """
    Wrap a condition with a NOT operator.

    Example:
        >>> not_("this_suit='black'").sql()
        "NOT this_suit = 'black'"

    Args:
        expression: the SQL code string to parse.
            If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy the expression or not.
        **opts: other options to use to parse the input expressions.

    Returns:
        The new condition.
    """
    this = condition(
        expression,
        dialect=dialect,
        copy=copy,
        **opts,
    )
    return Not(this=_wrap(this, Connector))


def paren(expression: ExpOrStr, copy: bool = True) -> Paren:
    """
    Wrap an expression in parentheses.

    Example:
        >>> paren("5 + 3").sql()
        '(5 + 3)'

    Args:
        expression: the SQL code string to parse.
            If an Expression instance is passed, this is used as-is.
        copy: whether to copy the expression or not.

    Returns:
        The wrapped expression.
    """
    return Paren(this=maybe_parse(expression, copy=copy))


SAFE_IDENTIFIER_RE = re.compile(r"^[_a-zA-Z][\w]*$")


@t.overload
def to_identifier(name: None, quoted: t.Optional[bool] = None, copy: bool = True) -> None:
    ...


@t.overload
def to_identifier(
    name: str | Identifier, quoted: t.Optional[bool] = None, copy: bool = True
) -> Identifier:
    ...


def to_identifier(name, quoted=None, copy=True):
    """Builds an identifier.

    Args:
        name: The name to turn into an identifier.
        quoted: Whether or not force quote the identifier.
        copy: Whether or not to copy a passed in Identefier node.

    Returns:
        The identifier ast node.
    """

    if name is None:
        return None

    if isinstance(name, Identifier):
        identifier = maybe_copy(name, copy)
    elif isinstance(name, str):
        identifier = Identifier(
            this=name,
            quoted=not SAFE_IDENTIFIER_RE.match(name) if quoted is None else quoted,
        )
    else:
        raise ValueError(f"Name needs to be a string or an Identifier, got: {name.__class__}")
    return identifier


INTERVAL_STRING_RE = re.compile(r"\s*([0-9]+)\s*([a-zA-Z]+)\s*")


def to_interval(interval: str | Literal) -> Interval:
    """Builds an interval expression from a string like '1 day' or '5 months'."""
    if isinstance(interval, Literal):
        if not interval.is_string:
            raise ValueError("Invalid interval string.")

        interval = interval.this

    interval_parts = INTERVAL_STRING_RE.match(interval)  # type: ignore

    if not interval_parts:
        raise ValueError("Invalid interval string.")

    return Interval(
        this=Literal.string(interval_parts.group(1)),
        unit=Var(this=interval_parts.group(2)),
    )


@t.overload
def to_table(sql_path: str | Table, **kwargs) -> Table:
    ...


@t.overload
def to_table(sql_path: None, **kwargs) -> None:
    ...


def to_table(
    sql_path: t.Optional[str | Table], dialect: DialectType = None, **kwargs
) -> t.Optional[Table]:
    """
    Create a table expression from a `[catalog].[schema].[table]` sql path. Catalog and schema are optional.
    If a table is passed in then that table is returned.

    Args:
        sql_path: a `[catalog].[schema].[table]` string.
        dialect: the source dialect according to which the table name will be parsed.
        kwargs: the kwargs to instantiate the resulting `Table` expression with.

    Returns:
        A table expression.
    """
    if sql_path is None or isinstance(sql_path, Table):
        return sql_path
    if not isinstance(sql_path, str):
        raise ValueError(f"Invalid type provided for a table: {type(sql_path)}")

    table = maybe_parse(sql_path, into=Table, dialect=dialect)
    if table:
        for k, v in kwargs.items():
            table.set(k, v)

    return table


def to_column(sql_path: str | Column, **kwargs) -> Column:
    """
    Create a column from a `[table].[column]` sql path. Schema is optional.

    If a column is passed in then that column is returned.

    Args:
        sql_path: `[table].[column]` string
    Returns:
        Table: A column expression
    """
    if sql_path is None or isinstance(sql_path, Column):
        return sql_path
    if not isinstance(sql_path, str):
        raise ValueError(f"Invalid type provided for column: {type(sql_path)}")
    return column(*reversed(sql_path.split(".")), **kwargs)  # type: ignore


def alias_(
    expression: ExpOrStr,
    alias: str | Identifier,
    table: bool | t.Sequence[str | Identifier] = False,
    quoted: t.Optional[bool] = None,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
):
    """Create an Alias expression.

    Example:
        >>> alias_('foo', 'bar').sql()
        'foo AS bar'

        >>> alias_('(select 1, 2)', 'bar', table=['a', 'b']).sql()
        '(SELECT 1, 2) AS bar(a, b)'

    Args:
        expression: the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        alias: the alias name to use. If the name has
            special characters it is quoted.
        table: Whether or not to create a table alias, can also be a list of columns.
        quoted: whether or not to quote the alias
        dialect: the dialect used to parse the input expression.
        copy: Whether or not to copy the expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Alias: the aliased expression
    """
    exp = maybe_parse(expression, dialect=dialect, copy=copy, **opts)
    alias = to_identifier(alias, quoted=quoted)

    if table:
        table_alias = TableAlias(this=alias)
        exp.set("alias", table_alias)

        if not isinstance(table, bool):
            for column in table:
                table_alias.append("columns", to_identifier(column, quoted=quoted))

        return exp

    # We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
    # the AST, representing a "named_window" [1] construct (eg. bigquery). What we want is an ALIAS node
    # for the complete Window expression.
    #
    # [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls

    if "alias" in exp.arg_types and not isinstance(exp, Window):
        exp.set("alias", alias)
        return exp
    return Alias(this=exp, alias=alias)


def subquery(
    expression: ExpOrStr,
    alias: t.Optional[Identifier | str] = None,
    dialect: DialectType = None,
    **opts,
) -> Select:
    """
    Build a subquery expression.

    Example:
        >>> subquery('select x from tbl', 'bar').select('x').sql()
        'SELECT x FROM (SELECT x FROM tbl) AS bar'

    Args:
        expression: the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        alias: the alias name to use.
        dialect: the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        A new Select instance with the subquery expression included.
    """

    expression = maybe_parse(expression, dialect=dialect, **opts).subquery(alias)
    return Select().from_(expression, dialect=dialect, **opts)


def column(
    col: str | Identifier,
    table: t.Optional[str | Identifier] = None,
    db: t.Optional[str | Identifier] = None,
    catalog: t.Optional[str | Identifier] = None,
    quoted: t.Optional[bool] = None,
) -> Column:
    """
    Build a Column.

    Args:
        col: Column name.
        table: Table name.
        db: Database name.
        catalog: Catalog name.
        quoted: Whether to force quotes on the column's identifiers.

    Returns:
        The new Column instance.
    """
    return Column(
        this=to_identifier(col, quoted=quoted),
        table=to_identifier(table, quoted=quoted),
        db=to_identifier(db, quoted=quoted),
        catalog=to_identifier(catalog, quoted=quoted),
    )


def cast(expression: ExpOrStr, to: str | DataType | DataType.Type, **opts) -> Cast:
    """Cast an expression to a data type.

    Example:
        >>> cast('x + 1', 'int').sql()
        'CAST(x + 1 AS INT)'

    Args:
        expression: The expression to cast.
        to: The datatype to cast to.

    Returns:
        The new Cast instance.
    """
    expression = maybe_parse(expression, **opts)
    return Cast(this=expression, to=DataType.build(to, **opts))


def table_(
    table: Identifier | str,
    db: t.Optional[Identifier | str] = None,
    catalog: t.Optional[Identifier | str] = None,
    quoted: t.Optional[bool] = None,
    alias: t.Optional[Identifier | str] = None,
) -> Table:
    """Build a Table.

    Args:
        table: Table name.
        db: Database name.
        catalog: Catalog name.
        quote: Whether to force quotes on the table's identifiers.
        alias: Table's alias.

    Returns:
        The new Table instance.
    """
    return Table(
        this=to_identifier(table, quoted=quoted),
        db=to_identifier(db, quoted=quoted),
        catalog=to_identifier(catalog, quoted=quoted),
        alias=TableAlias(this=to_identifier(alias)) if alias else None,
    )


def values(
    values: t.Iterable[t.Tuple[t.Any, ...]],
    alias: t.Optional[str] = None,
    columns: t.Optional[t.Iterable[str] | t.Dict[str, DataType]] = None,
) -> Values:
    """Build VALUES statement.

    Example:
        >>> values([(1, '2')]).sql()
        "VALUES (1, '2')"

    Args:
        values: values statements that will be converted to SQL
        alias: optional alias
        columns: Optional list of ordered column names or ordered dictionary of column names to types.
         If either are provided then an alias is also required.

    Returns:
        Values: the Values expression object
    """
    if columns and not alias:
        raise ValueError("Alias is required when providing columns")

    return Values(
        expressions=[convert(tup) for tup in values],
        alias=(
            TableAlias(this=to_identifier(alias), columns=[to_identifier(x) for x in columns])
            if columns
            else (TableAlias(this=to_identifier(alias)) if alias else None)
        ),
    )


def var(name: t.Optional[ExpOrStr]) -> Var:
    """Build a SQL variable.

    Example:
        >>> repr(var('x'))
        '(VAR this: x)'

        >>> repr(var(column('x', table='y')))
        '(VAR this: x)'

    Args:
        name: The name of the var or an expression who's name will become the var.

    Returns:
        The new variable node.
    """
    if not name:
        raise ValueError("Cannot convert empty name into var.")

    if isinstance(name, Expression):
        name = name.name
    return Var(this=name)


def rename_table(old_name: str | Table, new_name: str | Table) -> AlterTable:
    """Build ALTER TABLE... RENAME... expression

    Args:
        old_name: The old name of the table
        new_name: The new name of the table

    Returns:
        Alter table expression
    """
    old_table = to_table(old_name)
    new_table = to_table(new_name)
    return AlterTable(
        this=old_table,
        actions=[
            RenameTable(this=new_table),
        ],
    )


def convert(value: t.Any, copy: bool = False) -> Expression:
    """Convert a python value into an expression object.

    Raises an error if a conversion is not possible.

    Args:
        value: A python object.
        copy: Whether or not to copy `value` (only applies to Expressions and collections).

    Returns:
        Expression: the equivalent expression object.
    """
    if isinstance(value, Expression):
        return maybe_copy(value, copy)
    if isinstance(value, str):
        return Literal.string(value)
    if isinstance(value, bool):
        return Boolean(this=value)
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return NULL
    if isinstance(value, numbers.Number):
        return Literal.number(value)
    if isinstance(value, datetime.datetime):
        datetime_literal = Literal.string(
            (value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)).isoformat()
        )
        return TimeStrToTime(this=datetime_literal)
    if isinstance(value, datetime.date):
        date_literal = Literal.string(value.strftime("%Y-%m-%d"))
        return DateStrToDate(this=date_literal)
    if isinstance(value, tuple):
        return Tuple(expressions=[convert(v, copy=copy) for v in value])
    if isinstance(value, list):
        return Array(expressions=[convert(v, copy=copy) for v in value])
    if isinstance(value, dict):
        return Map(
            keys=[convert(k, copy=copy) for k in value],
            values=[convert(v, copy=copy) for v in value.values()],
        )
    raise ValueError(f"Cannot convert {value}")


def replace_children(expression: Expression, fun: t.Callable, *args, **kwargs) -> None:
    """
    Replace children of an expression with the result of a lambda fun(child) -> exp.
    """
    for k, v in expression.args.items():
        is_list_arg = type(v) is list

        child_nodes = v if is_list_arg else [v]
        new_child_nodes = []

        for cn in child_nodes:
            if isinstance(cn, Expression):
                for child_node in ensure_collection(fun(cn, *args, **kwargs)):
                    new_child_nodes.append(child_node)
                    child_node.parent = expression
                    child_node.arg_key = k
            else:
                new_child_nodes.append(cn)

        expression.args[k] = new_child_nodes if is_list_arg else seq_get(new_child_nodes, 0)


def column_table_names(expression: Expression, exclude: str = "") -> t.Set[str]:
    """
    Return all table names referenced through columns in an expression.

    Example:
        >>> import sqlglot
        >>> sorted(column_table_names(sqlglot.parse_one("a.b AND c.d AND c.e")))
        ['a', 'c']

    Args:
        expression: expression to find table names.
        exclude: a table name to exclude

    Returns:
        A list of unique names.
    """
    return {
        table
        for table in (column.table for column in expression.find_all(Column))
        if table and table != exclude
    }


def table_name(table: Table | str, dialect: DialectType = None) -> str:
    """Get the full name of a table as a string.

    Args:
        table: Table expression node or string.
        dialect: The dialect to generate the table name for.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> table_name(parse_one("select * from a.b.c").find(exp.Table))
        'a.b.c'

    Returns:
        The table name.
    """

    table = maybe_parse(table, into=Table)

    if not table:
        raise ValueError(f"Cannot parse {table}")

    return ".".join(
        part.sql(dialect=dialect, identify=True)
        if not SAFE_IDENTIFIER_RE.match(part.name)
        else part.name
        for part in table.parts
    )


def replace_tables(expression: E, mapping: t.Dict[str, str], copy: bool = True) -> E:
    """Replace all tables in expression according to the mapping.

    Args:
        expression: expression node to be transformed and replaced.
        mapping: mapping of table names.
        copy: whether or not to copy the expression.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> replace_tables(parse_one("select * from a.b"), {"a.b": "c"}).sql()
        'SELECT * FROM c'

    Returns:
        The mapped expression.
    """

    def _replace_tables(node: Expression) -> Expression:
        if isinstance(node, Table):
            new_name = mapping.get(table_name(node))
            if new_name:
                return to_table(
                    new_name,
                    **{k: v for k, v in node.args.items() if k not in ("this", "db", "catalog")},
                )
        return node

    return expression.transform(_replace_tables, copy=copy)


def replace_placeholders(expression: Expression, *args, **kwargs) -> Expression:
    """Replace placeholders in an expression.

    Args:
        expression: expression node to be transformed and replaced.
        args: positional names that will substitute unnamed placeholders in the given order.
        kwargs: keyword arguments that will substitute named placeholders.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> replace_placeholders(
        ...     parse_one("select * from :tbl where ? = ?"),
        ...     exp.to_identifier("str_col"), "b", tbl=exp.to_identifier("foo")
        ... ).sql()
        "SELECT * FROM foo WHERE str_col = 'b'"

    Returns:
        The mapped expression.
    """

    def _replace_placeholders(node: Expression, args, **kwargs) -> Expression:
        if isinstance(node, Placeholder):
            if node.name:
                new_name = kwargs.get(node.name)
                if new_name:
                    return convert(new_name)
            else:
                try:
                    return convert(next(args))
                except StopIteration:
                    pass
        return node

    return expression.transform(_replace_placeholders, iter(args), **kwargs)


def expand(
    expression: Expression, sources: t.Dict[str, Subqueryable], copy: bool = True
) -> Expression:
    """Transforms an expression by expanding all referenced sources into subqueries.

    Examples:
        >>> from sqlglot import parse_one
        >>> expand(parse_one("select * from x AS z"), {"x": parse_one("select * from y")}).sql()
        'SELECT * FROM (SELECT * FROM y) AS z /* source: x */'

        >>> expand(parse_one("select * from x AS z"), {"x": parse_one("select * from y"), "y": parse_one("select * from z")}).sql()
        'SELECT * FROM (SELECT * FROM (SELECT * FROM z) AS y /* source: y */) AS z /* source: x */'

    Args:
        expression: The expression to expand.
        sources: A dictionary of name to Subqueryables.
        copy: Whether or not to copy the expression during transformation. Defaults to True.

    Returns:
        The transformed expression.
    """

    def _expand(node: Expression):
        if isinstance(node, Table):
            name = table_name(node)
            source = sources.get(name)
            if source:
                subquery = source.subquery(node.alias or name)
                subquery.comments = [f"source: {name}"]
                return subquery.transform(_expand, copy=False)
        return node

    return expression.transform(_expand, copy=copy)


def func(name: str, *args, dialect: DialectType = None, **kwargs) -> Func:
    """
    Returns a Func expression.

    Examples:
        >>> func("abs", 5).sql()
        'ABS(5)'

        >>> func("cast", this=5, to=DataType.build("DOUBLE")).sql()
        'CAST(5 AS DOUBLE)'

    Args:
        name: the name of the function to build.
        args: the args used to instantiate the function of interest.
        dialect: the source dialect.
        kwargs: the kwargs used to instantiate the function of interest.

    Note:
        The arguments `args` and `kwargs` are mutually exclusive.

    Returns:
        An instance of the function of interest, or an anonymous function, if `name` doesn't
        correspond to an existing `sqlglot.expressions.Func` class.
    """
    if args and kwargs:
        raise ValueError("Can't use both args and kwargs to instantiate a function.")

    from sqlglot.dialects.dialect import Dialect

    converted: t.List[Expression] = [maybe_parse(arg, dialect=dialect) for arg in args]
    kwargs = {key: maybe_parse(value, dialect=dialect) for key, value in kwargs.items()}

    parser = Dialect.get_or_raise(dialect)().parser()
    from_args_list = parser.FUNCTIONS.get(name.upper())

    if from_args_list:
        function = from_args_list(converted) if converted else from_args_list.__self__(**kwargs)  # type: ignore
    else:
        kwargs = kwargs or {"expressions": converted}
        function = Anonymous(this=name, **kwargs)

    for error_message in function.error_messages(converted):
        raise ValueError(error_message)

    return function


def true() -> Boolean:
    """
    Returns a true Boolean expression.
    """
    return Boolean(this=True)


def false() -> Boolean:
    """
    Returns a false Boolean expression.
    """
    return Boolean(this=False)


def null() -> Null:
    """
    Returns a Null expression.
    """
    return Null()


# TODO: deprecate this
TRUE = Boolean(this=True)
FALSE = Boolean(this=False)
NULL = Null()
