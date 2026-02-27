"""sqlglot expressions core - base classes, traits, operators, and helpers."""

from __future__ import annotations

import datetime
import logging
import math
import numbers
import re
import sys
import textwrap
import typing as t
from collections import deque
from copy import deepcopy
from decimal import Decimal
from functools import reduce

from sqlglot._typing import E
from sqlglot.errors import ParseError
from sqlglot.helper import (
    camel_to_snake_case,
    ensure_list,
    seq_get,
    to_bool,
    trait,
)

if t.TYPE_CHECKING:
    from sqlglot._typing import Lit
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.expressions.datatypes import DATA_TYPE, DataType, DType, Interval, IntervalSpan
    from sqlglot.expressions.query import Select
    from sqlglot.tokens import Token

logger = logging.getLogger("sqlglot")

SQLGLOT_META: str = "sqlglot.meta"
SQLGLOT_ANONYMOUS = "sqlglot.anonymous"
TABLE_PARTS = ("this", "db", "catalog")
COLUMN_PARTS = ("this", "table", "db", "catalog")
POSITION_META_KEYS: t.Tuple[str, ...] = ("line", "col", "start", "end")
UNITTEST: bool = "unittest" in sys.modules or "pytest" in sys.modules


@trait
class Expr:
    """
    The base class for all expressions in a syntax tree. Each Expr encapsulates any necessary
    context, such as its child expressions, their names (arg keys), and whether a given child expression
    is optional or not.

    Attributes:
        key: a unique key for each class in the Expr hierarchy. This is useful for hashing
            and representing expressions as strings.
        arg_types: determines the arguments (child nodes) supported by an expression. It maps
            arg keys to booleans that indicate whether the corresponding args are optional.
        parent: a reference to the parent expression (or None, in case of root expressions).
        arg_key: the arg key an expression is associated with, i.e. the name its parent expression
            uses to refer to it.
        index: the index of an expression if it is inside of a list argument in its parent.
        comments: a list of comments that are associated with a given expression. This is used in
            order to preserve comments when transpiling SQL code.
        type: the `sqlglot.expressions.DataType` type of an expression. This is inferred by the
            optimizer, in order to enable some transformations that require type information.
        meta: a dictionary that can be used to store useful metadata for a given expression.

    Example:
        >>> class Foo(Expr):
        ...     arg_types = {"this": True, "expression": False}

        The above definition informs us that Foo is an Expr that requires an argument called
        "this" and may also optionally receive an argument called "expression".

    Args:
        args: a mapping used for retrieving the arguments of an expression, given their arg keys.
    """

    key: t.ClassVar[str] = "expression"
    arg_types: t.ClassVar[t.Dict[str, bool]] = {"this": True}
    required_args: t.ClassVar[t.Set[str]] = {"this"}
    is_var_len_args: t.ClassVar[bool] = False
    _hash_raw_args: t.ClassVar[bool] = False
    is_subquery: t.ClassVar[bool] = False
    is_cast: t.ClassVar[bool] = False

    args: t.Dict[str, t.Any]
    parent: t.Optional[Expr]
    arg_key: t.Optional[str]
    index: t.Optional[int]
    comments: t.Optional[t.List[str]]
    _type: t.Optional[DataType]
    _meta: t.Optional[t.Dict[str, t.Any]]
    _hash: t.Optional[int]

    @classmethod
    def __init_subclass__(cls, **kwargs: t.Any) -> None:
        super().__init_subclass__(**kwargs)
        # When an Expr class is created, its key is automatically set
        # to be the lowercase version of the class' name.
        cls.key = cls.__name__.lower()
        cls.required_args = {k for k, v in cls.arg_types.items() if v}
        # This is so that docstrings are not inherited in pdoc
        setattr(cls, "__doc__", getattr(cls, "__doc__", None) or "")

    def __init__(self, **args: object) -> None:
        self.args: t.Dict[str, t.Any] = args
        self.parent: t.Optional[Expr] = None
        self.arg_key: t.Optional[str] = None
        self.index: t.Optional[int] = None
        self.comments: t.Optional[t.List[str]] = None
        self._type: t.Optional[DataType] = None
        self._meta: t.Optional[t.Dict[str, t.Any]] = None
        self._hash: t.Optional[int] = None

        for arg_key, value in self.args.items():
            self._set_parent(arg_key, value)
        if hasattr(self, "_post_init"):
            self._post_init()

    @property
    def this(self) -> t.Any:
        raise NotImplementedError

    @property
    def expression(self) -> t.Any:
        raise NotImplementedError

    @property
    def expressions(self) -> t.List[t.Any]:
        raise NotImplementedError

    def text(self, key: str) -> str:
        raise NotImplementedError

    @property
    def is_string(self) -> bool:
        raise NotImplementedError

    @property
    def is_number(self) -> bool:
        raise NotImplementedError

    def to_py(self) -> t.Any:
        raise NotImplementedError

    @property
    def is_int(self) -> bool:
        raise NotImplementedError

    @property
    def is_star(self) -> bool:
        raise NotImplementedError

    @property
    def alias(self) -> str:
        raise NotImplementedError

    @property
    def alias_column_names(self) -> t.List[str]:
        raise NotImplementedError

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def alias_or_name(self) -> str:
        raise NotImplementedError

    @property
    def output_name(self) -> str:
        raise NotImplementedError

    @property
    def type(self) -> t.Optional[DataType]:
        raise NotImplementedError

    @type.setter
    def type(self, dtype: t.Optional[DataType | DType | str]) -> None:
        raise NotImplementedError

    def is_type(self, *dtypes: DATA_TYPE) -> bool:
        raise NotImplementedError

    def is_leaf(self) -> bool:
        raise NotImplementedError

    @property
    def meta(self) -> t.Dict[str, t.Any]:
        raise NotImplementedError

    def __deepcopy__(self, memo: t.Any) -> Expr:
        raise NotImplementedError

    def copy(self: E) -> E:
        raise NotImplementedError

    def add_comments(self, comments: t.Optional[t.List[str]] = None, prepend: bool = False) -> None:
        raise NotImplementedError

    def pop_comments(self) -> t.List[str]:
        raise NotImplementedError

    def append(self, arg_key: str, value: t.Any) -> None:
        raise NotImplementedError

    def set(
        self,
        arg_key: str,
        value: object,
        index: t.Optional[int] = None,
        overwrite: bool = True,
    ) -> None:
        raise NotImplementedError

    def _set_parent(self, arg_key: str, value: object, index: t.Optional[int] = None) -> None:
        raise NotImplementedError

    @property
    def depth(self) -> int:
        raise NotImplementedError

    def iter_expressions(self: E, reverse: bool = False) -> t.Iterator[E]:
        raise NotImplementedError

    def find(self, *expression_types: t.Type[E], bfs: bool = True) -> t.Optional[E]:
        raise NotImplementedError

    def find_all(self, *expression_types: t.Type[E], bfs: bool = True) -> t.Iterator[E]:
        raise NotImplementedError

    def find_ancestor(self, *expression_types: t.Type[E]) -> t.Optional[E]:
        raise NotImplementedError

    @property
    def parent_select(self) -> t.Optional[Select]:
        raise NotImplementedError

    @property
    def same_parent(self) -> bool:
        raise NotImplementedError

    def root(self) -> Expr:
        raise NotImplementedError

    def walk(
        self, bfs: bool = True, prune: t.Optional[t.Callable[[Expr], bool]] = None
    ) -> t.Iterator[Expr]:
        raise NotImplementedError

    def dfs(self, prune: t.Optional[t.Callable[[Expr], bool]] = None) -> t.Iterator[Expr]:
        raise NotImplementedError

    def bfs(self, prune: t.Optional[t.Callable[[Expr], bool]] = None) -> t.Iterator[Expr]:
        raise NotImplementedError

    def unnest(self) -> Expr:
        raise NotImplementedError

    def unalias(self) -> Expr:
        raise NotImplementedError

    def unnest_operands(self) -> t.Tuple[Expr, ...]:
        raise NotImplementedError

    def flatten(self, unnest: bool = True) -> t.Iterator[Expr]:
        raise NotImplementedError

    def to_s(self) -> str:
        raise NotImplementedError

    def sql(self, dialect: DialectType = None, **opts: t.Any) -> str:
        raise NotImplementedError

    def transform(
        self, fun: t.Callable, *args: object, copy: bool = True, **kwargs: object
    ) -> t.Any:
        raise NotImplementedError

    def replace(self, expression: t.Any) -> t.Any:
        raise NotImplementedError

    def pop(self: E) -> E:
        raise NotImplementedError

    def assert_is(self, type_: t.Type[E]) -> E:
        raise NotImplementedError

    def error_messages(self, args: t.Optional[t.Sequence] = None) -> t.List[str]:
        raise NotImplementedError

    def dump(self) -> t.Any:
        """
        Dump this Expr to a JSON-serializable dict.
        """
        from sqlglot.serde import dump

        return dump(self)

    @classmethod
    def load(cls, obj: t.Any) -> Expr:
        """
        Load a dict (as returned by `Expr.dump`) into an Expr instance.
        """
        from sqlglot.serde import load

        result = load(obj)
        assert isinstance(result, Expr)
        return result

    def and_(
        self,
        *expressions: t.Optional[ExpOrStr],
        dialect: DialectType = None,
        copy: bool = True,
        wrap: bool = True,
        **opts: t.Any,
    ) -> Condition:
        raise NotImplementedError

    def or_(
        self,
        *expressions: t.Optional[ExpOrStr],
        dialect: DialectType = None,
        copy: bool = True,
        wrap: bool = True,
        **opts: t.Any,
    ) -> Condition:
        raise NotImplementedError

    def not_(self, copy: bool = True) -> Not:
        raise NotImplementedError

    def update_positions(
        self: E,
        other: t.Optional[Token | Expr] = None,
        line: t.Optional[int] = None,
        col: t.Optional[int] = None,
        start: t.Optional[int] = None,
        end: t.Optional[int] = None,
    ) -> E:
        raise NotImplementedError

    def as_(
        self,
        alias: str | Identifier,
        quoted: t.Optional[bool] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: t.Any,
    ) -> Alias:
        raise NotImplementedError

    def _binop(self, klass: t.Type[E], other: t.Any, reverse: bool = False) -> E:
        raise NotImplementedError

    def __getitem__(self, other: ExpOrStr | t.Tuple[ExpOrStr]) -> Bracket:
        raise NotImplementedError

    def __iter__(self) -> t.Iterator:
        raise NotImplementedError

    def isin(
        self,
        *expressions: t.Any,
        query: t.Optional[ExpOrStr] = None,
        unnest: t.Optional[ExpOrStr] | t.Collection[ExpOrStr] = None,
        copy: bool = True,
        **opts,
    ) -> In:
        raise NotImplementedError

    def between(
        self,
        low: t.Any,
        high: t.Any,
        copy: bool = True,
        symmetric: t.Optional[bool] = None,
        **opts,
    ) -> Between:
        raise NotImplementedError

    def is_(self, other: ExpOrStr) -> Is:
        raise NotImplementedError

    def like(self, other: ExpOrStr) -> Like:
        raise NotImplementedError

    def ilike(self, other: ExpOrStr) -> ILike:
        raise NotImplementedError

    def eq(self, other: t.Any) -> EQ:
        raise NotImplementedError

    def neq(self, other: t.Any) -> NEQ:
        raise NotImplementedError

    def rlike(self, other: ExpOrStr) -> RegexpLike:
        raise NotImplementedError

    def div(self, other: ExpOrStr, typed: bool = False, safe: bool = False) -> Div:
        raise NotImplementedError

    def asc(self, nulls_first: bool = True) -> Ordered:
        raise NotImplementedError

    def desc(self, nulls_first: bool = False) -> Ordered:
        raise NotImplementedError

    def __lt__(self, other: t.Any) -> LT:
        raise NotImplementedError

    def __le__(self, other: t.Any) -> LTE:
        raise NotImplementedError

    def __gt__(self, other: t.Any) -> GT:
        raise NotImplementedError

    def __ge__(self, other: t.Any) -> GTE:
        raise NotImplementedError

    def __add__(self, other: t.Any) -> Add:
        raise NotImplementedError

    def __radd__(self, other: t.Any) -> Add:
        raise NotImplementedError

    def __sub__(self, other: t.Any) -> Sub:
        raise NotImplementedError

    def __rsub__(self, other: t.Any) -> Sub:
        raise NotImplementedError

    def __mul__(self, other: t.Any) -> Mul:
        raise NotImplementedError

    def __rmul__(self, other: t.Any) -> Mul:
        raise NotImplementedError

    def __truediv__(self, other: t.Any) -> Div:
        raise NotImplementedError

    def __rtruediv__(self, other: t.Any) -> Div:
        raise NotImplementedError

    def __floordiv__(self, other: t.Any) -> IntDiv:
        raise NotImplementedError

    def __rfloordiv__(self, other: t.Any) -> IntDiv:
        raise NotImplementedError

    def __mod__(self, other: t.Any) -> Mod:
        raise NotImplementedError

    def __rmod__(self, other: t.Any) -> Mod:
        raise NotImplementedError

    def __pow__(self, other: t.Any) -> Pow:
        raise NotImplementedError

    def __rpow__(self, other: t.Any) -> Pow:
        raise NotImplementedError

    def __and__(self, other: t.Any) -> And:
        raise NotImplementedError

    def __rand__(self, other: t.Any) -> And:
        raise NotImplementedError

    def __or__(self, other: t.Any) -> Or:
        raise NotImplementedError

    def __ror__(self, other: t.Any) -> Or:
        raise NotImplementedError

    def __neg__(self) -> Neg:
        raise NotImplementedError

    def __invert__(self) -> Not:
        raise NotImplementedError


class Expression(Expr):
    __slots__ = (
        "args",
        "parent",
        "arg_key",
        "index",
        "comments",
        "_type",
        "_meta",
        "_hash",
    )

    def __eq__(self, other: object) -> bool:
        return self is other or (type(self) is type(other) and hash(self) == hash(other))

    def __hash__(self) -> int:
        if self._hash is None:
            nodes: t.List[Expr] = []
            queue: t.Deque[Expr] = deque()
            queue.append(self)

            while queue:
                node = queue.popleft()
                nodes.append(node)

                for child in node.iter_expressions():
                    if child._hash is None:
                        queue.append(child)

            for node in reversed(nodes):
                hash_ = hash(node.key)

                if node._hash_raw_args:
                    for k, v in sorted(node.args.items()):
                        if v:
                            hash_ = hash((hash_, k, v))
                else:
                    for k, v in sorted(node.args.items()):
                        vt = type(v)

                        if vt is list:
                            for x in v:
                                if x is not None and x is not False:
                                    hash_ = hash((hash_, k, x.lower() if type(x) is str else x))
                                else:
                                    hash_ = hash((hash_, k))
                        elif v is not None and v is not False:
                            hash_ = hash((hash_, k, v.lower() if vt is str else v))

                node._hash = hash_
        assert self._hash
        return self._hash

    def __reduce__(self) -> t.Tuple[t.Callable, t.Tuple[t.List[t.Dict[str, t.Any]]]]:
        from sqlglot.serde import dump, load

        return (load, (dump(self),))

    @property
    def this(self) -> t.Any:
        """
        Retrieves the argument with key "this".
        """
        return self.args.get("this")

    @property
    def expression(self) -> t.Any:
        """
        Retrieves the argument with key "expression".
        """
        return self.args.get("expression")

    @property
    def expressions(self) -> t.List[t.Any]:
        """
        Retrieves the argument with key "expressions".
        """
        return self.args.get("expressions") or []

    def text(self, key: str) -> str:
        """
        Returns a textual representation of the argument corresponding to "key". This can only be used
        for args that are strings or leaf Expr instances, such as identifiers and literals.
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
        return (isinstance(self, Literal) and not self.args["is_string"]) or (
            isinstance(self, Neg) and self.this.is_number
        )

    def to_py(self) -> t.Any:
        """
        Returns a Python object equivalent of the SQL node.
        """
        raise ValueError(f"{self} cannot be converted to a Python object.")

    @property
    def is_int(self) -> bool:
        """
        Checks whether an expression is an integer.
        """
        return self.is_number and isinstance(self.to_py(), int)

    @property
    def is_star(self) -> bool:
        """Checks whether an expression is a star."""
        return isinstance(self, Star) or (isinstance(self, Column) and isinstance(self.this, Star))

    @property
    def alias(self) -> str:
        """
        Returns the alias of the expression, or an empty string if it's not aliased.
        """
        alias = self.args.get("alias")
        if type(alias).__name__ == "TableAlias":
            return alias.name  # type: ignore[union-attr]
        return self.text("alias")

    @property
    def alias_column_names(self) -> t.List[str]:
        table_alias = self.args.get("alias")
        if not table_alias:
            return []
        return [c.name for c in table_alias.args.get("columns") or []]

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

        If the Expr has no output name, an empty string is returned.

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
        if self.is_cast:
            return self._type or self.to  # type: ignore[attr-defined]
        return self._type

    @type.setter
    def type(self, dtype: t.Optional[DataType | DType | str]) -> None:
        if dtype and type(dtype).__name__ != "DataType":
            from sqlglot.expressions.datatypes import DataType as _DataType

            dtype = _DataType.build(dtype)
        self._type = dtype  # type: ignore[assignment]

    def is_type(self, *dtypes: DATA_TYPE) -> bool:
        return self.type is not None and self.type.is_type(*dtypes)

    def is_leaf(self) -> bool:
        return not any((isinstance(v, Expr) or type(v) is list) and v for v in self.args.values())

    @property
    def meta(self) -> t.Dict[str, t.Any]:
        if self._meta is None:
            self._meta = {}
        return self._meta

    def __deepcopy__(self, memo: t.Any) -> Expr:
        root = self.__class__()
        stack: t.List[t.Tuple[Expr, Expr]] = [(self, root)]

        while stack:
            node, copy = stack.pop()

            if node.comments is not None:
                copy.comments = deepcopy(node.comments)
            if node._type is not None:
                copy._type = deepcopy(node._type)
            if node._meta is not None:
                copy._meta = deepcopy(node._meta)
            if node._hash is not None:
                copy._hash = node._hash

            for k, vs in node.args.items():
                if isinstance(vs, Expr):
                    stack.append((vs, vs.__class__()))
                    copy.set(k, stack[-1][-1])
                elif type(vs) is list:
                    copy.args[k] = []

                    for v in vs:
                        if isinstance(v, Expr):
                            stack.append((v, v.__class__()))
                            copy.append(k, stack[-1][-1])
                        else:
                            copy.append(k, v)
                else:
                    copy.args[k] = vs

        return root

    def copy(self: E) -> E:
        """
        Returns a deep copy of the expression.
        """
        return deepcopy(self)

    def add_comments(self, comments: t.Optional[t.List[str]] = None, prepend: bool = False) -> None:
        if self.comments is None:
            self.comments = []

        if comments:
            for comment in comments:
                _, *meta = comment.split(SQLGLOT_META)
                if meta:
                    for kv in "".join(meta).split(","):
                        k, *v = kv.split("=")
                        self.meta[k.strip()] = to_bool(v[0].strip() if v else True)

                if not prepend:
                    self.comments.append(comment)

            if prepend:
                self.comments = comments + self.comments

    def pop_comments(self) -> t.List[str]:
        comments = self.comments or []
        self.comments = None
        return comments

    def append(self, arg_key: str, value: t.Any) -> None:
        """
        Appends value to arg_key if it's a list or sets it as a new list.

        Args:
            arg_key (str): name of the list expression arg
            value (Any): value to append to the list
        """
        if type(self.args.get(arg_key)) is not list:
            self.args[arg_key] = []
        self._set_parent(arg_key, value)
        values = self.args[arg_key]
        if hasattr(value, "parent"):
            value.index = len(values)
        values.append(value)

    def set(
        self,
        arg_key: str,
        value: object,
        index: t.Optional[int] = None,
        overwrite: bool = True,
    ) -> None:
        """
        Sets arg_key to value.

        Args:
            arg_key: name of the expression arg.
            value: value to set the arg to.
            index: if the arg is a list, this specifies what position to add the value in it.
            overwrite: assuming an index is given, this determines whether to overwrite the
                list entry instead of only inserting a new value (i.e., like list.insert).
        """
        node: t.Optional[Expr] = self

        while node and node._hash is not None:
            node._hash = None
            node = node.parent

        if index is not None:
            expressions = self.args.get(arg_key) or []

            if seq_get(expressions, index) is None:
                return

            if value is None:
                expressions.pop(index)
                for v in expressions[index:]:
                    v.index = v.index - 1
                return

            if isinstance(value, list):
                expressions.pop(index)
                expressions[index:index] = value
            elif overwrite:
                expressions[index] = value
            else:
                expressions.insert(index, value)

            value = expressions
        elif value is None:
            self.args.pop(arg_key, None)
            return

        self.args[arg_key] = value
        self._set_parent(arg_key, value, index)

    def _set_parent(self, arg_key: str, value: object, index: t.Optional[int] = None) -> None:
        if isinstance(value, Expr):
            value.parent = self
            value.arg_key = arg_key
            value.index = index
        elif isinstance(value, list):
            for i, v in enumerate(value):
                if isinstance(v, Expr):
                    v.parent = self
                    v.arg_key = arg_key
                    v.index = i

    @property
    def depth(self) -> int:
        """
        Returns the depth of this tree.
        """
        if self.parent:
            return self.parent.depth + 1
        return 0

    def iter_expressions(self: E, reverse: bool = False) -> t.Iterator[E]:
        """Yields the key and expression for all arguments, exploding list args."""
        for vs in reversed(self.args.values()) if reverse else self.args.values():
            if isinstance(vs, list):
                for v in reversed(vs) if reverse else vs:
                    if isinstance(v, Expr):
                        yield t.cast(E, v)
            elif isinstance(vs, Expr):
                yield t.cast(E, vs)

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
        for expression in self.walk(bfs=bfs):
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
        return ancestor  # type: ignore[return-value]

    @property
    def parent_select(self) -> t.Optional[Select]:
        """
        Returns the parent select statement.
        """
        from sqlglot.expressions.query import Select as _Select

        return self.find_ancestor(_Select)

    @property
    def same_parent(self) -> bool:
        """Returns if the parent is the same class as itself."""
        return type(self.parent) is self.__class__

    def root(self) -> Expr:
        """
        Returns the root expression of this tree.
        """
        expression: Expr = self
        while expression.parent:
            expression = expression.parent
        return expression

    def walk(
        self, bfs: bool = True, prune: t.Optional[t.Callable[[Expr], bool]] = None
    ) -> t.Iterator[Expr]:
        """
        Returns a generator object which visits all nodes in this tree.

        Args:
            bfs: if set to True the BFS traversal order will be applied,
                otherwise the DFS traversal will be used instead.
            prune: callable that returns True if the generator should stop traversing
                this branch of the tree.

        Returns:
            the generator object.
        """
        if bfs:
            yield from self.bfs(prune=prune)
        else:
            yield from self.dfs(prune=prune)

    def dfs(self, prune: t.Optional[t.Callable[[Expr], bool]] = None) -> t.Iterator[Expr]:
        """
        Returns a generator object which visits all nodes in this tree in
        the DFS (Depth-first) order.

        Returns:
            The generator object.
        """
        stack = [self]

        while stack:
            node = stack.pop()
            yield node
            if prune and prune(node):
                continue
            for v in node.iter_expressions(reverse=True):
                stack.append(v)

    def bfs(self, prune: t.Optional[t.Callable[[Expr], bool]] = None) -> t.Iterator[Expr]:
        """
        Returns a generator object which visits all nodes in this tree in
        the BFS (Breadth-first) order.

        Returns:
            The generator object.
        """
        queue: t.Deque[Expr] = deque()
        queue.append(self)

        while queue:
            node = queue.popleft()
            yield node
            if prune and prune(node):
                continue
            for v in node.iter_expressions():
                queue.append(v)

    def unnest(self) -> Expr:
        """
        Returns the first non parenthesis child or self.
        """
        expression = self
        while type(expression) is Paren:
            expression = expression.this
        return expression

    def unalias(self) -> Expr:
        """
        Returns the inner expression if this is an Alias.
        """
        if isinstance(self, Alias):
            return self.this
        return self

    def unnest_operands(self) -> t.Tuple[Expr, ...]:
        """
        Returns unnested operands as a tuple.
        """
        return tuple(arg.unnest() for arg in self.iter_expressions())

    def flatten(self, unnest: bool = True) -> t.Iterator[Expr]:
        """
        Returns a generator which yields child nodes whose parents are the same class.

        A AND B AND C -> [A, B, C]
        """
        for node in self.dfs(prune=lambda n: bool(n.parent and type(n) is not self.__class__)):
            if type(node) is not self.__class__:
                yield node.unnest() if unnest and not node.is_subquery else node

    def __str__(self) -> str:
        return self.sql()

    def __repr__(self) -> str:
        return _to_s(self)

    def to_s(self) -> str:
        """
        Same as __repr__, but includes additional information which can be useful
        for debugging, like empty or missing args and the AST nodes' object IDs.
        """
        return _to_s(self, verbose=True)

    def sql(self, dialect: DialectType = None, **opts: t.Any) -> str:
        """
        Returns SQL string representation of this tree.

        Args:
            dialect: the dialect of the output SQL string (eg. "spark", "hive", "presto", "mysql").
            opts: other `sqlglot.generator.Generator` options.

        Returns:
            The SQL string.
        """
        from sqlglot.dialects import Dialect

        return Dialect.get_or_raise(dialect).generate(self, **opts)

    def transform(
        self, fun: t.Callable, *args: object, copy: bool = True, **kwargs: object
    ) -> t.Any:
        """
        Visits all tree nodes (excluding already transformed ones)
        and applies the given transformation function to each node.

        Args:
            fun: a function which takes a node as an argument and returns a
                new transformed node or the same node without modifications. If the function
                returns None, then the corresponding node will be removed from the syntax tree.
            copy: if set to True a new tree instance is constructed, otherwise the tree is
                modified in place.

        Returns:
            The transformed tree.
        """
        root: t.Any = None
        new_node: t.Any = None

        for node in (self.copy() if copy else self).dfs(prune=lambda n: n is not new_node):
            parent, arg_key, index = node.parent, node.arg_key, node.index
            new_node = fun(node, *args, **kwargs)

            if not root:
                root = new_node
            elif parent and arg_key and new_node is not node:
                parent.set(arg_key, new_node, index)

        assert root
        return root

    def replace(self, expression: t.Any) -> t.Any:
        """
        Swap out this expression with a new expression.

        For example::

            >>> import sqlglot
            >>> tree = sqlglot.parse_one("SELECT x FROM tbl")
            >>> tree.find(sqlglot.exp.Column).replace(sqlglot.exp.column("y"))
            Column(
              this=Identifier(this=y, quoted=False))
            >>> tree.sql()
            'SELECT y FROM tbl'

        Args:
            expression: new node

        Returns:
            The new expression or expressions.
        """
        parent = self.parent

        if not parent or parent is expression:
            return expression

        key = self.arg_key

        if key:
            value = parent.args.get(key)

            if type(expression) is list and isinstance(value, Expr):
                # We are trying to replace an Expr with a list, so it's assumed that
                # the intention was to really replace the parent of this expression.
                if value.parent:
                    value.parent.replace(expression)
            else:
                parent.set(key, expression, self.index)

        if expression is not self:
            self.parent = None
            self.arg_key = None
            self.index = None

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
        Assert that this `Expr` is an instance of `type_`.

        If it is NOT an instance of `type_`, this raises an assertion error.
        Otherwise, this returns this expression.

        Examples:
            This is useful for type security in chained expressions:

            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT x from y").assert_is(sqlglot.exp.Select).select("z").sql()
            'SELECT x, z FROM y'
        """
        if not isinstance(self, type_):
            raise AssertionError(f"{self} is not {type_}.")
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

        if UNITTEST:
            for k in self.args:
                if k not in self.arg_types:
                    raise TypeError(f"Unexpected keyword: '{k}' for {self.__class__}")

        for k in self.required_args:
            v = self.args.get(k)
            if v is None or (isinstance(v, list) and not v):
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

    def and_(
        self,
        *expressions: t.Optional[ExpOrStr],
        dialect: DialectType = None,
        copy: bool = True,
        wrap: bool = True,
        **opts: t.Any,
    ) -> Condition:
        """
        AND this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").and_("y=1").sql()
            'x = 1 AND y = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expr` instance is passed, it will be used as-is.
            dialect: the dialect used to parse the input expression.
            copy: whether to copy the involved expressions (only applies to Exprs).
            wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
                precedence issues, but can be turned off when the produced AST is too deep and
                causes recursion-related issues.
            opts: other options to use to parse the input expressions.

        Returns:
            The new And condition.
        """
        return and_(self, *expressions, dialect=dialect, copy=copy, wrap=wrap, **opts)

    def or_(
        self,
        *expressions: t.Optional[ExpOrStr],
        dialect: DialectType = None,
        copy: bool = True,
        wrap: bool = True,
        **opts: t.Any,
    ) -> Condition:
        """
        OR this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").or_("y=1").sql()
            'x = 1 OR y = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expr` instance is passed, it will be used as-is.
            dialect: the dialect used to parse the input expression.
            copy: whether to copy the involved expressions (only applies to Exprs).
            wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
                precedence issues, but can be turned off when the produced AST is too deep and
                causes recursion-related issues.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Or condition.
        """
        return or_(self, *expressions, dialect=dialect, copy=copy, wrap=wrap, **opts)

    def not_(self, copy: bool = True) -> Not:
        """
        Wrap this condition with NOT.

        Example:
            >>> condition("x=1").not_().sql()
            'NOT x = 1'

        Args:
            copy: whether to copy this object.

        Returns:
            The new Not instance.
        """
        return not_(self, copy=copy)

    def update_positions(
        self: E,
        other: t.Optional[Token | Expr] = None,
        line: t.Optional[int] = None,
        col: t.Optional[int] = None,
        start: t.Optional[int] = None,
        end: t.Optional[int] = None,
    ) -> E:
        """
        Update this expression with positions from a token or other expression.

        Args:
            other: a token or expression to update this expression with.
            line: the line number to use if other is None
            col: column number
            start: start char index
            end:  end char index

        Returns:
            The updated expression.
        """
        if other is None:
            self.meta["line"] = line
            self.meta["col"] = col
            self.meta["start"] = start
            self.meta["end"] = end
        elif isinstance(other, Expr):
            for k in POSITION_META_KEYS:
                if k in other.meta:
                    self.meta[k] = other.meta[k]
        else:
            self.meta["line"] = other.line
            self.meta["col"] = other.col
            self.meta["start"] = other.start
            self.meta["end"] = other.end
        return self

    def as_(
        self,
        alias: str | Identifier,
        quoted: t.Optional[bool] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: t.Any,
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

    def __getitem__(self, other: ExpOrStr | t.Tuple[ExpOrStr]) -> Bracket:
        return Bracket(
            this=self.copy(), expressions=[convert(e, copy=True) for e in ensure_list(other)]
        )

    def __iter__(self) -> t.Iterator:
        if "expressions" in self.arg_types:
            return iter(self.args.get("expressions") or [])
        # We define this because __getitem__ converts Expr into an iterable, which is
        # problematic because one can hit infinite loops if they do "for x in some_expr: ..."
        # See: https://peps.python.org/pep-0234/
        raise TypeError(f"'{self.__class__.__name__}' object is not iterable")

    def isin(
        self,
        *expressions: t.Any,
        query: t.Optional[ExpOrStr] = None,
        unnest: t.Optional[ExpOrStr] | t.Collection[ExpOrStr] = None,
        copy: bool = True,
        **opts,
    ) -> In:
        subquery = maybe_parse(query, copy=copy, **opts) if query else None
        if subquery and not subquery.is_subquery:
            subquery = subquery.subquery(copy=False)

        return In(
            this=maybe_copy(self, copy),
            expressions=[convert(e, copy=copy) for e in expressions],
            query=subquery,
            unnest=(
                _lazy_unnest(
                    expressions=[
                        maybe_parse(t.cast(ExpOrStr, e), copy=copy, **opts)
                        for e in ensure_list(unnest)
                    ]
                )
                if unnest
                else None
            ),
        )

    def between(
        self,
        low: t.Any,
        high: t.Any,
        copy: bool = True,
        symmetric: t.Optional[bool] = None,
        **opts,
    ) -> Between:
        between = Between(
            this=maybe_copy(self, copy),
            low=convert(low, copy=copy, **opts),
            high=convert(high, copy=copy, **opts),
        )
        if symmetric is not None:
            between.set("symmetric", symmetric)

        return between

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

    def div(self, other: ExpOrStr, typed: bool = False, safe: bool = False) -> Div:
        div = self._binop(Div, other)
        div.set("typed", typed)
        div.set("safe", safe)
        return div

    def asc(self, nulls_first: bool = True) -> Ordered:
        return Ordered(this=self.copy(), nulls_first=nulls_first)

    def desc(self, nulls_first: bool = False) -> Ordered:
        return Ordered(this=self.copy(), desc=True, nulls_first=nulls_first)

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


IntoType = t.Union[
    str,
    t.Type[Expr],
    t.Collection[t.Union[str, t.Type[Expr]]],
]
ExpOrStr = t.Union[str, Expr]


@trait
class Condition(Expr):
    """Logical conditions like x AND y, or simply x"""


@trait
class Predicate(Condition):
    """Relationships like x = y, x > 1, x >= y."""


class Cache(Expression):
    arg_types = {
        "this": True,
        "lazy": False,
        "options": False,
        "expression": False,
    }


class Uncache(Expression):
    arg_types = {"this": True, "exists": False}


class Refresh(Expression):
    arg_types = {"this": True, "kind": True}


class LockingStatement(Expression):
    arg_types = {"this": True, "expression": True}


@trait
class ColumnConstraintKind(Expr):
    pass


@trait
class SubqueryPredicate(Predicate):
    pass


class All(Expression, SubqueryPredicate):
    pass


class Any(Expression, SubqueryPredicate):
    pass


@trait
class Binary(Condition):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {"this": True, "expression": True}

    @property
    def left(self) -> Expr:
        return self.this

    @property
    def right(self) -> Expr:
        return self.expression


@trait
class Connector(Binary):
    pass


@trait
class Func(Condition):
    """
    The base class for all function expressions.

    Attributes:
        is_var_len_args (bool): if set to True the last argument defined in arg_types will be
            treated as a variable length argument and the argument's value will be stored as a list.
        _sql_names (list): the SQL name (1st item in the list) and aliases (subsequent items) for this
            function expression. These values are used to map this node to a name during parsing as
            well as to provide the function's name during SQL string generation. By default the SQL
            name is set to the expression's class name transformed to snake case.
    """

    is_var_len_args: t.ClassVar[bool] = False
    _sql_names: t.ClassVar[t.List[str]] = []

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
        if not cls._sql_names:
            return [camel_to_snake_case(cls.__name__)]
        return cls._sql_names

    @classmethod
    def sql_name(cls):
        sql_names = cls.sql_names()
        assert sql_names, f"Expected non-empty 'sql_names' for Func: {cls.__name__}."
        return sql_names[0]

    @classmethod
    def default_parser_mappings(cls):
        return {name: cls.from_arg_list for name in cls.sql_names()}


@trait
class AggFunc(Func):
    pass


class Column(Expression, Condition):
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
            self.args[part] for part in ("catalog", "db", "table", "this") if self.args.get(part)
        ]

    def to_dot(self, include_dots: bool = True) -> Dot | Identifier:
        """Converts the column into a dot expression."""
        parts = self.parts
        parent = self.parent

        if include_dots:
            while isinstance(parent, Dot):
                parts.append(parent.expression)
                parent = parent.parent

        return Dot.build(deepcopy(parts)) if len(parts) > 1 else parts[0]


class Literal(Expression, Condition):
    arg_types = {"this": True, "is_string": True}
    _hash_raw_args = True

    @classmethod
    def number(cls, number) -> Literal | Neg:
        lit = cls(this=str(number), is_string=False)
        try:
            to_py = lit.to_py()
            if not isinstance(to_py, str) and to_py < 0:
                lit.set("this", str(abs(to_py)))
                return Neg(this=lit)
        except Exception:
            pass
        return lit

    @classmethod
    def string(cls, string) -> Literal:
        return cls(this=str(string), is_string=True)

    @property
    def output_name(self) -> str:
        return self.name

    def to_py(self) -> int | str | Decimal:
        if self.is_number:
            try:
                return int(self.this)
            except ValueError:
                return Decimal(self.this)
        return self.this


class Var(Expression):
    pass


class WithinGroup(Expression):
    arg_types = {"this": True, "expression": False}


class Pseudocolumn(Column):
    pass


class Hint(Expression):
    arg_types = {"expressions": True}


class JoinHint(Expression):
    arg_types = {"this": True, "expressions": True}


class Identifier(Expression):
    arg_types = {"this": True, "quoted": False, "global_": False, "temporary": False}
    _hash_raw_args = True

    @property
    def quoted(self) -> bool:
        return bool(self.args.get("quoted"))

    @property
    def output_name(self) -> str:
        return self.name


class Opclass(Expression):
    arg_types = {"this": True, "expression": True}


class Star(Expression):
    arg_types = {"except_": False, "replace": False, "rename": False}

    @property
    def name(self) -> str:
        return "*"

    @property
    def output_name(self) -> str:
        return self.name


class Parameter(Expression, Condition):
    arg_types = {"this": True, "expression": False}


class SessionParameter(Expression, Condition):
    arg_types = {"this": True, "kind": False}


class Placeholder(Expression, Condition):
    arg_types = {"this": False, "kind": False, "widget": False, "jdbc": False}

    @property
    def name(self) -> str:
        return self.this or "?"


class Null(Expression, Condition):
    arg_types = {}

    @property
    def name(self) -> str:
        return "NULL"

    def to_py(self) -> Lit[None]:
        return None


class Boolean(Expression, Condition):
    def to_py(self) -> bool:
        return self.this


class Dot(Expression, Binary):
    @property
    def is_star(self) -> bool:
        return self.expression.is_star

    @property
    def name(self) -> str:
        return self.expression.name

    @property
    def output_name(self) -> str:
        return self.name

    @classmethod
    def build(self, expressions: t.Sequence[Expr]) -> Dot:
        """Build a Dot object with a sequence of expressions."""
        if len(expressions) < 2:
            raise ValueError("Dot requires >= 2 expressions.")

        return t.cast(Dot, reduce(lambda x, y: Dot(this=x, expression=y), expressions))

    @property
    def parts(self) -> t.List[Expr]:
        """Return the parts of a table / column in order catalog, db, table."""
        this, *parts = self.flatten()

        parts.reverse()

        for arg in COLUMN_PARTS:
            part = this.args.get(arg)

            if isinstance(part, Expr):
                parts.append(part)

        parts.reverse()
        return parts


class Kwarg(Expression, Binary):
    """Kwarg in special functions like func(kwarg => y)."""


class Alias(Expression):
    arg_types = {"this": True, "alias": False}

    @property
    def output_name(self) -> str:
        return self.alias


class PivotAlias(Alias):
    pass


class PivotAny(Expression):
    arg_types = {"this": False}


class Aliases(Expression):
    arg_types = {"this": True, "expressions": True}

    @property
    def aliases(self) -> t.List[Expr]:
        return self.expressions


class Bracket(Expression, Condition):
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#array_subscript_operator
    arg_types = {
        "this": True,
        "expressions": True,
        "offset": False,
        "safe": False,
        "returns_list_for_maps": False,
    }

    @property
    def output_name(self) -> str:
        if len(self.expressions) == 1:
            return self.expressions[0].output_name

        return super().output_name


class ForIn(Expression):
    arg_types = {"this": True, "expression": True}


class IgnoreNulls(Expression):
    pass


class RespectNulls(Expression):
    pass


class HavingMax(Expression):
    arg_types = {"this": True, "expression": True, "max": True}


class SafeFunc(Expression, Func):
    pass


class Typeof(Expression, Func):
    pass


class ParameterizedAgg(Expression, AggFunc):
    arg_types = {"this": True, "expressions": True, "params": True}


class Anonymous(Expression, Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True

    @property
    def name(self) -> str:
        return self.this if isinstance(self.this, str) else self.this.name


class AnonymousAggFunc(Expression, AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class CombinedAggFunc(AnonymousAggFunc):
    arg_types = {"this": True, "expressions": False}


class CombinedParameterizedAgg(ParameterizedAgg):
    arg_types = {"this": True, "expressions": True, "params": True}


class HashAgg(Expression, AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Hll(Expression, AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ApproxDistinct(Expression, AggFunc):
    arg_types = {"this": True, "accuracy": False}
    _sql_names = ["APPROX_DISTINCT", "APPROX_COUNT_DISTINCT"]


class Slice(Expression):
    arg_types = {"this": False, "expression": False, "step": False}


@trait
class TimeUnit(Expr):
    """Automatically converts unit arg into a var."""

    UNABBREVIATED_UNIT_NAME: t.ClassVar[t.Dict[str, str]] = {
        "D": "DAY",
        "H": "HOUR",
        "M": "MINUTE",
        "MS": "MILLISECOND",
        "NS": "NANOSECOND",
        "Q": "QUARTER",
        "S": "SECOND",
        "US": "MICROSECOND",
        "W": "WEEK",
        "Y": "YEAR",
    }

    VAR_LIKE: t.ClassVar[t.Tuple[t.Type[Expr], ...]] = (Column, Literal, Var)

    def _post_init(self) -> None:
        unit = self.args.get("unit")
        if (
            unit
            and type(unit) in self.VAR_LIKE
            and not (isinstance(unit, Column) and len(unit.parts) != 1)
        ):
            unit = Var(this=(self.UNABBREVIATED_UNIT_NAME.get(unit.name) or unit.name).upper())
            self.args["unit"] = unit
            self._set_parent("unit", unit)
        elif type(unit).__name__ == "Week":
            unit.set("this", Var(this=unit.this.name.upper()))  # type: ignore[union-attr]

    @property
    def unit(self) -> t.Optional[Var | IntervalSpan]:
        return self.args.get("unit")


class _TimeUnit(Expression, TimeUnit):
    """Automatically converts unit arg into a var."""

    arg_types = {"unit": False}


@trait
class IntervalOp(TimeUnit):
    def interval(self) -> "Interval":
        from sqlglot.expressions.datatypes import Interval

        return Interval(
            this=self.expression.copy(),
            unit=self.unit.copy() if self.unit else None,
        )


class Filter(Expression):
    arg_types = {"this": True, "expression": True}


class Check(Expression):
    pass


class Ordered(Expression):
    arg_types = {"this": True, "desc": False, "nulls_first": True, "with_fill": False}

    @property
    def name(self) -> str:
        return self.this.name


class Add(Expression, Binary):
    pass


class BitwiseAnd(Expression, Binary):
    arg_types = {"this": True, "expression": True, "padside": False}


class BitwiseLeftShift(Expression, Binary):
    arg_types = {"this": True, "expression": True, "requires_int128": False}


class BitwiseOr(Expression, Binary):
    arg_types = {"this": True, "expression": True, "padside": False}


class BitwiseRightShift(Expression, Binary):
    arg_types = {"this": True, "expression": True, "requires_int128": False}


class BitwiseXor(Expression, Binary):
    arg_types = {"this": True, "expression": True, "padside": False}


class Div(Expression, Binary):
    arg_types = {"this": True, "expression": True, "typed": False, "safe": False}


class Overlaps(Expression, Binary):
    pass


class ExtendsLeft(Expression, Binary):
    pass


class ExtendsRight(Expression, Binary):
    pass


class DPipe(Expression, Binary):
    arg_types = {"this": True, "expression": True, "safe": False}


class EQ(Expression, Binary, Predicate):
    pass


class NullSafeEQ(Expression, Binary, Predicate):
    pass


class NullSafeNEQ(Expression, Binary, Predicate):
    pass


class PropertyEQ(Expression, Binary):
    pass


class Distance(Expression, Binary):
    pass


class Escape(Expression, Binary):
    pass


class Glob(Expression, Binary, Predicate):
    pass


class GT(Expression, Binary, Predicate):
    pass


class GTE(Expression, Binary, Predicate):
    pass


class ILike(Expression, Binary, Predicate):
    pass


class IntDiv(Expression, Binary):
    pass


class Is(Expression, Binary, Predicate):
    pass


class Like(Expression, Binary, Predicate):
    pass


class Match(Expression, Binary, Predicate):
    pass


class LT(Expression, Binary, Predicate):
    pass


class LTE(Expression, Binary, Predicate):
    pass


class Mod(Expression, Binary):
    pass


class Mul(Expression, Binary):
    pass


class NEQ(Expression, Binary, Predicate):
    pass


class NestedJSONSelect(Expression, Binary):
    pass


class Operator(Expression, Binary):
    arg_types = {"this": True, "operator": True, "expression": True}


class SimilarTo(Expression, Binary, Predicate):
    pass


class Sub(Expression, Binary):
    pass


class Adjacent(Expression, Binary):
    pass


class Unary(Expression, Condition):
    pass


class BitwiseNot(Unary):
    pass


class Not(Unary):
    pass


class Paren(Unary):
    @property
    def output_name(self) -> str:
        return self.this.name


class Neg(Unary):
    def to_py(self) -> int | Decimal:
        if self.is_number:
            return self.this.to_py() * -1
        return super().to_py()


class AtIndex(Expression):
    arg_types = {"this": True, "expression": True}


class AtTimeZone(Expression):
    arg_types = {"this": True, "zone": True}


class FromTimeZone(Expression):
    arg_types = {"this": True, "zone": True}


class FormatPhrase(Expression):
    """Format override for a column in Teradata.
    Can be expanded to additional dialects as needed

    https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/FORMAT
    """

    arg_types = {"this": True, "format": True}


class Between(Expression, Predicate):
    arg_types = {"this": True, "low": True, "high": True, "symmetric": False}


class Distinct(Expression):
    arg_types = {"expressions": False, "on": False}


class In(Expression, Predicate):
    arg_types = {
        "this": True,
        "expressions": False,
        "query": False,
        "unnest": False,
        "field": False,
        "is_global": False,
    }


class And(Expression, Connector, Func):
    pass


class Or(Expression, Connector, Func):
    pass


class Xor(Expression, Connector, Func):
    arg_types = {"this": False, "expression": False, "expressions": False, "round_input": False}
    is_var_len_args = True


class Pow(Expression, Binary, Func):
    _sql_names = ["POWER", "POW"]


class RegexpLike(Expression, Binary, Func):
    arg_types = {"this": True, "expression": True, "flag": False, "full_match": False}


def not_(expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts) -> Not:
    """
    Wrap a condition with a NOT operator.

    Example:
        >>> not_("this_suit='black'").sql()
        "NOT this_suit = 'black'"

    Args:
        expression: the SQL code string to parse.
            If an Expr instance is passed, this is used as-is.
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


def _lazy_unnest(**kwargs: object) -> "Expr":
    from sqlglot.expressions.functions import Unnest

    return Unnest(**kwargs)


def convert(value: t.Any, copy: bool = False) -> Expr:
    """Convert a python value into an expression object.

    Raises an error if a conversion is not possible.

    Args:
        value: A python object.
        copy: Whether to copy `value` (only applies to Exprs and collections).

    Returns:
        The equivalent expression object.
    """
    if isinstance(value, Expr):
        return maybe_copy(value, copy)
    if isinstance(value, str):
        return Literal.string(value)
    if isinstance(value, bool):
        return Boolean(this=value)
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return Null()
    if isinstance(value, numbers.Number):
        return Literal.number(value)
    if isinstance(value, bytes):
        from sqlglot.expressions.query import HexString as _HexString

        return _HexString(this=value.hex())
    if isinstance(value, datetime.datetime):
        datetime_literal = Literal.string(value.isoformat(sep=" "))

        tz = None
        if value.tzinfo:
            # this works for zoneinfo.ZoneInfo, pytz.timezone and datetime.datetime.utc to return IANA timezone names like "America/Los_Angeles"
            # instead of abbreviations like "PDT". This is for consistency with other timezone handling functions in SQLGlot
            tz = Literal.string(str(value.tzinfo))

        from sqlglot.expressions.functions import TimeStrToTime as _TimeStrToTime

        return _TimeStrToTime(this=datetime_literal, zone=tz)
    if isinstance(value, datetime.date):
        date_literal = Literal.string(value.strftime("%Y-%m-%d"))
        from sqlglot.expressions.functions import DateStrToDate as _DateStrToDate

        return _DateStrToDate(this=date_literal)
    if isinstance(value, datetime.time):
        time_literal = Literal.string(value.isoformat())
        from sqlglot.expressions.functions import TsOrDsToTime as _TsOrDsToTime

        return _TsOrDsToTime(this=time_literal)
    if isinstance(value, tuple):
        if hasattr(value, "_fields"):
            from sqlglot.expressions.functions import Struct as _Struct

            return _Struct(
                expressions=[
                    PropertyEQ(
                        this=to_identifier(k), expression=convert(getattr(value, k), copy=copy)
                    )
                    for k in value._fields
                ]
            )
        from sqlglot.expressions.query import Tuple as _Tuple

        return _Tuple(expressions=[convert(v, copy=copy) for v in value])
    if isinstance(value, list):
        from sqlglot.expressions.functions import Array as _Array

        return _Array(expressions=[convert(v, copy=copy) for v in value])
    if isinstance(value, dict):
        from sqlglot.expressions.functions import Array as _Array, Map as _Map

        return _Map(
            keys=_Array(expressions=[convert(k, copy=copy) for k in value]),
            values=_Array(expressions=[convert(v, copy=copy) for v in value.values()]),
        )
    if hasattr(value, "__dict__"):
        from sqlglot.expressions.functions import Struct as _Struct

        return _Struct(
            expressions=[
                PropertyEQ(this=to_identifier(k), expression=convert(v, copy=copy))
                for k, v in value.__dict__.items()
            ]
        )
    raise ValueError(f"Cannot convert {value}")


QUERY_MODIFIERS = {
    "match": False,
    "laterals": False,
    "joins": False,
    "connect": False,
    "pivots": False,
    "prewhere": False,
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
    "options": False,
}


TIMESTAMP_PARTS = {
    "year": False,
    "month": False,
    "day": False,
    "hour": False,
    "min": False,
    "sec": False,
    "nano": False,
}


@t.overload
def maybe_parse(
    sql_or_expression: ExpOrStr,
    *,
    into: t.Type[E],
    dialect: DialectType = None,
    prefix: t.Optional[str] = None,
    copy: bool = False,
    **opts,
) -> E: ...


@t.overload
def maybe_parse(
    sql_or_expression: str | E,
    *,
    into: t.Optional[IntoType] = None,
    dialect: DialectType = None,
    prefix: t.Optional[str] = None,
    copy: bool = False,
    **opts,
) -> E: ...


def maybe_parse(
    sql_or_expression: ExpOrStr,
    *,
    into: t.Optional[IntoType] = None,
    dialect: DialectType = None,
    prefix: t.Optional[str] = None,
    copy: bool = False,
    **opts: t.Any,
) -> Expr:
    """Gracefully handle a possible string or expression.

    Example:
        >>> maybe_parse("1")
        Literal(this=1, is_string=False)
        >>> maybe_parse(to_identifier("x"))
        Identifier(this=x, quoted=False)

    Args:
        sql_or_expression: the SQL code string or an expression
        into: the SQLGlot Expr to parse into
        dialect: the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        prefix: a string to prefix the sql with before it gets parsed
            (automatically includes a space)
        copy: whether to copy the expression.
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Expr: the parsed or given expression.
    """
    if isinstance(sql_or_expression, Expr):
        if copy:
            return sql_or_expression.copy()
        return sql_or_expression

    if sql_or_expression is None:
        raise ParseError("SQL cannot be None")

    import sqlglot

    sql = str(sql_or_expression)
    if prefix:
        sql = f"{prefix} {sql}"

    return sqlglot.parse_one(sql, read=dialect, into=into, **opts)


@t.overload
def maybe_copy(instance: None, copy: bool = True) -> None: ...


@t.overload
def maybe_copy(instance: E, copy: bool = True) -> E: ...


def maybe_copy(instance, copy=True):
    return instance.copy() if copy and instance else instance


def _to_s(node: t.Any, verbose: bool = False, level: int = 0, repr_str: bool = False) -> str:
    """Generate a textual representation of an Expr tree"""
    indent = "\n" + ("  " * (level + 1))
    delim = f",{indent}"

    if isinstance(node, Expr):
        args = {k: v for k, v in node.args.items() if (v is not None and v != []) or verbose}

        if (node.type or verbose) and type(node).__name__ != "DataType":
            args["_type"] = node.type
        if node.comments or verbose:
            args["_comments"] = node.comments

        if verbose:
            args["_id"] = id(node)

        # Inline leaves for a more compact representation
        if node.is_leaf():
            indent = ""
            delim = ", "

        repr_str = node.is_string or (isinstance(node, Identifier) and node.quoted)
        items = delim.join(
            [f"{k}={_to_s(v, verbose, level + 1, repr_str=repr_str)}" for k, v in args.items()]
        )
        return f"{node.__class__.__name__}({indent}{items})"

    if isinstance(node, list):
        items = delim.join(_to_s(i, verbose, level + 1) for i in node)
        items = f"{indent}{items}" if items else ""
        return f"[{items}]"

    # We use the representation of the string to avoid stripping out important whitespace
    if repr_str and isinstance(node, str):
        node = repr(node)

    # Indent multiline strings to match the current level
    return indent.join(textwrap.dedent(str(node).strip("\n")).splitlines())


def _is_wrong_expression(expression, into):
    return isinstance(expression, Expr) and not isinstance(expression, into)


def _apply_builder(
    expression,
    instance,
    arg,
    copy=True,
    prefix=None,
    into=None,
    dialect=None,
    into_arg="this",
    **opts,
):
    if _is_wrong_expression(expression, into):
        expression = into(**{into_arg: expression})
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
    properties = {} if properties is None else properties

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
            for k, v in expression.args.items():
                if k == "expressions":
                    parsed.extend(v)
                else:
                    properties[k] = v

    existing = instance.args.get(arg)
    if append and existing:
        parsed = existing.expressions + parsed

    child = into(expressions=parsed)
    for k, v in properties.items():
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

    parsed = [
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
        parsed = existing_expressions + parsed

    inst.set(arg, parsed)
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
    filtered = [exp for exp in expressions if exp is not None and exp != ""]
    if not filtered:
        return instance

    inst = maybe_copy(instance, copy)

    existing = inst.args.get(arg)
    if append and existing is not None:
        filtered = [existing.this if into else existing] + filtered

    node = and_(*filtered, dialect=dialect, copy=copy, **opts)

    inst.set(arg, into(this=node) if into else node)
    return inst


def _combine(
    expressions: t.Sequence[t.Optional[ExpOrStr]],
    operator: t.Any,
    dialect: DialectType = None,
    copy: bool = True,
    wrap: bool = True,
    **opts,
) -> Expr:
    conditions = [
        condition(expression, dialect=dialect, copy=copy, **opts)
        for expression in expressions
        if expression is not None
    ]

    this, *rest = conditions
    if rest and wrap:
        this = _wrap(this, Connector)
    for expression in rest:
        this = operator(this=this, expression=_wrap(expression, Connector) if wrap else expression)

    return this


@t.overload
def _wrap(expression: None, kind: t.Type[Expr]) -> None: ...


@t.overload
def _wrap(expression: E, kind: t.Type[Expr]) -> E | Paren: ...


def _wrap(expression: t.Optional[E], kind: t.Type[Expr]) -> t.Optional[E] | Paren:
    return Paren(this=expression) if isinstance(expression, kind) else expression


def _apply_set_operation(
    *expressions: ExpOrStr,
    set_operation: t.Type,
    distinct: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> t.Any:
    return reduce(
        lambda x, y: set_operation(this=x, expression=y, distinct=distinct, **opts),
        (maybe_parse(e, dialect=dialect, copy=copy, **opts) for e in expressions),
    )


SAFE_IDENTIFIER_RE: t.Pattern[str] = re.compile(r"^[_a-zA-Z][\w]*$")


@t.overload
def to_identifier(name: None, quoted: t.Optional[bool] = None, copy: bool = True) -> None: ...


@t.overload
def to_identifier(
    name: str | Identifier, quoted: t.Optional[bool] = None, copy: bool = True
) -> Identifier: ...


def to_identifier(name, quoted=None, copy=True):
    """Builds an identifier.

    Args:
        name: The name to turn into an identifier.
        quoted: Whether to force quote the identifier.
        copy: Whether to copy name if it's an Identifier.

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


def condition(expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts) -> Expr:
    """
    Initialize a logical condition expression.

    Example:
        >>> condition("x=1").sql()
        'x = 1'

        This is helpful for composing larger logical syntax trees:
        >>> where = condition("x=1")
        >>> where = where.and_("y=1")
        >>> where.sql()
        'x = 1 AND y = 1'

    Args:
        *expression: the SQL code string to parse.
            If an Expr instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        copy: Whether to copy `expression` (only applies to expressions).
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
    *expressions: t.Optional[ExpOrStr],
    dialect: DialectType = None,
    copy: bool = True,
    wrap: bool = True,
    **opts,
) -> Condition:
    """
    Combine multiple conditions with an AND logical operator.

    Example:
        >>> and_("x=1", and_("y=1", "z=1")).sql()
        'x = 1 AND (y = 1 AND z = 1)'

    Args:
        *expressions: the SQL code strings to parse.
            If an Expr instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy `expressions` (only applies to Exprs).
        wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
            precedence issues, but can be turned off when the produced AST is too deep and
            causes recursion-related issues.
        **opts: other options to use to parse the input expressions.

    Returns:
        The new condition
    """
    return t.cast(Condition, _combine(expressions, And, dialect, copy=copy, wrap=wrap, **opts))


def or_(
    *expressions: t.Optional[ExpOrStr],
    dialect: DialectType = None,
    copy: bool = True,
    wrap: bool = True,
    **opts,
) -> Condition:
    """
    Combine multiple conditions with an OR logical operator.

    Example:
        >>> or_("x=1", or_("y=1", "z=1")).sql()
        'x = 1 OR (y = 1 OR z = 1)'

    Args:
        *expressions: the SQL code strings to parse.
            If an Expr instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy `expressions` (only applies to Exprs).
        wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
            precedence issues, but can be turned off when the produced AST is too deep and
            causes recursion-related issues.
        **opts: other options to use to parse the input expressions.

    Returns:
        The new condition
    """
    return t.cast(Condition, _combine(expressions, Or, dialect, copy=copy, wrap=wrap, **opts))


def xor(
    *expressions: t.Optional[ExpOrStr],
    dialect: DialectType = None,
    copy: bool = True,
    wrap: bool = True,
    **opts,
) -> Condition:
    """
    Combine multiple conditions with an XOR logical operator.

    Example:
        >>> xor("x=1", xor("y=1", "z=1")).sql()
        'x = 1 XOR (y = 1 XOR z = 1)'

    Args:
        *expressions: the SQL code strings to parse.
            If an Expr instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy `expressions` (only applies to Exprs).
        wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
            precedence issues, but can be turned off when the produced AST is too deep and
            causes recursion-related issues.
        **opts: other options to use to parse the input expressions.

    Returns:
        The new condition
    """
    return t.cast(Condition, _combine(expressions, Xor, dialect, copy=copy, wrap=wrap, **opts))


def paren(expression: ExpOrStr, copy: bool = True) -> Paren:
    """
    Wrap an expression in parentheses.

    Example:
        >>> paren("5 + 3").sql()
        '(5 + 3)'

    Args:
        expression: the SQL code string to parse.
            If an Expr instance is passed, this is used as-is.
        copy: whether to copy the expression or not.

    Returns:
        The wrapped expression.
    """
    return Paren(this=maybe_parse(expression, copy=copy))


def alias_(
    expression: ExpOrStr,
    alias: t.Optional[str | Identifier],
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
            If an Expr instance is passed, this is used as-is.
        alias: the alias name to use. If the name has
            special characters it is quoted.
        table: Whether to create a table alias, can also be a list of columns.
        quoted: whether to quote the alias
        dialect: the dialect used to parse the input expression.
        copy: Whether to copy the expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Alias: the aliased expression
    """
    exp = maybe_parse(expression, dialect=dialect, copy=copy, **opts)
    alias = to_identifier(alias, quoted=quoted)

    if table:
        from sqlglot.expressions.query import TableAlias as _TableAlias

        table_alias = _TableAlias(this=alias)
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

    if "alias" in exp.arg_types and type(exp).__name__ != "Window":
        exp.set("alias", alias)
        return exp
    return Alias(this=exp, alias=alias)


@t.overload
def column(
    col: str | Identifier,
    table: t.Optional[str | Identifier] = None,
    db: t.Optional[str | Identifier] = None,
    catalog: t.Optional[str | Identifier] = None,
    *,
    fields: t.Collection[t.Union[str, Identifier]],
    quoted: t.Optional[bool] = None,
    copy: bool = True,
) -> Dot:
    pass


@t.overload
def column(
    col: str | Identifier | Star,
    table: t.Optional[str | Identifier] = None,
    db: t.Optional[str | Identifier] = None,
    catalog: t.Optional[str | Identifier] = None,
    *,
    fields: Lit[None] = None,
    quoted: t.Optional[bool] = None,
    copy: bool = True,
) -> Column:
    pass


def column(
    col,
    table=None,
    db=None,
    catalog=None,
    *,
    fields=None,
    quoted=None,
    copy=True,
):
    """
    Build a Column.

    Args:
        col: Column name.
        table: Table name.
        db: Database name.
        catalog: Catalog name.
        fields: Additional fields using dots.
        quoted: Whether to force quotes on the column's identifiers.
        copy: Whether to copy identifiers if passed in.

    Returns:
        The new Column instance.
    """
    if not isinstance(col, Star):
        col = to_identifier(col, quoted=quoted, copy=copy)

    this = Column(
        this=col,
        table=to_identifier(table, quoted=quoted, copy=copy),
        db=to_identifier(db, quoted=quoted, copy=copy),
        catalog=to_identifier(catalog, quoted=quoted, copy=copy),
    )

    if fields:
        this = Dot.build(
            (this, *(to_identifier(field, quoted=quoted, copy=copy) for field in fields))
        )
    return this
