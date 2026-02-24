from __future__ import annotations

import logging
from decimal import Decimal
from functools import reduce
import sys
import typing as t
from collections import deque
from copy import deepcopy

from sqlglot.helper import (
    camel_to_snake_case,
    ensure_list,
    mypyc_attr,
    to_bool,
    trait,
)
from sqlglot.tokenizer_core import Token

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlglot._typing import Lit


logger = logging.getLogger("sqlglot")

E = t.TypeVar("E", bound="Expression")

POSITION_META_KEYS: t.Tuple[str, ...] = ("line", "col", "start", "end")
SQLGLOT_META: str = "sqlglot.meta"
UNITTEST: bool = "unittest" in sys.modules or "pytest" in sys.modules


@mypyc_attr(allow_interpreted_subclasses=True)
class Expression:
    """
    The base class for all expressions in a syntax tree. Each Expression encapsulates any necessary
    context, such as its child expressions, their names (arg keys), and whether a given child expression
    is optional or not.

    Attributes:
        key: a unique key for each class in the Expression hierarchy. This is useful for hashing
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
        >>> class Foo(Expression):
        ...     arg_types = {"this": True, "expression": False}

        The above definition informs us that Foo is an Expression that requires an argument called
        "this" and may also optionally receive an argument called "expression".

    Args:
        args: a mapping used for retrieving the arguments of an expression, given their arg keys.
    """

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

    key: t.ClassVar[str] = "expression"
    arg_types: t.ClassVar[t.Dict[str, bool]] = {"this": True}
    required_args: t.ClassVar[t.Set[str]] = {"this"}
    is_var_len_args: t.ClassVar[bool] = False
    is_func: t.ClassVar[bool] = False
    _hash_raw_args: t.ClassVar[bool] = False

    def __init__(self, **args: object) -> None:
        self.args: t.Dict[str, t.Any] = args
        self.parent: t.Optional[Expression] = None
        self.arg_key: t.Optional[str] = None
        self.index: t.Optional[int] = None
        self.comments: t.Optional[t.List[str]] = None
        self._type: t.Optional[Expression] = None
        self._meta: t.Optional[t.Dict[str, t.Any]] = None
        self._hash: t.Optional[int] = None

        for arg_key, value in self.args.items():
            self._set_parent(arg_key, value)

    @classmethod
    def __init_subclass__(cls, **kwargs: t.Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.key = cls.__name__.lower()
        cls.required_args = {k for k, v in cls.arg_types.items() if v}

    def __reduce__(self) -> t.Tuple[t.Callable, t.Tuple[t.List[t.Dict[str, t.Any]]]]:
        from sqlglot.serde import dump, load

        return (load, (dump(self),))

    def _set_parent(self, arg_key: str, value: object, index: t.Optional[int] = None) -> None:
        if isinstance(value, Expression):
            value.parent = self
            value.arg_key = arg_key
            value.index = index
        elif isinstance(value, list):
            for i, v in enumerate(value):
                if isinstance(v, Expression):
                    v.parent = self
                    v.arg_key = arg_key
                    v.index = i

    def iter_expressions(self: E, reverse: bool = False) -> t.Iterator[E]:
        for vs in reversed(self.args.values()) if reverse else self.args.values():
            if isinstance(vs, list):
                for v in reversed(vs) if reverse else vs:
                    if isinstance(v, Expression):
                        yield t.cast(E, v)
            elif isinstance(vs, Expression):
                yield t.cast(E, vs)

    def bfs(self: E, prune: t.Optional[t.Callable[[E], bool]] = None) -> t.Iterator[E]:
        queue: t.Deque[E] = deque()
        queue.append(self)
        while queue:
            node = queue.popleft()
            yield node
            if prune and prune(node):
                continue
            for v in node.iter_expressions():
                queue.append(v)

    def dfs(self: E, prune: t.Optional[t.Callable[[E], bool]] = None) -> t.Iterator[E]:
        stack: t.List[E] = [self]
        while stack:
            node = stack.pop()
            yield node
            if prune and prune(node):
                continue
            for v in node.iter_expressions(reverse=True):
                stack.append(v)

    @property
    def meta(self) -> t.Dict[str, t.Any]:
        if self._meta is None:
            self._meta = {}
        return self._meta

    @property
    def this(self) -> t.Any:
        return self.args.get("this")

    @property
    def expression(self) -> t.Any:
        return self.args.get("expression")

    @property
    def expressions(self) -> t.List[t.Any]:
        return self.args.get("expressions") or []

    def text(self, key: str) -> str:
        field = self.args.get(key)
        if isinstance(field, str):
            return field
        if isinstance(field, (Identifier, Literal, Var)):
            return field.this
        if isinstance(field, (Star, Null)):
            return field.name
        return ""

    @property
    def name(self) -> str:
        return self.text("this")

    @property
    def is_string(self) -> bool:
        return isinstance(self, Literal) and self.args["is_string"]

    @property
    def is_number(self) -> bool:
        return (isinstance(self, Literal) and not self.args["is_string"]) or (
            isinstance(self, Neg) and self.this.is_number
        )

    @property
    def is_int(self) -> bool:
        return self.is_number and isinstance(self.to_py(), int)

    @property
    def is_star(self) -> bool:
        return isinstance(self, Star) or (isinstance(self, Column) and isinstance(self.this, Star))

    @property
    def alias(self) -> str:
        alias = self.args.get("alias")
        if isinstance(alias, Expression):
            return alias.name
        return self.text("alias")

    @property
    def alias_column_names(self) -> t.List[str]:
        table_alias = self.args.get("alias")
        if not table_alias:
            return []
        return [c.name for c in table_alias.args.get("columns") or []]

    @property
    def alias_or_name(self) -> str:
        return self.alias or self.name

    @property
    def output_name(self) -> str:
        return ""

    @property
    def type(self) -> t.Any:
        if isinstance(self, Cast):
            return self._type or self.to  # type: ignore[return-value]
        return self._type  # type: ignore[return-value]

    @type.setter
    def type(self, dtype: t.Any) -> None:
        import sqlglot.expressions

        if dtype and not isinstance(dtype, sqlglot.expressions.DataType):
            dtype = sqlglot.expressions.DataType.build(dtype)
        self._type = dtype  # type: ignore

    def is_type(self, *dtypes: t.Any) -> bool:
        _type = self._type
        return _type is not None and _type.is_type(*dtypes)

    @property
    def parent_select(self) -> t.Any:
        from sqlglot.expressions import Select

        return self.find_ancestor(Select)

    def unnest(self) -> Expression:
        expression = self
        while type(expression) is Paren:
            expression = expression.this
        return expression

    def unalias(self) -> Expression:
        if isinstance(self, Alias):
            return self.this
        return self

    def unnest_operands(self) -> t.Tuple[t.Any, ...]:
        return tuple(arg.unnest() for arg in self.iter_expressions())

    def flatten(self, unnest: bool = True) -> t.Iterator[Expression]:
        from sqlglot.expressions import Subquery

        for node in self.dfs(prune=lambda n: bool(n.parent and type(n) is not self.__class__)):
            if type(node) is not self.__class__:
                yield node.unnest() if unnest and not isinstance(node, Subquery) else node

    def __str__(self) -> str:
        return self.sql()

    def __repr__(self) -> str:
        from sqlglot.expressions import _to_s

        return _to_s(self)

    def to_s(self) -> str:
        """
        Same as __repr__, but includes additional information which can be useful
        for debugging, like empty or missing args and the AST nodes' object IDs.
        """
        from sqlglot.expressions import _to_s

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
        import sqlglot.dialects

        return sqlglot.dialects.Dialect.get_or_raise(dialect).generate(self, **opts)

    def dump(self) -> t.Any:
        """
        Dump this Expression to a JSON-serializable dict.
        """
        from sqlglot.serde import dump

        return dump(self)

    @classmethod
    def load(cls, obj: t.Any) -> Expression:
        """
        Load a dict (as returned by `Expression.dump`) into an Expression instance.
        """
        from sqlglot.serde import load

        result = load(obj)
        assert isinstance(result, Expression)
        return result

    def and_(
        self,
        *expressions: t.Optional[t.Any],
        dialect: DialectType = None,
        copy: bool = True,
        wrap: bool = True,
        **opts: t.Any,
    ) -> t.Any:
        """
        AND this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").and_("y=1").sql()  # doctest: +SKIP
            'x = 1 AND y = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect: the dialect used to parse the input expression.
            copy: whether to copy the involved expressions (only applies to Expressions).
            wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
                precedence issues, but can be turned off when the produced AST is too deep and
                causes recursion-related issues.
            opts: other options to use to parse the input expressions.

        Returns:
            The new And condition.
        """
        from sqlglot.expressions import and_

        return and_(self, *expressions, dialect=dialect, copy=copy, wrap=wrap, **opts)

    def or_(
        self,
        *expressions: t.Optional[t.Any],
        dialect: DialectType = None,
        copy: bool = True,
        wrap: bool = True,
        **opts: t.Any,
    ) -> t.Any:
        """
        OR this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").or_("y=1").sql()  # doctest: +SKIP
            'x = 1 OR y = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect: the dialect used to parse the input expression.
            copy: whether to copy the involved expressions (only applies to Expressions).
            wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
                precedence issues, but can be turned off when the produced AST is too deep and
                causes recursion-related issues.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Or condition.
        """
        from sqlglot.expressions import or_

        return or_(self, *expressions, dialect=dialect, copy=copy, wrap=wrap, **opts)

    def not_(self, copy: bool = True) -> t.Any:
        """
        Wrap this condition with NOT.

        Example:
            >>> condition("x=1").not_().sql()  # doctest: +SKIP
            'NOT x = 1'

        Args:
            copy: whether to copy this object.

        Returns:
            The new Not instance.
        """
        from sqlglot.expressions import not_

        return not_(self, copy=copy)

    def as_(
        self,
        alias: t.Any,
        quoted: t.Optional[bool] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: t.Any,
    ) -> t.Any:
        from sqlglot.expressions import alias_

        return alias_(self, alias, quoted=quoted, dialect=dialect, copy=copy, **opts)

    def _binop(self, klass: t.Type[E], other: t.Any, reverse: bool = False) -> E:
        from sqlglot.expressions import _wrap, convert

        this = self.copy()
        other = convert(other, copy=True)
        if not isinstance(this, klass) and not isinstance(other, klass):
            this = _wrap(this, Binary)
            other = _wrap(other, Binary)
        if reverse:
            return klass(this=other, expression=this)
        return klass(this=this, expression=other)

    def __getitem__(self, other: t.Any) -> t.Any:
        from sqlglot.expressions import convert

        return Bracket(
            this=self.copy(), expressions=[convert(e, copy=True) for e in ensure_list(other)]
        )

    def __iter__(self) -> t.Iterator:
        if "expressions" in self.arg_types:
            return iter(self.args.get("expressions") or [])
        # We define this because __getitem__ converts Expression into an iterable, which is
        # problematic because one can hit infinite loops if they do "for x in some_expr: ..."
        # See: https://peps.python.org/pep-0234/
        raise TypeError(f"'{self.__class__.__name__}' object is not iterable")

    def isin(
        self,
        *expressions: t.Any,
        query: t.Optional[t.Any] = None,
        unnest: t.Optional[t.Any] = None,
        copy: bool = True,
        **opts: t.Any,
    ) -> t.Any:
        from sqlglot.expressions import Subquery, Unnest, convert, maybe_copy, maybe_parse

        subquery = maybe_parse(query, copy=copy, **opts) if query else None
        if subquery and not isinstance(subquery, Subquery):
            subquery = subquery.subquery(copy=False)

        return In(
            this=maybe_copy(self, copy),
            expressions=[convert(e, copy=copy) for e in expressions],
            query=subquery,
            unnest=(
                Unnest(
                    expressions=[
                        maybe_parse(t.cast(t.Any, e), copy=copy, **opts)
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
        **opts: t.Any,
    ) -> t.Any:
        from sqlglot.expressions import convert, maybe_copy

        between = Between(
            this=maybe_copy(self, copy),
            low=convert(low, copy=copy, **opts),
            high=convert(high, copy=copy, **opts),
        )
        if symmetric is not None:
            between.set("symmetric", symmetric)
        return between

    def is_(self, other: t.Any) -> t.Any:
        return self._binop(Is, other)

    def like(self, other: t.Any) -> t.Any:
        return self._binop(Like, other)

    def ilike(self, other: t.Any) -> t.Any:
        return self._binop(ILike, other)

    def eq(self, other: t.Any) -> t.Any:
        return self._binop(EQ, other)

    def neq(self, other: t.Any) -> t.Any:
        return self._binop(NEQ, other)

    def rlike(self, other: t.Any) -> t.Any:
        from sqlglot.expressions import RegexpLike

        return self._binop(RegexpLike, other)

    def div(self, other: t.Any, typed: bool = False, safe: bool = False) -> t.Any:
        div = self._binop(Div, other)
        div.set("typed", typed)
        div.set("safe", safe)
        return div

    def asc(self, nulls_first: bool = True) -> t.Any:
        return Ordered(this=self.copy(), nulls_first=nulls_first)

    def desc(self, nulls_first: bool = False) -> t.Any:
        return Ordered(this=self.copy(), desc=True, nulls_first=nulls_first)

    def __lt__(self, other: t.Any) -> t.Any:
        return self._binop(LT, other)

    def __le__(self, other: t.Any) -> t.Any:
        return self._binop(LTE, other)

    def __gt__(self, other: t.Any) -> t.Any:
        return self._binop(GT, other)

    def __ge__(self, other: t.Any) -> t.Any:
        return self._binop(GTE, other)

    def __add__(self, other: t.Any) -> t.Any:
        return self._binop(Add, other)

    def __radd__(self, other: t.Any) -> t.Any:
        return self._binop(Add, other, reverse=True)

    def __sub__(self, other: t.Any) -> t.Any:
        return self._binop(Sub, other)

    def __rsub__(self, other: t.Any) -> t.Any:
        return self._binop(Sub, other, reverse=True)

    def __mul__(self, other: t.Any) -> t.Any:
        return self._binop(Mul, other)

    def __rmul__(self, other: t.Any) -> t.Any:
        return self._binop(Mul, other, reverse=True)

    def __truediv__(self, other: t.Any) -> t.Any:
        return self._binop(Div, other)

    def __rtruediv__(self, other: t.Any) -> t.Any:
        return self._binop(Div, other, reverse=True)

    def __floordiv__(self, other: t.Any) -> t.Any:
        return self._binop(IntDiv, other)

    def __rfloordiv__(self, other: t.Any) -> t.Any:
        return self._binop(IntDiv, other, reverse=True)

    def __mod__(self, other: t.Any) -> t.Any:
        return self._binop(Mod, other)

    def __rmod__(self, other: t.Any) -> t.Any:
        return self._binop(Mod, other, reverse=True)

    def __pow__(self, other: t.Any) -> t.Any:
        from sqlglot.expressions import Pow

        return self._binop(Pow, other)

    def __rpow__(self, other: t.Any) -> t.Any:
        from sqlglot.expressions import Pow

        return self._binop(Pow, other, reverse=True)

    def __and__(self, other: t.Any) -> t.Any:
        from sqlglot.expressions import And

        return self._binop(And, other)

    def __rand__(self, other: t.Any) -> t.Any:
        from sqlglot.expressions import And

        return self._binop(And, other, reverse=True)

    def __or__(self, other: t.Any) -> t.Any:
        from sqlglot.expressions import Or

        return self._binop(Or, other)

    def __ror__(self, other: t.Any) -> t.Any:
        from sqlglot.expressions import Or

        return self._binop(Or, other, reverse=True)

    def __neg__(self) -> t.Any:
        from sqlglot.expressions import _wrap

        return Neg(this=_wrap(self.copy(), Binary))

    def __invert__(self) -> t.Any:
        from sqlglot.expressions import not_

        return not_(self.copy())

    def pop_comments(self) -> t.List[str]:
        comments = self.comments or []
        self.comments = None
        return comments

    def append(self, arg_key: str, value: t.Any) -> None:
        if type(self.args.get(arg_key)) is not list:
            self.args[arg_key] = []
        self._set_parent(arg_key, value)
        values = self.args[arg_key]
        if hasattr(value, "parent"):
            value.index = len(values)
        values.append(value)

    @property
    def depth(self) -> int:
        if self.parent:
            return self.parent.depth + 1
        return 0

    def find_ancestor(self, *expression_types: t.Type[E]) -> t.Optional[E]:
        ancestor = self.parent
        while ancestor and not isinstance(ancestor, expression_types):
            ancestor = ancestor.parent
        return ancestor  # type: ignore[return-value]

    @property
    def same_parent(self) -> bool:
        return type(self.parent) is self.__class__

    def root(self) -> Expression:
        expression = self
        while expression.parent:
            expression = expression.parent
        return expression

    def __eq__(self, other: object) -> bool:
        return self is other or (type(self) is type(other) and hash(self) == hash(other))

    def __hash__(self) -> int:
        if self._hash is None:
            nodes: t.List[Expression] = []
            queue: t.Deque[Expression] = deque()
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

    def error_messages(self, args: t.Optional[t.Sequence] = None) -> t.List[str]:
        errors: t.List[str] = []

        if UNITTEST:
            for k in self.args:
                if k not in self.arg_types:
                    raise TypeError(f"Unexpected keyword: '{k}' for {self.__class__}")

        for k in self.required_args:
            v = self.args.get(k)
            if v is None or (isinstance(v, list) and not v):
                errors.append(f"Required keyword: '{k}' missing for {self.__class__}")

        if args and self.is_func and len(args) > len(self.arg_types) and not self.is_var_len_args:
            errors.append(
                f"The number of provided arguments ({len(args)}) is greater than "
                f"the maximum number of supported arguments ({len(self.arg_types)})"
            )

        return errors

    def update_positions(
        self: E,
        other: t.Optional[t.Union[Expression, Token]] = None,
        line: t.Optional[int] = None,
        col: t.Optional[int] = None,
        start: t.Optional[int] = None,
        end: t.Optional[int] = None,
    ) -> E:
        if other is None:
            self.meta["line"] = line
            self.meta["col"] = col
            self.meta["start"] = start
            self.meta["end"] = end
        elif isinstance(other, Expression):
            for k in POSITION_META_KEYS:
                if k in other.meta:
                    self.meta[k] = other.meta[k]
        else:
            # Token
            self.meta["line"] = other.line
            self.meta["col"] = other.col
            self.meta["start"] = other.start
            self.meta["end"] = other.end
        return self

    def to_py(self) -> t.Any:
        raise ValueError(f"{self} cannot be converted to a Python object.")

    def is_leaf(self) -> bool:
        return not any(
            (isinstance(v, Expression) or type(v) is list) and v for v in self.args.values()
        )

    def __deepcopy__(self, memo: t.Any) -> Expression:
        root = self.__class__()
        stack: t.List[t.Tuple[Expression, Expression]] = [(self, root)]

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
                if isinstance(vs, Expression):
                    stack.append((vs, vs.__class__()))
                    copy.set(k, stack[-1][-1])
                elif type(vs) is list:
                    copy.args[k] = []

                    for v in vs:
                        if isinstance(v, Expression):
                            stack.append((v, v.__class__()))
                            copy.append(k, stack[-1][-1])
                        else:
                            copy.append(k, v)
                else:
                    copy.args[k] = vs

        return root

    def copy(self: E) -> E:
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

    def set(
        self,
        arg_key: str,
        value: object,
        index: t.Optional[int] = None,
        overwrite: bool = True,
    ) -> None:
        node: t.Optional[Expression] = self

        while node and node._hash is not None:
            node._hash = None
            node = node.parent

        if index is not None:
            expressions = self.args.get(arg_key) or []

            try:
                if expressions[index] is None:
                    return
            except IndexError:
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

    def find(self, *expression_types: t.Type[E], bfs: bool = True) -> t.Optional[E]:
        return next(self.find_all(*expression_types, bfs=bfs), None)

    def find_all(self, *expression_types: t.Type[E], bfs: bool = True) -> t.Iterator[E]:
        for expression in self.walk(bfs=bfs):
            if isinstance(expression, expression_types):
                yield expression

    def walk(
        self: E,
        bfs: bool = True,
        prune: t.Optional[t.Callable[[E], bool]] = None,
    ) -> t.Iterator[E]:
        if bfs:
            yield from self.bfs(prune=prune)
        else:
            yield from self.dfs(prune=prune)

    def replace(self, expression: t.Any) -> t.Any:
        parent = self.parent

        if not parent or parent is expression:
            return expression

        key = self.arg_key
        if not key:
            return expression

        value = parent.args.get(key)

        if type(expression) is list and isinstance(value, Expression):
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
        self.replace(None)
        return self

    def assert_is(self, type_: t.Type[E]) -> E:
        if not isinstance(self, type_):
            raise AssertionError(f"{self} is not {type_}.")
        return self

    def transform(
        self, fun: t.Callable, *args: object, copy: bool = True, **kwargs: object
    ) -> t.Any:
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


@mypyc_attr(allow_interpreted_subclasses=True)
class Condition(Expression):
    """Logical conditions like x AND y, or simply x"""


@trait
@mypyc_attr(allow_interpreted_subclasses=True)
class Predicate:
    """Relationships like x = y, x > 1, x >= y."""


@mypyc_attr(allow_interpreted_subclasses=True)
class _Predicate(Condition, Predicate):
    pass


# Binary expressions like (ADD a b)
@mypyc_attr(allow_interpreted_subclasses=True)
class Binary(Condition):
    arg_types = {"this": True, "expression": True}

    @property
    def left(self) -> t.Any:
        return self.this

    @property
    def right(self) -> t.Any:
        return self.expression


class EQ(Binary, Predicate):
    pass


class NullSafeEQ(Binary, Predicate):
    pass


class NullSafeNEQ(Binary, Predicate):
    pass


# Represents e.g. := in DuckDB which is mostly used for setting parameters
class PropertyEQ(Binary):
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


class IntDiv(Binary):
    pass


class Is(Binary, Predicate):
    pass


class Kwarg(Binary):
    """Kwarg in special functions like func(kwarg => y)."""


class Like(Binary, Predicate):
    pass


class Match(Binary, Predicate):
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


# https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH
class Operator(Binary):
    arg_types = {"this": True, "operator": True, "expression": True}


class SimilarTo(Binary, Predicate):
    pass


class Sub(Binary):
    pass


# https://www.postgresql.org/docs/current/functions-range.html
# Represents range adjacency operator: -|-
class Adjacent(Binary):
    pass


class Add(Binary):
    pass


@mypyc_attr(allow_interpreted_subclasses=True)
class Connector(Binary):
    pass


class BitwiseAnd(Binary):
    arg_types = {"this": True, "expression": True, "padside": False}


class BitwiseLeftShift(Binary):
    arg_types = {"this": True, "expression": True, "requires_int128": False}


class BitwiseOr(Binary):
    arg_types = {"this": True, "expression": True, "padside": False}


class BitwiseRightShift(Binary):
    arg_types = {"this": True, "expression": True, "requires_int128": False}


class BitwiseXor(Binary):
    arg_types = {"this": True, "expression": True, "padside": False}


class Div(Binary):
    arg_types = {"this": True, "expression": True, "typed": False, "safe": False}


class Overlaps(Binary):
    pass


class ExtendsLeft(Binary):
    pass


class ExtendsRight(Binary):
    pass


class DPipe(Binary):
    arg_types = {"this": True, "expression": True, "safe": False}


# Unary Expressions
# (NOT a)
@mypyc_attr(allow_interpreted_subclasses=True)
class Unary(Condition):
    pass


class BitwiseNot(Unary):
    pass


class Not(Unary):
    pass


# WHERE x <OP> EXISTS|ALL|ANY|SOME(SELECT ...)
@mypyc_attr(allow_interpreted_subclasses=True)
class SubqueryPredicate(_Predicate):
    pass


class All(SubqueryPredicate):
    pass


class Any(SubqueryPredicate):
    pass


class Between(_Predicate):
    arg_types = {"this": True, "low": True, "high": True, "symmetric": False}


class In(_Predicate):
    arg_types = {
        "this": True,
        "expressions": False,
        "query": False,
        "unnest": False,
        "field": False,
        "is_global": False,
    }


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


# https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Manipulation-Language/Statement-Syntax/LOCKING-Request-Modifier/LOCKING-Request-Modifier-Syntax


class LockingStatement(Expression):
    arg_types = {"this": True, "expression": True}


class SequenceProperties(Expression):
    arg_types = {
        "increment": False,
        "minvalue": False,
        "maxvalue": False,
        "cache": False,
        "start": False,
        "owned": False,
        "options": False,
    }


# https://www.postgresql.org/docs/current/sql-createtrigger.html


class TriggerProperties(Expression):
    arg_types = {
        "table": True,
        "timing": True,
        "events": True,
        "execute": True,
        "constraint": False,
        "referenced_table": False,
        "deferrable": False,
        "initially": False,
        "referencing": False,
        "for_each": False,
        "when": False,
    }


class TriggerEvent(Expression):
    arg_types = {"this": True, "columns": False}


class TriggerReferencing(Expression):
    arg_types = {"old": False, "new": False}


class TruncateTable(Expression):
    arg_types = {
        "expressions": True,
        "is_database": False,
        "exists": False,
        "only": False,
        "cluster": False,
        "identity": False,
        "option": False,
        "partition": False,
    }


# https://docs.snowflake.com/en/sql-reference/sql/create-clone
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_clone_statement
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_copy


class Clone(Expression):
    arg_types = {"this": True, "shallow": False, "copy": False}


class Describe(Expression):
    arg_types = {
        "this": True,
        "style": False,
        "kind": False,
        "expressions": False,
        "partition": False,
        "format": False,
        "as_json": False,
    }


# https://duckdb.org/docs/sql/statements/attach.html#attach


class Attach(Expression):
    arg_types = {"this": True, "exists": False, "expressions": False}


# https://duckdb.org/docs/sql/statements/attach.html#detach


class Detach(Expression):
    arg_types = {"this": True, "exists": False}


# https://duckdb.org/docs/sql/statements/load_and_install.html


class Install(Expression):
    arg_types = {"this": True, "from_": False, "force": False}


# https://duckdb.org/docs/guides/meta/summarize.html


class Summarize(Expression):
    arg_types = {"this": True, "table": False}


class Kill(Expression):
    arg_types = {"this": True, "kind": False}


class Declare(Expression):
    arg_types = {"expressions": True}


class DeclareItem(Expression):
    arg_types = {"this": True, "kind": False, "default": False}


class Set(Expression):
    arg_types = {"expressions": False, "unset": False, "tag": False}


class Heredoc(Expression):
    arg_types = {"this": True, "tag": False}


class SetItem(Expression):
    arg_types = {
        "this": False,
        "expressions": False,
        "kind": False,
        "collate": False,  # MySQL SET NAMES statement
        "global_": False,
    }


class QueryBand(Expression):
    arg_types = {"this": True, "scope": False, "update": False}


class Show(Expression):
    arg_types = {
        "this": True,
        "history": False,
        "terse": False,
        "target": False,
        "offset": False,
        "starts_with": False,
        "limit": False,
        "from_": False,
        "like": False,
        "where": False,
        "db": False,
        "scope": False,
        "scope_kind": False,
        "full": False,
        "mutex": False,
        "query": False,
        "channel": False,
        "global_": False,
        "log": False,
        "position": False,
        "types": False,
        "privileges": False,
        "for_table": False,
        "for_group": False,
        "for_user": False,
        "for_role": False,
        "into_outfile": False,
        "json": False,
    }


class UserDefinedFunction(Expression):
    arg_types = {"this": True, "expressions": False, "wrapped": False}


class CharacterSet(Expression):
    arg_types = {"this": True, "default": False}


class RecursiveWithSearch(Expression):
    arg_types = {"kind": True, "this": True, "expression": True, "using": False}


class WithinGroup(Expression):
    arg_types = {"this": True, "expression": False}


class ProjectionDef(Expression):
    arg_types = {"this": True, "expression": True}


class HexString(Condition):
    arg_types = {"this": True, "is_integer": False}


class ByteString(Condition):
    arg_types = {"this": True, "is_bytes": False}


class UnicodeString(Condition):
    arg_types = {"this": True, "escape": False}


class ColumnPosition(Expression):
    arg_types = {"this": False, "position": True}


class AlterColumn(Expression):
    arg_types = {
        "this": True,
        "dtype": False,
        "collate": False,
        "using": False,
        "default": False,
        "drop": False,
        "comment": False,
        "allow_null": False,
        "visible": False,
        "rename_to": False,
    }


# https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html


class AlterIndex(Expression):
    arg_types = {"this": True, "visible": True}


class AlterSortKey(Expression):
    arg_types = {"this": False, "expressions": False, "compound": False}


class AlterSet(Expression):
    arg_types = {
        "expressions": False,
        "option": False,
        "tablespace": False,
        "access_method": False,
        "file_format": False,
        "copy_options": False,
        "tag": False,
        "location": False,
        "serde": False,
    }


class RenameColumn(Expression):
    arg_types = {"this": True, "to": True, "exists": False}


class Comment(Expression):
    arg_types = {
        "this": True,
        "kind": True,
        "expression": True,
        "exists": False,
        "materialized": False,
    }


class Comprehension(Expression):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "iterator": True,
        "condition": False,
    }


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


class WithOperator(Expression):
    arg_types = {"this": True, "op": True}


# https://docs.risingwave.com/processing/watermarks#syntax


class WatermarkColumnConstraint(Expression):
    arg_types = {"this": True, "expression": True}


class Constraint(Expression):
    arg_types = {"this": True, "expressions": True}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/export-statements


class Export(Expression):
    arg_types = {"this": True, "connection": False, "options": True}


class Filter(Expression):
    arg_types = {"this": True, "expression": True}


class Changes(Expression):
    arg_types = {"information": True, "at_before": False, "end": False}


# https://docs.snowflake.com/en/sql-reference/constructs/connect-by


class Connect(Expression):
    arg_types = {"start": False, "connect": True, "nocycle": False}


class CopyParameter(Expression):
    arg_types = {"this": True, "expression": False, "expressions": False}


class Credentials(Expression):
    arg_types = {
        "credentials": False,
        "encryption": False,
        "storage": False,
        "iam_role": False,
        "region": False,
    }


class Directory(Expression):
    arg_types = {"this": True, "local": False, "row_format": False}


class ForeignKey(Expression):
    arg_types = {
        "expressions": False,
        "reference": False,
        "delete": False,
        "update": False,
        "options": False,
    }


class ColumnPrefix(Expression):
    arg_types = {"this": True, "expression": True}


class PrimaryKey(Expression):
    arg_types = {"this": False, "expressions": True, "options": False, "include": False}


# https://www.postgresql.org/docs/9.1/sql-selectinto.html
# https://docs.aws.amazon.com/redshift/latest/dg/r_SELECT_INTO.html#r_SELECT_INTO-examples


class Into(Expression):
    arg_types = {
        "this": False,
        "temporary": False,
        "unlogged": False,
        "bulk_collect": False,
        "expressions": False,
    }


class Hint(Expression):
    arg_types = {"expressions": True}


class JoinHint(Expression):
    arg_types = {"this": True, "expressions": True}


# https://www.postgresql.org/docs/current/indexes-opclass.html


class Opclass(Expression):
    arg_types = {"this": True, "expression": True}


class Index(Expression):
    arg_types = {
        "this": False,
        "table": False,
        "unique": False,
        "primary": False,
        "amp": False,  # teradata
        "params": False,
    }


class IndexParameters(Expression):
    arg_types = {
        "using": False,
        "include": False,
        "columns": False,
        "with_storage": False,
        "partition_by": False,
        "tablespace": False,
        "where": False,
        "on": False,
    }


class ConditionalInsert(Expression):
    arg_types = {"this": True, "expression": False, "else_": False}


class MultitableInserts(Expression):
    arg_types = {"expressions": True, "kind": True, "source": True}


class OnConflict(Expression):
    arg_types = {
        "duplicate": False,
        "expressions": False,
        "action": False,
        "conflict_keys": False,
        "index_predicate": False,
        "constraint": False,
        "where": False,
    }


class OnCondition(Expression):
    arg_types = {"error": False, "empty": False, "null": False}


class Returning(Expression):
    arg_types = {"expressions": True, "into": False}


# https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html


class Introducer(Expression):
    arg_types = {"this": True, "expression": True}


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
    arg_types = {"expressions": True, "subpartition": False}


class PartitionRange(Expression):
    arg_types = {"this": True, "expression": False, "expressions": False}


class Fetch(Expression):
    arg_types = {
        "direction": False,
        "count": False,
        "limit_options": False,
    }


class Grant(Expression):
    arg_types = {
        "privileges": True,
        "kind": False,
        "securable": True,
        "principals": True,
        "grant_option": False,
    }


class Revoke(Expression):
    arg_types = {**Grant.arg_types, "cascade": False}


class Group(Expression):
    arg_types = {
        "expressions": False,
        "grouping_sets": False,
        "cube": False,
        "rollup": False,
        "totals": False,
        "all": False,
    }


class Cube(Expression):
    arg_types = {"expressions": False}


class Rollup(Expression):
    arg_types = {"expressions": False}


class GroupingSets(Expression):
    arg_types = {"expressions": True}


class Lambda(Expression):
    arg_types = {"this": True, "expressions": True, "colon": False}


class Limit(Expression):
    arg_types = {
        "this": False,
        "expression": True,
        "offset": False,
        "limit_options": False,
        "expressions": False,
    }


class LimitOptions(Expression):
    arg_types = {
        "percent": False,
        "rows": False,
        "with_ties": False,
    }


class MatchRecognizeMeasure(Expression):
    arg_types = {
        "this": True,
        "window_frame": False,
    }


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


class Offset(Expression):
    arg_types = {"this": False, "expression": True, "expressions": False}


class Order(Expression):
    arg_types = {"this": False, "expressions": True, "siblings": False}


# https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier


class WithFill(Expression):
    arg_types = {
        "from_": False,
        "to": False,
        "step": False,
        "interpolate": False,
    }


@mypyc_attr(allow_interpreted_subclasses=True)
class Property(Expression):
    arg_types = {"this": True, "value": True}


class GrantPrivilege(Expression):
    arg_types = {"this": True, "expressions": False}


class GrantPrincipal(Expression):
    arg_types = {"this": True, "kind": False}


class AllowedValuesProperty(Expression):
    arg_types = {"expressions": True}


# https://docs.starrocks.io/docs/table_design/data_distribution/#range-partitioning


class PartitionByRangePropertyDynamic(Expression):
    arg_types = {"this": False, "start": True, "end": True, "every": True}


# https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/#rollup-index


class RollupIndex(Expression):
    arg_types = {"this": True, "expressions": True, "from_index": False, "properties": False}


# https://doris.apache.org/docs/table-design/data-partitioning/manual-partitioning


class PartitionList(Expression):
    arg_types = {"this": True, "expressions": True}


# https://www.postgresql.org/docs/current/sql-createtable.html


class PartitionBoundSpec(Expression):
    # this -> IN / MODULUS, expression -> REMAINDER, from_expressions -> FROM (...), to_expressions -> TO (...)
    arg_types = {
        "this": False,
        "expression": False,
        "from_expressions": False,
        "to_expressions": False,
    }


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


class SemanticView(Expression):
    arg_types = {
        "this": True,
        "metrics": False,
        "dimensions": False,
        "facts": False,
        "where": False,
    }


class InputOutputFormat(Expression):
    arg_types = {"input_format": False, "output_format": False}


class Reference(Expression):
    arg_types = {"this": True, "expressions": False, "options": False}


# https://learn.microsoft.com/en-us/sql/t-sql/queries/option-clause-transact-sql?view=sql-server-ver16
# https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query?view=sql-server-ver16


class QueryOption(Expression):
    arg_types = {"this": True, "expression": False}


# https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16


class WithTableHint(Expression):
    arg_types = {"expressions": True}


# https://dev.mysql.com/doc/refman/8.0/en/index-hints.html


class IndexTableHint(Expression):
    arg_types = {"this": True, "expressions": False, "target": False}


# https://docs.snowflake.com/en/sql-reference/constructs/at-before


class HistoricalData(Expression):
    arg_types = {"this": True, "kind": True, "expression": True}


# https://docs.snowflake.com/en/sql-reference/sql/put


class Put(Expression):
    arg_types = {"this": True, "target": True, "properties": False}


# https://docs.snowflake.com/en/sql-reference/sql/get


class Get(Expression):
    arg_types = {"this": True, "target": True, "properties": False}


class Version(Expression):
    """
    Time travel, iceberg, bigquery etc
    https://trino.io/docs/current/connector/iceberg.html?highlight=snapshot#using-snapshots
    https://www.databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html
    https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#for_system_time_as_of
    https://learn.microsoft.com/en-us/sql/relational-databases/tables/querying-data-in-a-system-versioned-temporal-table?view=sql-server-ver16
    this is either TIMESTAMP or VERSION
    kind is ("AS OF", "BETWEEN")
    """

    arg_types = {"this": True, "kind": True, "expression": False}


class Schema(Expression):
    arg_types = {"this": False, "expressions": False}


# https://dev.mysql.com/doc/refman/8.0/en/select.html
# https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SELECT.html


class Lock(Expression):
    arg_types = {"update": True, "expressions": False, "wait": False, "key": False}


class TableSample(Expression):
    arg_types = {
        "expressions": False,
        "method": False,
        "bucket_numerator": False,
        "bucket_denominator": False,
        "bucket_field": False,
        "percent": False,
        "rows": False,
        "size": False,
        "seed": False,
    }


class Tag(Expression):
    """Tags are used for generating arbitrary sql like SELECT <span>x</span>."""

    arg_types = {
        "this": False,
        "prefix": False,
        "postfix": False,
    }


# https://duckdb.org/docs/sql/statements/unpivot#simplified-unpivot-syntax
# UNPIVOT ... INTO [NAME <col_name> VALUE <col_value>][...,]


class UnpivotColumns(Expression):
    arg_types = {"this": True, "expressions": True}


class Window(Condition):
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
        "exclude": False,
    }


class Parameter(Condition):
    arg_types = {"this": True, "expression": False}


class SessionParameter(Condition):
    arg_types = {"this": True, "kind": False}


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


class AlterSession(Expression):
    arg_types = {"expressions": True, "unset": False}


class Analyze(Expression):
    arg_types = {
        "kind": False,
        "this": False,
        "options": False,
        "mode": False,
        "partition": False,
        "expression": False,
        "properties": False,
    }


class AnalyzeStatistics(Expression):
    arg_types = {
        "kind": True,
        "option": False,
        "this": False,
        "expressions": False,
    }


class AnalyzeHistogram(Expression):
    arg_types = {
        "this": True,
        "expressions": True,
        "expression": False,
        "update_options": False,
    }


class AnalyzeSample(Expression):
    arg_types = {"kind": True, "sample": True}


class AnalyzeListChainedRows(Expression):
    arg_types = {"expression": False}


class AnalyzeDelete(Expression):
    arg_types = {"kind": False}


class AnalyzeWith(Expression):
    arg_types = {"expressions": True}


class AnalyzeValidate(Expression):
    arg_types = {
        "kind": True,
        "this": False,
        "expression": False,
    }


class AddConstraint(Expression):
    arg_types = {"expressions": True}


class AddPartition(Expression):
    arg_types = {"this": True, "exists": False, "location": False}


class AttachOption(Expression):
    arg_types = {"this": True, "expression": False}


class DropPartition(Expression):
    arg_types = {"expressions": True, "exists": False}


# https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#replace-partition


class ReplacePartition(Expression):
    arg_types = {"expression": True, "source": True}


# Represents Snowflake's ANY [ ORDER BY ... ] syntax
# https://docs.snowflake.com/en/sql-reference/constructs/pivot


class PivotAny(Expression):
    arg_types = {"this": False}


# https://docs.aws.amazon.com/redshift/latest/dg/query-super.html


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


class Distinct(Expression):
    arg_types = {"expressions": False, "on": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#for-in


class ForIn(Expression):
    arg_types = {"this": True, "expression": True}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate-function-calls#max_min_clause


class HavingMax(Expression):
    arg_types = {"this": True, "expression": True, "max": True}


# https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Functions-Expressions-and-Predicates/String-Operators-and-Functions/TRANSLATE/TRANSLATE-Function-Syntax


class TranslateCharacters(Expression):
    arg_types = {"this": True, "expression": True, "with_error": False}


# Trino's `ON OVERFLOW TRUNCATE [filler_string] {WITH | WITHOUT} COUNT`
# https://trino.io/docs/current/functions/aggregate.html#listagg


class OverflowTruncateBehavior(Expression):
    arg_types = {"this": False, "with_count": True}


# https://www.postgresql.org/docs/current/functions-json.html


class JSON(Expression):
    arg_types = {"this": False, "with_": False, "unique": False}


@mypyc_attr(allow_interpreted_subclasses=True)
class JSONPathPart(Expression):
    arg_types = {}


class JSONKeyValue(Expression):
    arg_types = {"this": True, "expression": True}


# https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/JSON_TABLE.html
# Note: parsing of JSON column definitions is currently incomplete.


class JSONColumnDef(Expression):
    arg_types = {
        "this": False,
        "kind": False,
        "path": False,
        "nested_schema": False,
        "ordinality": False,
    }


class JSONSchema(Expression):
    arg_types = {"expressions": True}


# https://dev.mysql.com/doc/refman/8.4/en/json-search-functions.html#function_json-value


class JSONValue(Expression):
    arg_types = {
        "this": True,
        "path": True,
        "returning": False,
        "on_condition": False,
    }


class OpenJSONColumnDef(Expression):
    arg_types = {"this": True, "kind": True, "path": False, "as_json": False}


# https://trino.io/docs/current/functions/json.html#json-query


class JSONExtractQuote(Expression):
    arg_types = {
        "option": True,
        "scalar": False,
    }


# https://learn.microsoft.com/en-us/sql/t-sql/language-elements/scope-resolution-operator-transact-sql?view=sql-server-ver16


class ScopeResolution(Expression):
    arg_types = {"this": False, "expression": True}


class Slice(Expression):
    arg_types = {"this": False, "expression": False, "step": False}


# Represents Snowflake's <model>!<attribute> syntax. For example: SELECT model!PREDICT(INPUT_DATA => {*})
# See: https://docs.snowflake.com/en/guides-overview-ml-functions


class ModelAttribute(Expression):
    arg_types = {"this": True, "expression": True}


# https://learn.microsoft.com/en-us/sql/t-sql/queries/select-for-clause-transact-sql?view=sql-server-ver17#syntax


class XMLKeyValueOption(Expression):
    arg_types = {"this": True, "expression": False}


class Use(Expression):
    arg_types = {"this": False, "expressions": False, "kind": False}


class When(Expression):
    arg_types = {"matched": True, "source": False, "condition": False, "then": True}


class Whens(Expression):
    """Wraps around one or more WHEN [NOT] MATCHED [...] clauses."""

    arg_types = {"expressions": True}


# Refers to a trailing semi-colon. This is only used to preserve trailing comments
# select 1; -- my comment


class Semicolon(Expression):
    arg_types = {}


class StoredProcedure(Expression):
    arg_types = {"this": True, "expressions": False, "wrapped": False}


class Block(Expression):
    arg_types = {"expressions": True}


class IfBlock(Expression):
    arg_types = {"this": True, "true": True, "false": False}


class WhileBlock(Expression):
    arg_types = {"this": True, "body": True}


class EndStatement(Expression):
    arg_types = {}


class Having(Expression):
    pass


class Where(Expression):
    pass


# national char, like n'utf8'
class National(Expression):
    pass


# https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#how-to-set-partition-expression
class PartitionId(Expression):
    pass


class Var(Expression):
    pass


class BitString(Condition):
    pass


class RawString(Condition):
    pass


class Star(Expression):
    arg_types = {"except_": False, "replace": False, "rename": False}

    @property
    def name(self) -> str:
        return "*"

    @property
    def output_name(self) -> str:
        return self.name


# https://www.databricks.com/blog/parameterized-queries-pyspark
# https://jdbc.postgresql.org/documentation/query/#using-the-statement-or-preparedstatement-interface
class Placeholder(Condition):
    arg_types = {"this": False, "kind": False, "widget": False, "jdbc": False}

    @property
    def name(self) -> str:
        this = self.this
        if isinstance(this, str):
            return this or "?"
        return this.name if this else "?"


class Null(Condition):
    arg_types: t.ClassVar[t.Dict[str, t.Any]] = {}

    @property
    def name(self) -> str:
        return "NULL"

    def to_py(self) -> Lit[None]:
        return None


class Boolean(Condition):
    def to_py(self) -> bool:
        return self.this


class Identifier(Expression):
    arg_types = {"this": True, "quoted": False, "global_": False, "temporary": False}
    _hash_raw_args = True

    @property
    def quoted(self) -> bool:
        return bool(self.args.get("quoted"))

    @property
    def output_name(self) -> str:
        return self.name


class From(Expression):
    @property
    def name(self) -> str:
        return self.this.name

    @property
    def alias_or_name(self) -> str:
        return self.this.alias_or_name


class DataTypeParam(Expression):
    arg_types = {"this": True, "expression": False}

    @property
    def name(self) -> str:
        return self.this.name


class Literal(Condition):
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


class TriggerExecute(Expression):
    pass


class Pragma(Expression):
    pass


class With(Expression):
    arg_types = {"expressions": True, "recursive": False, "search": False}

    @property
    def recursive(self) -> bool:
        return bool(self.args.get("recursive"))


class TableAlias(Expression):
    arg_types = {"this": False, "columns": False}

    @property
    def columns(self) -> t.List[t.Any]:
        return self.args.get("columns") or []


# https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE.html
class AlterDistStyle(Expression):
    pass


class AlterRename(Expression):
    pass


class SwapTable(Expression):
    pass


class ColumnConstraint(Expression):
    arg_types = {"this": False, "kind": True}

    @property
    def kind(self) -> Expression:
        return self.args["kind"]


@mypyc_attr(allow_interpreted_subclasses=True)
class ColumnConstraintKind(Expression):
    pass


class AutoIncrementColumnConstraint(ColumnConstraintKind):
    pass


class ZeroFillColumnConstraint(ColumnConstraint):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class PeriodForSystemTimeConstraint(ColumnConstraintKind):
    arg_types = {"this": True, "expression": True}


class CaseSpecificColumnConstraint(ColumnConstraintKind):
    arg_types = {"not_": True}


class CharacterSetColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True}


class CheckColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True, "enforced": False}


class ClusteredColumnConstraint(ColumnConstraintKind):
    pass


class CollateColumnConstraint(ColumnConstraintKind):
    pass


class CommentColumnConstraint(ColumnConstraintKind):
    pass


class CompressColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": False}


class DateFormatColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True}


class DefaultColumnConstraint(ColumnConstraintKind):
    pass


class EncodeColumnConstraint(ColumnConstraintKind):
    pass


# https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-EXCLUDE
class ExcludeColumnConstraint(ColumnConstraintKind):
    pass


class EphemeralColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": False}


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
        "order": False,
    }


class GeneratedAsRowColumnConstraint(ColumnConstraintKind):
    arg_types = {"start": False, "hidden": False}


# https://dev.mysql.com/doc/refman/8.0/en/create-table.html
# https://github.com/ClickHouse/ClickHouse/blob/master/src/Parsers/ParserCreateQuery.h#L646
class IndexColumnConstraint(ColumnConstraintKind):
    arg_types = {
        "this": False,
        "expressions": False,
        "kind": False,
        "index_type": False,
        "options": False,
        "expression": False,  # Clickhouse
        "granularity": False,
    }


class InlineLengthColumnConstraint(ColumnConstraintKind):
    pass


class NonClusteredColumnConstraint(ColumnConstraintKind):
    pass


class NotForReplicationColumnConstraint(ColumnConstraintKind):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


# https://docs.snowflake.com/en/sql-reference/sql/create-table
class MaskingPolicyColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True, "expressions": False}


class NotNullColumnConstraint(ColumnConstraintKind):
    arg_types = {"allow_null": False}


# https://dev.mysql.com/doc/refman/5.7/en/timestamp-initialization.html
class OnUpdateColumnConstraint(ColumnConstraintKind):
    pass


class PrimaryKeyColumnConstraint(ColumnConstraintKind):
    arg_types = {"desc": False, "options": False}


class TitleColumnConstraint(ColumnConstraintKind):
    pass


class UniqueColumnConstraint(ColumnConstraintKind):
    arg_types = {
        "this": False,
        "index_type": False,
        "on_conflict": False,
        "nulls": False,
        "options": False,
    }


class UppercaseColumnConstraint(ColumnConstraintKind):
    arg_types: t.ClassVar[t.Dict[str, t.Any]] = {}


# https://docs.risingwave.com/processing/watermarks#syntax
class PathColumnConstraint(ColumnConstraintKind):
    pass


# https://docs.snowflake.com/en/sql-reference/sql/create-table
class ProjectionPolicyColumnConstraint(ColumnConstraintKind):
    pass


# computed column expression
# https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver16
class ComputedColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True, "persisted": False, "not_null": False, "data_type": False}


# https://docs.oracle.com/en/database/other-databases/timesten/22.1/plsql-developer/examples-using-input-and-output-parameters-and-bind-variables.html#GUID-4B20426E-F93F-4835-88CB-6A79829A8D7F
class InOutColumnConstraint(ColumnConstraintKind):
    arg_types = {"input_": False, "output": False, "variadic": False}


class Drop(Expression):
    arg_types = {
        "this": False,
        "kind": False,
        "expressions": False,
        "exists": False,
        "temporary": False,
        "materialized": False,
        "cascade": False,
        "constraints": False,
        "purge": False,
        "cluster": False,
        "concurrently": False,
    }

    @property
    def kind(self) -> t.Optional[str]:
        kind = self.args.get("kind")
        return kind and kind.upper()


# https://cloud.google.com/bigquery/docs/reference/standard-sql/export-statements
class Check(Expression):
    pass


class Prior(Expression):
    pass


# https://docs.snowflake.com/en/user-guide/data-load-dirtables-query
class DirectoryStage(Expression):
    pass


# Clickhouse FROM FINAL modifier
# https://clickhouse.com/docs/en/sql-reference/statements/select/from/#final-modifier
class Final(Expression):
    pass


# hive specific sorts
# https://cwiki.apache.org/confluence/display/Hive/LanguageManual+SortBy
class Cluster(Order):
    pass


class Distribute(Order):
    pass


class Sort(Order):
    pass


class Ordered(Expression):
    arg_types = {"this": True, "desc": False, "nulls_first": True, "with_fill": False}

    @property
    def name(self) -> str:
        return self.this.name


class AlgorithmProperty(Property):
    arg_types = {"this": True}


class AutoIncrementProperty(Property):
    arg_types = {"this": True}


# https://docs.aws.amazon.com/prescriptive-guidance/latest/materialized-views-redshift/refreshing-materialized-views.html
class AutoRefreshProperty(Property):
    arg_types = {"this": True}


class BackupProperty(Property):
    arg_types = {"this": True}


# https://doris.apache.org/docs/sql-manual/sql-statements/table-and-view/async-materialized-view/CREATE-ASYNC-MATERIALIZED-VIEW/
class BuildProperty(Property):
    arg_types = {"this": True}


class BlockCompressionProperty(Property):
    arg_types = {
        "autotemp": False,
        "always": False,
        "default": False,
        "manual": False,
        "never": False,
    }


class CharacterSetProperty(Property):
    arg_types = {"this": True, "default": True}


class ChecksumProperty(Property):
    arg_types = {"on": False, "default": False}


class CollateProperty(Property):
    arg_types = {"this": True, "default": False}


class CopyGrantsProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class DataBlocksizeProperty(Property):
    arg_types = {
        "size": False,
        "units": False,
        "minimum": False,
        "maximum": False,
        "default": False,
    }


class DataDeletionProperty(Property):
    arg_types = {"on": True, "filter_column": False, "retention_period": False}


class DefinerProperty(Property):
    arg_types = {"this": True}


class DistKeyProperty(Property):
    arg_types = {"this": True}


# https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/#distribution_desc
# https://doris.apache.org/docs/sql-manual/sql-statements/Data-Definition-Statements/Create/CREATE-TABLE?_highlight=create&_highlight=table#distribution_desc
class DistributedByProperty(Property):
    arg_types = {"expressions": False, "kind": True, "buckets": False, "order": False}


class DistStyleProperty(Property):
    arg_types = {"this": True}


class DuplicateKeyProperty(Property):
    arg_types = {"expressions": True}


class EngineProperty(Property):
    arg_types = {"this": True}


class HeapProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class ToTableProperty(Property):
    arg_types = {"this": True}


class ExecuteAsProperty(Property):
    arg_types = {"this": True}


class ExternalProperty(Property):
    arg_types = {"this": False}


class FallbackProperty(Property):
    arg_types = {"no": True, "protection": False}


# https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-hiveformat
class FileFormatProperty(Property):
    arg_types = {"this": False, "expressions": False, "hive_format": False}


class CredentialsProperty(Property):
    arg_types = {"expressions": True}


class FreespaceProperty(Property):
    arg_types = {"this": True, "percent": False}


class GlobalProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class IcebergProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class InheritsProperty(Property):
    arg_types = {"expressions": True}


class InputModelProperty(Property):
    arg_types = {"this": True}


class OutputModelProperty(Property):
    arg_types = {"this": True}


class IsolatedLoadingProperty(Property):
    arg_types = {"no": False, "concurrent": False, "target": False}


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


class EnviromentProperty(Property):
    arg_types = {"expressions": True}


# spark ddl
class ClusteredByProperty(Property):
    arg_types = {"expressions": True, "sorted_by": False, "buckets": True}


class DictProperty(Property):
    arg_types = {"this": True, "kind": True, "settings": False}


class DictSubProperty(Property):
    pass


class DictRange(Property):
    arg_types = {"this": True, "min": True, "max": True}


class DynamicProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


# Clickhouse CREATE ... ON CLUSTER modifier
# https://clickhouse.com/docs/en/sql-reference/distributed-ddl
class OnCluster(Property):
    arg_types = {"this": True}


# Clickhouse EMPTY table "property"
class EmptyProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class LikeProperty(Property):
    arg_types = {"this": True, "expressions": False}


class LocationProperty(Property):
    arg_types = {"this": True}


class LockProperty(Property):
    arg_types = {"this": True}


class LockingProperty(Property):
    arg_types = {
        "this": False,
        "kind": True,
        "for_or_in": False,
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
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class OnProperty(Property):
    arg_types = {"this": True}


class OnCommitProperty(Property):
    arg_types = {"delete": False}


class PartitionedByProperty(Property):
    arg_types = {"this": True}


class PartitionedByBucket(Property):
    arg_types = {"this": True, "expression": True}


class PartitionByTruncate(Property):
    arg_types = {"this": True, "expression": True}


# https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/
class PartitionByRangeProperty(Property):
    arg_types = {"partition_expressions": True, "create_expressions": True}


# https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/#rollup-index
class RollupProperty(Property):
    arg_types = {"expressions": True}


# https://doris.apache.org/docs/table-design/data-partitioning/manual-partitioning
class PartitionByListProperty(Property):
    arg_types = {"partition_expressions": True, "create_expressions": True}


# https://doris.apache.org/docs/sql-manual/sql-statements/table-and-view/async-materialized-view/CREATE-ASYNC-MATERIALIZED-VIEW
class RefreshTriggerProperty(Property):
    arg_types = {
        "method": False,
        "kind": False,
        "every": False,
        "unit": False,
        "starts": False,
    }


# https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/
class UniqueKeyProperty(Property):
    arg_types = {"expressions": True}


# https://www.postgresql.org/docs/current/sql-createtable.html
class PartitionedOfProperty(Property):
    # this -> parent_table (schema), expression -> FOR VALUES ... / DEFAULT
    arg_types = {"this": True, "expression": True}


class StreamingTableProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class RemoteWithConnectionModelProperty(Property):
    arg_types = {"this": True}


class ReturnsProperty(Property):
    arg_types = {"this": False, "is_table": False, "table": False, "null": False}


class StrictProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


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
class SampleProperty(Property):
    arg_types = {"this": True}


# https://prestodb.io/docs/current/sql/create-view.html#synopsis
class SecurityProperty(Property):
    arg_types = {"this": True}


class SchemaCommentProperty(Property):
    arg_types = {"this": True}


class SerdeProperties(Property):
    arg_types = {"expressions": True, "with_": False}


class SetProperty(Property):
    arg_types = {"multi": True}


class SharingProperty(Property):
    arg_types = {"this": False}


class SetConfigProperty(Property):
    arg_types = {"this": True}


class SettingsProperty(Property):
    arg_types = {"expressions": True}


class SortKeyProperty(Property):
    arg_types = {"this": True, "compound": False}


class SqlReadWriteProperty(Property):
    arg_types = {"this": True}


class SqlSecurityProperty(Property):
    arg_types = {"this": True}


class StabilityProperty(Property):
    arg_types = {"this": True}


class StorageHandlerProperty(Property):
    arg_types = {"this": True}


class TemporaryProperty(Property):
    arg_types = {"this": False}


class SecureProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class TransformModelProperty(Property):
    arg_types = {"expressions": True}


class TransientProperty(Property):
    arg_types = {"this": False}


class UnloggedProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


# https://docs.snowflake.com/en/sql-reference/sql/create-table#create-table-using-template
class UsingTemplateProperty(Property):
    arg_types = {"this": True}


# https://learn.microsoft.com/en-us/sql/t-sql/statements/create-view-transact-sql?view=sql-server-ver16
class ViewAttributeProperty(Property):
    arg_types = {"this": True}


class VolatileProperty(Property):
    arg_types = {"this": False}


class WithDataProperty(Property):
    arg_types = {"no": True, "statistics": False}


class WithJournalTableProperty(Property):
    arg_types = {"this": True}


class WithSchemaBindingProperty(Property):
    arg_types = {"this": True}


class WithSystemVersioningProperty(Property):
    arg_types = {
        "on": False,
        "this": False,
        "data_consistency": False,
        "retention_period": False,
        "with_": True,
    }


class WithProcedureOptions(Property):
    arg_types = {"expressions": True}


class EncodeProperty(Property):
    arg_types = {"this": True, "properties": False, "key": False}


class IncludeProperty(Property):
    arg_types = {"this": True, "alias": False, "column_def": False}


class ForceProperty(Property):
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}


class Qualify(Expression):
    pass


# https://www.ibm.com/docs/en/ias?topic=procedures-return-statement-in-sql
class Return(Expression):
    pass


COLUMN_PARTS: t.Tuple[str, ...] = ("this", "table", "db", "catalog")


class Dot(Binary):
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
    def build(self, expressions: t.Sequence[Expression]) -> Dot:
        """Build a Dot object with a sequence of expressions."""
        if len(expressions) < 2:
            raise ValueError("Dot requires >= 2 expressions.")

        return t.cast(Dot, reduce(lambda x, y: Dot(this=x, expression=y), expressions))

    @property
    def parts(self) -> t.List[Expression]:
        """Return the parts of a table / column in order catalog, db, table."""
        this, *parts = self.flatten()

        parts.reverse()

        for arg in COLUMN_PARTS:
            part = this.args.get(arg)

            if isinstance(part, Expression):
                parts.append(part)

        parts.reverse()
        return parts


class Paren(Unary):
    @property
    def output_name(self) -> str:
        return self.this.name


class Neg(Unary):
    def to_py(self) -> int | Decimal:
        if self.is_number:
            return self.this.to_py() * -1
        return super().to_py()


@mypyc_attr(allow_interpreted_subclasses=True)
class Alias(Expression):
    arg_types = {"this": True, "alias": False}

    @property
    def output_name(self) -> str:
        return self.alias


# BigQuery requires the UNPIVOT column list aliases to be either strings or ints, but
# other dialects require identifiers. This enables us to transpile between them easily.
class PivotAlias(Alias):
    pass


class Aliases(Expression):
    arg_types = {"this": True, "expressions": True}

    @property
    def aliases(self) -> t.List[Expression]:
        return self.expressions


class Bracket(Condition):
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


class PreWhere(Expression):
    pass


class IgnoreNulls(Expression):
    pass


class RespectNulls(Expression):
    pass


# Represents both the standard SQL PIVOT operator and DuckDB's "simplified" PIVOT syntax
# https://duckdb.org/docs/sql/statements/pivot
class Pivot(Expression):
    arg_types = {
        "this": False,
        "alias": False,
        "expressions": False,
        "fields": False,
        "unpivot": False,
        "using": False,
        "group": False,
        "columns": False,
        "include_nulls": False,
        "default_on_null": False,
        "into": False,
        "with_": False,
    }

    @property
    def unpivot(self) -> bool:
        return bool(self.args.get("unpivot"))

    @property
    def fields(self) -> t.List[Expression]:
        return self.args.get("fields", [])


class Alter(Expression):
    arg_types = {
        "this": False,
        "kind": True,
        "actions": True,
        "exists": False,
        "only": False,
        "options": False,
        "cluster": False,
        "not_valid": False,
        "check": False,
        "cascade": False,
    }

    @property
    def kind(self) -> t.Optional[str]:
        kind = self.args.get("kind")
        return kind and kind.upper()

    @property
    def actions(self) -> t.List[Expression]:
        return self.args.get("actions") or []


class AnalyzeColumns(Expression):
    pass


class UsingData(Expression):
    pass


class FormatJson(Expression):
    pass


class Stream(Expression):
    pass


class WeekStart(Expression):
    pass


class XMLNamespace(Expression):
    pass


# BigQuery allows SELECT t FROM t and treats the projection as a struct value. This expression
# type is intended to be constructed by qualify so that we can properly annotate its type later
class TableColumn(Expression):
    pass


# https://www.postgresql.org/docs/current/typeconv-func.html
# https://www.postgresql.org/docs/current/xfunc-sql.html
class Variadic(Expression):
    pass


class PositionalColumn(Expression):
    pass


@mypyc_attr(allow_interpreted_subclasses=True)
class Execute(Expression):
    arg_types = {"this": True, "expressions": False}

    @property
    def name(self) -> str:
        return self.this.name


class ExecuteSql(Execute):
    pass


class JSONPath(Expression):
    arg_types = {"expressions": True, "escape": False}

    @property
    def output_name(self) -> str:
        last_segment = self.expressions[-1].this
        return last_segment if isinstance(last_segment, str) else ""


class JSONPathFilter(JSONPathPart):
    arg_types = {"this": True}


class JSONPathKey(JSONPathPart):
    arg_types = {"this": True}


class JSONPathRecursive(JSONPathPart):
    arg_types = {"this": False}


class JSONPathRoot(JSONPathPart):
    pass


class JSONPathScript(JSONPathPart):
    arg_types = {"this": True}


class JSONPathSlice(JSONPathPart):
    arg_types = {"start": False, "end": False, "step": False}


class JSONPathSelector(JSONPathPart):
    arg_types = {"this": True}


class JSONPathSubscript(JSONPathPart):
    arg_types = {"this": True}


class JSONPathUnion(JSONPathPart):
    arg_types = {"expressions": True}


class JSONPathWildcard(JSONPathPart):
    pass


@mypyc_attr(allow_interpreted_subclasses=True)
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
    def parts(self) -> t.List[Expression]:
        """Return the parts of a column in order catalog, db, table, name."""
        return [
            self.args[part] for part in ("catalog", "db", "table", "this") if self.args.get(part)
        ]

    def to_dot(self, include_dots: bool = True) -> t.Any:
        """Converts the column into a dot expression."""
        parts = self.parts
        parent = self.parent

        if include_dots:
            while isinstance(parent, Dot):
                parts.append(parent.expression)
                parent = parent.parent

        return Dot.build(deepcopy(parts)) if len(parts) > 1 else parts[0]


class Pseudocolumn(Column):
    pass


# Functions
@mypyc_attr(allow_interpreted_subclasses=True)
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

    is_var_len_args = False
    is_func = True
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


# Function returns NULL instead of error
# https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#safe_prefix
class SafeFunc(Func):
    pass


class Typeof(Func):
    pass


class Acos(Func):
    pass


class Acosh(Func):
    pass


class Asin(Func):
    pass


class Asinh(Func):
    pass


class Atan(Func):
    arg_types = {"this": True, "expression": False}


class Atanh(Func):
    pass


class Atan2(Func):
    arg_types = {"this": True, "expression": True}


class Cot(Func):
    pass


class Coth(Func):
    pass


class Cos(Func):
    pass


class Csc(Func):
    pass


class Csch(Func):
    pass


class Sec(Func):
    pass


class Sech(Func):
    pass


class Sin(Func):
    pass


class Sinh(Func):
    pass


class Tan(Func):
    pass


class Tanh(Func):
    pass


class Degrees(Func):
    pass


class Cosh(Func):
    pass


class CosineDistance(Func):
    arg_types = {"this": True, "expression": True}


class DotProduct(Func):
    arg_types = {"this": True, "expression": True}


class EuclideanDistance(Func):
    arg_types = {"this": True, "expression": True}


class ManhattanDistance(Func):
    arg_types = {"this": True, "expression": True}


class JarowinklerSimilarity(Func):
    arg_types = {"this": True, "expression": True, "case_insensitive": False}


@mypyc_attr(allow_interpreted_subclasses=True)
class AggFunc(Func):
    pass


class BitwiseAndAgg(AggFunc):
    pass


class BitwiseOrAgg(AggFunc):
    pass


class BitwiseXorAgg(AggFunc):
    pass


class BoolxorAgg(AggFunc):
    pass


class BitwiseCount(Func):
    pass


class BitmapBucketNumber(Func):
    pass


class BitmapCount(Func):
    pass


class BitmapBitPosition(Func):
    pass


class BitmapConstructAgg(AggFunc):
    pass


class BitmapOrAgg(AggFunc):
    pass


class ByteLength(Func):
    pass


class Boolnot(Func):
    arg_types = {"this": True, "round_input": False}


class Booland(Func):
    arg_types = {"this": True, "expression": True, "round_input": False}


class Boolor(Func):
    arg_types = {"this": True, "expression": True, "round_input": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#bool_for_json
class JSONBool(Func):
    pass


class ArrayRemove(Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ParameterizedAgg(AggFunc):
    arg_types = {"this": True, "expressions": True, "params": True}


class Abs(Func):
    pass


class ArgMax(AggFunc):
    arg_types = {"this": True, "expression": True, "count": False}
    _sql_names = ["ARG_MAX", "ARGMAX", "MAX_BY"]


class ArgMin(AggFunc):
    arg_types = {"this": True, "expression": True, "count": False}
    _sql_names = ["ARG_MIN", "ARGMIN", "MIN_BY"]


class ApproxTopK(AggFunc):
    arg_types = {"this": True, "expression": False, "counters": False}


# https://docs.snowflake.com/en/sql-reference/functions/approx_top_k_accumulate
# https://spark.apache.org/docs/preview/api/sql/index.html#approx_top_k_accumulate
class ApproxTopKAccumulate(AggFunc):
    arg_types = {"this": True, "expression": False}


# https://docs.snowflake.com/en/sql-reference/functions/approx_top_k_combine
class ApproxTopKCombine(AggFunc):
    arg_types = {"this": True, "expression": False}


class ApproxTopKEstimate(Func):
    arg_types = {"this": True, "expression": False}


class ApproxTopSum(AggFunc):
    arg_types = {"this": True, "expression": True, "count": True}


class ApproxQuantiles(AggFunc):
    arg_types = {"this": True, "expression": False}


# https://docs.snowflake.com/en/sql-reference/functions/approx_percentile_combine
class ApproxPercentileCombine(AggFunc):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/minhash
class Minhash(AggFunc):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


# https://docs.snowflake.com/en/sql-reference/functions/minhash_combine
class MinhashCombine(AggFunc):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/approximate_similarity
class ApproximateSimilarity(AggFunc):
    _sql_names = ["APPROXIMATE_SIMILARITY", "APPROXIMATE_JACCARD_INDEX"]


class FarmFingerprint(Func):
    arg_types = {"expressions": True}
    is_var_len_args = True
    _sql_names = ["FARM_FINGERPRINT", "FARMFINGERPRINT64"]


class Flatten(Func):
    pass


class Float64(Func):
    arg_types = {"this": True, "expression": False}


# https://spark.apache.org/docs/latest/api/sql/index.html#transform
class Transform(Func):
    arg_types = {"this": True, "expression": True}


class Translate(Func):
    arg_types = {"this": True, "from_": True, "to": True}


class Grouping(AggFunc):
    arg_types = {"expressions": True}
    is_var_len_args = True


class GroupingId(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Anonymous(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True

    @property
    def name(self) -> str:
        return self.this if isinstance(self.this, str) else self.this.name


class AnonymousAggFunc(AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


# https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators
class CombinedAggFunc(AnonymousAggFunc):
    arg_types = {"this": True, "expressions": False}


class CombinedParameterizedAgg(ParameterizedAgg):
    arg_types = {"this": True, "expressions": True, "params": True}


# https://docs.snowflake.com/en/sql-reference/functions/hash_agg
class HashAgg(AggFunc):
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


class Apply(Func):
    arg_types = {"this": True, "expression": True}


class Array(Func):
    arg_types = {
        "expressions": False,
        "bracket_notation": False,
        "struct_name_inheritance": False,
    }
    is_var_len_args = True


class Ascii(Func):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/to_array
class ToArray(Func):
    pass


class ToBoolean(Func):
    arg_types = {"this": True, "safe": False}


# https://materialize.com/docs/sql/types/list/
class List(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


# String pad, kind True -> LPAD, False -> RPAD
class Pad(Func):
    arg_types = {"this": True, "expression": True, "fill_pattern": False, "is_left": True}


# https://docs.snowflake.com/en/sql-reference/functions/to_char
# https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/TO_CHAR-number.html
class ToChar(Func):
    arg_types = {
        "this": True,
        "format": False,
        "nlsparam": False,
        "is_numeric": False,
    }


class ToCodePoints(Func):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/to_decimal
# https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/TO_NUMBER.html
class ToNumber(Func):
    arg_types = {
        "this": True,
        "format": False,
        "nlsparam": False,
        "precision": False,
        "scale": False,
        "safe": False,
        "safe_name": False,
    }


# https://docs.snowflake.com/en/sql-reference/functions/to_double
class ToDouble(Func):
    arg_types = {
        "this": True,
        "format": False,
        "safe": False,
    }


# https://docs.snowflake.com/en/sql-reference/functions/to_decfloat
class ToDecfloat(Func):
    arg_types = {
        "this": True,
        "format": False,
    }


# https://docs.snowflake.com/en/sql-reference/functions/try_to_decfloat
class TryToDecfloat(Func):
    arg_types = {
        "this": True,
        "format": False,
    }


# https://docs.snowflake.com/en/sql-reference/functions/to_file
class ToFile(Func):
    arg_types = {
        "this": True,
        "path": False,
        "safe": False,
    }


class CodePointsToBytes(Func):
    pass


class Columns(Func):
    arg_types = {"this": True, "unpack": False}


# https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16#syntax
class Convert(Func):
    arg_types = {"this": True, "expression": True, "style": False, "safe": False}


# https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/CONVERT.html
class ConvertToCharset(Func):
    arg_types = {"this": True, "dest": True, "source": False}


class ConvertTimezone(Func):
    arg_types = {
        "source_tz": False,
        "target_tz": True,
        "timestamp": True,
        "options": False,
    }


class CodePointsToString(Func):
    pass


class GenerateSeries(Func):
    arg_types = {"start": True, "end": True, "step": False, "is_end_exclusive": False}


# Postgres' GENERATE_SERIES function returns a row set, i.e. it implicitly explodes when it's
# used in a projection, so this expression is a helper that facilitates transpilation to other
# dialects. For example, we'd generate UNNEST(GENERATE_SERIES(...)) in DuckDB
class ExplodingGenerateSeries(GenerateSeries):
    pass


class ArrayAgg(AggFunc):
    arg_types = {"this": True, "nulls_excluded": False}


class ArrayUniqueAgg(AggFunc):
    pass


class AIAgg(AggFunc):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["AI_AGG"]


class AISummarizeAgg(AggFunc):
    _sql_names = ["AI_SUMMARIZE_AGG"]


class AIClassify(Func):
    arg_types = {"this": True, "categories": True, "config": False}
    _sql_names = ["AI_CLASSIFY"]


class ArrayAll(Func):
    arg_types = {"this": True, "expression": True}


# Represents Python's `any(f(x) for x in array)`, where `array` is `this` and `f` is `expression`
class ArrayAny(Func):
    arg_types = {"this": True, "expression": True}


class ArrayAppend(Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayPrepend(Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayConcat(Func):
    _sql_names = ["ARRAY_CONCAT", "ARRAY_CAT"]
    arg_types = {"this": True, "expressions": False, "null_propagation": False}
    is_var_len_args = True


class ArrayConcatAgg(AggFunc):
    pass


class ArrayCompact(Func):
    pass


class ArrayInsert(Func):
    arg_types = {"this": True, "position": True, "expression": True, "offset": False}


class ArrayRemoveAt(Func):
    arg_types = {"this": True, "position": True}


class ArrayConstructCompact(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ArrayFilter(Func):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["FILTER", "ARRAY_FILTER"]


class ArrayFirst(Func):
    pass


class ArrayLast(Func):
    pass


class ArrayReverse(Func):
    pass


class ArraySlice(Func):
    arg_types = {"this": True, "start": True, "end": False, "step": False}


class ArrayToString(Func):
    arg_types = {"this": True, "expression": True, "null": False}
    _sql_names = ["ARRAY_TO_STRING", "ARRAY_JOIN"]


class ArrayIntersect(Func):
    arg_types = {"expressions": True}
    is_var_len_args = True
    _sql_names = ["ARRAY_INTERSECT", "ARRAY_INTERSECTION"]


class ArrayExcept(Func):
    arg_types = {"this": True, "expression": True}


class StPoint(Func):
    arg_types = {"this": True, "expression": True, "null": False}
    _sql_names = ["ST_POINT", "ST_MAKEPOINT"]


class StDistance(Func):
    arg_types = {"this": True, "expression": True, "use_spheroid": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#string
class String(Func):
    arg_types = {"this": True, "zone": False}


class StringToArray(Func):
    arg_types = {"this": True, "expression": False, "null": False}
    _sql_names = ["STRING_TO_ARRAY", "SPLIT_BY_STRING", "STRTOK_TO_ARRAY"]


class ArraySize(Func):
    arg_types = {"this": True, "expression": False}
    _sql_names = ["ARRAY_SIZE", "ARRAY_LENGTH"]


class ArraySort(Func):
    arg_types = {"this": True, "expression": False}


class ArraySum(Func):
    arg_types = {"this": True, "expression": False}


class ArrayDistinct(Func):
    arg_types = {"this": True, "check_null": False}


class ArrayMax(Func):
    pass


class ArrayMin(Func):
    pass


class ArrayUnionAgg(AggFunc):
    pass


class ArraysZip(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Avg(AggFunc):
    pass


class AnyValue(AggFunc):
    pass


class Lag(AggFunc):
    arg_types = {"this": True, "offset": False, "default": False}


class Lead(AggFunc):
    arg_types = {"this": True, "offset": False, "default": False}


# some dialects have a distinction between first and first_value, usually first is an aggregate func
# and first_value is a window func
class First(AggFunc):
    arg_types = {"this": True, "expression": False}


class Last(AggFunc):
    arg_types = {"this": True, "expression": False}


class FirstValue(AggFunc):
    pass


class LastValue(AggFunc):
    pass


class NthValue(AggFunc):
    arg_types = {"this": True, "offset": True, "from_first": False}


class ObjectAgg(AggFunc):
    arg_types = {"this": True, "expression": True}


class Cast(Func):
    arg_types = {
        "this": True,
        "to": True,
        "format": False,
        "safe": False,
        "action": False,
        "default": False,
    }

    @property
    def name(self) -> str:
        return self.this.name

    @property
    def to(self) -> t.Any:
        return self.args["to"]

    @property
    def output_name(self) -> str:
        return self.name

    def is_type(self, *dtypes: t.Any) -> bool:
        """
        Checks whether this Cast's DataType matches one of the provided data types. Nested types
        like arrays or structs will be compared using "structural equivalence" semantics, so e.g.
        array<int> != array<float>.

        Args:
            dtypes: the data types to compare this Cast's DataType to.

        Returns:
            True, if and only if there is a type in `dtypes` which is equal to this Cast's DataType.
        """
        return self.to.is_type(*dtypes)


class TryCast(Cast):
    arg_types = {**Cast.arg_types, "requires_string": False}


# https://clickhouse.com/docs/sql-reference/data-types/newjson#reading-json-paths-as-sub-columns
class JSONCast(Cast):
    pass


class JustifyDays(Func):
    pass


class JustifyHours(Func):
    pass


class JustifyInterval(Func):
    pass


class Try(Func):
    pass


class CastToStrType(Func):
    arg_types = {"this": True, "to": True}


class CheckJson(Func):
    arg_types = {"this": True}


class CheckXml(Func):
    arg_types = {"this": True, "disable_auto_convert": False}


class Collation(Func):
    pass


class Ceil(Func):
    arg_types = {"this": True, "decimals": False, "to": False}
    _sql_names = ["CEIL", "CEILING"]


class Coalesce(Func):
    arg_types = {"this": True, "expressions": False, "is_nvl": False, "is_null": False}
    is_var_len_args = True
    _sql_names = ["COALESCE", "IFNULL", "NVL"]


class Chr(Func):
    arg_types = {"expressions": True, "charset": False}
    is_var_len_args = True
    _sql_names = ["CHR", "CHAR"]


class Concat(Func):
    arg_types = {"expressions": True, "safe": False, "coalesce": False}
    is_var_len_args = True


class ConcatWs(Concat):
    _sql_names = ["CONCAT_WS"]


# https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#contains_substr
class Contains(Func):
    arg_types = {"this": True, "expression": True, "json_scope": False}


# https://docs.oracle.com/cd/B13789_01/server.101/b10759/operators004.htm#i1035022
class ConnectByRoot(Func):
    pass


class Count(AggFunc):
    arg_types = {"this": False, "expressions": False, "big_int": False}
    is_var_len_args = True


class CountIf(AggFunc):
    _sql_names = ["COUNT_IF", "COUNTIF"]


# cube root
class Cbrt(Func):
    pass


class CurrentAccount(Func):
    arg_types = {}


class CurrentAccountName(Func):
    arg_types = {}


class CurrentAvailableRoles(Func):
    arg_types = {}


class CurrentClient(Func):
    arg_types = {}


class CurrentIpAddress(Func):
    arg_types = {}


class CurrentDatabase(Func):
    arg_types = {}


class CurrentSchemas(Func):
    arg_types = {"this": False}


class CurrentSecondaryRoles(Func):
    arg_types = {}


class CurrentSession(Func):
    arg_types = {}


class CurrentStatement(Func):
    arg_types = {}


class CurrentVersion(Func):
    arg_types = {}


class CurrentTransaction(Func):
    arg_types = {}


class CurrentWarehouse(Func):
    arg_types = {}


class CurrentDate(Func):
    arg_types = {"this": False}


class CurrentDatetime(Func):
    arg_types = {"this": False}


class CurrentTime(Func):
    arg_types = {"this": False}


# https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT
# In Postgres, the difference between CURRENT_TIME vs LOCALTIME etc is that the latter does not have tz
class Localtime(Func):
    arg_types = {"this": False}


class Localtimestamp(Func):
    arg_types = {"this": False}


class Systimestamp(Func):
    arg_types = {"this": False}


class CurrentTimestamp(Func):
    arg_types = {"this": False, "sysdate": False}


class CurrentTimestampLTZ(Func):
    arg_types = {}


class CurrentTimezone(Func):
    arg_types = {}


class CurrentOrganizationName(Func):
    arg_types = {}


class CurrentSchema(Func):
    arg_types = {"this": False}


class CurrentUser(Func):
    arg_types = {"this": False}


class CurrentCatalog(Func):
    arg_types = {}


class CurrentRegion(Func):
    arg_types = {}


class CurrentRole(Func):
    arg_types = {}


class CurrentRoleType(Func):
    arg_types = {}


class CurrentOrganizationUser(Func):
    arg_types = {}


class SessionUser(Func):
    arg_types = {}


class UtcDate(Func):
    arg_types = {}


class UtcTime(Func):
    arg_types = {"this": False}


class UtcTimestamp(Func):
    arg_types = {"this": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime
# expression can either be time_expr or time_zone
class Datetime(Func):
    arg_types = {"this": True, "expression": False}


class DateFromUnixDate(Func):
    pass


class DayOfWeek(Func):
    _sql_names = ["DAY_OF_WEEK", "DAYOFWEEK"]


# https://duckdb.org/docs/sql/functions/datepart.html#part-specifiers-only-usable-as-date-part-specifiers
# ISO day of week function in duckdb is ISODOW
class DayOfWeekIso(Func):
    _sql_names = ["DAYOFWEEK_ISO", "ISODOW"]


class DayOfMonth(Func):
    _sql_names = ["DAY_OF_MONTH", "DAYOFMONTH"]


class DayOfYear(Func):
    _sql_names = ["DAY_OF_YEAR", "DAYOFYEAR"]


class Dayname(Func):
    arg_types = {"this": True, "abbreviated": False}


class ToDays(Func):
    pass


class WeekOfYear(Func):
    _sql_names = ["WEEK_OF_YEAR", "WEEKOFYEAR"]


class YearOfWeek(Func):
    _sql_names = ["YEAR_OF_WEEK", "YEAROFWEEK"]


class YearOfWeekIso(Func):
    _sql_names = ["YEAR_OF_WEEK_ISO", "YEAROFWEEKISO"]


class MonthsBetween(Func):
    arg_types = {"this": True, "expression": True, "roundoff": False}


class MakeInterval(Func):
    arg_types = {
        "year": False,
        "month": False,
        "week": False,
        "day": False,
        "hour": False,
        "minute": False,
        "second": False,
    }


class PreviousDay(Func):
    arg_types = {"this": True, "expression": True}


class LaxBool(Func):
    pass


class LaxFloat64(Func):
    pass


class LaxInt64(Func):
    pass


class LaxString(Func):
    pass


class Extract(Func):
    arg_types = {"this": True, "expression": True}


class Elt(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class Timestamp(Func):
    arg_types = {"this": False, "zone": False, "with_tz": False}


class DateFromParts(Func):
    _sql_names = ["DATE_FROM_PARTS", "DATEFROMPARTS"]
    arg_types = {"year": True, "month": False, "day": False, "allow_overflow": False}


class TimeFromParts(Func):
    _sql_names = ["TIME_FROM_PARTS", "TIMEFROMPARTS"]
    arg_types = {
        "hour": True,
        "min": True,
        "sec": True,
        "nano": False,
        "fractions": False,
        "precision": False,
        "overflow": False,
    }


class DateStrToDate(Func):
    pass


class DateToDateStr(Func):
    pass


class DateToDi(Func):
    pass


# https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date
class Date(Func):
    arg_types = {"this": False, "zone": False, "expressions": False}
    is_var_len_args = True


class Day(Func):
    pass


class Decode(Func):
    arg_types = {"this": True, "charset": True, "replace": False}


class DecodeCase(Func):
    arg_types = {"expressions": True}
    is_var_len_args = True


# https://docs.snowflake.com/en/sql-reference/functions/decrypt
class Decrypt(Func):
    arg_types = {
        "this": True,
        "passphrase": True,
        "aad": False,
        "encryption_method": False,
        "safe": False,
    }


# https://docs.snowflake.com/en/sql-reference/functions/decrypt_raw
class DecryptRaw(Func):
    arg_types = {
        "this": True,
        "key": True,
        "iv": True,
        "aad": False,
        "encryption_method": False,
        "aead": False,
        "safe": False,
    }


class DenseRank(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class DiToDate(Func):
    pass


class Encode(Func):
    arg_types = {"this": True, "charset": True}


# https://docs.snowflake.com/en/sql-reference/functions/encrypt
class Encrypt(Func):
    arg_types = {"this": True, "passphrase": True, "aad": False, "encryption_method": False}


# https://docs.snowflake.com/en/sql-reference/functions/encrypt_raw
class EncryptRaw(Func):
    arg_types = {"this": True, "key": True, "iv": True, "aad": False, "encryption_method": False}


class EqualNull(Func):
    arg_types = {"this": True, "expression": True}


class Exp(Func):
    pass


class Factorial(Func):
    pass


# https://spark.apache.org/docs/latest/api/sql/#inline
class Inline(Func):
    pass


class Floor(Func):
    arg_types = {"this": True, "decimals": False, "to": False}


class FromBase32(Func):
    pass


class FromBase64(Func):
    pass


class ToBase32(Func):
    pass


class ToBase64(Func):
    pass


class ToBinary(Func):
    arg_types = {"this": True, "format": False, "safe": False}


# https://docs.snowflake.com/en/sql-reference/functions/base64_decode_binary
class Base64DecodeBinary(Func):
    arg_types = {"this": True, "alphabet": False}


# https://docs.snowflake.com/en/sql-reference/functions/base64_decode_string
class Base64DecodeString(Func):
    arg_types = {"this": True, "alphabet": False}


# https://docs.snowflake.com/en/sql-reference/functions/base64_encode
class Base64Encode(Func):
    arg_types = {"this": True, "max_line_length": False, "alphabet": False}


# https://docs.snowflake.com/en/sql-reference/functions/try_base64_decode_binary
class TryBase64DecodeBinary(Func):
    arg_types = {"this": True, "alphabet": False}


# https://docs.snowflake.com/en/sql-reference/functions/try_base64_decode_string
class TryBase64DecodeString(Func):
    arg_types = {"this": True, "alphabet": False}


# https://docs.snowflake.com/en/sql-reference/functions/try_hex_decode_binary
class TryHexDecodeBinary(Func):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/try_hex_decode_string
class TryHexDecodeString(Func):
    pass


# https://trino.io/docs/current/functions/datetime.html#from_iso8601_timestamp
class FromISO8601Timestamp(Func):
    _sql_names = ["FROM_ISO8601_TIMESTAMP"]


class GapFill(Func):
    arg_types = {
        "this": True,
        "ts_column": True,
        "bucket_width": True,
        "partitioning_columns": False,
        "value_columns": False,
        "origin": False,
        "ignore_nulls": False,
    }


# https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
class GenerateDateArray(Func):
    arg_types = {"start": True, "end": True, "step": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_timestamp_array
class GenerateTimestampArray(Func):
    arg_types = {"start": True, "end": True, "step": True}


# https://docs.snowflake.com/en/sql-reference/functions/get
class GetExtract(Func):
    arg_types = {"this": True, "expression": True}


class Getbit(Func):
    _sql_names = ["GETBIT", "GET_BIT"]
    # zero_is_msb means the most significant bit is indexed 0
    arg_types = {"this": True, "expression": True, "zero_is_msb": False}


class Greatest(Func):
    arg_types = {"this": True, "expressions": False, "ignore_nulls": True}
    is_var_len_args = True


class GroupConcat(AggFunc):
    arg_types = {"this": True, "separator": False, "on_overflow": False}


class Hex(Func):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/hex_decode_string
class HexDecodeString(Func):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/hex_encode
class HexEncode(Func):
    arg_types = {"this": True, "case": False}


class Hour(Func):
    pass


class Minute(Func):
    pass


class Second(Func):
    pass


# T-SQL: https://learn.microsoft.com/en-us/sql/t-sql/functions/compress-transact-sql?view=sql-server-ver17
# Snowflake: https://docs.snowflake.com/en/sql-reference/functions/compress
class Compress(Func):
    arg_types = {"this": True, "method": False}


# Snowflake: https://docs.snowflake.com/en/sql-reference/functions/decompress_binary
class DecompressBinary(Func):
    arg_types = {"this": True, "method": True}


# Snowflake: https://docs.snowflake.com/en/sql-reference/functions/decompress_string
class DecompressString(Func):
    arg_types = {"this": True, "method": True}


class LowerHex(Hex):
    pass


class If(Func):
    arg_types = {"this": True, "true": True, "false": False}
    _sql_names = ["IF", "IIF"]


class Nullif(Func):
    arg_types = {"this": True, "expression": True}


class Initcap(Func):
    arg_types = {"this": True, "expression": False}


class IsAscii(Func):
    pass


class IsNan(Func):
    _sql_names = ["IS_NAN", "ISNAN"]


# https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#int64_for_json
class Int64(Func):
    pass


class IsInf(Func):
    _sql_names = ["IS_INF", "ISINF"]


class IsNullValue(Func):
    pass


class IsArray(Func):
    pass


class Format(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class JSONKeys(Func):
    arg_types = {"this": True, "expression": False, "expressions": False}
    is_var_len_args = True
    _sql_names = ["JSON_KEYS"]


# https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_keys
class JSONKeysAtDepth(Func):
    arg_types = {"this": True, "expression": False, "mode": False}


class JSONObject(Func):
    arg_types = {
        "expressions": False,
        "null_handling": False,
        "unique_keys": False,
        "return_type": False,
        "encoding": False,
    }


class JSONObjectAgg(AggFunc):
    arg_types = {
        "expressions": False,
        "null_handling": False,
        "unique_keys": False,
        "return_type": False,
        "encoding": False,
    }


# https://www.postgresql.org/docs/9.5/functions-aggregate.html
class JSONBObjectAgg(AggFunc):
    arg_types = {"this": True, "expression": True}


# https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/JSON_ARRAY.html
class JSONArray(Func):
    arg_types = {
        "expressions": False,
        "null_handling": False,
        "return_type": False,
        "strict": False,
    }


# https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/JSON_ARRAYAGG.html
class JSONArrayAgg(AggFunc):
    arg_types = {
        "this": True,
        "order": False,
        "null_handling": False,
        "return_type": False,
        "strict": False,
    }


class JSONExists(Func):
    arg_types = {
        "this": True,
        "path": True,
        "passing": False,
        "on_condition": False,
        "from_dcolonqmark": False,
    }


class JSONSet(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True
    _sql_names = ["JSON_SET"]


# https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_strip_nulls
class JSONStripNulls(Func):
    arg_types = {
        "this": True,
        "expression": False,
        "include_arrays": False,
        "remove_empty": False,
    }
    _sql_names = ["JSON_STRIP_NULLS"]


class JSONValueArray(Func):
    arg_types = {"this": True, "expression": False}


class JSONRemove(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True
    _sql_names = ["JSON_REMOVE"]


# https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/JSON_TABLE.html
class JSONTable(Func):
    arg_types = {
        "this": True,
        "schema": True,
        "path": False,
        "error_handling": False,
        "empty_handling": False,
    }


# https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
# https://doris.apache.org/docs/sql-manual/sql-functions/scalar-functions/json-functions/json-type#description
class JSONType(Func):
    arg_types = {"this": True, "expression": False}
    _sql_names = ["JSON_TYPE"]


# https://docs.snowflake.com/en/sql-reference/functions/object_insert
class ObjectInsert(Func):
    arg_types = {
        "this": True,
        "key": True,
        "value": True,
        "update_flag": False,
    }


class OpenJSON(Func):
    arg_types = {"this": True, "path": False, "expressions": False}


class ObjectId(Func):
    arg_types = {"this": True, "expression": False}


class JSONBExists(Func):
    arg_types = {"this": True, "path": True}
    _sql_names = ["JSONB_EXISTS"]


class JSONExtractArray(Func):
    arg_types = {"this": True, "expression": False}
    _sql_names = ["JSON_EXTRACT_ARRAY"]


class JSONFormat(Func):
    arg_types = {"this": False, "options": False, "is_json": False, "to_json": False}
    _sql_names = ["JSON_FORMAT"]


class JSONArrayAppend(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True
    _sql_names = ["JSON_ARRAY_APPEND"]


class JSONArrayInsert(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True
    _sql_names = ["JSON_ARRAY_INSERT"]


class ParseBignumeric(Func):
    pass


class ParseNumeric(Func):
    pass


class ParseJSON(Func):
    # BigQuery, Snowflake have PARSE_JSON, Presto has JSON_PARSE
    # Snowflake also has TRY_PARSE_JSON, which is represented using `safe`
    _sql_names = ["PARSE_JSON", "JSON_PARSE"]
    arg_types = {"this": True, "expression": False, "safe": False}


# Snowflake: https://docs.snowflake.com/en/sql-reference/functions/parse_url
# Databricks: https://docs.databricks.com/aws/en/sql/language-manual/functions/parse_url
class ParseUrl(Func):
    arg_types = {"this": True, "part_to_extract": False, "key": False, "permissive": False}


class ParseIp(Func):
    arg_types = {"this": True, "type": True, "permissive": False}


class ParseTime(Func):
    arg_types = {"this": True, "format": True}


class ParseDatetime(Func):
    arg_types = {"this": True, "format": False, "zone": False}


class Least(Func):
    arg_types = {"this": True, "expressions": False, "ignore_nulls": True}
    is_var_len_args = True


class Left(Func):
    arg_types = {"this": True, "expression": True}


class Right(Func):
    arg_types = {"this": True, "expression": True}


class Reverse(Func):
    pass


class Length(Func):
    arg_types = {"this": True, "binary": False, "encoding": False}
    _sql_names = ["LENGTH", "LEN", "CHAR_LENGTH", "CHARACTER_LENGTH"]


class RtrimmedLength(Func):
    pass


class BitLength(Func):
    pass


class Levenshtein(Func):
    arg_types = {
        "this": True,
        "expression": False,
        "ins_cost": False,
        "del_cost": False,
        "sub_cost": False,
        "max_dist": False,
    }


class Ln(Func):
    pass


class Log(Func):
    arg_types = {"this": True, "expression": False}


class LogicalOr(AggFunc):
    _sql_names = ["LOGICAL_OR", "BOOL_OR", "BOOLOR_AGG"]


class LogicalAnd(AggFunc):
    _sql_names = ["LOGICAL_AND", "BOOL_AND", "BOOLAND_AGG"]


class Lower(Func):
    _sql_names = ["LOWER", "LCASE"]


class Map(Func):
    arg_types = {"keys": False, "values": False}

    @property
    def keys(self) -> t.List[Expression]:
        keys = self.args.get("keys")
        return keys.expressions if keys else []

    @property
    def values(self) -> t.List[Expression]:
        values = self.args.get("values")
        return values.expressions if values else []


# Represents the MAP {...} syntax in DuckDB - basically convert a struct to a MAP
class ToMap(Func):
    pass


class MapFromEntries(Func):
    pass


class MapCat(Func):
    arg_types = {"this": True, "expression": True}


class MapContainsKey(Func):
    arg_types = {"this": True, "key": True}


class MapDelete(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MapInsert(Func):
    arg_types = {"this": True, "key": False, "value": True, "update_flag": False}


class MapKeys(Func):
    pass


class MapPick(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MapSize(Func):
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
# Var len args due to Exasol:
# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/hashtype_md5.htm
class MD5Digest(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True
    _sql_names = ["MD5_DIGEST"]


# https://docs.snowflake.com/en/sql-reference/functions/md5_number_lower64
class MD5NumberLower64(Func):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/md5_number_upper64
class MD5NumberUpper64(Func):
    pass


class Median(AggFunc):
    pass


class Mode(AggFunc):
    arg_types = {"this": False, "deterministic": False}


class Min(AggFunc):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Month(Func):
    pass


class Monthname(Func):
    arg_types = {"this": True, "abbreviated": False}


class AddMonths(Func):
    arg_types = {"this": True, "expression": True, "preserve_end_of_month": False}


class Nvl2(Func):
    arg_types = {"this": True, "true": True, "false": False}


class Ntile(AggFunc):
    arg_types = {"this": False}


class Normalize(Func):
    arg_types = {"this": True, "form": False, "is_casefold": False}


class Normal(Func):
    arg_types = {"this": True, "stddev": True, "gen": True}


# https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/net_functions
class NetFunc(Func):
    pass


# https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#nethost
class Host(Func):
    pass


# https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netreg_domain
class RegDomain(Func):
    pass


class Overlay(Func):
    arg_types = {"this": True, "expression": True, "from_": True, "for_": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-predict#mlpredict_function
class Predict(Func):
    arg_types = {"this": True, "expression": True, "params_struct": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-translate#mltranslate_function
class MLTranslate(Func):
    arg_types = {"this": True, "expression": True, "params_struct": True}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-feature-time
class FeaturesAtTime(Func):
    arg_types = {"this": True, "time": False, "num_rows": False, "ignore_feature_nulls": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-embedding
class GenerateEmbedding(Func):
    arg_types = {"this": True, "expression": True, "params_struct": False, "is_text": False}


class MLForecast(Func):
    arg_types = {"this": True, "expression": False, "params_struct": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/search_functions#vector_search
class VectorSearch(Func):
    arg_types = {
        "this": True,
        "column_to_search": True,
        "query_table": True,
        "query_column_to_search": False,
        "top_k": False,
        "distance_type": False,
        "options": False,
    }


class Pi(Func):
    arg_types = {}


class PercentileCont(AggFunc):
    arg_types = {"this": True, "expression": False}


class PercentileDisc(AggFunc):
    arg_types = {"this": True, "expression": False}


class PercentRank(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Quantile(AggFunc):
    arg_types = {"this": True, "quantile": True}


class ApproxQuantile(Quantile):
    arg_types = {
        "this": True,
        "quantile": True,
        "accuracy": False,
        "weight": False,
        "error_tolerance": False,
    }


# https://docs.snowflake.com/en/sql-reference/functions/approx_percentile_accumulate
class ApproxPercentileAccumulate(AggFunc):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/approx_percentile_estimate
class ApproxPercentileEstimate(Func):
    arg_types = {"this": True, "percentile": True}


class Quarter(Func):
    pass


# https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Functions-Expressions-and-Predicates/Arithmetic-Trigonometric-Hyperbolic-Operators/Functions/RANDOM/RANDOM-Function-Syntax
# teradata lower and upper bounds
class Rand(Func):
    _sql_names = ["RAND", "RANDOM"]
    arg_types = {"this": False, "lower": False, "upper": False}


class Randn(Func):
    arg_types = {"this": False}


class Randstr(Func):
    arg_types = {"this": True, "generator": False}


class RangeN(Func):
    arg_types = {"this": True, "expressions": True, "each": False}


class RangeBucket(Func):
    arg_types = {"this": True, "expression": True}


class Rank(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ReadCSV(Func):
    _sql_names = ["READ_CSV"]
    is_var_len_args = True
    arg_types = {"this": True, "expressions": False}


class ReadParquet(Func):
    is_var_len_args = True
    arg_types = {"expressions": True}


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
        "null_if_pos_overflow": False,  # for transpilation target behavior
    }


class RegexpExtractAll(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "group": False,
        "parameters": False,
        "position": False,
        "occurrence": False,
    }


class RegexpReplace(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "replacement": False,
        "position": False,
        "occurrence": False,
        "modifiers": False,
        "single_replace": False,
    }


class RegexpInstr(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "occurrence": False,
        "option": False,
        "parameters": False,
        "group": False,
    }


# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.split.html
# limit is the number of times a pattern is applied
class RegexpSplit(Func):
    arg_types = {"this": True, "expression": True, "limit": False}


class RegexpCount(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "parameters": False,
    }


class RegrValx(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrValy(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrAvgy(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrAvgx(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrCount(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrIntercept(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrR2(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSxx(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSxy(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSyy(AggFunc):
    arg_types = {"this": True, "expression": True}


class RegrSlope(AggFunc):
    arg_types = {"this": True, "expression": True}


class Repeat(Func):
    arg_types = {"this": True, "times": True}


# Some dialects like Snowflake support two argument replace
class Replace(Func):
    arg_types = {"this": True, "expression": True, "replacement": False}


class Radians(Func):
    pass


# https://learn.microsoft.com/en-us/sql/t-sql/functions/round-transact-sql?view=sql-server-ver16
# tsql third argument function == trunctaion if not 0
class Round(Func):
    arg_types = {
        "this": True,
        "decimals": False,
        "truncate": False,
        "casts_non_integer_decimals": False,
    }


# Numeric truncation - distinct from DateTrunc/TimestampTrunc
# Most dialects: TRUNC(number, decimals) or TRUNCATE(number, decimals)
# T-SQL: ROUND(number, decimals, 1) - handled in generator
class Trunc(Func):
    arg_types = {"this": True, "decimals": False}
    _sql_names = ["TRUNC", "TRUNCATE"]


class RowNumber(Func):
    arg_types = {"this": False}


class Seq1(Func):
    arg_types = {"this": False}


class Seq2(Func):
    arg_types = {"this": False}


class Seq4(Func):
    arg_types = {"this": False}


class Seq8(Func):
    arg_types = {"this": False}


class SafeAdd(Func):
    arg_types = {"this": True, "expression": True}


class SafeDivide(Func):
    arg_types = {"this": True, "expression": True}


class SafeMultiply(Func):
    arg_types = {"this": True, "expression": True}


class SafeNegate(Func):
    pass


class SafeSubtract(Func):
    arg_types = {"this": True, "expression": True}


class SafeConvertBytesToString(Func):
    pass


class SHA(Func):
    _sql_names = ["SHA", "SHA1"]


class SHA2(Func):
    _sql_names = ["SHA2"]
    arg_types = {"this": True, "length": False}


# Represents the variant of the SHA1 function that returns a binary value
class SHA1Digest(Func):
    pass


# Represents the variant of the SHA2 function that returns a binary value
class SHA2Digest(Func):
    arg_types = {"this": True, "length": False}


class Sign(Func):
    _sql_names = ["SIGN", "SIGNUM"]


class SortArray(Func):
    arg_types = {"this": True, "asc": False, "nulls_first": False}


class Soundex(Func):
    pass


# https://docs.snowflake.com/en/sql-reference/functions/soundex_p123
class SoundexP123(Func):
    pass


class Split(Func):
    arg_types = {"this": True, "expression": True, "limit": False}


# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.split_part.html
# https://docs.snowflake.com/en/sql-reference/functions/split_part
# https://docs.snowflake.com/en/sql-reference/functions/strtok
class SplitPart(Func):
    arg_types = {"this": True, "delimiter": False, "part_index": False}


# Start may be omitted in the case of postgres
# https://www.postgresql.org/docs/9.1/functions-string.html @ Table 9-6
class Substring(Func):
    _sql_names = ["SUBSTRING", "SUBSTR"]
    arg_types = {"this": True, "start": False, "length": False}


class SubstringIndex(Func):
    """
    SUBSTRING_INDEX(str, delim, count)

    *count* > 0  → left slice before the *count*-th delimiter
    *count* < 0  → right slice after the |count|-th delimiter
    """

    arg_types = {"this": True, "delimiter": True, "count": True}


class StandardHash(Func):
    arg_types = {"this": True, "expression": False}


class StartsWith(Func):
    _sql_names = ["STARTS_WITH", "STARTSWITH"]
    arg_types = {"this": True, "expression": True}


class EndsWith(Func):
    _sql_names = ["ENDS_WITH", "ENDSWITH"]
    arg_types = {"this": True, "expression": True}


class StrPosition(Func):
    arg_types = {
        "this": True,
        "substr": True,
        "position": False,
        "occurrence": False,
    }


# Snowflake: https://docs.snowflake.com/en/sql-reference/functions/search
# BigQuery: https://cloud.google.com/bigquery/docs/reference/standard-sql/search_functions#search
class Search(Func):
    arg_types = {
        "this": True,  # data_to_search / search_data
        "expression": True,  # search_query / search_string
        "json_scope": False,  # BigQuery: JSON_VALUES | JSON_KEYS | JSON_KEYS_AND_VALUES
        "analyzer": False,  # Both: analyzer / ANALYZER
        "analyzer_options": False,  # BigQuery: analyzer_options_values
        "search_mode": False,  # Snowflake: OR | AND
    }


# Snowflake: https://docs.snowflake.com/en/sql-reference/functions/search_ip
class SearchIp(Func):
    arg_types = {"this": True, "expression": True}


class StrToDate(Func):
    arg_types = {"this": True, "format": False, "safe": False}


class StrToTime(Func):
    arg_types = {"this": True, "format": True, "zone": False, "safe": False, "target_type": False}


# Spark allows unix_timestamp()
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_timestamp.html
class StrToUnix(Func):
    arg_types = {"this": False, "format": False}


# https://prestodb.io/docs/current/functions/string.html
# https://spark.apache.org/docs/latest/api/sql/index.html#str_to_map
class StrToMap(Func):
    arg_types = {
        "this": True,
        "pair_delim": False,
        "key_value_delim": False,
        "duplicate_resolution_callback": False,
    }


class NumberToStr(Func):
    arg_types = {"this": True, "format": True, "culture": False}


class FromBase(Func):
    arg_types = {"this": True, "expression": True}


class Space(Func):
    """
    SPACE(n) → string consisting of n blank characters
    """

    pass


class Struct(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class StructExtract(Func):
    arg_types = {"this": True, "expression": True}


# https://learn.microsoft.com/en-us/sql/t-sql/functions/stuff-transact-sql?view=sql-server-ver16
# https://docs.snowflake.com/en/sql-reference/functions/insert
class Stuff(Func):
    _sql_names = ["STUFF", "INSERT"]
    arg_types = {"this": True, "start": True, "length": True, "expression": True}


class Sqrt(Func):
    pass


class Stddev(AggFunc):
    _sql_names = ["STDDEV", "STDEV"]


class StddevPop(AggFunc):
    pass


class StddevSamp(AggFunc):
    pass


class Sum(AggFunc):
    pass


# https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time
class Time(Func):
    arg_types = {"this": False, "zone": False}


class TimeToStr(Func):
    arg_types = {"this": True, "format": True, "culture": False, "zone": False}


class TimeToTimeStr(Func):
    pass


class TimeToUnix(Func):
    pass


class TimeStrToDate(Func):
    pass


class TimeStrToTime(Func):
    arg_types = {"this": True, "zone": False}


class TimeStrToUnix(Func):
    pass


class Trim(Func):
    arg_types = {
        "this": True,
        "expression": False,
        "position": False,
        "collation": False,
    }


class TsOrDsToDateStr(Func):
    pass


class TsOrDsToDate(Func):
    arg_types = {"this": True, "format": False, "safe": False}


class TsOrDsToDatetime(Func):
    pass


class TsOrDsToTime(Func):
    arg_types = {"this": True, "format": False, "safe": False}


class TsOrDsToTimestamp(Func):
    pass


class TsOrDiToDi(Func):
    pass


class Unhex(Func):
    arg_types = {"this": True, "expression": False}


class Unicode(Func):
    pass


class Uniform(Func):
    arg_types = {"this": True, "expression": True, "gen": False, "seed": False}


# https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#unix_date
class UnixDate(Func):
    pass


class UnixToStr(Func):
    arg_types = {"this": True, "format": False}


# https://prestodb.io/docs/current/functions/datetime.html
# presto has weird zone/hours/minutes
class UnixToTime(Func):
    arg_types = {
        "this": True,
        "scale": False,
        "zone": False,
        "hours": False,
        "minutes": False,
        "format": False,
        "target_type": False,
    }

    SECONDS = Literal.number(0)
    DECIS = Literal.number(1)
    CENTIS = Literal.number(2)
    MILLIS = Literal.number(3)
    DECIMILLIS = Literal.number(4)
    CENTIMILLIS = Literal.number(5)
    MICROS = Literal.number(6)
    DECIMICROS = Literal.number(7)
    CENTIMICROS = Literal.number(8)
    NANOS = Literal.number(9)


class UnixToTimeStr(Func):
    pass


class UnixSeconds(Func):
    pass


class UnixMicros(Func):
    pass


class UnixMillis(Func):
    pass


class Uuid(Func):
    _sql_names = ["UUID", "GEN_RANDOM_UUID", "GENERATE_UUID", "UUID_STRING"]

    arg_types = {"this": False, "name": False, "is_string": False}


TIMESTAMP_PARTS = {
    "year": False,
    "month": False,
    "day": False,
    "hour": False,
    "min": False,
    "sec": False,
    "nano": False,
}


class TimestampFromParts(Func):
    _sql_names = ["TIMESTAMP_FROM_PARTS", "TIMESTAMPFROMPARTS"]
    arg_types = {
        **TIMESTAMP_PARTS,
        "zone": False,
        "milli": False,
        "this": False,
        "expression": False,
    }


class TimestampLtzFromParts(Func):
    _sql_names = ["TIMESTAMP_LTZ_FROM_PARTS", "TIMESTAMPLTZFROMPARTS"]
    arg_types = TIMESTAMP_PARTS.copy()


class TimestampTzFromParts(Func):
    _sql_names = ["TIMESTAMP_TZ_FROM_PARTS", "TIMESTAMPTZFROMPARTS"]
    arg_types = {
        **TIMESTAMP_PARTS,
        "zone": False,
    }


class Upper(Func):
    _sql_names = ["UPPER", "UCASE"]


# https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/CUME_DIST.html
class CumeDist(AggFunc):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Variance(AggFunc):
    _sql_names = ["VARIANCE", "VARIANCE_SAMP", "VAR_SAMP"]


class VariancePop(AggFunc):
    _sql_names = ["VARIANCE_POP", "VAR_POP"]


class Kurtosis(AggFunc):
    pass


class Skewness(AggFunc):
    pass


class WidthBucket(Func):
    arg_types = {
        "this": True,
        "min_value": False,
        "max_value": False,
        "num_buckets": False,
        "threshold": False,
    }


class CovarSamp(AggFunc):
    arg_types = {"this": True, "expression": True}


class CovarPop(AggFunc):
    arg_types = {"this": True, "expression": True}


class Week(Func):
    arg_types = {"this": True, "mode": False}


class NextDay(Func):
    arg_types = {"this": True, "expression": True}


class XMLElement(Func):
    _sql_names = ["XMLELEMENT"]
    arg_types = {"this": True, "expressions": False, "evalname": False}


class XMLGet(Func):
    _sql_names = ["XMLGET"]
    arg_types = {"this": True, "expression": True, "instance": False}


class XMLTable(Func):
    arg_types = {
        "this": True,
        "namespaces": False,
        "passing": False,
        "columns": False,
        "by_ref": False,
    }


class Year(Func):
    pass


class Zipf(Func):
    arg_types = {"this": True, "elementcount": True, "gen": True}


# https://docs.oracle.com/javadb/10.8.3.0/ref/rrefsqljnextvaluefor.html
# https://learn.microsoft.com/en-us/sql/t-sql/functions/next-value-for-transact-sql?view=sql-server-ver16
class NextValueFor(Func):
    arg_types = {"this": True, "order": False}


# mypyc sets ClassVar overrides after __init_subclass__ runs, so required_args is computed from
# the parent's arg_types. Re-fix all compiled subclasses now that all ClassVars are set.
_stack = list(Expression.__subclasses__())
while _stack:
    _cls = _stack.pop()
    _cls.required_args = {k for k, v in _cls.arg_types.items() if v}
    _stack.extend(_cls.__subclasses__())
