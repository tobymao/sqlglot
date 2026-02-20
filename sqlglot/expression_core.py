from __future__ import annotations

import sys
import typing as t
from collections import deque
from copy import deepcopy
from mypy_extensions import mypyc_attr

from sqlglot.helper import to_bool


EC = t.TypeVar("EC", bound="ExpressionCore")


POSITION_META_KEYS: t.Tuple[str, ...] = ("line", "col", "start", "end")
SQLGLOT_META: str = "sqlglot.meta"
UNITTEST: bool = "unittest" in sys.modules or "pytest" in sys.modules


@mypyc_attr(allow_interpreted_subclasses=True)
class ExpressionCore:
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

    key: t.ClassVar[str]
    arg_types: t.ClassVar[t.Dict[str, bool]] = {}
    required_args: t.ClassVar[t.Set[str]] = set()
    is_var_len_args: t.ClassVar[bool] = False
    is_func: t.ClassVar[bool] = False
    _hash_raw_args: t.ClassVar[bool] = False

    def __init__(self, **args: t.Any) -> None:
        self.args: t.Dict[str, t.Any] = args
        self.parent: t.Optional[ExpressionCore] = None
        self.arg_key: t.Optional[str] = None
        self.index: t.Optional[int] = None
        self.comments: t.Optional[t.List[str]] = None
        self._type: t.Optional[t.Any] = None
        self._meta: t.Optional[t.Dict[str, t.Any]] = None
        self._hash: t.Optional[int] = None

        for arg_key, value in self.args.items():
            self._set_parent(arg_key, value)

    def _set_parent(self, arg_key: str, value: t.Any, index: t.Optional[int] = None) -> None:
        if isinstance(value, ExpressionCore):
            value.parent = self
            value.arg_key = arg_key
            value.index = index
        elif isinstance(value, list):
            for i, v in enumerate(value):
                if isinstance(v, ExpressionCore):
                    v.parent = self
                    v.arg_key = arg_key
                    v.index = i

    def iter_expressions(self: EC, reverse: bool = False) -> t.Iterator[EC]:
        for vs in reversed(self.args.values()) if reverse else self.args.values():
            if isinstance(vs, list):
                for v in reversed(vs) if reverse else vs:
                    if isinstance(v, ExpressionCore):
                        yield t.cast(EC, v)
            elif isinstance(vs, ExpressionCore):
                yield t.cast(EC, vs)

    def bfs(self: EC, prune: t.Optional[t.Callable[[EC], bool]] = None) -> t.Iterator[EC]:
        queue: t.Deque[EC] = deque()
        queue.append(self)
        while queue:
            node = queue.popleft()
            yield node
            if prune and prune(node):
                continue
            for v in node.iter_expressions():
                queue.append(v)

    def dfs(self: EC, prune: t.Optional[t.Callable[[EC], bool]] = None) -> t.Iterator[EC]:
        stack: t.List[EC] = [self]
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

    def find_ancestor(self, *expression_types: t.Any) -> t.Optional[t.Any]:
        ancestor = self.parent
        while ancestor and not isinstance(ancestor, expression_types):
            ancestor = ancestor.parent
        return ancestor

    @property
    def same_parent(self) -> bool:
        return type(self.parent) is self.__class__

    def root(self) -> ExpressionCore:
        expression = self
        while expression.parent:
            expression = expression.parent
        return expression

    def __eq__(self, other: object) -> bool:
        return self is other or (type(self) is type(other) and hash(self) == hash(other))

    def __hash__(self) -> int:
        if self._hash is None:
            nodes: t.List[ExpressionCore] = []
            queue: t.Deque[ExpressionCore] = deque()
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
        self: EC,
        other: t.Optional[t.Any] = None,
        line: t.Optional[int] = None,
        col: t.Optional[int] = None,
        start: t.Optional[int] = None,
        end: t.Optional[int] = None,
    ) -> EC:
        if other is None:
            self.meta["line"] = line
            self.meta["col"] = col
            self.meta["start"] = start
            self.meta["end"] = end
        elif isinstance(other, ExpressionCore):
            for k in POSITION_META_KEYS:
                if k in other.meta:
                    self.meta[k] = other.meta[k]
        else:
            # Token: has .line, .col, .start, .end attributes
            self.meta["line"] = other.line
            self.meta["col"] = other.col
            self.meta["start"] = other.start
            self.meta["end"] = other.end
        return self

    def to_py(self) -> t.Any:
        raise ValueError(f"{self} cannot be converted to a Python object.")

    def text(self, key: str) -> str:
        field = self.args.get(key)
        if isinstance(field, str):
            return field
        return ""

    @property
    def name(self) -> str:
        return self.text("this")

    @property
    def alias(self) -> str:
        alias = self.args.get("alias")
        if isinstance(alias, ExpressionCore):
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

    def is_leaf(self) -> bool:
        return not any(
            (isinstance(v, ExpressionCore) or type(v) is list) and v for v in self.args.values()
        )

    def __deepcopy__(self, memo: t.Any) -> ExpressionCore:
        root = self.__class__()
        stack: t.List[t.Tuple[ExpressionCore, ExpressionCore]] = [(self, root)]

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
                if hasattr(vs, "parent"):
                    stack.append((vs, vs.__class__()))
                    copy.set(k, stack[-1][-1])
                elif type(vs) is list:
                    copy.args[k] = []

                    for v in vs:
                        if hasattr(v, "parent"):
                            stack.append((v, v.__class__()))
                            copy.append(k, stack[-1][-1])
                        else:
                            copy.append(k, v)
                else:
                    copy.args[k] = vs

        return root

    def copy(self: EC) -> EC:
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
                        value: t.Any = v[0].strip() if v else True
                        self.meta[k.strip()] = to_bool(value)

                if not prepend:
                    self.comments.append(comment)

            if prepend:
                self.comments = comments + self.comments

    def set(
        self,
        arg_key: str,
        value: t.Any,
        index: t.Optional[int] = None,
        overwrite: bool = True,
    ) -> None:
        node: t.Optional[ExpressionCore] = self

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

    def find(self, *expression_types: t.Any, bfs: bool = True) -> t.Optional[t.Any]:
        return next(self.find_all(*expression_types, bfs=bfs), None)

    def find_all(self, *expression_types: t.Any, bfs: bool = True) -> t.Iterator[t.Any]:
        for expression in self.walk(bfs=bfs):
            if isinstance(expression, expression_types):
                yield expression

    def walk(
        self: EC,
        bfs: bool = True,
        prune: t.Optional[t.Callable[[EC], bool]] = None,
    ) -> t.Iterator[EC]:
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

        if type(expression) is list and isinstance(value, ExpressionCore):
            if value.parent:
                value.parent.replace(expression)
        else:
            parent.set(key, expression, self.index)

        if expression is not self:
            self.parent = None
            self.arg_key = None
            self.index = None

        return expression

    def pop(self: EC) -> EC:
        self.replace(None)
        return self

    def assert_is(self, type_: t.Any) -> t.Any:
        if not isinstance(self, type_):
            raise AssertionError(f"{self} is not {type_}.")
        return self

    def transform(self, fun: t.Callable, *args: t.Any, copy: bool = True, **kwargs: t.Any) -> t.Any:
        root: t.Optional[t.Any] = None
        new_node: t.Optional[t.Any] = None

        for node in (self.copy() if copy else self).dfs(prune=lambda n: n is not new_node):
            parent, arg_key, index = node.parent, node.arg_key, node.index
            new_node = fun(node, *args, **kwargs)

            if not root:
                root = new_node
            elif parent and arg_key and new_node is not node:
                parent.set(arg_key, new_node, index)

        assert root
        return root
