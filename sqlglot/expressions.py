"""
.. include:: ../pdoc/docs/expressions.md
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

from sqlglot.errors import ParseError
from sqlglot.helper import (
    AutoName,
    camel_to_snake_case,
    ensure_collection,
    seq_get,
    split_num_words,
    subclasses,
)
from sqlglot.tokens import Token

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import Dialect


class _Expression(type):
    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)
        klass.key = clsname.lower()
        return klass


class Expression(metaclass=_Expression):
    """
    The base class for all expressions in a syntax tree.

    Attributes:
        arg_types (dict): determines arguments supported by this expression.
            The key in a dictionary defines a unique key of an argument using
            which the argument's value can be retrieved. The value is a boolean
            flag which indicates whether the argument's value is required (True)
            or optional (False).
    """

    key = "Expression"
    arg_types = {"this": True}
    __slots__ = ("args", "parent", "arg_key", "comments", "_type")

    def __init__(self, **args):
        self.args = args
        self.parent = None
        self.arg_key = None
        self.comments = None
        self._type: t.Optional[DataType] = None

        for arg_key, value in self.args.items():
            self._set_parent(arg_key, value)

    def __eq__(self, other) -> bool:
        return type(self) is type(other) and _norm_args(self) == _norm_args(other)

    def __hash__(self) -> int:
        return hash(
            (
                self.key,
                tuple(
                    (k, tuple(v) if isinstance(v, list) else v) for k, v in _norm_args(self).items()
                ),
            )
        )

    @property
    def this(self):
        return self.args.get("this")

    @property
    def expression(self):
        return self.args.get("expression")

    @property
    def expressions(self):
        return self.args.get("expressions") or []

    def text(self, key):
        field = self.args.get(key)
        if isinstance(field, str):
            return field
        if isinstance(field, (Identifier, Literal, Var)):
            return field.this
        return ""

    @property
    def is_string(self):
        return isinstance(self, Literal) and self.args["is_string"]

    @property
    def is_number(self):
        return isinstance(self, Literal) and not self.args["is_string"]

    @property
    def is_int(self):
        if self.is_number:
            try:
                int(self.name)
                return True
            except ValueError:
                pass
        return False

    @property
    def alias(self):
        if isinstance(self.args.get("alias"), TableAlias):
            return self.args["alias"].name
        return self.text("alias")

    @property
    def name(self):
        return self.text("this")

    @property
    def alias_or_name(self):
        if isinstance(self, Null):
            return "NULL"
        return self.alias or self.name

    @property
    def type(self) -> t.Optional[DataType]:
        return self._type

    @type.setter
    def type(self, dtype: t.Optional[DataType | DataType.Type | str]) -> None:
        if dtype and not isinstance(dtype, DataType):
            dtype = DataType.build(dtype)
        self._type = dtype  # type: ignore

    def __deepcopy__(self, memo):
        copy = self.__class__(**deepcopy(self.args))
        copy.comments = self.comments
        copy.type = self.type
        return copy

    def copy(self):
        new = deepcopy(self)
        for item, parent, _ in new.bfs():
            if isinstance(item, Expression) and parent:
                item.parent = parent
        return new

    def append(self, arg_key, value):
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

    def set(self, arg_key, value):
        """
        Sets `arg_key` to `value`.

        Args:
            arg_key (str): name of the expression arg
            value: value to set the arg to.
        """
        self.args[arg_key] = value
        self._set_parent(arg_key, value)

    def _set_parent(self, arg_key, value):
        if isinstance(value, Expression):
            value.parent = self
            value.arg_key = arg_key
        elif isinstance(value, list):
            for v in value:
                if isinstance(v, Expression):
                    v.parent = self
                    v.arg_key = arg_key

    @property
    def depth(self):
        """
        Returns the depth of this tree.
        """
        if self.parent:
            return self.parent.depth + 1
        return 0

    def find(self, *expression_types, bfs=True):
        """
        Returns the first node in this tree which matches at least one of
        the specified types.

        Args:
            expression_types (type): the expression type(s) to match.

        Returns:
            the node which matches the criteria or None if no node matching
            the criteria was found.
        """
        return next(self.find_all(*expression_types, bfs=bfs), None)

    def find_all(self, *expression_types, bfs=True):
        """
        Returns a generator object which visits all nodes in this tree and only
        yields those that match at least one of the specified expression types.

        Args:
            expression_types (type): the expression type(s) to match.

        Returns:
            the generator object.
        """
        for expression, _, _ in self.walk(bfs=bfs):
            if isinstance(expression, expression_types):
                yield expression

    def find_ancestor(self, *expression_types):
        """
        Returns a nearest parent matching expression_types.

        Args:
            expression_types (type): the expression type(s) to match.

        Returns:
            the parent node
        """
        ancestor = self.parent
        while ancestor and not isinstance(ancestor, expression_types):
            ancestor = ancestor.parent
        return ancestor

    @property
    def parent_select(self):
        """
        Returns the parent select statement.
        """
        return self.find_ancestor(Select)

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
            the generator object.
        """
        parent = parent or self.parent
        yield self, parent, key
        if prune and prune(self, parent, key):
            return

        for k, v in self.args.items():
            for node in ensure_collection(v):
                if isinstance(node, Expression):
                    yield from node.dfs(self, k, prune)

    def bfs(self, prune=None):
        """
        Returns a generator object which visits all nodes in this tree in
        the BFS (Breadth-first) order.

        Returns:
            the generator object.
        """
        queue = deque([(self, self.parent, None)])

        while queue:
            item, parent, key = queue.popleft()

            yield item, parent, key
            if prune and prune(item, parent, key):
                continue

            if isinstance(item, Expression):
                for k, v in item.args.items():
                    for node in ensure_collection(v):
                        if isinstance(node, Expression):
                            queue.append((node, item, k))

    def unnest(self):
        """
        Returns the first non parenthesis child or self.
        """
        expression = self
        while isinstance(expression, Paren):
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
        return tuple(arg.unnest() for arg in self.args.values() if arg)

    def flatten(self, unnest=True):
        """
        Returns a generator which yields child nodes who's parents are the same class.

        A AND B AND C -> [A, B, C]
        """
        for node, _, _ in self.dfs(prune=lambda n, p, *_: p and not isinstance(n, self.__class__)):
            if not isinstance(node, self.__class__):
                yield node.unnest() if unnest else node

    def __str__(self):
        return self.sql()

    def __repr__(self):
        return self.to_s()

    def sql(self, dialect: Dialect | str | None = None, **opts) -> str:
        """
        Returns SQL string representation of this tree.

        Args
            dialect (str): the dialect of the output SQL string
                (eg. "spark", "hive", "presto", "mysql").
            opts (dict): other :class:`~sqlglot.generator.Generator` options.

        Returns
            the SQL string.
        """
        from sqlglot.dialects import Dialect

        return Dialect.get_or_raise(dialect)().generate(self, **opts)

    def to_s(self, hide_missing: bool = True, level: int = 0) -> str:
        indent = "" if not level else "\n"
        indent += "".join(["  "] * level)
        left = f"({self.key.upper()} "

        args: t.Dict[str, t.Any] = {
            k: ", ".join(
                v.to_s(hide_missing=hide_missing, level=level + 1) if hasattr(v, "to_s") else str(v)
                for v in ensure_collection(vs)
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
            the transformed tree.
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
            expression (Expression|None): new node

        Returns :
            the new expression or expressions
        """
        if not self.parent:
            return expression

        parent = self.parent
        self.parent = None

        replace_children(parent, lambda child: expression if child is self else child)
        return expression

    def pop(self):
        """
        Remove this expression from its AST.
        """
        self.replace(None)

    def assert_is(self, type_):
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


class Condition(Expression):
    def and_(self, *expressions, dialect=None, **opts):
        """
        AND this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").and_("y=1").sql()
            'x = 1 AND y = 1'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect (str): the dialect used to parse the input expression.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            And: the new condition.
        """
        return and_(self, *expressions, dialect=dialect, **opts)

    def or_(self, *expressions, dialect=None, **opts):
        """
        OR this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").or_("y=1").sql()
            'x = 1 OR y = 1'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect (str): the dialect used to parse the input expression.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Or: the new condition.
        """
        return or_(self, *expressions, dialect=dialect, **opts)

    def not_(self):
        """
        Wrap this condition with NOT.

        Example:
            >>> condition("x=1").not_().sql()
            'NOT x = 1'

        Returns:
            Not: the new condition.
        """
        return not_(self)


class Predicate(Condition):
    """Relationships like x = y, x > 1, x >= y."""


class DerivedTable(Expression):
    @property
    def alias_column_names(self):
        table_alias = self.args.get("alias")
        if not table_alias:
            return []
        column_list = table_alias.assert_is(TableAlias).args.get("columns") or []
        return [c.name for c in column_list]

    @property
    def selects(self):
        alias = self.args.get("alias")

        if alias:
            return alias.columns
        return []

    @property
    def named_selects(self):
        return [select.alias_or_name for select in self.selects]


class Unionable(Expression):
    def union(self, expression, distinct=True, dialect=None, **opts):
        """
        Builds a UNION expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").union("SELECT * FROM bla").sql()
            'SELECT * FROM foo UNION SELECT * FROM bla'

        Args:
            expression (str | Expression): the SQL code string.
                If an `Expression` instance is passed, it will be used as-is.
            distinct (bool): set the DISTINCT flag if and only if this is true.
            dialect (str): the dialect used to parse the input expression.
            opts (kwargs): other options to use to parse the input expressions.
        Returns:
            Union: the Union expression.
        """
        return union(left=self, right=expression, distinct=distinct, dialect=dialect, **opts)

    def intersect(self, expression, distinct=True, dialect=None, **opts):
        """
        Builds an INTERSECT expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").intersect("SELECT * FROM bla").sql()
            'SELECT * FROM foo INTERSECT SELECT * FROM bla'

        Args:
            expression (str | Expression): the SQL code string.
                If an `Expression` instance is passed, it will be used as-is.
            distinct (bool): set the DISTINCT flag if and only if this is true.
            dialect (str): the dialect used to parse the input expression.
            opts (kwargs): other options to use to parse the input expressions.
        Returns:
            Intersect: the Intersect expression
        """
        return intersect(left=self, right=expression, distinct=distinct, dialect=dialect, **opts)

    def except_(self, expression, distinct=True, dialect=None, **opts):
        """
        Builds an EXCEPT expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").except_("SELECT * FROM bla").sql()
            'SELECT * FROM foo EXCEPT SELECT * FROM bla'

        Args:
            expression (str | Expression): the SQL code string.
                If an `Expression` instance is passed, it will be used as-is.
            distinct (bool): set the DISTINCT flag if and only if this is true.
            dialect (str): the dialect used to parse the input expression.
            opts (kwargs): other options to use to parse the input expressions.
        Returns:
            Except: the Except expression
        """
        return except_(left=self, right=expression, distinct=distinct, dialect=dialect, **opts)


class UDTF(DerivedTable, Unionable):
    pass


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
        "temporary": False,
        "transient": False,
        "external": False,
        "replace": False,
        "unique": False,
        "materialized": False,
        "data": False,
        "statistics": False,
        "no_primary_index": False,
        "indexes": False,
        "no_schema_binding": False,
    }


class Describe(Expression):
    arg_types = {"this": True, "kind": False}


class Set(Expression):
    arg_types = {"expressions": True}


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
    arg_types = {"this": True, "expressions": False}


class UserDefinedFunctionKwarg(Expression):
    arg_types = {"this": True, "kind": True, "default": False}


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


class Column(Condition):
    arg_types = {"this": True, "table": False}

    @property
    def table(self):
        return self.text("table")


class ColumnDef(Expression):
    arg_types = {
        "this": True,
        "kind": False,
        "constraints": False,
        "exists": False,
    }


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


class ColumnConstraint(Expression):
    arg_types = {"this": False, "kind": True}


class ColumnConstraintKind(Expression):
    pass


class AutoIncrementColumnConstraint(ColumnConstraintKind):
    pass


class CheckColumnConstraint(ColumnConstraintKind):
    pass


class CollateColumnConstraint(ColumnConstraintKind):
    pass


class CommentColumnConstraint(ColumnConstraintKind):
    pass


class DefaultColumnConstraint(ColumnConstraintKind):
    pass


class EncodeColumnConstraint(ColumnConstraintKind):
    pass


class GeneratedAsIdentityColumnConstraint(ColumnConstraintKind):
    # this: True -> ALWAYS, this: False -> BY DEFAULT
    arg_types = {"this": True, "start": False, "increment": False}


class NotNullColumnConstraint(ColumnConstraintKind):
    arg_types = {"allow_null": False}


class PrimaryKeyColumnConstraint(ColumnConstraintKind):
    arg_types = {"desc": False}


class UniqueColumnConstraint(ColumnConstraintKind):
    pass


class Constraint(Expression):
    arg_types = {"this": True, "expressions": True}


class Delete(Expression):
    arg_types = {"with": False, "this": False, "using": False, "where": False}


class Drop(Expression):
    arg_types = {
        "this": False,
        "kind": False,
        "exists": False,
        "temporary": False,
        "materialized": False,
        "cascade": False,
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


class Unique(Expression):
    arg_types = {"expressions": True}


# https://www.postgresql.org/docs/9.1/sql-selectinto.html
# https://docs.aws.amazon.com/redshift/latest/dg/r_SELECT_INTO.html#r_SELECT_INTO-examples
class Into(Expression):
    arg_types = {"this": True, "temporary": False, "unlogged": False}


class From(Expression):
    arg_types = {"expressions": True}


class Having(Expression):
    pass


class Hint(Expression):
    arg_types = {"expressions": True}


class JoinHint(Expression):
    arg_types = {"this": True, "expressions": True}


class Identifier(Expression):
    arg_types = {"this": True, "quoted": False}

    @property
    def quoted(self):
        return bool(self.args.get("quoted"))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and _norm_arg(self.this) == _norm_arg(other.this)

    def __hash__(self):
        return hash((self.key, self.this.lower()))


class Index(Expression):
    arg_types = {
        "this": False,
        "table": False,
        "where": False,
        "columns": False,
        "unique": False,
        "primary": False,
        "amp": False,  # teradata
    }


class Insert(Expression):
    arg_types = {
        "with": False,
        "this": True,
        "expression": False,
        "overwrite": False,
        "exists": False,
        "partition": False,
    }


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
    pass


class Fetch(Expression):
    arg_types = {"direction": False, "count": False}


class Group(Expression):
    arg_types = {
        "expressions": False,
        "grouping_sets": False,
        "cube": False,
        "rollup": False,
    }


class Lambda(Expression):
    arg_types = {"this": True, "expressions": True}


class Limit(Expression):
    arg_types = {"this": False, "expression": True}


class Literal(Condition):
    arg_types = {"this": True, "is_string": True}

    def __eq__(self, other):
        return (
            isinstance(other, Literal)
            and self.this == other.this
            and self.args["is_string"] == other.args["is_string"]
        )

    def __hash__(self):
        return hash((self.key, self.this, self.args["is_string"]))

    @classmethod
    def number(cls, number) -> Literal:
        return cls(this=str(number), is_string=False)

    @classmethod
    def string(cls, string) -> Literal:
        return cls(this=str(string), is_string=True)


class Join(Expression):
    arg_types = {
        "this": True,
        "on": False,
        "side": False,
        "kind": False,
        "using": False,
        "natural": False,
    }

    @property
    def kind(self):
        return self.text("kind").upper()

    @property
    def side(self):
        return self.text("side").upper()

    @property
    def alias_or_name(self):
        return self.this.alias_or_name

    def on(self, *expressions, append=True, dialect=None, copy=True, **opts):
        """
        Append to or set the ON expressions.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("JOIN x", into=Join).on("y = 1").sql()
            'JOIN x ON y = 1'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append (bool): if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect (str): the dialect used to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Join: the modified join expression.
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

    def using(self, *expressions, append=True, dialect=None, copy=True, **opts):
        """
        Append to or set the USING expressions.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("JOIN x", into=Join).using("foo", "bla").sql()
            'JOIN x USING (foo, bla)'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append (bool): if `True`, concatenate the new expressions to the existing "using" list.
                Otherwise, this resets the expression.
            dialect (str): the dialect used to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Join: the modified join expression.
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


class TableFormatProperty(Property):
    arg_types = {"this": True}


class PartitionedByProperty(Property):
    arg_types = {"this": True}


class FileFormatProperty(Property):
    arg_types = {"this": True}


class DistKeyProperty(Property):
    arg_types = {"this": True}


class SortKeyProperty(Property):
    arg_types = {"this": True, "compound": False}


class DistStyleProperty(Property):
    arg_types = {"this": True}


class LikeProperty(Property):
    arg_types = {"this": True, "expressions": False}


class LocationProperty(Property):
    arg_types = {"this": True}


class EngineProperty(Property):
    arg_types = {"this": True}


class AutoIncrementProperty(Property):
    arg_types = {"this": True}


class CharacterSetProperty(Property):
    arg_types = {"this": True, "default": True}


class CollateProperty(Property):
    arg_types = {"this": True}


class SchemaCommentProperty(Property):
    arg_types = {"this": True}


class ReturnsProperty(Property):
    arg_types = {"this": True, "is_table": False}


class LanguageProperty(Property):
    arg_types = {"this": True}


class ExecuteAsProperty(Property):
    arg_types = {"this": True}


class VolatilityProperty(Property):
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
    arg_types = {"this": True}


class SerdeProperties(Property):
    arg_types = {"expressions": True}


class Properties(Expression):
    arg_types = {"expressions": True}

    NAME_TO_PROPERTY = {
        "AUTO_INCREMENT": AutoIncrementProperty,
        "CHARACTER SET": CharacterSetProperty,
        "COLLATE": CollateProperty,
        "COMMENT": SchemaCommentProperty,
        "DISTKEY": DistKeyProperty,
        "DISTSTYLE": DistStyleProperty,
        "ENGINE": EngineProperty,
        "EXECUTE AS": ExecuteAsProperty,
        "FORMAT": FileFormatProperty,
        "LANGUAGE": LanguageProperty,
        "LOCATION": LocationProperty,
        "PARTITIONED_BY": PartitionedByProperty,
        "RETURNS": ReturnsProperty,
        "SORTKEY": SortKeyProperty,
        "TABLE_FORMAT": TableFormatProperty,
    }

    PROPERTY_TO_NAME = {v: k for k, v in NAME_TO_PROPERTY.items()}

    @classmethod
    def from_dict(cls, properties_dict) -> Properties:
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


class Reference(Expression):
    arg_types = {"this": True, "expressions": True}


class Tuple(Expression):
    arg_types = {"expressions": False}


class Subqueryable(Unionable):
    def subquery(self, alias=None, copy=True) -> Subquery:
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
        instance = _maybe_copy(self, copy)
        return Subquery(
            this=instance,
            alias=TableAlias(this=to_identifier(alias)),
        )

    def limit(self, expression, dialect=None, copy=True, **opts) -> Select:
        raise NotImplementedError

    @property
    def ctes(self):
        with_ = self.args.get("with")
        if not with_:
            return []
        return with_.expressions

    @property
    def selects(self):
        raise NotImplementedError("Subqueryable objects must implement `selects`")

    @property
    def named_selects(self):
        raise NotImplementedError("Subqueryable objects must implement `named_selects`")

    def with_(
        self,
        alias,
        as_,
        recursive=None,
        append=True,
        dialect=None,
        copy=True,
        **opts,
    ):
        """
        Append to or set the common table expressions.

        Example:
            >>> Select().with_("tbl2", as_="SELECT * FROM tbl").select("x").from_("tbl2").sql()
            'WITH tbl2 AS (SELECT * FROM tbl) SELECT x FROM tbl2'

        Args:
            alias (str | Expression): the SQL code string to parse as the table name.
                If an `Expression` instance is passed, this is used as-is.
            as_ (str | Expression): the SQL code string to parse as the table expression.
                If an `Expression` instance is passed, it will be used as-is.
            recursive (bool): set the RECURSIVE part of the expression. Defaults to `False`.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect (str): the dialect used to parse the input expression.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
        """
        alias_expression = maybe_parse(
            alias,
            dialect=dialect,
            into=TableAlias,
            **opts,
        )
        as_expression = maybe_parse(
            as_,
            dialect=dialect,
            **opts,
        )
        cte = CTE(
            this=as_expression,
            alias=alias_expression,
        )
        return _apply_child_list_builder(
            cte,
            instance=self,
            arg="with",
            append=append,
            copy=copy,
            into=With,
            properties={"recursive": recursive or False},
        )


QUERY_MODIFIERS = {
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
}


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
    }


class Union(Subqueryable):
    arg_types = {
        "with": False,
        "this": True,
        "expression": True,
        "distinct": False,
        **QUERY_MODIFIERS,
    }

    def limit(self, expression, dialect=None, copy=True, **opts) -> Select:
        """
        Set the LIMIT expression.

        Example:
            >>> select("1").union(select("1")).limit(1).sql()
            'SELECT * FROM (SELECT 1 UNION SELECT 1) AS _l_0 LIMIT 1'

        Args:
            expression (str | int | Expression): the SQL code string to parse.
                This can also be an integer.
                If a `Limit` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Limit`.
            dialect (str): the dialect used to parse the input expression.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: The limited subqueryable.
        """
        return (
            select("*")
            .from_(self.subquery(alias="_l_0", copy=copy))
            .limit(expression, dialect=dialect, copy=False, **opts)
        )

    @property
    def named_selects(self):
        return self.this.unnest().named_selects

    @property
    def selects(self):
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


class Select(Subqueryable):
    arg_types = {
        "with": False,
        "expressions": False,
        "hint": False,
        "distinct": False,
        "into": False,
        "from": False,
        **QUERY_MODIFIERS,
    }

    def from_(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
        """
        Set the FROM expression.

        Example:
            >>> Select().from_("tbl").select("x").sql()
            'SELECT x FROM tbl'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If a `From` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `From`.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this flattens all the `From` expression into a single expression.
            dialect (str): the dialect used to parse the input expression.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
        """
        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="from",
            append=append,
            copy=copy,
            prefix="FROM",
            into=From,
            dialect=dialect,
            **opts,
        )

    def group_by(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
        """
        Set the GROUP BY expression.

        Example:
            >>> Select().from_("tbl").select("x", "COUNT(1)").group_by("x").sql()
            'SELECT x, COUNT(1) FROM tbl GROUP BY x'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Group`.
                If nothing is passed in then a group by is not applied to the expression
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Group` expression into a single expression.
            dialect (str): the dialect used to parse the input expression.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
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

    def order_by(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
        """
        Set the ORDER BY expression.

        Example:
            >>> Select().from_("tbl").select("x").order_by("x DESC").sql()
            'SELECT x FROM tbl ORDER BY x DESC'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Order`.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect (str): the dialect used to parse the input expression.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
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

    def sort_by(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
        """
        Set the SORT BY expression.

        Example:
            >>> Select().from_("tbl").select("x").sort_by("x DESC").sql()
            'SELECT x FROM tbl SORT BY x DESC'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `SORT`.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect (str): the dialect used to parse the input expression.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
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

    def cluster_by(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
        """
        Set the CLUSTER BY expression.

        Example:
            >>> Select().from_("tbl").select("x").cluster_by("x DESC").sql()
            'SELECT x FROM tbl CLUSTER BY x DESC'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Cluster`.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect (str): the dialect used to parse the input expression.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
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

    def limit(self, expression, dialect=None, copy=True, **opts) -> Select:
        """
        Set the LIMIT expression.

        Example:
            >>> Select().from_("tbl").select("x").limit(10).sql()
            'SELECT x FROM tbl LIMIT 10'

        Args:
            expression (str | int | Expression): the SQL code string to parse.
                This can also be an integer.
                If a `Limit` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Limit`.
            dialect (str): the dialect used to parse the input expression.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

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

    def offset(self, expression, dialect=None, copy=True, **opts) -> Select:
        """
        Set the OFFSET expression.

        Example:
            >>> Select().from_("tbl").select("x").offset(10).sql()
            'SELECT x FROM tbl OFFSET 10'

        Args:
            expression (str | int | Expression): the SQL code string to parse.
                This can also be an integer.
                If a `Offset` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Offset`.
            dialect (str): the dialect used to parse the input expression.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
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

    def select(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
        """
        Append to or set the SELECT expressions.

        Example:
            >>> Select().select("x", "y").sql()
            'SELECT x, y'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect (str): the dialect used to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
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

    def lateral(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
        """
        Append to or set the LATERAL expressions.

        Example:
            >>> Select().select("x").lateral("OUTER explode(y) tbl2 AS z").from_("tbl").sql()
            'SELECT x FROM tbl LATERAL VIEW OUTER EXPLODE(y) tbl2 AS z'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect (str): the dialect used to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
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
        expression,
        on=None,
        using=None,
        append=True,
        join_type=None,
        join_alias=None,
        dialect=None,
        copy=True,
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
            expression (str | Expression): the SQL code string to parse.
                If an `Expression` instance is passed, it will be used as-is.
            on (str | Expression): optionally specify the join "on" criteria as a SQL string.
                If an `Expression` instance is passed, it will be used as-is.
            using (str | Expression): optionally specify the join "using" criteria as a SQL string.
                If an `Expression` instance is passed, it will be used as-is.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            join_type (str): If set, alter the parsed join type
            dialect (str): the dialect used to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
        """
        parse_args = {"dialect": dialect, **opts}

        try:
            expression = maybe_parse(expression, into=Join, prefix="JOIN", **parse_args)
        except ParseError:
            expression = maybe_parse(expression, into=(Join, Expression), **parse_args)

        join = expression if isinstance(expression, Join) else Join(this=expression)

        if isinstance(join.this, Select):
            join.this.replace(join.this.subquery())

        if join_type:
            natural: t.Optional[Token]
            side: t.Optional[Token]
            kind: t.Optional[Token]

            natural, side, kind = maybe_parse(join_type, into="JOIN_TYPE", **parse_args)  # type: ignore

            if natural:
                join.set("natural", True)
            if side:
                join.set("side", side.text)
            if kind:
                join.set("kind", kind.text)

        if on:
            on = and_(*ensure_collection(on), dialect=dialect, **opts)
            join.set("on", on)

        if using:
            join = _apply_list_builder(
                *ensure_collection(using),
                instance=join,
                arg="using",
                append=append,
                copy=copy,
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

    def where(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
        """
        Append to or set the WHERE expressions.

        Example:
            >>> Select().select("x").from_("tbl").where("x = 'a' OR x < 'b'").sql()
            "SELECT x FROM tbl WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append (bool): if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect (str): the dialect used to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

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

    def having(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
        """
        Append to or set the HAVING expressions.

        Example:
            >>> Select().select("x", "COUNT(y)").from_("tbl").group_by("x").having("COUNT(y) > 3").sql()
            'SELECT x, COUNT(y) FROM tbl GROUP BY x HAVING COUNT(y) > 3'

        Args:
            *expressions (str | Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append (bool): if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect (str): the dialect used to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
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

    def window(self, *expressions, append=True, dialect=None, copy=True, **opts) -> Select:
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

    def distinct(self, distinct=True, copy=True) -> Select:
        """
        Set the OFFSET expression.

        Example:
            >>> Select().from_("tbl").select("x").distinct().sql()
            'SELECT DISTINCT x FROM tbl'

        Args:
            distinct (bool): whether the Select should be distinct
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Select: the modified expression.
        """
        instance = _maybe_copy(self, copy)
        instance.set("distinct", Distinct() if distinct else None)
        return instance

    def ctas(self, table, properties=None, dialect=None, copy=True, **opts) -> Create:
        """
        Convert this expression to a CREATE TABLE AS statement.

        Example:
            >>> Select().select("*").from_("tbl").ctas("x").sql()
            'CREATE TABLE x AS SELECT * FROM tbl'

        Args:
            table (str | Expression): the SQL code string to parse as the table name.
                If another `Expression` instance is passed, it will be used as-is.
            properties (dict): an optional mapping of table properties
            dialect (str): the dialect used to parse the input table.
            copy (bool): if `False`, modify this expression instance in-place.
            opts (kwargs): other options to use to parse the input table.

        Returns:
            Create: the CREATE TABLE AS expression
        """
        instance = _maybe_copy(self, copy)
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

    @property
    def named_selects(self) -> t.List[str]:
        return [e.alias_or_name for e in self.expressions if e.alias_or_name]

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
    }


class Pivot(Expression):
    arg_types = {
        "this": False,
        "expressions": True,
        "field": True,
        "unpivot": True,
    }


class Window(Expression):
    arg_types = {
        "this": True,
        "partition_by": False,
        "order": False,
        "spec": False,
        "alias": False,
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
    def name(self):
        return "*"


class Parameter(Expression):
    pass


class SessionParameter(Expression):
    arg_types = {"this": True, "kind": False}


class Placeholder(Expression):
    arg_types = {"this": False}


class Null(Condition):
    arg_types: t.Dict[str, t.Any] = {}


class Boolean(Condition):
    pass


class DataType(Expression):
    arg_types = {
        "this": True,
        "expressions": False,
        "nested": False,
        "values": False,
    }

    class Type(AutoName):
        CHAR = auto()
        NCHAR = auto()
        VARCHAR = auto()
        NVARCHAR = auto()
        TEXT = auto()
        MEDIUMTEXT = auto()
        LONGTEXT = auto()
        BINARY = auto()
        VARBINARY = auto()
        INT = auto()
        TINYINT = auto()
        SMALLINT = auto()
        BIGINT = auto()
        FLOAT = auto()
        DOUBLE = auto()
        DECIMAL = auto()
        BOOLEAN = auto()
        JSON = auto()
        JSONB = auto()
        INTERVAL = auto()
        TIME = auto()
        TIMESTAMP = auto()
        TIMESTAMPTZ = auto()
        TIMESTAMPLTZ = auto()
        DATE = auto()
        DATETIME = auto()
        ARRAY = auto()
        MAP = auto()
        UUID = auto()
        GEOGRAPHY = auto()
        GEOMETRY = auto()
        STRUCT = auto()
        NULLABLE = auto()
        HLLSKETCH = auto()
        HSTORE = auto()
        SUPER = auto()
        SERIAL = auto()
        SMALLSERIAL = auto()
        BIGSERIAL = auto()
        XML = auto()
        UNIQUEIDENTIFIER = auto()
        MONEY = auto()
        SMALLMONEY = auto()
        ROWVERSION = auto()
        IMAGE = auto()
        VARIANT = auto()
        OBJECT = auto()
        NULL = auto()
        UNKNOWN = auto()  # Sentinel value, useful for type annotation

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
    }

    FLOAT_TYPES = {
        Type.FLOAT,
        Type.DOUBLE,
    }

    NUMERIC_TYPES = {*INTEGER_TYPES, *FLOAT_TYPES}

    TEMPORAL_TYPES = {
        Type.TIMESTAMP,
        Type.TIMESTAMPTZ,
        Type.TIMESTAMPLTZ,
        Type.DATE,
        Type.DATETIME,
    }

    @classmethod
    def build(
        cls, dtype: str | DataType.Type, dialect: t.Optional[str | Dialect] = None, **kwargs
    ) -> DataType:
        from sqlglot import parse_one

        if isinstance(dtype, str):
            data_type_exp: t.Optional[Expression]
            if dtype.upper() in cls.Type.__members__:
                data_type_exp = DataType(this=DataType.Type[dtype.upper()])
            else:
                data_type_exp = parse_one(dtype, read=dialect, into=DataType)
            if data_type_exp is None:
                raise ValueError(f"Unparsable data type value: {dtype}")
        elif isinstance(dtype, DataType.Type):
            data_type_exp = DataType(this=dtype)
        else:
            raise ValueError(f"Invalid data type: {type(dtype)}. Expected str or DataType.Type")
        return DataType(**{**data_type_exp.args, **kwargs})


# https://www.postgresql.org/docs/15/datatype-pseudo.html
class PseudoType(Expression):
    pass


class StructKwarg(Expression):
    arg_types = {"this": True, "expression": True}


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
    arg_types = {"this": False, "modes": False}


class Commit(Expression):
    arg_types = {"chain": False}


class Rollback(Expression):
    arg_types = {"savepoint": False}


class AlterTable(Expression):
    arg_types = {
        "this": True,
        "actions": True,
        "exists": False,
    }


# Binary expressions like (ADD a b)
class Binary(Expression):
    arg_types = {"this": True, "expression": True}

    @property
    def left(self):
        return self.this

    @property
    def right(self):
        return self.expression


class Add(Binary):
    pass


class Connector(Binary, Condition):
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


class Dot(Binary):
    pass


class DPipe(Binary):
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


# Unary Expressions
# (NOT a)
class Unary(Expression):
    pass


class BitwiseNot(Unary):
    pass


class Not(Unary, Condition):
    pass


class Paren(Unary, Condition):
    arg_types = {"this": True, "with": False}


class Neg(Unary):
    pass


# Special Functions
class Alias(Expression):
    arg_types = {"this": True, "alias": False}


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
        if isinstance(unit, Column):
            args["unit"] = Var(this=unit.name)
        elif isinstance(unit, Week):
            unit.set("this", Var(this=unit.this.name))
        super().__init__(**args)


class Interval(TimeUnit):
    arg_types = {"this": False, "unit": False}


class IgnoreNulls(Expression):
    pass


class RespectNulls(Expression):
    pass


# Functions
class Func(Condition):
    """
    The base class for all function expressions.

    Attributes
        is_var_len_args (bool): if set to True the last argument defined in
            arg_types will be treated as a variable length argument and the
            argument's value will be stored as a list.
        _sql_names (list): determines the SQL name (1st item in the list) and
            aliases (subsequent items) for this function expression. These
            values are used to map this node to a name during parsing as well
            as to provide the function's name during SQL string generation. By
            default the SQL name is set to the expression's class name transformed
            to snake case.
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
        if not hasattr(cls, "_sql_names"):
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


class Abs(Func):
    pass


class Anonymous(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ApproxDistinct(AggFunc):
    arg_types = {"this": True, "accuracy": False}


class Array(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ArrayAgg(AggFunc):
    pass


class ArrayAll(Func):
    arg_types = {"this": True, "expression": True}


class ArrayAny(Func):
    arg_types = {"this": True, "expression": True}


class ArrayConcat(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ArrayContains(Func):
    arg_types = {"this": True, "expression": True}


class ArrayFilter(Func):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["FILTER", "ARRAY_FILTER"]


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
    pass


class Case(Func):
    arg_types = {"this": False, "ifs": True, "default": False}


class Cast(Func):
    arg_types = {"this": True, "to": True}

    @property
    def name(self):
        return self.this.name

    @property
    def to(self):
        return self.args["to"]


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


class Concat(Func):
    arg_types = {"expressions": True}
    is_var_len_args = True


class ConcatWs(Concat):
    _sql_names = ["CONCAT_WS"]


class Count(AggFunc):
    arg_types = {"this": False}


class CurrentDate(Func):
    arg_types = {"this": False}


class CurrentDatetime(Func):
    arg_types = {"this": False}


class CurrentTime(Func):
    arg_types = {"this": False}


class CurrentTimestamp(Func):
    arg_types = {"this": False}


class DateAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateTrunc(Func):
    arg_types = {"this": True, "expression": True, "zone": False}


class DatetimeAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


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


class Greatest(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class GroupConcat(Func):
    arg_types = {"this": True, "separator": False}


class Hex(Func):
    pass


class If(Func):
    arg_types = {"this": True, "true": True, "false": False}


class IfNull(Func):
    arg_types = {"this": True, "expression": False}
    _sql_names = ["IFNULL", "NVL"]


class Initcap(Func):
    pass


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


class Least(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Length(Func):
    pass


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
    _sql_names = ["LOGICAL_OR", "BOOL_OR"]


class Lower(Func):
    _sql_names = ["LOWER", "LCASE"]


class Map(Func):
    arg_types = {"keys": False, "values": False}


class VarMap(Func):
    arg_types = {"keys": True, "values": True}
    is_var_len_args = True


class Matches(Func):
    """Oracle/Snowflake decode.
    https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions040.htm
    Pattern matching MATCHES(value, search1, result1, ...searchN, resultN, else)
    """

    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class Max(AggFunc):
    arg_types = {"this": True, "expression": False}


class Min(AggFunc):
    arg_types = {"this": True, "expression": False}


class Month(Func):
    pass


class Nvl2(Func):
    arg_types = {"this": True, "true": True, "false": False}


class Posexplode(Func):
    pass


class Pow(Func):
    arg_types = {"this": True, "power": True}
    _sql_names = ["POWER", "POW"]


class Quantile(AggFunc):
    arg_types = {"this": True, "quantile": True}


# Clickhouse-specific:
# https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/quantiles/#quantiles
class Quantiles(AggFunc):
    arg_types = {"parameters": True, "expressions": True}


class QuantileIf(AggFunc):
    arg_types = {"parameters": True, "expressions": True}


class ApproxQuantile(Quantile):
    arg_types = {"this": True, "quantile": True, "accuracy": False, "weight": False}


class ReadCSV(Func):
    _sql_names = ["READ_CSV"]
    is_var_len_args = True
    arg_types = {"this": True, "expressions": False}


class Reduce(Func):
    arg_types = {"this": True, "initial": True, "merge": True, "finish": True}


class RegexpLike(Func):
    arg_types = {"this": True, "expression": True, "flag": False}


class RegexpILike(Func):
    arg_types = {"this": True, "expression": True, "flag": False}


class RegexpSplit(Func):
    arg_types = {"this": True, "expression": True}


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


class SortArray(Func):
    arg_types = {"this": True, "asc": False}


class Split(Func):
    arg_types = {"this": True, "expression": True, "limit": False}


# Start may be omitted in the case of postgres
# https://www.postgresql.org/docs/9.1/functions-string.html @ Table 9-6
class Substring(Func):
    arg_types = {"this": True, "start": False, "length": False}


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
    arg_types = {"this": True, "format": True}


# Spark allows unix_timestamp()
# https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.unix_timestamp.html
class StrToUnix(Func):
    arg_types = {"this": False, "format": False}


class NumberToStr(Func):
    arg_types = {"this": True, "format": True}


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


class Year(Func):
    pass


class Use(Expression):
    pass


class Merge(Expression):
    arg_types = {"this": True, "using": True, "on": True, "expressions": True}


class When(Func):
    arg_types = {"this": True, "then": True}


def _norm_args(expression):
    args = {}

    for k, arg in expression.args.items():
        if isinstance(arg, list):
            arg = [_norm_arg(a) for a in arg]
            if not arg:
                arg = None
        else:
            arg = _norm_arg(arg)

        if arg is not None and arg is not False:
            args[k] = arg

    return args


def _norm_arg(arg):
    return arg.lower() if isinstance(arg, str) else arg


ALL_FUNCTIONS = subclasses(__name__, Func, (AggFunc, Anonymous, Func))


def maybe_parse(
    sql_or_expression,
    *,
    into=None,
    dialect=None,
    prefix=None,
    **opts,
) -> Expression:
    """Gracefully handle a possible string or expression.

    Example:
        >>> maybe_parse("1")
        (LITERAL this: 1, is_string: False)
        >>> maybe_parse(to_identifier("x"))
        (IDENTIFIER this: x, quoted: False)

    Args:
        sql_or_expression (str | Expression): the SQL code string or an expression
        into (Expression): the SQLGlot Expression to parse into
        dialect (str): the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        prefix (str): a string to prefix the sql with before it gets parsed
            (automatically includes a space)
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Expression: the parsed or given expression.
    """
    if isinstance(sql_or_expression, Expression):
        return sql_or_expression

    import sqlglot

    sql = str(sql_or_expression)
    if prefix:
        sql = f"{prefix} {sql}"
    return sqlglot.parse_one(sql, read=dialect, into=into, **opts)


def _maybe_copy(instance, copy=True):
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
    instance = _maybe_copy(instance, copy)
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
    instance = _maybe_copy(instance, copy)
    parsed = []
    for expression in expressions:
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
    inst = _maybe_copy(instance, copy)

    expressions = [
        maybe_parse(
            sql_or_expression=expression,
            into=into,
            prefix=prefix,
            dialect=dialect,
            **opts,
        )
        for expression in expressions
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

    inst = _maybe_copy(instance, copy)

    existing = inst.args.get(arg)
    if append and existing is not None:
        expressions = [existing.this if into else existing] + list(expressions)

    node = and_(*expressions, dialect=dialect, **opts)

    inst.set(arg, into(this=node) if into else node)
    return inst


def _combine(expressions, operator, dialect=None, **opts):
    expressions = [condition(expression, dialect=dialect, **opts) for expression in expressions]
    this = expressions[0]
    if expressions[1:]:
        this = _wrap_operator(this)
    for expression in expressions[1:]:
        this = operator(this=this, expression=_wrap_operator(expression))
    return this


def _wrap_operator(expression):
    if isinstance(expression, (And, Or, Not)):
        expression = Paren(this=expression)
    return expression


def union(left, right, distinct=True, dialect=None, **opts):
    """
    Initializes a syntax tree from one UNION expression.

    Example:
        >>> union("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo UNION SELECT * FROM bla'

    Args:
        left (str | Expression): the SQL code string corresponding to the left-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        right (str | Expression): the SQL code string corresponding to the right-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        distinct (bool): set the DISTINCT flag if and only if this is true.
        dialect (str): the dialect used to parse the input expression.
        opts (kwargs): other options to use to parse the input expressions.
    Returns:
        Union: the syntax tree for the UNION expression.
    """
    left = maybe_parse(sql_or_expression=left, dialect=dialect, **opts)
    right = maybe_parse(sql_or_expression=right, dialect=dialect, **opts)

    return Union(this=left, expression=right, distinct=distinct)


def intersect(left, right, distinct=True, dialect=None, **opts):
    """
    Initializes a syntax tree from one INTERSECT expression.

    Example:
        >>> intersect("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo INTERSECT SELECT * FROM bla'

    Args:
        left (str | Expression): the SQL code string corresponding to the left-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        right (str | Expression): the SQL code string corresponding to the right-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        distinct (bool): set the DISTINCT flag if and only if this is true.
        dialect (str): the dialect used to parse the input expression.
        opts (kwargs): other options to use to parse the input expressions.
    Returns:
        Intersect: the syntax tree for the INTERSECT expression.
    """
    left = maybe_parse(sql_or_expression=left, dialect=dialect, **opts)
    right = maybe_parse(sql_or_expression=right, dialect=dialect, **opts)

    return Intersect(this=left, expression=right, distinct=distinct)


def except_(left, right, distinct=True, dialect=None, **opts):
    """
    Initializes a syntax tree from one EXCEPT expression.

    Example:
        >>> except_("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo EXCEPT SELECT * FROM bla'

    Args:
        left (str | Expression): the SQL code string corresponding to the left-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        right (str | Expression): the SQL code string corresponding to the right-hand side.
            If an `Expression` instance is passed, it will be used as-is.
        distinct (bool): set the DISTINCT flag if and only if this is true.
        dialect (str): the dialect used to parse the input expression.
        opts (kwargs): other options to use to parse the input expressions.
    Returns:
        Except: the syntax tree for the EXCEPT statement.
    """
    left = maybe_parse(sql_or_expression=left, dialect=dialect, **opts)
    right = maybe_parse(sql_or_expression=right, dialect=dialect, **opts)

    return Except(this=left, expression=right, distinct=distinct)


def select(*expressions, dialect=None, **opts) -> Select:
    """
    Initializes a syntax tree from one or multiple SELECT expressions.

    Example:
        >>> select("col1", "col2").from_("tbl").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expressions (str | Expression): the SQL code string to parse as the expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().select(*expressions, dialect=dialect, **opts)


def from_(*expressions, dialect=None, **opts) -> Select:
    """
    Initializes a syntax tree from a FROM expression.

    Example:
        >>> from_("tbl").select("col1", "col2").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expressions (str | Expression): the SQL code string to parse as the FROM expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().from_(*expressions, dialect=dialect, **opts)


def update(table, properties, where=None, from_=None, dialect=None, **opts) -> Update:
    """
    Creates an update statement.

    Example:
        >>> update("my_table", {"x": 1, "y": "2", "z": None}, from_="baz", where="id > 1").sql()
        "UPDATE my_table SET x = 1, y = '2', z = NULL FROM baz WHERE id > 1"

    Args:
        *properties (Dict[str, Any]): dictionary of properties to set which are
            auto converted to sql objects eg None -> NULL
        where (str): sql conditional parsed into a WHERE statement
        from_ (str): sql statement parsed into a FROM statement
        dialect (str): the dialect used to parse the input expressions.
        **opts: other options to use to parse the input expressions.

    Returns:
        Update: the syntax tree for the UPDATE statement.
    """
    update = Update(this=maybe_parse(table, into=Table, dialect=dialect))
    update.set(
        "expressions",
        [
            EQ(this=maybe_parse(k, dialect=dialect, **opts), expression=convert(v))
            for k, v in properties.items()
        ],
    )
    if from_:
        update.set(
            "from",
            maybe_parse(from_, into=From, dialect=dialect, prefix="FROM", **opts),
        )
    if isinstance(where, Condition):
        where = Where(this=where)
    if where:
        update.set(
            "where",
            maybe_parse(where, into=Where, dialect=dialect, prefix="WHERE", **opts),
        )
    return update


def delete(table, where=None, dialect=None, **opts) -> Delete:
    """
    Builds a delete statement.

    Example:
        >>> delete("my_table", where="id > 1").sql()
        'DELETE FROM my_table WHERE id > 1'

    Args:
        where (str|Condition): sql conditional parsed into a WHERE statement
        dialect (str): the dialect used to parse the input expressions.
        **opts: other options to use to parse the input expressions.

    Returns:
        Delete: the syntax tree for the DELETE statement.
    """
    return Delete(
        this=maybe_parse(table, into=Table, dialect=dialect, **opts),
        where=Where(this=where)
        if isinstance(where, Condition)
        else maybe_parse(where, into=Where, dialect=dialect, prefix="WHERE", **opts),
    )


def condition(expression, dialect=None, **opts) -> Condition:
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
        *expression (str | Expression): the SQL code string to parse.
            If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        Condition: the expression
    """
    return maybe_parse(  # type: ignore
        expression,
        into=Condition,
        dialect=dialect,
        **opts,
    )


def and_(*expressions, dialect=None, **opts) -> And:
    """
    Combine multiple conditions with an AND logical operator.

    Example:
        >>> and_("x=1", and_("y=1", "z=1")).sql()
        'x = 1 AND (y = 1 AND z = 1)'

    Args:
        *expressions (str | Expression): the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        And: the new condition
    """
    return _combine(expressions, And, dialect, **opts)


def or_(*expressions, dialect=None, **opts) -> Or:
    """
    Combine multiple conditions with an OR logical operator.

    Example:
        >>> or_("x=1", or_("y=1", "z=1")).sql()
        'x = 1 OR (y = 1 OR z = 1)'

    Args:
        *expressions (str | Expression): the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Or: the new condition
    """
    return _combine(expressions, Or, dialect, **opts)


def not_(expression, dialect=None, **opts) -> Not:
    """
    Wrap a condition with a NOT operator.

    Example:
        >>> not_("this_suit='black'").sql()
        "NOT this_suit = 'black'"

    Args:
        expression (str | Expression): the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Not: the new condition
    """
    this = condition(
        expression,
        dialect=dialect,
        **opts,
    )
    return Not(this=_wrap_operator(this))


def paren(expression) -> Paren:
    return Paren(this=expression)


SAFE_IDENTIFIER_RE = re.compile(r"^[_a-zA-Z][\w]*$")


def to_identifier(alias, quoted=None) -> t.Optional[Identifier]:
    if alias is None:
        return None
    if isinstance(alias, Identifier):
        identifier = alias
    elif isinstance(alias, str):
        if quoted is None:
            quoted = not re.match(SAFE_IDENTIFIER_RE, alias)
        identifier = Identifier(this=alias, quoted=quoted)
    else:
        raise ValueError(f"Alias needs to be a string or an Identifier, got: {alias.__class__}")
    return identifier


@t.overload
def to_table(sql_path: str | Table, **kwargs) -> Table:
    ...


@t.overload
def to_table(sql_path: None, **kwargs) -> None:
    ...


def to_table(sql_path: t.Optional[str | Table], **kwargs) -> t.Optional[Table]:
    """
    Create a table expression from a `[catalog].[schema].[table]` sql path. Catalog and schema are optional.
    If a table is passed in then that table is returned.

    Args:
        sql_path: a `[catalog].[schema].[table]` string.

    Returns:
        A table expression.
    """
    if sql_path is None or isinstance(sql_path, Table):
        return sql_path
    if not isinstance(sql_path, str):
        raise ValueError(f"Invalid type provided for a table: {type(sql_path)}")

    catalog, db, table_name = (to_identifier(x) for x in split_num_words(sql_path, ".", 3))
    return Table(this=table_name, db=db, catalog=catalog, **kwargs)


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
    table_name, column_name = (to_identifier(x) for x in split_num_words(sql_path, ".", 2))
    return Column(this=column_name, table=table_name, **kwargs)


def alias_(expression, alias, table=False, dialect=None, quoted=None, **opts):
    """
    Create an Alias expression.
    Example:
        >>> alias_('foo', 'bar').sql()
        'foo AS bar'

    Args:
        expression (str | Expression): the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        alias (str | Identifier): the alias name to use. If the name has
            special characters it is quoted.
        table (bool): create a table alias, default false
        dialect (str): the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Alias: the aliased expression
    """
    exp = maybe_parse(expression, dialect=dialect, **opts)
    alias = to_identifier(alias, quoted=quoted)

    if table:
        expression.set("alias", TableAlias(this=alias))
        return expression

    # We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
    # the AST, representing a "named_window" [1] construct (eg. bigquery). What we want is an ALIAS node
    # for the complete Window expression.
    #
    # [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls

    if "alias" in exp.arg_types and not isinstance(exp, Window):
        exp = exp.copy()
        exp.set("alias", alias)
        return exp
    return Alias(this=exp, alias=alias)


def subquery(expression, alias=None, dialect=None, **opts):
    """
    Build a subquery expression.
    Expample:
        >>> subquery('select x from tbl', 'bar').select('x').sql()
        'SELECT x FROM (SELECT x FROM tbl) AS bar'

    Args:
        expression (str | Expression): the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        alias (str | Expression): the alias name to use.
        dialect (str): the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Select: a new select with the subquery expression included
    """

    expression = maybe_parse(expression, dialect=dialect, **opts).subquery(alias)
    return Select().from_(expression, dialect=dialect, **opts)


def column(col, table=None, quoted=None) -> Column:
    """
    Build a Column.
    Args:
        col (str | Expression): column name
        table (str | Expression): table name
    Returns:
        Column: column instance
    """
    return Column(
        this=to_identifier(col, quoted=quoted),
        table=to_identifier(table, quoted=quoted),
    )


def table_(table, db=None, catalog=None, quoted=None, alias=None) -> Table:
    """Build a Table.

    Args:
        table (str | Expression): column name
        db (str | Expression): db name
        catalog (str | Expression): catalog name

    Returns:
        Table: table instance
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
         If a dictionary is provided then the first column of the values will be casted to the expected type
         in order to help with type inference.

    Returns:
        Values: the Values expression object
    """
    if columns and not alias:
        raise ValueError("Alias is required when providing columns")
    table_alias = (
        TableAlias(this=to_identifier(alias), columns=[to_identifier(x) for x in columns])
        if columns
        else TableAlias(this=to_identifier(alias) if alias else None)
    )
    expressions = [convert(tup) for tup in values]
    if columns and isinstance(columns, dict):
        types = list(columns.values())
        expressions[0].set(
            "expressions",
            [Cast(this=x, to=types[i]) for i, x in enumerate(expressions[0].expressions)],
        )
    return Values(
        expressions=expressions,
        alias=table_alias,
    )


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


def convert(value) -> Expression:
    """Convert a python value into an expression object.

    Raises an error if a conversion is not possible.

    Args:
        value (Any): a python object

    Returns:
        Expression: the equivalent expression object
    """
    if isinstance(value, Expression):
        return value
    if value is None:
        return NULL
    if isinstance(value, bool):
        return Boolean(this=value)
    if isinstance(value, str):
        return Literal.string(value)
    if isinstance(value, float) and math.isnan(value):
        return NULL
    if isinstance(value, numbers.Number):
        return Literal.number(value)
    if isinstance(value, tuple):
        return Tuple(expressions=[convert(v) for v in value])
    if isinstance(value, list):
        return Array(expressions=[convert(v) for v in value])
    if isinstance(value, dict):
        return Map(
            keys=[convert(k) for k in value],
            values=[convert(v) for v in value.values()],
        )
    if isinstance(value, datetime.datetime):
        datetime_literal = Literal.string(
            (value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)).isoformat()
        )
        return TimeStrToTime(this=datetime_literal)
    if isinstance(value, datetime.date):
        date_literal = Literal.string(value.strftime("%Y-%m-%d"))
        return DateStrToDate(this=date_literal)
    raise ValueError(f"Cannot convert {value}")


def replace_children(expression, fun):
    """
    Replace children of an expression with the result of a lambda fun(child) -> exp.
    """
    for k, v in expression.args.items():
        is_list_arg = isinstance(v, list)

        child_nodes = v if is_list_arg else [v]
        new_child_nodes = []

        for cn in child_nodes:
            if isinstance(cn, Expression):
                for child_node in ensure_collection(fun(cn)):
                    new_child_nodes.append(child_node)
                    child_node.parent = expression
                    child_node.arg_key = k
            else:
                new_child_nodes.append(cn)

        expression.args[k] = new_child_nodes if is_list_arg else seq_get(new_child_nodes, 0)


def column_table_names(expression):
    """
    Return all table names referenced through columns in an expression.

    Example:
        >>> import sqlglot
        >>> column_table_names(sqlglot.parse_one("a.b AND c.d AND c.e"))
        ['c', 'a']

    Args:
        expression (sqlglot.Expression): expression to find table names

    Returns:
        list: A list of unique names
    """
    return list(dict.fromkeys(column.table for column in expression.find_all(Column)))


def table_name(table) -> str:
    """Get the full name of a table as a string.

    Args:
        table (exp.Table | str): Table expression node or string.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> table_name(parse_one("select * from a.b.c").find(exp.Table))
        'a.b.c'

    Returns:
        str: the table name
    """

    table = maybe_parse(table, into=Table)

    if not table:
        raise ValueError(f"Cannot parse {table}")

    return ".".join(
        part
        for part in (
            table.text("catalog"),
            table.text("db"),
            table.name,
        )
        if part
    )


def replace_tables(expression, mapping):
    """Replace all tables in expression according to the mapping.

    Args:
        expression (sqlglot.Expression): Expression node to be transformed and replaced
        mapping (Dict[str, str]): Mapping of table names

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> replace_tables(parse_one("select * from a.b"), {"a.b": "c"}).sql()
        'SELECT * FROM c'

    Returns:
        The mapped expression
    """

    def _replace_tables(node):
        if isinstance(node, Table):
            new_name = mapping.get(table_name(node))
            if new_name:
                return to_table(
                    new_name,
                    **{k: v for k, v in node.args.items() if k not in ("this", "db", "catalog")},
                )
        return node

    return expression.transform(_replace_tables)


def replace_placeholders(expression, *args, **kwargs):
    """Replace placeholders in an expression.

    Args:
        expression (sqlglot.Expression): Expression node to be transformed and replaced
        args: Positional names that will substitute unnamed placeholders in the given order
        kwargs: Keyword arguments that will substitute named placeholders

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> replace_placeholders(
        ...     parse_one("select * from :tbl where ? = ?"), "a", "b", tbl="foo"
        ... ).sql()
        'SELECT * FROM foo WHERE a = b'

    Returns:
        The mapped expression
    """

    def _replace_placeholders(node, args, **kwargs):
        if isinstance(node, Placeholder):
            if node.name:
                new_name = kwargs.get(node.name)
                if new_name:
                    return to_identifier(new_name)
            else:
                try:
                    return to_identifier(next(args))
                except StopIteration:
                    pass
        return node

    return expression.transform(_replace_placeholders, iter(args), **kwargs)


def true():
    return Boolean(this=True)


def false():
    return Boolean(this=False)


def null():
    return Null()


# TODO: deprecate this
TRUE = Boolean(this=True)
FALSE = Boolean(this=False)
NULL = Null()
