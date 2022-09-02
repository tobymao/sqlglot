from copy import deepcopy
from collections import deque
from enum import auto
import re
import inspect
import sys

from sqlglot.errors import ParseError
from sqlglot.helper import AutoName, camel_to_snake_case, ensure_list


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

    key = None
    arg_types = {"this": True}
    __slots__ = ("args", "parent", "arg_key")

    def __init__(self, **args):
        self.args = args
        self.parent = None
        self.arg_key = None

        for arg_key, value in self.args.items():
            self._set_parent(arg_key, value)

    def __eq__(self, other):
        return type(self) is type(other) and _norm_args(self) == _norm_args(other)

    def __hash__(self):
        return hash(
            (
                self.key,
                tuple(
                    (k, tuple(v) if isinstance(v, list) else v)
                    for k, v in _norm_args(self).items()
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
        return self.alias or self.name

    def __deepcopy__(self, memo):
        return self.__class__(**deepcopy(self.args))

    def copy(self):
        new = deepcopy(self)
        for item, parent, _ in new.bfs():
            if isinstance(item, Expression) and parent:
                item.parent = parent
        return new

    def set(self, arg_key, value):
        """
        Sets `arg` to `value`.

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

    def walk(self, bfs=True):
        """
        Returns a generator object which visits all nodes in this tree.

        Args:
            bfs (bool): if set to True the BFS traversal order will be applied,
                otherwise the DFS traversal will be used instead.

        Returns:
            the generator object.
        """
        if bfs:
            yield from self.bfs()
        else:
            yield from self.dfs()

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
            nodes = ensure_list(v)

            for node in nodes:
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
                    nodes = ensure_list(v)

                    for node in nodes:
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
        for node, _, _ in self.dfs(
            prune=lambda n, p, *_: p and not isinstance(n, self.__class__)
        ):
            if not isinstance(node, self.__class__):
                yield node.unnest() if unnest else node

    def __str__(self):
        return self.sql()

    def __repr__(self):
        return self.to_s()

    def sql(self, dialect=None, **opts):
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

    def to_s(self, hide_missing=True, level=0):
        indent = "" if not level else "\n"
        indent += "".join(["  "] * level)
        left = f"({self.key.upper()} "

        args = {
            k: ", ".join(
                v.to_s(hide_missing=hide_missing, level=level + 1)
                if hasattr(v, "to_s")
                else str(v)
                for v in ensure_list(vs)
                if v is not None
            )
            for k, vs in self.args.items()
        }
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
                new transformed node or the same node without modifications.
            copy (bool): if set to True a new tree instance is constructed, otherwise the tree is
                modified in place.

        Returns:
            the transformed tree.
        """
        node = self.copy() if copy else self
        new_node = fun(node, *args, **kwargs)

        if new_node is None:
            raise ValueError("A transformed node cannot be None")
        if not isinstance(new_node, Expression):
            return new_node
        if new_node is not node:
            new_node.parent = node.parent
            return new_node

        replace_children(
            new_node, lambda child: child.transform(fun, *args, copy=False, **kwargs)
        )
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
            expression (Expression): new node

        Returns :
            the new expression or expressions
        """
        if not self.parent:
            return expression

        parent = self.parent
        self.parent = None

        replace_children(parent, lambda child: expression if child is self else child)
        return expression

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


class Condition(Expression):
    def and_(self, *expressions, dialect=None, parser_opts=None):
        """
        AND this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").and_("y=1").sql()
            'x = 1 AND y = 1'

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect (str): the dialect used to parse the input expression.
            parser_opts (dict): other options to use to parse the input expressions.

        Returns:
            And: the new condition.
        """
        return and_(self, *expressions, dialect=dialect, **(parser_opts or {}))

    def or_(self, *expressions, dialect=None, parser_opts=None):
        """
        OR this condition with one or multiple expressions.

        Example:
            >>> condition("x=1").or_("y=1").sql()
            'x = 1 OR y = 1'

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect (str): the dialect used to parse the input expression.
            parser_opts (dict): other options to use to parse the input expressions.

        Returns:
            Or: the new condition.
        """
        return or_(self, *expressions, dialect=dialect, **(parser_opts or {}))

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
        table_alias = self.args.get("alias")  # pylint: disable=no-member
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


class Annotation(Expression):
    arg_types = {
        "this": True,
        "expression": True,
    }


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
        "replace": False,
    }


class CharacterSet(Expression):
    arg_types = {"this": True, "default": False}


class With(Expression):
    arg_types = {"expressions": True, "recursive": False}


class WithinGroup(Expression):
    arg_types = {"this": True, "expression": False}


class CTE(DerivedTable):
    arg_types = {"this": True, "alias": True}


class TableAlias(Expression):
    arg_types = {"this": False, "columns": False}

    @property
    def columns(self):
        return self.args.get("columns") or []


class Column(Condition):
    arg_types = {"this": True, "table": False}

    @property
    def table(self):
        return self.text("table")


class ColumnDef(Expression):
    arg_types = {
        "this": True,
        "kind": True,
        "auto_increment": False,
        "comment": False,
        "collate": False,
        "default": False,
        "not_null": False,
        "primary": False,
        "unique": False,
    }


class Constraint(Expression):
    arg_types = {"this": True, "expressions": True}


class Delete(Expression):
    arg_types = {"with": False, "this": True, "where": False}


class Drop(Expression):
    arg_types = {"this": False, "kind": False, "exists": False}


class Filter(Expression):
    arg_types = {"this": True, "expression": True}


class ForeignKey(Expression):
    arg_types = {
        "expressions": True,
        "reference": False,
        "delete": False,
        "update": False,
    }


class From(Expression):
    arg_types = {"expressions": True}


class Having(Expression):
    pass


class Hint(Expression):
    arg_types = {"expressions": True}


class Identifier(Expression):
    arg_types = {"this": True, "quoted": False}

    def __eq__(self, other):
        return isinstance(other, self.__class__) and _norm_arg(self.this) == _norm_arg(
            other.this
        )

    def __hash__(self):
        return hash((self.key, self.this.lower()))


class Insert(Expression):
    arg_types = {
        "with": False,
        "this": True,
        "expression": True,
        "overwrite": False,
        "exists": False,
        "partition": False,
    }


class Partition(Expression):
    pass


class Fetch(Expression):
    arg_types = {"direction": False, "count": True}


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
    def number(cls, number):
        return cls(this=str(number), is_string=False)

    @classmethod
    def string(cls, string):
        return cls(this=str(string), is_string=True)


class Join(Expression):
    arg_types = {
        "this": True,
        "on": False,
        "side": False,
        "kind": False,
        "using": False,
    }

    @property
    def kind(self):
        return self.text("kind").upper()

    @property
    def side(self):
        return self.text("side").upper()

    def on(self, *expressions, append=True, dialect=None, parser_opts=None, copy=True):
        """
        Append to or set the ON expressions.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("JOIN x", into=Join).on("y = 1").sql()
            'JOIN x ON y = 1'

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append (bool): if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect (str): the dialect used to parse the input expressions.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Join: the modified join expression.
        """
        join = _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="on",
            append=append,
            dialect=dialect,
            parser_opts=parser_opts,
            copy=copy,
        )

        if join.kind == "CROSS":
            join.set("kind", None)

        return join


class Lateral(DerivedTable):
    arg_types = {"this": True, "outer": False, "alias": False}


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


class Properties(Expression):
    arg_types = {"expressions": True}


class Property(Expression):
    arg_types = {"this": True, "value": True}


class TableFormatProperty(Property):
    pass


class PartitionedByProperty(Property):
    pass


class FileFormatProperty(Property):
    pass


class LocationProperty(Property):
    pass


class EngineProperty(Property):
    pass


class AutoIncrementProperty(Property):
    pass


class CharacterSetProperty(Property):
    arg_types = {"this": True, "value": True, "default": True}


class CollateProperty(Property):
    pass


class SchemaCommentProperty(Property):
    pass


class AnonymousProperty(Property):
    pass


class Qualify(Expression):
    pass


class Reference(Expression):
    arg_types = {"this": True, "expressions": True}


class Table(Expression):
    arg_types = {"this": True, "db": False, "catalog": False}


class Tuple(Expression):
    arg_types = {"expressions": False}


class Subqueryable:
    def subquery(self, alias=None, copy=True):
        """
        Convert this expression to an aliased expression that can be used as a Subquery.

        Example:
            >>> subquery = Select().select("x").from_("tbl").subquery()
            >>> Select().select("x").from_(subquery).sql()
            'SELECT x FROM (SELECT x FROM tbl)'

        Args:
            alias (str or Identifier): an optional alias for the subquery
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Alias: the subquery
        """
        instance = _maybe_copy(self, copy)
        return Subquery(
            this=instance,
            alias=TableAlias(this=to_identifier(alias)),
        )

    @property
    def ctes(self):
        with_ = self.args.get("with")  # pylint: disable=no-member
        if not with_:
            return []
        return with_.expressions

    def with_(
        self,
        alias,
        as_,
        recursive=None,
        append=True,
        dialect=None,
        parser_opts=None,
        copy=True,
    ):
        """
        Append to or set the common table expressions.

        Example:
            >>> Select().with_("tbl2", as_="SELECT * FROM tbl").select("x").from_("tbl2").sql()
            'WITH tbl2 AS (SELECT * FROM tbl) SELECT x FROM tbl2'

        Args:
            alias (str or Expression): the SQL code string to parse as the table name.
                If an `Expression` instance is passed, this is used as-is.
            as_ (str or Expression): the SQL code string to parse as the table expression.
                If an `Expression` instance is passed, it will be used as-is.
            recursive (bool): set the RECURSIVE part of the expression. Defaults to `False`.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect (str): the dialect used to parse the input expression.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Select: the modified expression.
        """
        alias_expression = _maybe_parse(
            alias,
            dialect=dialect,
            into=TableAlias,
            parser_opts=parser_opts,
        )
        as_expression = _maybe_parse(
            as_,
            dialect=dialect,
            parser_opts=parser_opts,
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
            recursive=recursive or False,
        )


QUERY_MODIFIERS = {
    "laterals": False,
    "joins": False,
    "where": False,
    "group": False,
    "having": False,
    "qualify": False,
    "window": False,
    "distribute": False,
    "sort": False,
    "cluster": False,
    "order": False,
    "limit": False,
    "offset": False,
}


class Union(Subqueryable, Expression):
    arg_types = {
        "with": False,
        "this": True,
        "expression": True,
        "distinct": False,
        **QUERY_MODIFIERS,
    }

    @property
    def named_selects(self):
        return self.args["this"].unnest().named_selects

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


class Unnest(DerivedTable):
    arg_types = {
        "expressions": True,
        "ordinality": False,
        "alias": False,
    }


class Update(Expression):
    arg_types = {
        "with": False,
        "this": True,
        "expressions": True,
        "from": False,
        "where": False,
    }


class Values(Expression):
    arg_types = {"expressions": True}


class Var(Expression):
    pass


class Schema(Expression):
    arg_types = {"this": False, "expressions": True}


class Select(Subqueryable, Expression):
    arg_types = {
        "with": False,
        "expressions": False,
        "hint": False,
        "distinct": False,
        "from": False,
        **QUERY_MODIFIERS,
    }

    def from_(
        self, *expressions, append=True, dialect=None, parser_opts=None, copy=True
    ):
        """
        Set the FROM expression.

        Example:
            >>> Select().from_("tbl").select("x").sql()
            'SELECT x FROM tbl'

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If a `From` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `From`.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this flattens all the `From` expression into a single expression.
            dialect (str): the dialect used to parse the input expression.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

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
            parser_opts=parser_opts,
        )

    def group_by(
        self, *expressions, append=True, dialect=None, parser_opts=None, copy=True
    ):
        """
        Set the GROUP BY expression.

        Example:
            >>> Select().from_("tbl").select("x", "COUNT(1)").group_by("x").sql()
            'SELECT x, COUNT(1) FROM tbl GROUP BY x'

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Group`.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Group` expression into a single expression.
            dialect (str): the dialect used to parse the input expression.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Select: the modified expression.
        """
        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="group",
            append=append,
            copy=copy,
            prefix="GROUP BY",
            into=Group,
            dialect=dialect,
            parser_opts=parser_opts,
        )

    def order_by(
        self, *expressions, append=True, dialect=None, parser_opts=None, copy=True
    ):
        """
        Set the ORDER BY expression.

        Example:
            >>> Select().from_("tbl").select("x").order_by("x DESC").sql()
            'SELECT x FROM tbl ORDER BY x DESC'

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Order`.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect (str): the dialect used to parse the input expression.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

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
            parser_opts=parser_opts,
        )

    def limit(self, expression, dialect=None, parser_opts=None, copy=True):
        """
        Set the LIMIT expression.

        Example:
            >>> Select().from_("tbl").select("x").limit(10).sql()
            'SELECT x FROM tbl LIMIT 10'

        Args:
            expression (str or int or Expression): the SQL code string to parse.
                This can also be an integer.
                If a `Limit` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Limit`.
            dialect (str): the dialect used to parse the input expression.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

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
            parser_opts=parser_opts,
            copy=copy,
        )

    def offset(self, expression, dialect=None, parser_opts=None, copy=True):
        """
        Set the OFFSET expression.

        Example:
            >>> Select().from_("tbl").select("x").offset(10).sql()
            'SELECT x FROM tbl OFFSET 10'

        Args:
            expression (str or int or Expression): the SQL code string to parse.
                This can also be an integer.
                If a `Offset` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Offset`.
            dialect (str): the dialect used to parse the input expression.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

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
            parser_opts=parser_opts,
            copy=copy,
        )

    def select(
        self, *expressions, append=True, dialect=None, parser_opts=None, copy=True
    ):
        """
        Append to or set the SELECT expressions.

        Example:
            >>> Select().select("x", "y").sql()
            'SELECT x, y'

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect (str): the dialect used to parse the input expressions.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Select: the modified expression.
        """
        return _apply_list_builder(
            *expressions,
            instance=self,
            arg="expressions",
            append=append,
            dialect=dialect,
            parser_opts=parser_opts,
            copy=copy,
        )

    def lateral(
        self, *expressions, append=True, dialect=None, parser_opts=None, copy=True
    ):
        """
        Append to or set the LATERAL expressions.

        Example:
            >>> Select().select("x").lateral("OUTER explode(y) tbl2 AS z").from_("tbl").sql()
            'SELECT x FROM tbl LATERAL VIEW OUTER EXPLODE(y) tbl2 AS z'

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect (str): the dialect used to parse the input expressions.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

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
            parser_opts=parser_opts,
            copy=copy,
        )

    def join(
        self,
        expression,
        on=None,
        append=True,
        join_type=None,
        join_alias=None,
        dialect=None,
        parser_opts=None,
        copy=True,
    ):
        """
        Append to or set the JOIN expressions.

        Example:
            >>> Select().select("*").from_("tbl").join("tbl2", on="tbl1.y = tbl2.y").sql()
            'SELECT * FROM tbl JOIN tbl2 ON tbl1.y = tbl2.y'

            Use `join_type` to change the type of join:

            >>> Select().select("*").from_("tbl").join("tbl2", on="tbl1.y = tbl2.y", join_type="left outer").sql()
            'SELECT * FROM tbl LEFT OUTER JOIN tbl2 ON tbl1.y = tbl2.y'

        Args:
            expression (str or Expression): the SQL code string to parse.
                If an `Expression` instance is passed, it will be used as-is.
            on (str or Expression): optionally specify the join criteria as a SQL string.
                If an `Expression` instance is passed, it will be used as-is.
            append (bool): if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            join_type (str): If set, alter the parsed join type
            dialect (str): the dialect used to parse the input expressions.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Select: the modified expression.
        """
        parse_args = {"dialect": dialect, "parser_opts": parser_opts}

        try:
            expression = _maybe_parse(
                expression, into=Join, prefix="JOIN", **parse_args
            )
        except ParseError:
            expression = _maybe_parse(expression, into=(Join, Expression), **parse_args)

        join = expression if isinstance(expression, Join) else Join(this=expression)

        if isinstance(join.this, Select):
            join.this.replace(join.this.subquery())

        if join_type:
            side, kind = _maybe_parse(join_type, into="JOIN_TYPE", **parse_args)
            if side:
                join.set("side", side.text)
            if kind:
                join.set("kind", kind.text)

        if on:
            on = and_(*ensure_list(on), dialect=dialect, **(parser_opts or {}))
            join.set("on", on)

        if join_alias:
            join.set("this", alias_(join.args["this"], join_alias, table=True))
        return _apply_list_builder(
            join,
            instance=self,
            arg="joins",
            append=append,
            copy=copy,
        )

    def where(
        self, *expressions, append=True, dialect=None, parser_opts=None, copy=True
    ):
        """
        Append to or set the WHERE expressions.

        Example:
            >>> Select().select("x").from_("tbl").where("x = 'a' OR x < 'b'").sql()
            "SELECT x FROM tbl WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append (bool): if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect (str): the dialect used to parse the input expressions.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

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
            parser_opts=parser_opts,
            copy=copy,
        )

    def having(
        self, *expressions, append=True, dialect=None, parser_opts=None, copy=True
    ):
        """
        Append to or set the HAVING expressions.

        Example:
            >>> Select().select("x", "COUNT(y)").from_("tbl").group_by("x").having("COUNT(y) > 3").sql()
            'SELECT x, COUNT(y) FROM tbl GROUP BY x HAVING COUNT(y) > 3'

        Args:
            *expressions (str or Expression): the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append (bool): if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect (str): the dialect used to parse the input expressions.
            parser_opts (dict): other options to use to parse the input expressions.
            copy (bool): if `False`, modify this expression instance in-place.

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
            parser_opts=parser_opts,
            copy=copy,
        )

    def distinct(self, distinct=True, copy=True):
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

    def ctas(self, table, properties=None, dialect=None, parser_opts=None, copy=True):
        """
        Convert this expression to a CREATE TABLE AS statement.

        Example:
            >>> Select().select("*").from_("tbl").ctas("x").sql()
            'CREATE TABLE x AS SELECT * FROM tbl'

        Args:
            table (str or Expression): the SQL code string to parse as the table name.
                If another `Expression` instance is passed, it will be used as-is.
            properties (dict): an optional mapping of table properties
            dialect (str): the dialect used to parse the input table.
            parser_opts (dict): other options to use to parse the input table.
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Create: the CREATE TABLE AS expression
        """
        instance = _maybe_copy(self, copy)
        table_expression = _maybe_parse(
            table,
            into=Table,
            dialect=dialect,
            parser_opts=parser_opts,
        )
        properties_expression = None
        if properties:
            properties_str = " ".join(
                [
                    f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
                    for k, v in properties.items()
                ]
            )
            properties_expression = _maybe_parse(
                properties_str,
                into=Properties,
                dialect=dialect,
                parser_opts=parser_opts,
            )

        return Create(
            this=table_expression,
            kind="table",
            expression=instance,
            properties=properties_expression,
        )

    @property
    def named_selects(self):
        return [e.alias_or_name for e in self.expressions if e.alias_or_name]

    @property
    def selects(self):
        return self.expressions


class Subquery(DerivedTable):
    arg_types = {
        "this": True,
        "alias": False,
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


class Placeholder(Expression):
    arg_types = {}


class Null(Condition):
    arg_types = {}


class Boolean(Condition):
    pass


class DataType(Expression):
    arg_types = {
        "this": True,
        "expressions": False,
        "nested": False,
    }

    class Type(AutoName):
        CHAR = auto()
        NCHAR = auto()
        VARCHAR = auto()
        NVARCHAR = auto()
        TEXT = auto()
        BINARY = auto()
        INT = auto()
        TINYINT = auto()
        SMALLINT = auto()
        BIGINT = auto()
        FLOAT = auto()
        DOUBLE = auto()
        DECIMAL = auto()
        BOOLEAN = auto()
        JSON = auto()
        TIMESTAMP = auto()
        TIMESTAMPTZ = auto()
        DATE = auto()
        DATETIME = auto()
        ARRAY = auto()
        MAP = auto()
        UUID = auto()
        GEOGRAPHY = auto()
        STRUCT = auto()
        NULLABLE = auto()

    @classmethod
    def build(cls, dtype, **kwargs):
        return DataType(
            this=dtype
            if isinstance(dtype, DataType.Type)
            else DataType.Type[dtype.upper()],
            **kwargs,
        )


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


# Commands to interact with the databases or engines
# These expressions don't truly parse the expression and consume
# whatever exists as a string until the end or a semicolon
class Command(Expression):
    arg_types = {"this": True, "expression": False}


# Binary Expressions
# (ADD a b)
# (FROM table selects)
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
    pass


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
    arg_types = {"this": False, "on": False}


class In(Predicate):
    arg_types = {"this": True, "expressions": False, "query": False, "unnest": False}


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
    arg_types = {"this": True, "unit": False}


class IgnoreNulls(Expression):
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
        args_num = len(args)

        all_arg_keys = list(cls.arg_types)
        # If this function supports variable length argument treat the last argument as such.
        non_var_len_arg_keys = (
            all_arg_keys[:-1] if cls.is_var_len_args else all_arg_keys
        )

        args_dict = {}
        arg_idx = 0
        for arg_key in non_var_len_arg_keys:
            if arg_idx >= args_num:
                break
            if args[arg_idx] is not None:
                args_dict[arg_key] = args[arg_idx]
            arg_idx += 1

        if arg_idx < args_num and cls.is_var_len_args:
            args_dict[all_arg_keys[-1]] = args[arg_idx:]
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


class ArrayContains(Func):
    arg_types = {"this": True, "expression": True}


class ArrayFilter(Func):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["FILTER", "ARRAY_FILTER"]


class ArraySize(Func):
    pass


class ArraySort(Func):
    arg_types = {"this": True, "expression": False}


class ArraySum(Func):
    pass


class Avg(AggFunc):
    pass


class Case(Func):
    arg_types = {"this": False, "ifs": True, "default": False}


class Cast(Func):
    arg_types = {"this": True, "to": True}


class TryCast(Cast):
    pass


class Ceil(Func):
    _sql_names = ["CEIL", "CEILING"]


class Coalesce(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ConcatWs(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Count(AggFunc):
    pass


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


class DateTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


class DatetimeAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


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


class DateStrToDate(Func):
    pass


class DateToDateStr(Func):
    pass


class DateToDi(Func):
    pass


class Day(Func):
    pass


class DiToDate(Func):
    pass


class Exp(Func):
    pass


class Explode(Func):
    pass


class Floor(Func):
    pass


class Greatest(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class If(Func):
    arg_types = {"this": True, "true": True, "false": False}


class Initcap(Func):
    pass


class JSONExtract(Func):
    arg_types = {"this": True, "path": True}
    _sql_names = ["JSON_EXTRACT"]


class JSONExtractScalar(JSONExtract):
    _sql_names = ["JSON_EXTRACT_SCALAR"]


class JSONBExtract(JSONExtract):
    _sql_names = ["JSONB_EXTRACT"]


class JSONBExtractScalar(JSONExtract):
    _sql_names = ["JSONB_EXTRACT_SCALAR"]


class Least(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class Length(Func):
    pass


class Levenshtein(Func):
    arg_types = {"this": True, "expression": False}


class Ln(Func):
    pass


class Log(Func):
    arg_types = {"this": True, "expression": False}


class Log2(Func):
    pass


class Log10(Func):
    pass


class Lower(Func):
    pass


class Map(Func):
    arg_types = {"keys": True, "values": True}


class Max(AggFunc):
    pass


class Min(AggFunc):
    pass


class Month(Func):
    pass


class Posexplode(Func):
    pass


class Pow(Func):
    arg_types = {"this": True, "power": True}
    _sql_names = ["POWER", "POW"]


class Quantile(AggFunc):
    arg_types = {"this": True, "quantile": True}


class Reduce(Func):
    arg_types = {"this": True, "initial": True, "merge": True, "finish": True}


class RegexpLike(Func):
    arg_types = {"this": True, "expression": True}


class RegexpSplit(Func):
    arg_types = {"this": True, "expression": True}


class Round(Func):
    arg_types = {"this": True, "decimals": False}


class SafeDivide(Func):
    arg_types = {"this": True, "expression": True}


class SetAgg(AggFunc):
    pass


class SortArray(Func):
    arg_types = {"this": True, "asc": False}


class Split(Func):
    arg_types = {"this": True, "expression": True}


class Substring(Func):
    arg_types = {"this": True, "start": True, "length": False}


class StrPosition(Func):
    arg_types = {"this": True, "substr": True, "position": False}


class StrToDate(Func):
    arg_types = {"this": True, "format": True}


class StrToTime(Func):
    arg_types = {"this": True, "format": True}


class StrToUnix(Func):
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


class TsOrDsAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TsOrDsToDateStr(Func):
    pass


class TsOrDsToDate(Func):
    arg_types = {"this": True, "format": False}


class TsOrDiToDi(Func):
    pass


class UnixToStr(Func):
    arg_types = {"this": True, "format": True}


class UnixToTime(Func):
    arg_types = {"this": True, "scale": False}

    SECONDS = Literal.string("seconds")
    MILLIS = Literal.string("millis")
    MICROS = Literal.string("micros")


class UnixToTimeStr(Func):
    pass


class Upper(Func):
    pass


class Variance(AggFunc):
    pass


class VariancePop(AggFunc):
    pass


class VarianceSamp(AggFunc):
    pass


class Week(Func):
    arg_types = {"this": True, "mode": False}


class Year(Func):
    pass


def _norm_args(expression):
    args = {}

    for k, arg in expression.args.items():
        if isinstance(arg, list):
            arg = [_norm_arg(a) for a in arg]
        else:
            arg = _norm_arg(arg)

        if arg is not None:
            args[k] = arg

    return args


def _norm_arg(arg):
    return arg.lower() if isinstance(arg, str) else arg


def _all_functions():
    return [
        obj
        for _, obj in inspect.getmembers(
            sys.modules[__name__],
            lambda obj: inspect.isclass(obj)
            and issubclass(obj, Func)
            and obj not in (AggFunc, Anonymous, Func),
        )
    ]


ALL_FUNCTIONS = _all_functions()


def _maybe_parse(
    sql_or_expression,
    *,
    into=None,
    dialect=None,
    prefix=None,
    parser_opts=None,
):
    if isinstance(sql_or_expression, Expression):
        return sql_or_expression

    import sqlglot

    sql = str(sql_or_expression)
    if prefix:
        sql = f"{prefix} {sql}"
    return sqlglot.parse_one(sql, read=dialect, into=into, **(parser_opts or {}))


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
    parser_opts=None,
):
    if _is_wrong_expression(expression, into):
        expression = into(this=expression)
    instance = _maybe_copy(instance, copy)
    expression = _maybe_parse(
        sql_or_expression=expression,
        prefix=prefix,
        into=into,
        dialect=dialect,
        parser_opts=parser_opts,
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
    parser_opts=None,
    **kwargs,
):
    instance = _maybe_copy(instance, copy)
    parsed = []
    for expression in expressions:
        if _is_wrong_expression(expression, into):
            expression = into(expressions=[expression])
        expression = _maybe_parse(
            expression,
            into=into,
            dialect=dialect,
            prefix=prefix,
            parser_opts=parser_opts,
        )
        parsed.extend(expression.expressions)

    existing = instance.args.get(arg)
    if append and existing:
        parsed = existing.expressions + parsed

    child = into(expressions=parsed)
    for k, v in kwargs.items():
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
    parser_opts=None,
):
    inst = _maybe_copy(instance, copy)

    expressions = [
        _maybe_parse(
            sql_or_expression=expression,
            into=into,
            prefix=prefix,
            dialect=dialect,
            parser_opts=parser_opts,
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
    parser_opts=None,
):
    expressions = [exp for exp in expressions if exp is not None and exp != ""]
    if not expressions:
        return instance

    inst = _maybe_copy(instance, copy)

    existing = inst.args.get(arg)
    if append and existing is not None:
        expressions = [existing.this if into else existing] + list(expressions)

    node = and_(*expressions, dialect=dialect, **(parser_opts or {}))

    inst.set(arg, into(this=node) if into else node)
    return inst


def _combine(expressions, operator, dialect=None, **opts):
    expressions = [
        condition(expression, dialect=dialect, **opts) for expression in expressions
    ]
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


def select(*expressions, dialect=None, **opts):
    """
    Initializes a syntax tree from one or multiple SELECT expressions.

    Example:
        >>> select("col1", "col2").from_("tbl").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expressions (str or Expression): the SQL code string to parse as the expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().select(*expressions, dialect=dialect, parser_opts=opts)


def from_(*expressions, dialect=None, **opts):
    """
    Initializes a syntax tree from a FROM expression.

    Example:
        >>> from_("tbl").select("col1", "col2").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expressions (str or Expression): the SQL code string to parse as the FROM expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().from_(*expressions, dialect=dialect, parser_opts=opts)


def condition(expression, dialect=None, **opts):
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
        *expression (str or Expression): the SQL code string to parse.
            If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        Condition: the expression
    """
    return _maybe_parse(
        expression,
        into=Condition,
        dialect=dialect,
        parser_opts=opts,
    )


def and_(*expressions, dialect=None, **opts):
    """
    Combine multiple conditions with an AND logical operator.

    Example:
        >>> and_("x=1", and_("y=1", "z=1")).sql()
        'x = 1 AND (y = 1 AND z = 1)'

    Args:
        *expressions (str or Expression): the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        And: the new condition
    """
    return _combine(expressions, And, dialect, **opts)


def or_(*expressions, dialect=None, **opts):
    """
    Combine multiple conditions with an OR logical operator.

    Example:
        >>> or_("x=1", or_("y=1", "z=1")).sql()
        'x = 1 OR (y = 1 OR z = 1)'

    Args:
        *expressions (str or Expression): the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Or: the new condition
    """
    return _combine(expressions, Or, dialect, **opts)


def not_(expression, dialect=None, **opts):
    """
    Wrap a condition with a NOT operator.

    Example:
        >>> not_("this_suit='black'").sql()
        "NOT this_suit = 'black'"

    Args:
        expression (str or Expression): the SQL code strings to parse.
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


def paren(expression):
    return Paren(this=expression)


SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z][\w]*$")


def to_identifier(alias, quoted=None):
    if alias is None:
        return None
    if isinstance(alias, Identifier):
        identifier = alias
    elif isinstance(alias, str):
        if quoted is None:
            quoted = not re.match(SAFE_IDENTIFIER_RE, alias)
        identifier = Identifier(this=alias, quoted=quoted)
    else:
        raise ValueError(
            f"Alias needs to be a string or an Identifier, got: {alias.__class__}"
        )
    return identifier


def alias_(expression, alias, table=False, dialect=None, quoted=None, **opts):
    """
    Create an Alias expression.
    Expample:
        >>> alias_('foo', 'bar').sql()
        'foo AS bar'

    Args:
        expression (str or Expression): the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        alias (str or Identifier): the alias name to use. If the name has
            special characters it is quoted.
        table (boolean): create a table alias, default false
        dialect (str): the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Alias: the aliased expression
    """
    exp = _maybe_parse(expression, dialect=dialect, parser_opts=opts)
    alias = to_identifier(alias, quoted=quoted)
    alias = TableAlias(this=alias) if table else alias

    if "alias" in exp.arg_types:
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
        expression (str or Expression): the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        alias (str or Expression): the alias name to use.
        dialect (str): the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Select: a new select with the subquery epxresion included
    """

    expression = _maybe_parse(expression, dialect=dialect, **opts).subquery(alias)
    return Select().from_(expression, dialect=dialect, **opts)


def column(col, table=None, quoted=None):
    """
    Build a Column.
    Args:
        col (str or Expression): column name
        table (str or Expression): table name
    Returns:
        Column: column instance
    """
    return Column(
        this=to_identifier(col, quoted=quoted),
        table=to_identifier(table, quoted=quoted),
    )


def table_(table, db=None, catalog=None, quoted=None):
    """
    Build a Table.
    Args:
        table (str or Expression): column name
        db (str or Expression): db name
        catalog (str or Expression): catalog name
    Returns:
        Table: table instance
    """
    return Table(
        this=to_identifier(table, quoted=quoted),
        db=to_identifier(db, quoted=quoted),
        catalog=to_identifier(catalog, quoted=quoted),
    )


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
                cns = ensure_list(fun(cn))
                for child_node in cns:
                    new_child_nodes.append(child_node)
                    child_node.parent = expression
                    child_node.arg_key = k
            else:
                new_child_nodes.append(cn)

        expression.args[k] = new_child_nodes if is_list_arg else new_child_nodes[0]


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


TRUE = Boolean(this=True)
FALSE = Boolean(this=False)
NULL = Null()
