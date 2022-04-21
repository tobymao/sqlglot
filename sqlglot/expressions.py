from copy import deepcopy
from enum import auto
import typing
import inspect
import sys

from sqlglot.helper import AutoName, camel_to_snake_case, ensure_list

T = typing.TypeVar("T")


class Expression:
    """
    The base class for all expressions in a syntax tree.

    Attributes:
        arg_types (dict): determines arguments supported by this expression.
            The key in a dictionary defines a unique key of an argument using
            which the argument's value can be retrieved. The value is a boolean
            flag which indiciates whether the argument's value is required (True)
            or optional (False).
    """

    arg_types = {"this": True}
    __slots__ = ("key", "args", "parent", "arg_key")

    def __init__(self, **args):
        self.key = self.__class__.__name__.lower()
        self.args = args
        self.parent = None
        self.arg_key = None

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

    def text(self, key):
        field = self.args.get(key)
        if isinstance(field, str):
            return field
        if isinstance(field, (Identifier, Literal)):
            return field.this
        return ""

    def copy(self):
        new = deepcopy(self)
        for item, parent, _ in new.bfs():
            if isinstance(item, Expression) and parent:
                item.parent = parent
        return new

    def set(self, arg, value):
        """
        Sets `arg` to `value`.

        Args:
            arg (str): name of the expression arg
            value: value to set the arg to.
        """
        self.args[arg] = value
        for v in ensure_list(value):
            if isinstance(v, Expression):
                v.parent = self

    @property
    def depth(self):
        """
        Returns the depth of this tree.
        """
        if self.parent:
            return self.parent.depth + 1
        return 0

    def find(self, *expression_types):
        """
        Returns the first node in this tree which matches at least one of
        the specified types.

        Args:
            expression_types (type): the expression type to match.

        Returns:
            the node which matches the criteria or None if no node matching
            the criteria was found.
        """
        return next(self.find_all(*expression_types), None)

    def find_all(self, *expression_types):
        """
        Returns a generator object which visits all nodes in this tree and only
        yields those that match at least one of the specified expression types.

        Args:
            expression_types (type): the expression type to match.

        Returns:
            the generator object.
        """
        for expression, _, _ in self.walk():
            if isinstance(expression, expression_types):
                yield expression

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
            yield from self.dfs(self.parent, None)

    def dfs(self, parent, key):
        """
        Returns a generator object which visits all nodes in this tree in
        the DFS (Depth-first) order.

        Returns:
            the generator object.
        """
        yield self, parent, key

        for k, v in self.args.items():
            nodes = ensure_list(v)

            for node in nodes:
                if isinstance(node, Expression):
                    yield from node.dfs(self, k)
                else:
                    yield node, self, k

    def bfs(self):
        """
        Returns a generator object which visits all nodes in this tree in
        the BFS (Breadth-first) order.

        Returns:
            the generator object.
        """
        queue = [(self, self.parent, None)]

        while queue:
            item, parent, key = queue.pop()

            yield item, parent, key

            if isinstance(item, Expression):
                for k, v in item.args.items():
                    nodes = ensure_list(v)

                    for node in nodes:
                        queue.append((node, item, k))

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
        if not isinstance(new_node, Expression) or new_node is not node:
            return new_node

        for k, v in new_node.args.items():
            is_list_arg = isinstance(v, list)

            child_nodes = v if is_list_arg else [v]
            new_child_nodes = []

            for cn in child_nodes:
                if isinstance(cn, Expression):
                    new_child_node = cn.transform(fun, *args, copy=False, **kwargs)
                    new_child_node.parent = new_node
                else:
                    new_child_node = cn
                new_child_nodes.append(new_child_node)

            new_node.args[k] = new_child_nodes if is_list_arg else new_child_nodes[0]
        return new_node

    def assert_is(self, type_: typing.Type[T]) -> T:
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
        "engine": False,
        "auto_increment": False,
        "character_set": False,
        "collate": False,
        "comment": False,
    }


class CharacterSet(Expression):
    arg_types = {"this": True, "default": False}


class With(Expression):
    arg_types = {"expressions": True, "recursive": False}


class CTE(Expression):
    arg_types = {"this": True, "alias": True}


class TableAlias(Expression):
    arg_types = {"this": True, "columns": False}


class Column(Expression):
    arg_types = {"this": False, "table": False}

    @property
    def table(self):
        return self.args.get("table")


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
    }


class Delete(Expression):
    arg_types = {"with": False, "this": True, "where": False}


class Drop(Expression):
    arg_types = {"this": False, "kind": False, "exists": False}


class Except(Expression):
    arg_types = {"this": True, "expression": True, "distinct": False}


class Exists(Expression):
    arg_types = {"this": True, "not": False}


class From(Expression):
    arg_types = {"expressions": True}


class Having(Expression):
    pass


class Hint(Expression):
    arg_types = {"expressions": True}


class Identifier(Expression):
    arg_types = {"this": True, "quoted": False}

    def __eq__(self, other):
        return (
            isinstance(other, Identifier)
            and (self.this or "").upper() == (other.this or "").upper()
        )

    def __hash__(self):
        return hash((self.key, self.this.upper()))


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


class Intersect(Expression):
    arg_types = {"this": True, "expression": True, "distinct": False}


class Group(Expression):
    arg_types = {"expressions": True}


class Lambda(Expression):
    arg_types = {"this": True, "expressions": True}


class Limit(Expression):
    pass


class Literal(Expression):
    arg_types = {"this": True, "is_string": True}

    def __eq__(self, other):
        return (
            isinstance(other, Literal)
            and self.this == other.this
            and self.args.get("is_string") == other.args.get("is_string")
        )

    def __hash__(self):
        return hash((self.key, self.this, self.args.get("is_string")))

    @classmethod
    def number(cls, number):
        return cls(this=str(number), is_string=False)

    @classmethod
    def string(cls, string):
        return cls(this=string, is_string=True)

    @property
    def is_string(self):
        return self.args["is_string"]

    @property
    def is_int(self):
        return not self.is_string and self.this.isdigit()


class Join(Expression):
    arg_types = {"this": True, "on": False, "side": False, "kind": False}


class Lateral(Expression):
    arg_types = {"this": True, "outer": False, "table": False, "columns": False}


class Offset(Expression):
    pass


class Order(Expression):
    arg_types = {"expressions": True}


class Ordered(Expression):
    arg_types = {"this": True, "desc": False}


class Properties(Expression):
    arg_types = {"expressions": True}


class Property(Expression):
    arg_types = {"this": True, "value": True}


class Table(Expression):
    arg_types = {"this": True, "db": False, "catalog": False}


class Tuple(Expression):
    arg_types = {"expressions": True}


class Union(Expression):
    arg_types = {"this": True, "expression": True, "distinct": False}


class Unnest(Expression):
    arg_types = {
        "expressions": True,
        "ordinality": False,
        "table": False,
        "columns": False,
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


class Schema(Expression):
    arg_types = {"this": False, "expressions": True}


class Select(Expression):
    arg_types = {
        "with": False,
        "expressions": False,
        "hint": False,
        "distinct": False,
        "from": False,
        "laterals": False,
        "joins": False,
        "where": False,
        "group": False,
        "having": False,
        "order": False,
        "limit": False,
        "offset": False,
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
            parse_into=From,
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
            parse_into=Group,
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
            parse_into=Order,
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
            parse_into=Limit,
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
            parse_into=Offset,
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
            parse_into=Lateral,
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
        prefix = "JOIN"
        if join_type:
            prefix = f"{join_type} {prefix}"
        expression = _maybe_parse(
            expression,
            parse_into=Join,
            dialect=dialect,
            prefix=prefix,
            parser_opts=parser_opts,
        )
        if on:
            on = _maybe_parse(
                on,
                parse_into=CONJUNCTION,
                dialect=dialect,
                parser_opts=parser_opts,
            )
            expression.set("on", on)
        return _apply_list_builder(
            expression,
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
            parse_into=Where,
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
            parse_into=Having,
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
        instance.set("distinct", distinct)
        return instance

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
            parse_into=TableAlias,
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
            parse_into=With,
            recursive=recursive or False,
        )

    def subquery(self, alias=None, copy=True):
        """
        Convert this expression to an aliased expression that can be used as a subquery.

        Example:
            >>> subquery = Select().select("x").from_("tbl").subquery()
            >>> Select().select("x").from_(subquery).sql()
            'SELECT x FROM (SELECT x FROM tbl)'

        Args:
            alias (str): an optional alias for the subquery
            copy (bool): if `False`, modify this expression instance in-place.

        Returns:
            Alias: the subquery
        """
        instance = _maybe_copy(self, copy)
        return Alias(
            this=instance,
            alias=alias,
        )

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
            parse_into=Table,
            dialect=dialect,
            parser_opts=parser_opts,
        )
        return Create(
            this=table_expression,
            kind="table",
            expression=instance,
            properties=Properties(
                expressions=[
                    Property(
                        this=Literal.string(k),
                        value=Literal.string(v),
                    )
                    for k, v in (properties or {}).items()
                ]
            ),
        )


class TableSample(Expression):
    arg_types = {
        "this": True,
        "bucket_numerator": False,
        "bucket_denominator": False,
        "bucket_field": False,
        "percent": False,
        "rows": False,
        "size": False,
    }


class Window(Expression):
    arg_types = {"this": True, "partition_by": False, "order": False, "spec": False}


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
    arg_types = {}


class Null(Expression):
    arg_types = {}


class Boolean(Expression):
    pass


class DataType(Expression):
    arg_types = {
        "this": True,
        "expressions": False,
        "nested": True,
    }

    class Type(AutoName):
        CHAR = auto()
        TEXT = auto()
        VARCHAR = auto()
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
        ARRAY = auto()
        MAP = auto()
        UUID = auto()


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
        return self.args.get("expression")


class Add(Binary):
    pass


class And(Binary):
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


class EQ(Binary):
    pass


class GT(Binary):
    pass


class GTE(Binary):
    pass


class ILike(Binary):
    pass


class IntDiv(Binary):
    pass


class Is(Binary):
    pass


class Like(Binary):
    pass


class LT(Binary):
    pass


class LTE(Binary):
    pass


class Mod(Binary):
    pass


class Mul(Binary):
    pass


class NEQ(Binary):
    pass


class Or(Binary):
    pass


class Sub(Binary):
    pass


# Unary Expressions
# (NOT a)
class Unary(Expression):
    pass


class BitwiseNot(Unary):
    pass


class Not(Unary):
    pass


class Paren(Unary):
    pass


class Neg(Unary):
    pass


# Special Functions
class Alias(Expression):
    arg_types = {"this": True, "alias": False}

    @property
    def alias(self):
        return self.args.get("alias")


class Aliases(Expression):
    arg_types = {"this": True, "expressions": True}

    @property
    def aliases(self):
        return self.args["expressions"]


class Between(Expression):
    arg_types = {"this": True, "low": True, "high": True}


class Bracket(Expression):
    arg_types = {"this": True, "expressions": True}


class Case(Expression):
    arg_types = {"this": False, "ifs": True, "default": False}


class Cast(Expression):
    arg_types = {"this": True, "to": True}


class Decimal(Expression):
    arg_types = {"precision": False, "scale": False}


class Extract(Expression):
    arg_types = {"this": True, "expression": True}


class In(Expression):
    arg_types = {"this": True, "expressions": False, "query": False}


class Interval(Expression):
    arg_types = {"this": True, "unit": True}


class TryCast(Cast):
    pass


# Functions
class Func(Expression):
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


class ArrayContains(Func):
    arg_types = {"this": True, "expression": True}


class ArraySize(Func):
    pass


class Avg(AggFunc):
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
    arg_types = {"this": False, "distinct": False}


class DateAdd(Func):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateDiff(Func):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateStrToDate(Func):
    pass


class Day(Func):
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


class Least(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class Length(Func):
    pass


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
    _sql_names = ["POW", "POWER"]


class Quantile(AggFunc):
    arg_types = {"this": True, "quantile": True}


class RegexpLike(Func):
    arg_types = {"this": True, "expression": True}


class RegexpSplit(Func):
    arg_types = {"this": True, "expression": True}


class Round(Func):
    arg_types = {"this": True, "decimals": False}


class SetAgg(AggFunc):
    pass


class Split(Func):
    arg_types = {"this": True, "expression": True}


class Substring(Func):
    arg_types = {"this": True, "start": True, "length": False}


class StrPosition(Func):
    arg_types = {"this": True, "substr": True, "position": False}


class StrToTime(Func):
    arg_types = {"this": True, "format": True}


class StrToUnix(Func):
    arg_types = {"this": True, "format": True}


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


class TsOrDsAdd(Func):
    arg_types = {"this": True, "expression": True, "unit": False}


class TsOrDsToDateStr(Func):
    pass


class TsOrDsToDate(Func):
    pass


class UnixToStr(Func):
    arg_types = {"this": True, "format": True}


class UnixToTime(Func):
    pass


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


class Year(Func):
    pass


def _norm_args(expression):
    return {
        k: _norm_arg(arg) if not isinstance(arg, list) else [_norm_arg(a) for a in arg]
        for k, arg in expression.args.items()
    }


def _norm_arg(arg):
    arg = arg or ""
    return arg.upper() if isinstance(arg, str) else arg


def _all_functions():
    predicate = (
        lambda obj: inspect.isclass(obj)
        and issubclass(obj, Func)
        and obj not in (AggFunc, Anonymous, Func)
    )
    return [obj for _, obj in inspect.getmembers(sys.modules[__name__], predicate)]


ALL_FUNCTIONS = _all_functions()

# Type alias to help use Parser.parse_into for expressions that
# can output one of multiple different expression types
CONJUNCTION = (And, Or)


def _maybe_parse(
    code_or_expression,
    *,
    parse_into=None,
    dialect=None,
    prefix=None,
    parser_opts=None,
):
    if isinstance(code_or_expression, Expression):
        return code_or_expression

    parser_opts = parser_opts or {}
    code = str(code_or_expression)
    if prefix:
        code = f"{prefix} {code}"

    from sqlglot.dialects import Dialect

    dialect = Dialect.get_or_raise(dialect)()

    if parse_into:
        result = dialect.parse_into(parse_into, code, **parser_opts)
    else:
        result = dialect.parse(code, **parser_opts)

    if not result:
        return None
    return result[0]


def _maybe_copy(instance, copy=True):
    return instance.copy() if copy else instance


def _is_wrong_expression(expression, parse_into):
    return isinstance(expression, Expression) and not isinstance(expression, parse_into)


def _apply_builder(
    expression,
    instance,
    arg,
    copy=True,
    prefix=None,
    parse_into=None,
    dialect=None,
    parser_opts=None,
):
    if _is_wrong_expression(expression, parse_into):
        expression = parse_into(this=expression)
    instance = _maybe_copy(instance, copy)
    expression = _maybe_parse(
        code_or_expression=expression,
        prefix=prefix,
        parse_into=parse_into,
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
    parse_into=None,
    dialect=None,
    parser_opts=None,
    **kwargs,
):
    # pylint: disable=too-many-locals
    instance = _maybe_copy(instance, copy)
    parsed = []
    for expression in expressions:
        if _is_wrong_expression(expression, parse_into):
            expression = parse_into(expressions=[expression])
        expression = _maybe_parse(
            expression,
            parse_into=parse_into,
            dialect=dialect,
            prefix=prefix,
            parser_opts=parser_opts,
        )
        parsed.extend(expression.args["expressions"])

    existing = instance.args.get(arg)
    if append and existing:
        parsed = ensure_list(existing.args.get("expressions")) + parsed

    child = parse_into(expressions=parsed)
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
    parse_into=None,
    dialect=None,
    parser_opts=None,
):
    inst = _maybe_copy(instance, copy)

    expressions = [
        _maybe_parse(
            code_or_expression=expression,
            parse_into=parse_into,
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
    parse_into,
    append=True,
    copy=True,
    dialect=None,
    parser_opts=None,
):
    inst = _maybe_copy(instance, copy)
    expressions = [
        _maybe_parse(
            code_or_expression=expression,
            dialect=dialect,
            parser_opts=parser_opts,
        )
        for expression in expressions
    ]

    existing = inst.args.get(arg)
    if append and existing is not None:
        expressions = [existing.this] + expressions

    node = expressions[0]

    for expression in expressions[1:]:
        node = And(this=node, expression=expression)

    inst.set(arg, parse_into(this=node))
    return inst
