"""sqlglot expressions query."""

from __future__ import annotations

import typing as t

from sqlglot._typing import E
from sqlglot.errors import ParseError
from sqlglot.helper import trait, ensure_list
from sqlglot.expressions.core import (
    Aliases,
    Condition,
    Distinct,
    Dot,
    Expr,
    Expression,
    Func,
    Hint,
    Identifier,
    In,
    _apply_builder,
    _apply_child_list_builder,
    _apply_list_builder,
    _apply_conjunction_builder,
    _apply_set_operation,
    ExpOrStr,
    QUERY_MODIFIERS,
    maybe_parse,
    maybe_copy,
    to_identifier,
    convert,
    and_,
    alias_,
    column,
)
from sqlglot.expressions.datatypes import DataType

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.expressions.constraints import ColumnConstraint
    from sqlglot.expressions.ddl import Create
    from sqlglot.expressions.functions import Unnest
    from sqlglot.tokens import Token

    S = t.TypeVar("S", bound="SetOperation")


def _apply_cte_builder(
    instance: E,
    alias: ExpOrStr,
    as_: ExpOrStr,
    recursive: t.Optional[bool] = None,
    materialized: t.Optional[bool] = None,
    append: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    scalar: t.Optional[bool] = None,
    **opts,
) -> E:
    alias_expression = maybe_parse(alias, dialect=dialect, into=TableAlias, **opts)
    as_expression = maybe_parse(as_, dialect=dialect, copy=copy, **opts)
    if scalar and not isinstance(as_expression, Subquery):
        # scalar CTE must be wrapped in a subquery
        as_expression = Subquery(this=as_expression)
    cte = CTE(this=as_expression, alias=alias_expression, materialized=materialized, scalar=scalar)
    return _apply_child_list_builder(
        cte,
        instance=instance,
        arg="with_",
        append=append,
        copy=copy,
        into=With,
        properties={"recursive": recursive} if recursive else {},
    )


@trait
class DerivedTable(Expr):
    @property
    def selects(self) -> t.List[Expr]:
        return self.this.selects if isinstance(self.this, Query) else []

    @property
    def named_selects(self) -> t.List[str]:
        return [select.output_name for select in self.selects]


@trait
class UDTF(DerivedTable):
    @property
    def selects(self) -> t.List[Expr]:
        alias = self.args.get("alias")
        return alias.columns if alias else []


Q = t.TypeVar("Q", bound="Query")


@trait
class Query(Expr):
    """Trait for any SELECT/UNION/etc. query expression."""

    @property
    def ctes(self) -> t.List[CTE]:
        with_ = self.args.get("with_")
        return with_.expressions if with_ else []

    @property
    def selects(self) -> t.List[Expr]:
        raise NotImplementedError("Subclasses must implement selects")

    @property
    def named_selects(self) -> t.List[str]:
        raise NotImplementedError("Subclasses must implement named_selects")

    def select(
        self: Q,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Q:
        raise NotImplementedError("Query objects must implement `select`")

    def subquery(self, alias: t.Optional[ExpOrStr] = None, copy: bool = True) -> Subquery:
        """
        Returns a `Subquery` that wraps around this query.

        Example:
            >>> subquery = Select().select("x").from_("tbl").subquery()
            >>> Select().select("x").from_(subquery).sql()
            'SELECT x FROM (SELECT x FROM tbl)'

        Args:
            alias: an optional alias for the subquery.
            copy: if `False`, modify this expression instance in-place.
        """
        instance = maybe_copy(self, copy)
        if not isinstance(alias, Expr):
            alias = TableAlias(this=to_identifier(alias)) if alias else None

        return Subquery(this=instance, alias=alias)

    def limit(
        self: Q, expression: ExpOrStr | int, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Q:
        """
        Adds a LIMIT clause to this query.

        Example:
            >>> Select().select("1").union(Select().select("1")).limit(1).sql()
            'SELECT 1 UNION SELECT 1 LIMIT 1'

        Args:
            expression: the SQL code string to parse.
                This can also be an integer.
                If a `Limit` instance is passed, it will be used as-is.
                If another `Expr` instance is passed, it will be wrapped in a `Limit`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            A limited Select expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="limit",
            into=Limit,
            prefix="LIMIT",
            dialect=dialect,
            copy=copy,
            into_arg="expression",
            **opts,
        )

    def offset(
        self: Q, expression: ExpOrStr | int, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Q:
        """
        Set the OFFSET expression.

        Example:
            >>> Select().from_("tbl").select("x").offset(10).sql()
            'SELECT x FROM tbl OFFSET 10'

        Args:
            expression: the SQL code string to parse.
                This can also be an integer.
                If a `Offset` instance is passed, this is used as-is.
                If another `Expr` instance is passed, it will be wrapped in a `Offset`.
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
            into_arg="expression",
            **opts,
        )

    def order_by(
        self: Q,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Q:
        """
        Set the ORDER BY expression.

        Example:
            >>> Select().from_("tbl").select("x").order_by("x DESC").sql()
            'SELECT x FROM tbl ORDER BY x DESC'

        Args:
            *expressions: the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expr` instance is passed, it will be wrapped in a `Order`.
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

    def where(
        self: Q,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Q:
        """
        Append to or set the WHERE expressions.

        Examples:
            >>> Select().select("x").from_("tbl").where("x = 'a' OR x < 'b'").sql()
            "SELECT x FROM tbl WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expr` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_conjunction_builder(
            *[expr.this if isinstance(expr, Where) else expr for expr in expressions],
            instance=self,
            arg="where",
            append=append,
            into=Where,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def with_(
        self: Q,
        alias: ExpOrStr,
        as_: ExpOrStr,
        recursive: t.Optional[bool] = None,
        materialized: t.Optional[bool] = None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        scalar: t.Optional[bool] = None,
        **opts,
    ) -> Q:
        """
        Append to or set the common table expressions.

        Example:
            >>> Select().with_("tbl2", as_="SELECT * FROM tbl").select("x").from_("tbl2").sql()
            'WITH tbl2 AS (SELECT * FROM tbl) SELECT x FROM tbl2'

        Args:
            alias: the SQL code string to parse as the table name.
                If an `Expr` instance is passed, this is used as-is.
            as_: the SQL code string to parse as the table expression.
                If an `Expr` instance is passed, it will be used as-is.
            recursive: set the RECURSIVE part of the expression. Defaults to `False`.
            materialized: set the MATERIALIZED part of the expression.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            scalar: if `True`, this is a scalar common table expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_cte_builder(
            self,
            alias,
            as_,
            recursive=recursive,
            materialized=materialized,
            append=append,
            dialect=dialect,
            copy=copy,
            scalar=scalar,
            **opts,
        )

    def union(
        self, *expressions: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
    ) -> Union:
        """
        Builds a UNION expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").union("SELECT * FROM bla").sql()
            'SELECT * FROM foo UNION SELECT * FROM bla'

        Args:
            expressions: the SQL code strings.
                If `Expr` instances are passed, they will be used as-is.
            distinct: set the DISTINCT flag if and only if this is true.
            dialect: the dialect used to parse the input expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Union expression.
        """
        return union(self, *expressions, distinct=distinct, dialect=dialect, **opts)

    def intersect(
        self, *expressions: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
    ) -> Intersect:
        """
        Builds an INTERSECT expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").intersect("SELECT * FROM bla").sql()
            'SELECT * FROM foo INTERSECT SELECT * FROM bla'

        Args:
            expressions: the SQL code strings.
                If `Expr` instances are passed, they will be used as-is.
            distinct: set the DISTINCT flag if and only if this is true.
            dialect: the dialect used to parse the input expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Intersect expression.
        """
        return intersect(self, *expressions, distinct=distinct, dialect=dialect, **opts)

    def except_(
        self, *expressions: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
    ) -> Except:
        """
        Builds an EXCEPT expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").except_("SELECT * FROM bla").sql()
            'SELECT * FROM foo EXCEPT SELECT * FROM bla'

        Args:
            expressions: the SQL code strings.
                If `Expr` instance are passed, they will be used as-is.
            distinct: set the DISTINCT flag if and only if this is true.
            dialect: the dialect used to parse the input expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Except expression.
        """
        return except_(self, *expressions, distinct=distinct, dialect=dialect, **opts)


class QueryBand(Expression):
    arg_types = {"this": True, "scope": False, "update": False}


class RecursiveWithSearch(Expression):
    arg_types = {"kind": True, "this": True, "expression": True, "using": False}


class With(Expression):
    arg_types = {"expressions": True, "recursive": False, "search": False}

    @property
    def recursive(self) -> bool:
        return bool(self.args.get("recursive"))


class CTE(Expression, DerivedTable):
    arg_types = {
        "this": True,
        "alias": True,
        "scalar": False,
        "materialized": False,
        "key_expressions": False,
    }


class ProjectionDef(Expression):
    arg_types = {"this": True, "expression": True}


class TableAlias(Expression):
    arg_types = {"this": False, "columns": False}

    @property
    def columns(self) -> t.List[t.Any]:
        return self.args.get("columns") or []


class BitString(Expression, Condition):
    pass


class HexString(Expression, Condition):
    arg_types = {"this": True, "is_integer": False}


class ByteString(Expression, Condition):
    arg_types = {"this": True, "is_bytes": False}


class RawString(Expression, Condition):
    pass


class UnicodeString(Expression, Condition):
    arg_types = {"this": True, "escape": False}


class ColumnPosition(Expression):
    arg_types = {"this": False, "position": True}


class ColumnDef(Expression):
    arg_types = {
        "this": True,
        "kind": False,
        "constraints": False,
        "exists": False,
        "position": False,
        "default": False,
        "output": False,
    }

    @property
    def constraints(self) -> t.List[ColumnConstraint]:
        return self.args.get("constraints") or []

    @property
    def kind(self) -> t.Optional[DataType]:
        return self.args.get("kind")


class Changes(Expression):
    arg_types = {"information": True, "at_before": False, "end": False}


class Connect(Expression):
    arg_types = {"start": False, "connect": True, "nocycle": False}


class Prior(Expression):
    pass


class Into(Expression):
    arg_types = {
        "this": False,
        "temporary": False,
        "unlogged": False,
        "bulk_collect": False,
        "expressions": False,
    }


class From(Expression):
    @property
    def name(self) -> str:
        return self.this.name

    @property
    def alias_or_name(self) -> str:
        return self.this.alias_or_name


class Having(Expression):
    pass


class Index(Expression):
    arg_types = {
        "this": False,
        "table": False,
        "unique": False,
        "primary": False,
        "amp": False,  # teradata
        "params": False,
    }


class ConditionalInsert(Expression):
    arg_types = {"this": True, "expression": False, "else_": False}


class MultitableInserts(Expression):
    arg_types = {"expressions": True, "kind": True, "source": True}


class OnCondition(Expression):
    arg_types = {"error": False, "empty": False, "null": False}


class Introducer(Expression):
    arg_types = {"this": True, "expression": True}


class National(Expression):
    pass


class Partition(Expression):
    arg_types = {"expressions": True, "subpartition": False}


class PartitionRange(Expression):
    arg_types = {"this": True, "expression": False, "expressions": False}


class PartitionId(Expression):
    pass


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


class Join(Expression):
    arg_types = {
        "this": True,
        "on": False,
        "side": False,
        "kind": False,
        "using": False,
        "method": False,
        "global_": False,
        "hint": False,
        "match_condition": False,  # Snowflake
        "directed": False,  # Snowflake
        "expressions": False,
        "pivots": False,
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

    @property
    def is_semi_or_anti_join(self) -> bool:
        return self.kind in ("SEMI", "ANTI")

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
                If an `Expr` instance is passed, it will be used as-is.
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
                If an `Expr` instance is passed, it will be used as-is.
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


class Lateral(Expression, UDTF):
    arg_types = {
        "this": True,
        "view": False,
        "outer": False,
        "alias": False,
        "cross_apply": False,  # True -> CROSS APPLY, False -> OUTER APPLY
        "ordinality": False,
    }


class TableFromRows(Expression, UDTF):
    arg_types = {
        "this": True,
        "alias": False,
        "joins": False,
        "pivots": False,
        "sample": False,
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


class Final(Expression):
    pass


class Offset(Expression):
    arg_types = {"this": False, "expression": True, "expressions": False}


class Order(Expression):
    arg_types = {"this": False, "expressions": True, "siblings": False}


class WithFill(Expression):
    arg_types = {
        "from_": False,
        "to": False,
        "step": False,
        "interpolate": False,
    }


class SkipJSONColumn(Expression):
    arg_types = {"regexp": False, "expression": True}


class Cluster(Order):
    pass


class Distribute(Order):
    pass


class Sort(Order):
    pass


class Qualify(Expression):
    pass


class InputOutputFormat(Expression):
    arg_types = {"input_format": False, "output_format": False}


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
            unnest=(
                Unnest(
                    expressions=[
                        maybe_parse(t.cast(ExpOrStr, e), copy=copy, **opts)
                        for e in ensure_list(unnest)
                    ]
                )
                if unnest
                else None
            ),
        )


class QueryOption(Expression):
    arg_types = {"this": True, "expression": False}


class WithTableHint(Expression):
    arg_types = {"expressions": True}


class IndexTableHint(Expression):
    arg_types = {"this": True, "expressions": False, "target": False}


class HistoricalData(Expression):
    arg_types = {"this": True, "kind": True, "expression": True}


class Put(Expression):
    arg_types = {"this": True, "target": True, "properties": False}


class Get(Expression):
    arg_types = {"this": True, "target": True, "properties": False}


class Table(Expression):
    arg_types = {
        "this": False,
        "alias": False,
        "db": False,
        "catalog": False,
        "laterals": False,
        "joins": False,
        "pivots": False,
        "hints": False,
        "system_time": False,
        "version": False,
        "format": False,
        "pattern": False,
        "ordinality": False,
        "when": False,
        "only": False,
        "partition": False,
        "changes": False,
        "rows_from": False,
        "sample": False,
        "indexed": False,
    }

    @property
    def name(self) -> str:
        if not self.this or isinstance(self.this, Func):
            return ""
        return self.this.name

    @property
    def db(self) -> str:
        return self.text("db")

    @property
    def catalog(self) -> str:
        return self.text("catalog")

    @property
    def selects(self) -> t.List[Expr]:
        return []

    @property
    def named_selects(self) -> t.List[str]:
        return []

    @property
    def parts(self) -> t.List[Expr]:
        """Return the parts of a table in order catalog, db, table."""
        parts: t.List[Expr] = []

        for arg in ("catalog", "db", "this"):
            part = self.args.get(arg)

            if isinstance(part, Dot):
                parts.extend(part.flatten())
            elif isinstance(part, Expr):
                parts.append(part)

        return parts

    def to_column(self, copy: bool = True) -> Expr:
        parts = self.parts
        last_part = parts[-1]

        if isinstance(last_part, Identifier):
            col: Expr = column(*reversed(parts[0:4]), fields=parts[4:], copy=copy)  # type: ignore
        else:
            # This branch will be reached if a function or array is wrapped in a `Table`
            col = last_part

        alias = self.args.get("alias")
        if alias:
            col = alias_(col, alias.this, copy=copy)

        return col


class SetOperation(Expression, Query):
    arg_types = {
        "with_": False,
        "this": True,
        "expression": True,
        "distinct": False,
        "by_name": False,
        "side": False,
        "kind": False,
        "on": False,
        **QUERY_MODIFIERS,
    }

    def select(
        self: S,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> S:
        this = maybe_copy(self, copy)
        this.this.unnest().select(*expressions, append=append, dialect=dialect, copy=False, **opts)
        this.expression.unnest().select(
            *expressions, append=append, dialect=dialect, copy=False, **opts
        )
        return this

    @property
    def named_selects(self) -> t.List[str]:
        expression = self
        while isinstance(expression, SetOperation):
            expression = expression.this.unnest()
        return expression.named_selects

    @property
    def is_star(self) -> bool:
        return self.this.is_star or self.expression.is_star

    @property
    def selects(self) -> t.List[Expr]:
        expression = self
        while isinstance(expression, SetOperation):
            expression = expression.this.unnest()
        return expression.selects

    @property
    def left(self) -> Query:
        return self.this

    @property
    def right(self) -> Query:
        return self.expression

    @property
    def kind(self) -> str:
        return self.text("kind").upper()

    @property
    def side(self) -> str:
        return self.text("side").upper()


class Union(SetOperation):
    pass


class Except(SetOperation):
    pass


class Intersect(SetOperation):
    pass


class Values(Expression, UDTF):
    arg_types = {
        "expressions": True,
        "alias": False,
        "order": False,
        "limit": False,
        "offset": False,
    }


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


class Lock(Expression):
    arg_types = {"update": True, "expressions": False, "wait": False, "key": False}


class Select(Expression, Query):
    arg_types = {
        "with_": False,
        "kind": False,
        "expressions": False,
        "hint": False,
        "distinct": False,
        "into": False,
        "from_": False,
        "operation_modifiers": False,
        "exclude": False,
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
                If another `Expr` instance is passed, it will be wrapped in a `From`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="from_",
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
                If another `Expr` instance is passed, it will be wrapped in a `Group`.
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
                If another `Expr` instance is passed, it will be wrapped in a `SORT`.
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
                If another `Expr` instance is passed, it will be wrapped in a `Cluster`.
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

    def select(
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
            arg="expressions",
            append=append,
            dialect=dialect,
            into=Expr,
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
                If an `Expr` instance is passed, it will be used as-is.
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
                If an `Expr` instance is passed, it will be used as-is.
            on: optionally specify the join "on" criteria as a SQL string.
                If an `Expr` instance is passed, it will be used as-is.
            using: optionally specify the join "using" criteria as a SQL string.
                If an `Expr` instance is passed, it will be used as-is.
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
            expression = maybe_parse(expression, into=(Join, Expr), **parse_args)

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
                If an `Expr` instance is passed, it will be used as-is.
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
                If another `Expr` instance is passed, it will be used as-is.
            properties: an optional mapping of table properties
            dialect: the dialect used to parse the input table.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input table.

        Returns:
            The new Create expression.
        """
        instance = maybe_copy(self, copy)
        table_expression = maybe_parse(table, into=Table, dialect=dialect, **opts)

        properties_expression = None
        if properties:
            from sqlglot.expressions.properties import Properties as _Properties

            properties_expression = _Properties.from_dict(properties)

        from sqlglot.expressions.ddl import Create as _Create

        return _Create(
            this=table_expression,
            kind="TABLE",
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
                If an `Expr` instance is passed, it will be used as-is.
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
        selects = []

        for e in self.expressions:
            if e.alias_or_name:
                selects.append(e.output_name)
            elif isinstance(e, Aliases):
                selects.extend([a.name for a in e.aliases])
        return selects

    @property
    def is_star(self) -> bool:
        return any(expression.is_star for expression in self.expressions)

    @property
    def selects(self) -> t.List[Expr]:
        return self.expressions


class Subquery(Expression, DerivedTable, Query):
    is_subquery: t.ClassVar[bool] = True
    arg_types = {
        "this": True,
        "alias": False,
        "with_": False,
        **QUERY_MODIFIERS,
    }

    def unnest(self) -> Query:
        """Returns the first non subquery."""
        expression = self
        while isinstance(expression, Subquery):
            expression = expression.this
        return expression

    def unwrap(self) -> Subquery:
        expression = self
        while expression.same_parent and expression.is_wrapper:
            expression = t.cast(Subquery, expression.parent)
        return expression

    def select(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Subquery:
        this = maybe_copy(self, copy)
        this.unnest().select(*expressions, append=append, dialect=dialect, copy=False, **opts)
        return this

    @property
    def is_wrapper(self) -> bool:
        """
        Whether this Subquery acts as a simple wrapper around another expression.

        SELECT * FROM (((SELECT * FROM t)))
                      ^
                      This corresponds to a "wrapper" Subquery node
        """
        return all(v is None for k, v in self.args.items() if k != "this")

    @property
    def is_star(self) -> bool:
        return self.this.is_star

    @property
    def output_name(self) -> str:
        return self.alias


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
    def fields(self) -> t.List[Expr]:
        return self.args.get("fields", [])


class UnpivotColumns(Expression):
    arg_types = {"this": True, "expressions": True}


class Window(Expression, Condition):
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


class PreWhere(Expression):
    pass


class Where(Expression):
    pass


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


class AnalyzeColumns(Expression):
    pass


class UsingData(Expression):
    pass


class AddPartition(Expression):
    arg_types = {"this": True, "exists": False, "location": False}


class AttachOption(Expression):
    arg_types = {"this": True, "expression": False}


class DropPartition(Expression):
    arg_types = {"expressions": True, "exists": False}


class ReplacePartition(Expression):
    arg_types = {"expression": True, "source": True}


class TranslateCharacters(Expression):
    arg_types = {"this": True, "expression": True, "with_error": False}


class OverflowTruncateBehavior(Expression):
    arg_types = {"this": False, "with_count": True}


class JSON(Expression):
    arg_types = {"this": False, "with_": False, "unique": False}


class JSONPath(Expression):
    arg_types = {"expressions": True, "escape": False}

    @property
    def output_name(self) -> str:
        last_segment = self.expressions[-1].this
        return last_segment if isinstance(last_segment, str) else ""


class JSONPathPart(Expression):
    arg_types = {}


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


class FormatJson(Expression):
    pass


class JSONKeyValue(Expression):
    arg_types = {"this": True, "expression": True}


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


class JSONValue(Expression):
    arg_types = {
        "this": True,
        "path": True,
        "returning": False,
        "on_condition": False,
    }


class JSONValueArray(Expression, Func):
    arg_types = {"this": True, "expression": False}


class OpenJSONColumnDef(Expression):
    arg_types = {"this": True, "kind": True, "path": False, "as_json": False}


class JSONExtractQuote(Expression):
    arg_types = {
        "option": True,
        "scalar": False,
    }


class ScopeResolution(Expression):
    arg_types = {"this": False, "expression": True}


class Stream(Expression):
    pass


class ModelAttribute(Expression):
    arg_types = {"this": True, "expression": True}


class WeekStart(Expression):
    pass


class XMLNamespace(Expression):
    pass


class XMLKeyValueOption(Expression):
    arg_types = {"this": True, "expression": False}


class Semicolon(Expression):
    arg_types = {}


class TableColumn(Expression):
    pass


class Variadic(Expression):
    pass


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


UNWRAPPED_QUERIES = (Select, SetOperation)


def union(
    *expressions: ExpOrStr,
    distinct: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Union:
    """
    Initializes a syntax tree for the `UNION` operation.

    Example:
        >>> union("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo UNION SELECT * FROM bla'

    Args:
        expressions: the SQL code strings, corresponding to the `UNION`'s operands.
            If `Expr` instances are passed, they will be used as-is.
        distinct: set the DISTINCT flag if and only if this is true.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy the expression.
        opts: other options to use to parse the input expressions.

    Returns:
        The new Union instance.
    """
    assert len(expressions) >= 2, "At least two expressions are required by `union`."
    return _apply_set_operation(
        *expressions, set_operation=Union, distinct=distinct, dialect=dialect, copy=copy, **opts
    )


def intersect(
    *expressions: ExpOrStr,
    distinct: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Intersect:
    """
    Initializes a syntax tree for the `INTERSECT` operation.

    Example:
        >>> intersect("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo INTERSECT SELECT * FROM bla'

    Args:
        expressions: the SQL code strings, corresponding to the `INTERSECT`'s operands.
            If `Expr` instances are passed, they will be used as-is.
        distinct: set the DISTINCT flag if and only if this is true.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy the expression.
        opts: other options to use to parse the input expressions.

    Returns:
        The new Intersect instance.
    """
    assert len(expressions) >= 2, "At least two expressions are required by `intersect`."
    return _apply_set_operation(
        *expressions, set_operation=Intersect, distinct=distinct, dialect=dialect, copy=copy, **opts
    )


def except_(
    *expressions: ExpOrStr,
    distinct: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Except:
    """
    Initializes a syntax tree for the `EXCEPT` operation.

    Example:
        >>> except_("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo EXCEPT SELECT * FROM bla'

    Args:
        expressions: the SQL code strings, corresponding to the `EXCEPT`'s operands.
            If `Expr` instances are passed, they will be used as-is.
        distinct: set the DISTINCT flag if and only if this is true.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy the expression.
        opts: other options to use to parse the input expressions.

    Returns:
        The new Except instance.
    """
    assert len(expressions) >= 2, "At least two expressions are required by `except_`."
    return _apply_set_operation(
        *expressions, set_operation=Except, distinct=distinct, dialect=dialect, copy=copy, **opts
    )
