"""sqlglot expressions query."""

from __future__ import annotations

import typing as t

from sqlglot.errors import ParseError
from sqlglot.helper import trait, ensure_list
from sqlglot.expressions.core import (
    Aliases,
    Column,
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

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.expressions.datatypes import DataType
    from sqlglot.expressions.constraints import ColumnConstraint
    from sqlglot.expressions.ddl import Create
    from sqlglot.expressions.array import Unnest
    from sqlglot._typing import E, ParserArgs, ParserNoDialectArgs
    from typing_extensions import Unpack

    S = t.TypeVar("S", bound="SetOperation")
    Q = t.TypeVar("Q", bound="Query")


def _apply_cte_builder(
    instance: E,
    alias: ExpOrStr,
    as_: ExpOrStr,
    recursive: bool | None = None,
    materialized: bool | None = None,
    append: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    scalar: bool | None = None,
    **opts: Unpack[ParserNoDialectArgs],
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
class Selectable(Expr):
    @property
    def selects(self) -> list[Expr]:
        raise NotImplementedError("Subclasses must implement selects")

    @property
    def named_selects(self) -> list[str]:
        return _named_selects(self)


def _named_selects(self: Expr) -> list[str]:
    selectable = t.cast(Selectable, self)
    return [select.output_name for select in selectable.selects]


@trait
class DerivedTable(Selectable):
    @property
    def selects(self) -> list[Expr]:
        this = self.this
        return this.selects if isinstance(this, Query) else []


@trait
class UDTF(DerivedTable):
    @property
    def selects(self) -> list[Expr]:
        alias = self.args.get("alias")
        return alias.columns if alias else []


@trait
class Query(Selectable):
    """Trait for any SELECT/UNION/etc. query expression."""

    @property
    def ctes(self) -> list[CTE]:
        with_ = self.args.get("with_")
        return with_.expressions if with_ else []

    def select(
        self: Q,
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
    ) -> Q:
        raise NotImplementedError("Query objects must implement `select`")

    def subquery(self, alias: ExpOrStr | None = None, copy: bool = True) -> Subquery:
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
        self: Q,
        expression: ExpOrStr | int,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        self: Q,
        expression: ExpOrStr | int,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        recursive: bool | None = None,
        materialized: bool | None = None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        scalar: bool | None = None,
        **opts: Unpack[ParserNoDialectArgs],
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
        self,
        *expressions: ExpOrStr,
        distinct: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        return union(self, *expressions, distinct=distinct, dialect=dialect, copy=copy, **opts)

    def intersect(
        self,
        *expressions: ExpOrStr,
        distinct: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        return intersect(self, *expressions, distinct=distinct, dialect=dialect, copy=copy, **opts)

    def except_(
        self,
        *expressions: ExpOrStr,
        distinct: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        return except_(self, *expressions, distinct=distinct, dialect=dialect, copy=copy, **opts)


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
    def columns(self) -> list[t.Any]:
        return self.args.get("columns") or []


class BitString(Expression, Condition):
    is_primitive = True


class HexString(Expression, Condition):
    arg_types = {"this": True, "is_integer": False}
    is_primitive = True


class ByteString(Expression, Condition):
    arg_types = {"this": True, "is_bytes": False}
    is_primitive = True


class RawString(Expression, Condition):
    is_primitive = True


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
    def constraints(self) -> list[ColumnConstraint]:
        return self.args.get("constraints") or []

    @property
    def kind(self) -> DataType | None:
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
    is_primitive = True


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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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


class Tuple(Expression):
    arg_types = {"expressions": False}

    def isin(
        self,
        *expressions: t.Any,
        query: ExpOrStr | None = None,
        unnest: ExpOrStr | None | list[ExpOrStr] | tuple[ExpOrStr, ...] = None,
        copy: bool = True,
        **opts: Unpack[ParserArgs],
    ) -> In:
        return In(
            this=maybe_copy(self, copy),
            expressions=[convert(e, copy=copy) for e in expressions],
            query=maybe_parse(query, copy=copy, **opts) if query else None,
            unnest=(
                Unnest(
                    expressions=[
                        maybe_parse(e, copy=copy, **opts)
                        for e in t.cast(list[ExpOrStr], ensure_list(unnest))
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


class Table(Expression, Selectable):
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
    def selects(self) -> list[Expr]:
        return []

    @property
    def named_selects(self) -> list[str]:
        return []

    @property
    def parts(self) -> list[Expr]:
        """Return the parts of a table in order catalog, db, table."""
        parts: list[Expr] = []

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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
    ) -> S:
        this = maybe_copy(self, copy)
        this.this.unnest().select(*expressions, append=append, dialect=dialect, copy=False, **opts)
        this.expression.unnest().select(
            *expressions, append=append, dialect=dialect, copy=False, **opts
        )
        return this

    @property
    def named_selects(self) -> list[str]:
        expr: Expr = self
        while isinstance(expr, SetOperation):
            expr = expr.this.unnest()
        return _named_selects(expr)

    @property
    def is_star(self) -> bool:
        return self.this.is_star or self.expression.is_star

    @property
    def selects(self) -> list[Expr]:
        expr: Expr = self
        while isinstance(expr, SetOperation):
            expr = expr.this.unnest()
        return getattr(expr, "selects", [])

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
        self,
        expression: ExpOrStr,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        on: ExpOrStr | list[ExpOrStr] | tuple[ExpOrStr, ...] | None = None,
        using: ExpOrStr | list[ExpOrStr] | tuple[ExpOrStr, ...] | None = None,
        append: bool = True,
        join_type: str | None = None,
        join_alias: Identifier | str | None = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        parse_args: ParserArgs = {"dialect": dialect, **opts}
        try:
            expression = maybe_parse(expression, into=Join, prefix="JOIN", **parse_args)
        except ParseError:
            expression = maybe_parse(expression, into=(Join, Expr), **parse_args)

        join = expression if isinstance(expression, Join) else Join(this=expression)

        if isinstance(join.this, Select):
            join.this.replace(join.this.subquery())

        if join_type:
            new_join: Join = maybe_parse(f"FROM _ {join_type} JOIN _", **parse_args).find(Join)
            method = new_join.method
            side = new_join.side
            kind = new_join.kind

            if method:
                join.set("method", method)
            if side:
                join.set("side", side)
            if kind:
                join.set("kind", kind)

        if on:
            on_exprs: list[ExpOrStr] = ensure_list(on)
            on = and_(*on_exprs, dialect=dialect, copy=copy, **opts)
            join.set("on", on)

        if using:
            using_exprs: list[ExpOrStr] = ensure_list(using)
            join = _apply_list_builder(
                *using_exprs,
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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

    def distinct(self, *ons: ExpOrStr | None, distinct: bool = True, copy: bool = True) -> Select:
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
        properties: dict | None = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
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
    def named_selects(self) -> list[str]:
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
    def selects(self) -> list[Expr]:
        return self.expressions


class Subquery(Expression, DerivedTable, Query):
    is_subquery: t.ClassVar[bool] = True
    arg_types = {
        "this": True,
        "alias": False,
        "with_": False,
        **QUERY_MODIFIERS,
    }

    def unnest(self) -> Expr:
        """Returns the first non subquery."""
        expression: Expr = self
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
        *expressions: ExpOrStr | None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts: Unpack[ParserNoDialectArgs],
    ) -> Subquery:
        this = maybe_copy(self, copy)
        inner = this.unnest()
        if hasattr(inner, "select"):
            inner.select(*expressions, append=append, dialect=dialect, copy=False, **opts)
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
    def fields(self) -> list[Expr]:
        return self.args.get("fields", [])

    def output_columns(self, pre_pivot_columns: t.Iterable[str]) -> list[str]:
        """
        Returns the columns produced by this (UN)PIVOT, in order.

        Example:
            >>> from sqlglot import parse_one, exp
            >>> piv = parse_one("SELECT * FROM t UNPIVOT(val FOR name IN (a, b))").find(exp.Pivot)
            >>> piv.output_columns(["a", "b", "c"])
            ['c', 'name', 'val']

        AST shape:
            PIVOT(SUM(val) FOR name IN ('a', 'b')):
                expressions: aggregate(s), e.g. [Sum(this=Column(val))]
                fields:      [In(this=Column(name), expressions=[Literal('a'), Literal('b')])]
                columns:     optional explicit output identifiers (e.g. set by Snowflake)

            UNPIVOT(val FOR name IN (a, b)):
                expressions: value Identifier(s), or Tuple(Identifiers) for multi-value
                fields:      [In(this=Identifier(name), expressions=[Column(a), Column(b)])]
                             For literal-aliased entries (`a AS 'x'`) the IN expressions
                             are wrapped in PivotAlias(this=Column, alias=Literal).

        Args:
            pre_pivot_columns: Columns visible to the operator before it runs
                (e.g. the source table or subquery's projections).
        """
        if self.unpivot:
            excluded: set[str] = set()
            name_columns: list[Identifier] = []
            for field in self.fields:
                if not isinstance(field, In):
                    continue
                if isinstance(field.this, Identifier):
                    name_columns.append(field.this)
                for e in field.expressions:
                    excluded.update(c.output_name for c in e.find_all(Column))
            value_columns = [
                ident
                for e in self.expressions
                for ident in (e.expressions if isinstance(e, Tuple) else [e])
                if isinstance(ident, Identifier)
            ]
            outputs = [i.name for i in name_columns + value_columns]
        else:
            excluded = {c.output_name for c in self.find_all(Column)}
            outputs = [c.output_name for c in self.args.get("columns") or []]
            if not outputs:
                outputs = [c.alias_or_name for c in self.expressions]

        if not excluded or not outputs:
            return []

        return [c for c in pre_pivot_columns if c not in excluded] + outputs


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
        "format_json": False,
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


class XMLNamespace(Expression):
    pass


class XMLKeyValueOption(Expression):
    arg_types = {"this": True, "expression": False}


class Semicolon(Expression):
    arg_types = {}


class TableColumn(Expression):
    @property
    def output_name(self) -> str:
        return self.name


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
    **opts: Unpack[ParserNoDialectArgs],
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
    **opts: Unpack[ParserNoDialectArgs],
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
    **opts: Unpack[ParserNoDialectArgs],
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
