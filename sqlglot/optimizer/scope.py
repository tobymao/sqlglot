from copy import copy
from enum import Enum, auto

from sqlglot import expressions as exp
from sqlglot.errors import OptimizeError


class ScopeType(Enum):
    ROOT = auto()
    SUBQUERY = auto()
    DERIVED_TABLE = auto()
    CTE = auto()
    UNION = auto()


class Scope:
    """
    Selection scope.

    Attributes:
        expression (exp.Select|exp.Union): Root expression of this scope
        sources (dict[str, exp.Table|Scope]): Mapping of source name to either
            a Table expression or another Scope instance. For example:
                SELECT * FROM x                     {"x": Table(this="x")}
                SELECT * FROM x AS y                {"y": Table(this="x")}
                SELECT * FROM (SELECT ...) AS y     {"y": Scope(...)}
        outer_column_list (list[str]): If this is a derived table or CTE, and the outer query
            defines a column list of it's alias of this scope, this is that list of columns.
            For example:
                SELECT * FROM (SELECT ...) AS y(col1, col2)
            The inner query would have `["col1", "col2"]` for its `outer_column_list`
        parent (Scope): Parent scope
        scope_type (ScopeType): Type of this scope, relative to it's parent
        subquery_scopes (list[Scope]): List of all child scopes for subqueries.
            This does not include derived tables or CTEs.
        union (tuple[Scope, Scope]): If this Scope is for a Union expression, this will be
            a tuple of the left and right child scopes.
    """

    def __init__(
        self,
        expression,
        sources=None,
        outer_column_list=None,
        parent=None,
        scope_type=ScopeType.ROOT,
    ):
        self.expression = expression
        self.sources = sources or {}
        self.outer_column_list = outer_column_list or []
        self.parent = parent
        self.scope_type = scope_type
        self.subquery_scopes = []
        self.union = None

    def branch(self, expression, scope_type, add_sources=None, **kwargs):
        """Branch from the current scope to a new, inner scope"""
        sources = copy(self.sources)
        if add_sources:
            sources.update(add_sources)
        return Scope(
            expression=expression,
            sources=sources,
            parent=self,
            scope_type=scope_type,
            **kwargs,
        )

    def _find_in_scope(self, predicate):
        return [
            node
            for node, *_ in _bfs_until_next_scope(self.expression)
            if node is not self.expression and predicate(node)
        ]

    @property
    def tables(self):
        """
        List of tables in this scope.

        Returns:
            list[exp.Table]: tables
        """
        return self._find_in_scope(lambda n: isinstance(n, exp.Table))

    @property
    def ctes(self):
        """
        List of CTEs in this scope.

        Returns:
            list[exp.CTE]: ctes
        """
        return self._find_in_scope(lambda n: isinstance(n, exp.CTE))

    @property
    def derived_tables(self):
        """
        List of derived tables in this scope.

        For example:
            SELECT * FROM (SELECT ...) <- that's a derived table

        Returns:
            list[exp.Subquery]: derived tables
        """
        return self._find_in_scope(lambda n: isinstance(n, exp.Subquery))

    @property
    def subqueries(self):
        """
        List of subqueries in this scope.

        For example:
            SELECT * FROM x WHERE a IN (SELECT ...) <- that's a subquery

        Returns:
            list[exp.Subqueryable]: subqueries
        """
        return self._find_in_scope(
            lambda n: isinstance(n, exp.Subqueryable)
            and not isinstance(n.parent, (exp.CTE, exp.Subquery))
        )

    @property
    def columns(self):
        """
        List of columns in this scope.

        Returns:
            list[exp.Column]: Column instances in this scope, plus any
                Columns that reference this scope from correlated subqueries.
        """
        columns = self._find_in_scope(
            lambda n: isinstance(n, exp.Column) and not isinstance(n.this, exp.Star)
        )

        external_columns = [
            column
            for scope in self.subquery_scopes
            for column in scope.external_columns
        ]

        return [
            c
            for c in columns + external_columns
            if not self._is_reference_to_named_select(c)
        ]

    @property
    def selected_sources(self):
        """
        Mapping of nodes and sources that are actually selected from in this scope.

        That is, all tables in a schema are selectable at any point. But a
        table only becomes a selected source if it's included in a FROM or JOIN clause.

        Returns:
            dict[str, (exp.Table|exp.Subquery, exp.Table|Scope)]: selected sources and nodes
        """
        referenced_names = []

        for table in self.tables:
            referenced_names.append(
                (
                    table.parent.alias
                    if isinstance(table.parent, exp.Alias)
                    else table.name,
                    table,
                )
            )
        for derived_table in self.derived_tables:
            referenced_names.append((derived_table.alias, derived_table.this))

        result = {}

        for name, node in referenced_names:
            if name in self.sources:
                result[name] = (node, self.sources[name])

        return result

    @property
    def selects(self):
        """
        Select expressions of this scope.

        For example, for the following expression:
            SELECT 1 as a, 2 as b FROM x

        The outputs are the "1 as a" and "2 as b" expressions.

        Returns:
            list[exp.Expression]: expressions
        """
        if isinstance(self.expression, exp.Union):
            return []
        return self.expression.selects

    @property
    def external_columns(self):
        """
        Columns that appear to reference sources in outer scopes.

        Returns:
            list[exp.Column]: Column instances that don't reference
                sources in the current scope.
        """
        return [c for c in self.columns if c.text("table") not in self.sources]

    def source_columns(self, source_name):
        """
        Get all columns in the current scope for a particular source.

        Args:
            source_name (str): Name of the source
        Returns:
            list[exp.Column]: Column instances that reference `source_name`
        """
        return [
            column for column in self.columns if column.text("table") == source_name
        ]

    @property
    def is_subquery(self):
        """Determine if this scope is a subquery"""
        return self.scope_type == ScopeType.SUBQUERY

    @property
    def is_correlated_subquery(self):
        """Determine if this scope is a correlated subquery"""
        return bool(self.is_subquery and self.external_columns)

    def rename_source(self, old_name, new_name):
        """Rename a source in this scope"""
        columns = self.sources.pop(old_name or "", [])
        self.sources[new_name] = columns

    def _is_reference_to_named_select(self, column):
        """Determine if column is a reference to a SELECT output column"""
        table_name = column.text("table")
        column_name = column.text("this")

        # Expression.named_selects also includes unaliased columns.
        # In this case, we want to be sure to only include selects that are aliased.
        aliased_outputs = {
            e.alias
            for e in self.expression.args.get("expressions", [])
            if isinstance(e, exp.Alias)
        }

        return (
            not table_name
            and column.find_ancestor(exp.Group, exp.Order)
            and column_name in aliased_outputs
        )


def traverse_scope(expression):
    """
    Traverse an expression by it's "scopes".

    "Scope" represents the current context of a Select statement.

    This is helpful for optimizing queries, where we need more information than
    the expression tree itself. For example, we might care about the source
    names within a subquery. Returns a list because a generator could result in
    incomplete properties which is confusing.

    Examples:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT a FROM (SELECT a FROM x) AS y")
        >>> scopes = traverse_scope(expression)
        >>> scopes[0].expression.sql(), list(scopes[0].sources)
        ('SELECT a FROM x', ['x'])
        >>> scopes[1].expression.sql(), list(scopes[1].sources)
        ('SELECT a FROM (SELECT a FROM x) AS y', ['y'])

    Args:
        expression (exp.Expression): expression to traverse
    Returns:
        List[Scope]: scope instances
    """
    return list(_traverse_scope(Scope(expression)))


def _traverse_scope(scope):
    if isinstance(scope.expression, exp.Select):
        yield from _traverse_select(scope)
    elif isinstance(scope.expression, exp.Union):
        yield from _traverse_union(scope)
    else:
        raise OptimizeError(f"Unexpected expression type: {scope.expression}")
    yield scope


def _traverse_select(scope):
    yield from _traverse_derived_tables(scope.ctes, scope, ScopeType.CTE)
    yield from _traverse_subqueries(scope)
    yield from _traverse_derived_tables(
        scope.derived_tables, scope, ScopeType.DERIVED_TABLE
    )
    _add_table_sources(scope)


def _traverse_union(scope):
    ctes = scope.ctes
    if ctes:
        yield from _traverse_derived_tables(
            ctes, scope, scope_type=ScopeType.DERIVED_TABLE
        )

    # The last scope to be yield should be the top most scope
    left = None
    for left in _traverse_scope(
        scope.branch(scope.expression.left, scope_type=ScopeType.UNION)
    ):
        yield left

    right = None
    for right in _traverse_scope(
        scope.branch(scope.expression.right, scope_type=ScopeType.UNION)
    ):
        yield right

    scope.union = (left, right)


def _traverse_derived_tables(derived_tables, scope, scope_type):
    sources = {}
    chain = scope_type == ScopeType.CTE
    for derived_table in derived_tables:
        for child_scope in _traverse_scope(
            scope.branch(
                derived_table.this,
                add_sources=sources if chain else None,
                outer_column_list=derived_table.alias_column_names,
                scope_type=scope_type,
            )
        ):
            yield child_scope
            # Tables without aliases will be set as ""
            # This shouldn't be a problem once qualify_columns runs, as it adds aliases on everything.
            # Until then, this means that only a single, unaliased derived table is allowed (rather,
            # the latest one wins.
            sources[derived_table.alias] = child_scope
    scope.sources.update(sources)


def _add_table_sources(scope):
    sources = {}
    for table in scope.tables:
        table_name = table.text("this")

        if isinstance(table.parent, exp.Alias):
            source_name = table.parent.alias
        else:
            source_name = table_name

        if table_name in scope.sources:
            # This is a reference to a parent source (e.g. a CTE), not an actual table.
            scope.sources[source_name] = scope.sources[table_name]
        elif source_name in scope.sources:
            raise OptimizeError(f"Duplicate table name: {source_name}")
        else:
            sources[source_name] = table

    scope.sources.update(sources)


def _traverse_subqueries(scope):
    for subquery in scope.subqueries:
        for child_scope in _traverse_scope(
            scope.branch(subquery, scope_type=ScopeType.SUBQUERY)
        ):
            yield child_scope
            scope.subquery_scopes.append(child_scope)


def _bfs_until_next_scope(expression):
    """
    Walk the expression tree in BFS order yielding all nodes until a Select or Union instance is found.

    This will yield the Select or Union node itself, but it won't recurse any further.
    """
    yield from expression.bfs(
        prune=lambda n, *_: isinstance(n, exp.Subqueryable) and n is not expression
    )
