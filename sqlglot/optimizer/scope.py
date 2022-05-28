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
        selectables (dict[str, exp.Table|Scope]): Mapping of selectable name to either
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
        columns (list[exp.Column]): list of columns in this scope
        tables (list[exp.Table]): list of tables in this scope
        derived_tables (list[exp.Subquery]): list of derived tables in this scope.
            For example:
                SELECT * FROM (SELECT ...) <- that's a derived table
        subqueries (list[exp.Select]): list of subqueries in this scope.
            For example:
                SELECT * FROM x WHERE a IN (SELECT ...) < that's a subquery
        ctes (list[exp.CTE]): list of ctes in this scope
    """

    def __init__(
        self,
        expression,
        selectables=None,
        outer_column_list=None,
        parent=None,
        scope_type=ScopeType.ROOT,
    ):
        self.expression = expression
        self.selectables = selectables or {}
        self.outer_column_list = outer_column_list or []
        self.parent = parent
        self.scope_type = scope_type
        self.subquery_scopes = []

        self.union = None
        self.columns = []
        self.tables = []
        self.derived_tables = []
        self.subqueries = []
        self.ctes = []

    def branch(self, expression, scope_type, add_selectables=None, **kwargs):
        """Branch from the current scope to a new, inner scope"""
        selectables = copy(self.selectables)
        if add_selectables:
            selectables.update(add_selectables)
        return Scope(
            expression=expression,
            selectables=selectables,
            parent=self,
            scope_type=scope_type,
            **kwargs,
        )

    @property
    def referenced_selectables(self):
        """
        Mapping of selectables that are actually selected from in this scope.

        For example, all tables in a schema are selectable at any point. But a
        table only becomes a referenced selectable if it's included in a FROM or JOIN clause.

        Returns:
            dict[str, exp.Table|Scope]: referenced selectables
        """
        referenced_names = []

        for table in self.tables:
            if isinstance(table.parent, exp.Alias):
                referenced_names.append(table.parent.alias)
            else:
                referenced_names.append(table.name)
        for derived_table in self.derived_tables:
            referenced_names.append(derived_table.alias)

        result = {}

        for name in referenced_names:
            if name in self.selectables:
                result[name] = self.selectables[name]

        return result

    @property
    def referenced_scopes(self):
        """
        Mapping of selectables that are actually selected from in this scope.

        This is like `referenced_selectables`, except it only returns referenced
        scopes, not referenced tables.

        Returns:
            dict[str, Scope]: referenced scopes
        """
        return {
            k: v for k, v in self.referenced_selectables.items() if isinstance(v, Scope)
        }

    @property
    def outputs(self):
        """
        Column outputs of this scope.

        For example, for the following expression:
            SELECT 1 as a, 2 as b FROM x

        The outputs are ["a", "b"]

        Returns:
            list[str]: outputs
        """
        return self.expression.named_selects

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
    def correlations(self):
        """
        Columns referenced by correlated subqueries.

        Returns:
            list[tuple[exp.Column, Scope]]: List of Column instances from
                child scopes that reference this scope, along with the child
                scope instance itself.
        """
        result = []
        for scope in self.subquery_scopes:
            for column in scope.external_references:
                result.append((column, scope))
        return result

    @property
    def external_references(self):
        """
        Columns that appear to reference selectables in outer scopes.

        Returns:
            list[exp.Column]: Column instances that don't reference
                tables in the current scope.
        """
        return [c for c in self.references if c.text("table") not in self.selectables]

    @property
    def references(self):
        """
        All column references in the current scope.

        Returns:
            list[exp.Column]: Column instances in this scope, plus any
                Columns that reference this scope from correlated subqueries.
        """
        return [
            c
            for c in self.columns + [c for c, _ in self.correlations]
            if not self._is_output_reference(c)
        ]

    @property
    def is_subquery(self):
        """Determine if this scope is a subquery"""
        return self.scope_type == ScopeType.SUBQUERY

    @property
    def is_correlated_subquery(self):
        """Determine if this scope is a correlated subquery"""
        return self.is_subquery and self.external_references

    def rename_selectable(self, old_name, new_name):
        """Rename a selectable in this scope"""
        columns = self.selectables.pop(old_name or "", [])
        self.selectables[new_name] = columns

    def _is_output_reference(self, column):
        """Determine if column is a reference to a SELECT output column"""
        table_name = column.text("table")
        column_name = column.text("this")

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
    the expression tree itself. For example, we might care about the selectable
    names within a subquery. Returns a list because a generator could result in
    incomplete properties which is confusing.

    Examples:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT a FROM (SELECT a FROM x) AS y")
        >>> scopes = traverse_scope(expression)
        >>> scopes[0].expression.sql(), list(scopes[0].selectables)
        ('SELECT a FROM x', ['x'])
        >>> scopes[1].expression.sql(), list(scopes[1].selectables)
        ('SELECT a FROM (SELECT a FROM x) AS y', ['y'])

    Args:
        expression (exp.Expression): expression to traverse
    Returns:
        List[Scope]: scope instances
    """
    return list(_traverse_scope(Scope(expression)))


def _traverse_scope(scope):
    # Collect all the relevant Expression parts in this scope
    for node, parent, _ in _bfs_until_next_scope(scope.expression):
        if node is scope.expression:
            continue  # Skip this node itself - we only care about children
        if isinstance(node, (exp.Select, exp.Union)):
            if isinstance(parent, exp.CTE):
                scope.ctes.append(parent)
            elif isinstance(parent, exp.Subquery):
                scope.derived_tables.append(parent)
            else:
                scope.subqueries.append(node)
        elif isinstance(node, exp.Table):
            scope.tables.append(node)
        elif isinstance(node, exp.Column) and not isinstance(node.this, exp.Star):
            scope.columns.append(node)

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
    _add_table_selectables(scope)


def _traverse_union(scope):
    if scope.ctes:
        yield from _traverse_derived_tables(
            scope.ctes, scope, scope_type=ScopeType.DERIVED_TABLE
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
    selectables = {}
    chain = scope_type == ScopeType.CTE
    for derived_table in derived_tables:
        for child_scope in _traverse_scope(
            scope.branch(
                derived_table.this,
                add_selectables=selectables if chain else None,
                outer_column_list=derived_table.alias_column_names,
                scope_type=scope_type,
            )
        ):
            yield child_scope
            # Tables without aliases will be set as ""
            # This shouldn't be a problem once qualify_columns runs, as it adds aliases on everything.
            # Until then, this means that only a single, unaliased derived table is allowed (rather,
            # the latest one wins.
            selectables[derived_table.alias] = child_scope
    scope.selectables.update(selectables)


def _add_table_selectables(scope):
    selectables = {}
    for table in scope.tables:
        table_name = table.text("this")

        if isinstance(table.parent, exp.Alias):
            selectable_table = table.parent.alias
        else:
            selectable_table = table_name

        if table_name in scope.selectables:
            # This is a reference to a parent selectable (e.g. a CTE), not an actual table.
            scope.selectables[selectable_table] = scope.selectables[table_name]
        elif selectable_table in scope.selectables:
            raise OptimizeError(f"Duplicate table name: {selectable_table}")
        else:
            selectables[selectable_table] = table

    scope.selectables.update(selectables)


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
        prune=lambda n, *_: isinstance(n, (exp.Select, exp.Union))
        and n is not expression
    )
