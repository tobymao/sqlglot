from collections import defaultdict
from copy import copy

from sqlglot import expressions as exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.helper import select_scope

# Sentinel value that means an outer query selecting ALL columns
SELECT_ALL = "*"


def remove_unused_columns(expression):
    """
    Rewrite sqlglot AST to remove unused columns projections.

    Example:
        >>> import sqlglot
        >>> sql = "SELECT y.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x) AS y"
        >>> expression = sqlglot.parse_one(sql)
        >>> remove_unused_columns(expression).sql()
        'SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y'

    Args:
        expression (sqlglot.Expression): expression to optimize
    Returns:
        sqlglot.Expression: optimized expression
    """
    expression = expression.copy()
    _pushdown_statement(expression, SELECT_ALL)
    return expression


def _pushdown_statement(expression, parent_selections):
    """
    Search SELECT or UNION for columns that can be removed.

    Args:
        expression (exp.Select or exp.Union): expression to search
        parent_selections (set or str): columns being selected by an outer query.
            This can be the special value `SELECT_ALL`, which mean the outer query
            is selecting everything.
    Returns:
        dict: Mapping of selectable names to columns.
        This is used during recursion, so the outer query can:
            1. pullup any selected columns
            2. pushdown selected columns into CTEs
    """
    if isinstance(expression, exp.Select):
        return _pushdown_select(expression, parent_selections)
    if isinstance(expression, exp.Union):
        if expression.args.get("distinct"):
            # We can't remove selections on UNION ALL
            parent_selections = SELECT_ALL

        selections = _pushdown_select(expression.this, parent_selections)
        selections = _merge_selections(
            selections,
            _pushdown_select(expression.args.get("expression"), parent_selections),
        )

        _pushdown_ctes(expression.ctes, selections)

        return selections
    raise OptimizeError(f"Unexpected statement type: {type(expression)}")


def _pushdown_select(expression, parent_selections):
    """
    Search SELECT for columns that can be removed.

    Returns:
         Same as `_pushdown_statement`
    """
    scope = select_scope(expression)

    # Collect a map of all referenced columns
    columns = {}
    for column in scope.columns:
        selectable_name = column.text("table")
        column_name = column.text("this")
        if not selectable_name:
            msg = (
                "Expected all columns to have table prefixes. "
                "Did you run 'qualify_columns' first?\n"
                f"Received: {column_name}"
            )
            raise OptimizeError(msg)

        # Use the Expression identity for the key since Expressions are hashed by value.
        columns[id(column)] = column

    # Collect all the selections
    if not expression.args.get("distinct"):
        columns = _remove_unused_columns(expression, columns, parent_selections)

    for subquery in scope.subqueries:
        # Subqueries (as opposed to "derived_tables") aren't "selectable".
        # So the none of the columns in the current scope to reference these.
        _pushdown_statement(subquery, SELECT_ALL)

    # Now that we've removed all the unused columns from the selections, let's
    # build a map of all the columns we're selecting from derived tables.
    derived_table_selections = defaultdict(set)
    for column in columns.values():
        derived_table_selections[column.text("table")].add(column.text("this"))

    for subquery in scope.derived_tables:
        _pushdown_statement(subquery.this, derived_table_selections[subquery.alias])

    _pushdown_ctes(scope.ctes, derived_table_selections)

    # Push the selections back UP so they can be used by CTEs in outer queries
    return derived_table_selections


def _pushdown_ctes(ctes, selections):
    if not ctes:
        return

    # Iterate in reversed order as a CTE can reference outputs in previous CTEs
    for cte in reversed(ctes):
        selections = _merge_selections(
            selections, _pushdown_statement(cte.this, selections.get(cte.alias, set()))
        )


def _remove_unused_columns(expression, columns, parent_selections):
    columns = copy(columns)

    new_selections = []
    for selection in expression.selects:
        if not isinstance(selection, exp.Alias):
            msg = (
                "Expected all selections to have aliases. "
                "Did you run 'qualify_columns' first?\n"
                f"Received: {selection}"
            )
            raise OptimizeError(msg)

        if parent_selections == SELECT_ALL or selection.alias in parent_selections:
            new_selections.append(selection)
        else:
            # Pop the column out of the set of all columns.
            # Later, we'll use this set of columns to pushdown the selected columns from inner queries.
            for column in selection.find_all(exp.Column):
                columns.pop(id(column))

    # If there are no remaining selections, just select a single constant
    if not new_selections:
        new_selections.append(exp.alias_("1", "_"))

    expression.set("expressions", new_selections)

    return columns


def _merge_selections(*selections):
    result = defaultdict(set)
    for s in selections:
        for name, columns in s.items():
            result[name].update(columns)
    return result
