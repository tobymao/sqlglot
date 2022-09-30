from collections import defaultdict

from sqlglot import alias, exp
from sqlglot.optimizer.scope import Scope, traverse_scope

# Sentinel value that means an outer query selecting ALL columns
SELECT_ALL = object()


def pushdown_projections(expression):
    """
    Rewrite sqlglot AST to remove unused columns projections.

    Example:
        >>> import sqlglot
        >>> sql = "SELECT y.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x) AS y"
        >>> expression = sqlglot.parse_one(sql)
        >>> pushdown_projections(expression).sql()
        'SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y'

    Args:
        expression (sqlglot.Expression): expression to optimize
    Returns:
        sqlglot.Expression: optimized expression
    """
    # Map of Scope to all columns being selected by outer queries.
    referenced_columns = defaultdict(set)

    # We build the scope tree (which is traversed in DFS postorder), then iterate
    # over the result in reverse order. This should ensure that the set of selected
    # columns for a particular scope are completely build by the time we get to it.
    for scope in reversed(traverse_scope(expression)):
        parent_selections = referenced_columns.get(scope, {SELECT_ALL})

        if scope.expression.args.get("distinct"):
            # We can't remove columns SELECT DISTINCT nor UNION DISTINCT
            parent_selections = {SELECT_ALL}

        if isinstance(scope.expression, exp.Union):
            left, right = scope.union_scopes
            referenced_columns[left] = parent_selections
            referenced_columns[right] = parent_selections

        if isinstance(scope.expression, exp.Select):
            _remove_unused_selections(scope, parent_selections)

            # Group columns by source name
            selects = defaultdict(set)
            for col in scope.columns:
                table_name = col.table
                col_name = col.name
                selects[table_name].add(col_name)

            # Push the selected columns down to the next scope
            for name, (_, source) in scope.selected_sources.items():
                if isinstance(source, Scope):
                    columns = selects.get(name) or set()
                    referenced_columns[source].update(columns)

    return expression


def _remove_unused_selections(scope, parent_selections):
    order = scope.expression.args.get("order")

    if order:
        # Assume columns without a qualified table are references to output columns
        order_refs = {c.name for c in order.find_all(exp.Column) if not c.table}
    else:
        order_refs = set()

    new_selections = []
    for selection in scope.selects:
        if (
            SELECT_ALL in parent_selections
            or selection.alias_or_name in parent_selections
            or selection.alias_or_name in order_refs
        ):
            new_selections.append(selection)

    # If there are no remaining selections, just select a single constant
    if not new_selections:
        new_selections.append(alias("1", "_"))

    scope.expression.set("expressions", new_selections)
