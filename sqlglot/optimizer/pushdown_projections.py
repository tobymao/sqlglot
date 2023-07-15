from collections import defaultdict

from sqlglot import alias, exp
from sqlglot.optimizer.qualify_columns import Resolver
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import ensure_schema

# Sentinel value that means an outer query selecting ALL columns
SELECT_ALL = object()

# Selection to use if selection list is empty
DEFAULT_SELECTION = lambda: alias("1", "_")


def pushdown_projections(expression, schema=None, remove_unused_selections=True):
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
        remove_unused_selections (bool): remove selects that are unused
    Returns:
        sqlglot.Expression: optimized expression
    """
    # Map of Scope to all columns being selected by outer queries.
    schema = ensure_schema(schema)
    referenced_columns = defaultdict(set)

    # We build the scope tree (which is traversed in DFS postorder), then iterate
    # over the result in reverse order. This should ensure that the set of selected
    # columns for a particular scope are completely build by the time we get to it.
    for scope in reversed(traverse_scope(expression)):
        parent_selections = referenced_columns.get(scope, {SELECT_ALL})

        if scope.expression.args.get("distinct") or scope.parent and scope.parent.pivots:
            # We can't remove columns SELECT DISTINCT nor UNION DISTINCT. The same holds if
            # we select from a pivoted source in the parent scope.
            parent_selections = {SELECT_ALL}

        if isinstance(scope.expression, exp.Union):
            left, right = scope.union_scopes
            referenced_columns[left] = parent_selections

            if any(select.is_star for select in right.expression.selects):
                referenced_columns[right] = parent_selections
            elif not any(select.is_star for select in left.expression.selects):
                referenced_columns[right] = [
                    right.expression.selects[i].alias_or_name
                    for i, select in enumerate(left.expression.selects)
                    if SELECT_ALL in parent_selections or select.alias_or_name in parent_selections
                ]

        if isinstance(scope.expression, exp.Select):
            if remove_unused_selections:
                _remove_unused_selections(scope, parent_selections, schema)

            if scope.expression.is_star:
                continue

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


def _remove_unused_selections(scope, parent_selections, schema):
    order = scope.expression.args.get("order")

    if order:
        # Assume columns without a qualified table are references to output columns
        order_refs = {c.name for c in order.find_all(exp.Column) if not c.table}
    else:
        order_refs = set()

    new_selections = []
    removed = False
    star = False

    for selection in scope.expression.selects:
        name = selection.alias_or_name

        if SELECT_ALL in parent_selections or name in parent_selections or name in order_refs:
            new_selections.append(selection)
        else:
            if selection.is_star:
                star = True
            removed = True

    if star:
        resolver = Resolver(scope, schema)
        names = {s.alias_or_name for s in new_selections}

        for name in sorted(parent_selections):
            if name not in names:
                new_selections.append(
                    alias(exp.column(name, table=resolver.get_table(name)), name, copy=False)
                )

    # If there are no remaining selections, just select a single constant
    if not new_selections:
        new_selections.append(DEFAULT_SELECTION())

    scope.expression.select(*new_selections, append=False, copy=False)

    if removed:
        scope.clear_cache()
