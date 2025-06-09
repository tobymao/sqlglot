from __future__ import annotations

import typing as t
from collections import defaultdict

from sqlglot import alias, exp
from sqlglot.optimizer.qualify_columns import Resolver
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import ensure_schema
from sqlglot.errors import OptimizeError
from sqlglot.helper import seq_get

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.schema import Schema
    from sqlglot.dialects.dialect import DialectType

# Sentinel value that means an outer query selecting ALL columns
SELECT_ALL = object()


# Selection to use if selection list is empty
def default_selection(is_agg: bool) -> exp.Alias:
    return alias(exp.Max(this=exp.Literal.number(1)) if is_agg else "1", "_")


def pushdown_projections(
    expression: E,
    schema: t.Optional[t.Dict | Schema] = None,
    remove_unused_selections: bool = True,
    dialect: DialectType = None,
) -> E:
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
    schema = ensure_schema(schema, dialect=dialect)
    source_column_alias_count: t.Dict[exp.Expression | Scope, int] = {}
    referenced_columns: t.DefaultDict[Scope, t.Set[str | object]] = defaultdict(set)

    # We build the scope tree (which is traversed in DFS postorder), then iterate
    # over the result in reverse order. This should ensure that the set of selected
    # columns for a particular scope are completely build by the time we get to it.
    for scope in reversed(traverse_scope(expression)):
        parent_selections = referenced_columns.get(scope, {SELECT_ALL})
        alias_count = source_column_alias_count.get(scope, 0)

        # We can't remove columns SELECT DISTINCT nor UNION DISTINCT.
        if scope.expression.args.get("distinct"):
            parent_selections = {SELECT_ALL}

        if isinstance(scope.expression, exp.SetOperation):
            set_op = scope.expression
            if not (set_op.kind or set_op.side):
                # Do not optimize this set operation if it's using the BigQuery specific
                # kind / side syntax (e.g INNER UNION ALL BY NAME) which changes the semantics of the operation
                left, right = scope.union_scopes
                if len(left.expression.selects) != len(right.expression.selects):
                    scope_sql = scope.expression.sql(dialect=dialect)
                    raise OptimizeError(
                        f"Invalid set operation due to column mismatch: {scope_sql}."
                    )

                referenced_columns[left] = parent_selections

                if any(select.is_star for select in right.expression.selects):
                    referenced_columns[right] = parent_selections
                elif not any(select.is_star for select in left.expression.selects):
                    if scope.expression.args.get("by_name"):
                        referenced_columns[right] = referenced_columns[left]
                    else:
                        referenced_columns[right] = {
                            right.expression.selects[i].alias_or_name
                            for i, select in enumerate(left.expression.selects)
                            if SELECT_ALL in parent_selections
                            or select.alias_or_name in parent_selections
                        }

        if isinstance(scope.expression, exp.Select):
            if remove_unused_selections:
                _remove_unused_selections(scope, parent_selections, schema, alias_count)

            if scope.expression.is_star:
                continue

            # Group columns by source name
            selects = defaultdict(set)
            for col in scope.columns:
                table_name = col.table
                col_name = col.name
                selects[table_name].add(col_name)

            # Push the selected columns down to the next scope
            for name, (node, source) in scope.selected_sources.items():
                if isinstance(source, Scope):
                    select = seq_get(source.expression.selects, 0)

                    if scope.pivots or isinstance(select, exp.QueryTransform):
                        columns = {SELECT_ALL}
                    else:
                        columns = selects.get(name) or set()

                    referenced_columns[source].update(columns)

                column_aliases = node.alias_column_names
                if column_aliases:
                    source_column_alias_count[source] = len(column_aliases)

    return expression


def _remove_unused_selections(scope, parent_selections, schema, alias_count):
    order = scope.expression.args.get("order")

    if order:
        # Assume columns without a qualified table are references to output columns
        order_refs = {c.name for c in order.find_all(exp.Column) if not c.table}
    else:
        order_refs = set()

    new_selections = []
    removed = False
    star = False
    is_agg = False

    select_all = SELECT_ALL in parent_selections

    for selection in scope.expression.selects:
        name = selection.alias_or_name

        if select_all or name in parent_selections or name in order_refs or alias_count > 0:
            new_selections.append(selection)
            alias_count -= 1
        else:
            if selection.is_star:
                star = True
            removed = True

        if not is_agg and selection.find(exp.AggFunc):
            is_agg = True

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
        new_selections.append(default_selection(is_agg))

    scope.expression.select(*new_selections, append=False, copy=False)

    if removed:
        scope.clear_cache()
