from __future__ import annotations

import typing as t
from collections import defaultdict

from sqlglot import alias, exp
from sqlglot.optimizer.journal import Journal, record
from sqlglot.optimizer.qualify_columns import Resolver
from sqlglot.optimizer.scope import Scope, find_in_scope, traverse_scope
from sqlglot.schema import ensure_schema
from sqlglot.errors import OptimizeError
from sqlglot.helper import seq_get

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.schema import Schema
    from sqlglot.dialects.dialect import DialectType

# Sentinel value that means an outer query selecting ALL columns
SELECT_ALL = object()

# Set-returning (table) functions multiply the rows of the entire query, so a projection
# containing one affects the cardinality of every output column and must never be pruned,
# even when the projection itself is otherwise unreferenced. Posexplode and the *Outer
# variants are subclasses of Explode, so matching Explode covers them too.
SET_RETURNING_FUNCTIONS = (exp.Explode, exp.Inline, exp.Unnest)


def _is_self_referencing_cte(scope: Scope) -> bool:
    cte = scope.expression.parent
    return (
        isinstance(cte, exp.CTE)
        and isinstance(cte.parent, exp.With)
        and cte.parent.recursive
        and any(
            not table.db and table.name == cte.alias
            for table in scope.expression.find_all(exp.Table)
        )
    )


# Selection to use if selection list is empty
def default_selection(is_agg: bool) -> exp.Alias:
    return alias(exp.Max(this=exp.Literal.number(1)) if is_agg else "1", "_").assert_is(exp.Alias)


def pushdown_projections(
    expression: E,
    schema: dict[str, object] | Schema | None = None,
    remove_unused_selections: bool = True,
    dialect: DialectType = None,
    journal: Journal | None = None,
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
        expression (sqlglot.Expr): expression to optimize
        remove_unused_selections (bool): remove selects that are unused
    Returns:
        sqlglot.Expr: optimized expression
    """
    # Map of Scope to all columns being selected by outer queries.
    schema = ensure_schema(schema, dialect=dialect)
    source_column_alias_count: dict[exp.Expr | Scope, int] = {}
    referenced_columns: defaultdict[Scope, set[str | object]] = defaultdict(set)

    # We build the scope tree (which is traversed in DFS postorder), then iterate
    # over the result in reverse order. This should ensure that the set of selected
    # columns for a particular scope are completely build by the time we get to it.
    for scope in reversed(traverse_scope(expression)):
        parent_selections = referenced_columns.get(scope, {SELECT_ALL})
        alias_count = source_column_alias_count.get(scope, 0)

        # We can't remove columns SELECT DISTINCT nor UNION DISTINCT.
        if scope.expression.args.get("distinct"):
            parent_selections = {SELECT_ALL}

        # A recursive CTE's body reads the CTE's own output, so its projections
        # can't be pruned based only on what the enclosing query selects.
        if _is_self_referencing_cte(scope):
            parent_selections = {SELECT_ALL}

        if isinstance(scope.expression, exp.SetOperation):
            set_op = scope.expression
            if set_op.kind or set_op.side:
                # Do not optimize this set operation if it's using the BigQuery specific
                # kind / side syntax (e.g INNER UNION ALL BY NAME) which changes the semantics of the operation
                continue

            left, right = scope.union_scopes
            le = left.expression
            re = right.expression

            if not (isinstance(le, exp.Selectable) and isinstance(re, exp.Selectable)):
                continue

            if len(le.selects) != len(re.selects):
                scope_sql = scope.expression.sql(dialect=dialect)
                raise OptimizeError(f"Invalid set operation due to column mismatch: {scope_sql}.")

            referenced_columns[left] = parent_selections

            if re.is_star:
                referenced_columns[right] = parent_selections
            elif not le.is_star:
                if scope.expression.args.get("by_name"):
                    referenced_columns[right] = referenced_columns[left]
                else:
                    referenced_columns[right] = {
                        re.selects[i].alias_or_name
                        for i, select in enumerate(le.selects)
                        if SELECT_ALL in parent_selections
                        or select.alias_or_name in parent_selections
                    }

        if isinstance(scope.expression, exp.Select):
            if remove_unused_selections:
                _remove_unused_selections(scope, parent_selections, schema, alias_count, journal)

            if scope.scans_all_subscope_columns:
                continue

            # Group columns by source name
            selects: dict[str, set[object]] = defaultdict(set)
            for col in scope.columns:
                table_name = col.table
                col_name = col.name
                selects[table_name].add(col_name)

            # Push the selected columns down to the next scope
            for name, (node, source) in scope.selected_sources.items():
                if isinstance(source, Scope) and isinstance(source.expression, exp.Selectable):
                    select = seq_get(source.expression.selects, 0)

                    if scope.pivots or isinstance(select, exp.QueryTransform):
                        columns: set[object] = {SELECT_ALL}
                    else:
                        columns = selects.get(name) or set()

                    referenced_columns[source].update(columns)

                column_aliases = node.alias_column_names
                if column_aliases:
                    source_column_alias_count[source] = len(column_aliases)

    return expression


def _remove_unused_selections(scope, parent_selections, schema, alias_count, journal=None):
    order = scope.expression.args.get("order")

    if order:
        # Assume columns without a qualified table are references to output columns
        order_refs = {c.name for c in order.find_all(exp.Column) if not c.table}
    else:
        order_refs = set()

    # Resolve bare GROUP BY ordinals before pruning
    ordinal_refs = _bare_group_by_ordinal_refs(scope.expression)
    group_ordinal_selection_ids = {id(selection) for _, selection in ordinal_refs}

    new_selections = []
    removed = False
    star = False
    is_agg = False

    select_all = SELECT_ALL in parent_selections

    for selection in scope.expression.selects:
        name = selection.alias_or_name

        if (
            select_all
            or name in parent_selections
            or name in order_refs
            or alias_count > 0
            or id(selection) in group_ordinal_selection_ids
        ):
            new_selections.append(selection)
            alias_count -= 1
        elif find_in_scope(selection, *SET_RETURNING_FUNCTIONS):
            # A set-returning function multiplies the rows of the whole query, so this
            # projection affects the cardinality of every output column and must be kept
            # even though it is otherwise unreferenced. It is not a positional alias slot,
            # so alias_count is left untouched.
            new_selections.append(selection)
        else:
            if selection.is_star:
                star = True
            removed = True

        if not is_agg and find_in_scope(selection, exp.AggFunc):
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

    if journal is not None and removed:
        record(journal, scope.expression, "expressions")

    scope.expression.select(*new_selections, append=False, copy=False)

    # Rewrite bare GROUP BY ordinals to their positions in the pruned SELECT list
    if ordinal_refs:
        new_pos = {id(selection): i + 1 for i, selection in enumerate(new_selections)}
        for node, old_selection in ordinal_refs:
            pos = new_pos.get(id(old_selection))
            if pos is not None and int(node.this) != pos:
                if journal is not None:
                    record(journal, node, "this")
                node.set("this", str(pos))

    if removed:
        scope.clear_cache()


def _bare_group_by_ordinal_refs(
    select: exp.Select,
) -> list[tuple[exp.Literal, exp.Expr]]:
    """Map each bare GROUP BY integer ordinal to its pre-prune projection."""
    group = select.args.get("group")
    if not group:
        return []

    selects = select.selects
    n = len(selects)
    refs: list[tuple[exp.Literal, exp.Expr]] = []

    for node in group.expressions:
        if node.is_int and isinstance(node, exp.Literal):
            pos = int(node.this)
            if 1 <= pos <= n:
                refs.append((node, selects[pos - 1]))

    return refs
