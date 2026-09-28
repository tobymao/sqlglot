from __future__ import annotations

import typing as t
from collections import defaultdict, deque

from sqlglot import alias, exp
from sqlglot.errors import OptimizeError
from sqlglot.helper import seq_get
from sqlglot.optimizer.helpers import projection_has_aggregate
from sqlglot.optimizer.journal import Journal, record
from sqlglot.optimizer.scope import Scope, find_all_in_scope, traverse_scope

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def pushdown_projections(expression: E, journal: Journal | None = None) -> E:
    """
    Remove unused projections and CTEs while preserving all outermost outputs.

    The expression must be qualified, with projection stars expanded. An unresolved
    projection star raises OptimizeError before any changes are made.

    Example:
        >>> import sqlglot
        >>> sql = "SELECT y.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x) AS y"
        >>> expression = sqlglot.parse_one(sql)
        >>> pushdown_projections(expression).sql()
        'SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y'

    Args:
        expression: the expression to optimize, mutated in place.
        journal: if given, records every mutation so that `revert(journal)` undoes this rule.

    Returns:
        The optimized expression.
    """
    reachability = projection_reachability(expression, whole_query=True)
    prune_projections(reachability, root=0, journal=journal)
    return expression


# GROUP BY constructs whose children are grouping items.
GROUPING_CONSTRUCTS = (exp.Cube, exp.GroupingSets, exp.Paren, exp.Rollup, exp.Tuple)


class ProjectionReachability(t.NamedTuple):
    """Which root outputs reach each scope and output column through dependency edges.

    Reachability is stored as integer bitsets: bit i represents root output i.
    """

    # Scopes as returned by traverse_scope, reused when pruning projections.
    scopes: list[Scope]

    # scope -> set of root outputs that require this scope
    live: dict[Scope, int]

    # scope -> for each of its output columns, the set of root outputs that require it
    selections: dict[Scope, list[int]]

    # SELECT scope -> whether an empty projection list needs an aggregate placeholder
    is_agg: dict[Scope, bool]

    # SELECT scope -> original GROUP BY ordinals and the projections they reference
    group_by_ordinals: dict[Scope, list[tuple[exp.Literal, exp.Expr]]]

    # Set-operation scope -> output names before either operand is pruned
    set_names: dict[Scope, list[str]]


class DependencyNode:
    """Tracks dependencies of a scope or one output column.

    required_by: identifies the root outputs that need this node.
    dependencies: lists the other nodes they also need.
    """

    __slots__ = ("dependencies", "required_by")

    def __init__(self) -> None:
        self.required_by = 0
        self.dependencies: list[DependencyNode] = []


def projection_reachability(
    expression: exp.Expr, whole_query: bool = False
) -> ProjectionReachability:
    """Find which scopes and projections each outermost output reaches through dependencies.

    This analyzes the query without changing it.

    Keeping an output must preserve both its values and the rows in which they appear.
    For example:

        WITH q AS (SELECT a, b, c FROM t)
        SELECT q.a AS x, q.b AS y FROM q WHERE q.c > 0

    Keeping x needs q.a and q.c; keeping y needs q.b and q.c. Both need q.c because
    the filter determines which rows appear, even though neither output reads its value.

    We build a graph with one scope node per scope and one output node per exposed column.
    An edge A -> B means keeping A requires B. Every output depends on its scope node,
    which collects dependencies common to all its outputs. In the example, x depends on
    q.a, y depends on q.b, and the outer scope node depends on q.c for the WHERE condition.

    Each node's required_by bitset records which outermost outputs need it. We assign
    each outermost output its own bit and propagate bits along dependency edges until
    nothing changes. The result records which scopes and columns each output reaches.

    The input must be qualified with projection stars expanded. Normally bit i represents
    output i of the outermost query; whole_query=True uses bit 0 for all its outputs together.
    For statements containing multiple queries, whole_query=True retains all top-level
    outputs, including statement-level CTE outputs that the statement itself may reference.
    """
    if not whole_query and not isinstance(expression, exp.Query):
        raise OptimizeError("projection_reachability requires a query")

    scopes = traverse_scope(expression)
    if not scopes:
        if whole_query:
            return ProjectionReachability(
                scopes=[],
                live={},
                selections={},
                is_agg={},
                group_by_ordinals={},
                set_names={},
            )
        raise OptimizeError("projection_reachability requires a query scope")

    # Fast path: a single SELECT retaining all outputs needs no dependency graph.
    if whole_query and len(scopes) == 1:
        scope = scopes[0]
        query = scope.expression
        if isinstance(query, exp.Select):
            if query.is_star:
                raise OptimizeError("projection_reachability requires star-free selections")

            windows = query.args.get("windows")
            return ProjectionReachability(
                scopes=scopes,
                live={scope: 1},
                selections={scope: [1] * len(query.selects)},
                is_agg={scope: any(projection_has_aggregate(s, windows) for s in query.selects)},
                group_by_ordinals={scope: _group_by_ordinal_refs(query)},
                set_names={},
            )

    # One node per scope for dependencies shared by all its outputs
    scope_nodes: dict[Scope, DependencyNode] = {}

    # Output names in order, including names from either BY NAME operand
    output_names: dict[Scope, list[str]] = {}

    # One node per output, in the same order as output_names
    output_nodes: dict[Scope, list[DependencyNode]] = {}

    # Resolve output names to nodes, retaining all matches for duplicate names
    outputs_by_name: dict[Scope, dict[str, list[DependencyNode]]] = {}

    # Projection and scope expression IDs mapped to the nodes responsible for their references,
    # so for the docstring example we'd have:
    #
    #   owners[id(outer_select)] = Scope node
    #   owners[id(x_projection)] = Output node for q.a AS x
    #   owners[id(y_projection)] = Output node for q.b AS y
    owners: dict[int, DependencyNode] = {}

    # Resolve alternate Scope objects for the same expression, as in recursive references
    scopes_by_expression: dict[int, Scope] = {}

    # Whether a SELECT needs an aggregate placeholder if all its projections are removed
    is_agg: dict[Scope, bool] = {}
    group_by_ordinals: dict[Scope, list[tuple[exp.Literal, exp.Expr]]] = {}

    # Register every node before connecting scopes: correlated and recursive references
    # can point outside the descendants already visited in this post-order traversal.
    for scope in scopes:
        query = scope.expression
        if isinstance(query, exp.Select) and query.is_star:
            raise OptimizeError("projection_reachability requires star-free selections")

        scopes_by_expression[id(query)] = scope
        scope_node = scope_nodes[scope] = DependencyNode()
        owners[id(query)] = scope_node

        if isinstance(query, exp.SetOperation):
            left, right = scope.set_operation_scopes
            output_names[scope] = (
                list(dict.fromkeys(output_names[left] + output_names[right]))
                if query.args.get("by_name")
                else output_names[left]
            )
        else:
            output_names[scope] = (
                [s.alias_or_name for s in query.selects]
                if isinstance(query, exp.Selectable)
                else []
            )

        outputs = output_nodes[scope] = [DependencyNode() for _ in output_names[scope]]
        outputs_by_name[scope] = defaultdict(list)
        for name, output_node in zip(output_names[scope], outputs):
            outputs_by_name[scope][name].append(output_node)
            output_node.dependencies.append(scope_node)

        if isinstance(query, exp.Select):
            owners.update((id(s), c) for s, c in zip(query.selects, outputs))

    def owner(node: exp.Expr) -> DependencyNode:
        """Find the node whose dependencies include this expression's references.

        Walk up to a registered projection or scope expression. A projection maps to
        its output node; a scope expression maps to its scope node. WHERE and other
        clauses need no separate registration. Nested queries have their own owners.
        """
        while id(node) not in owners:
            assert node.parent is not None
            node = node.parent

        return owners[id(node)]

    for scope in scopes:
        query = scope.expression
        scope_node = scope_nodes[scope]
        outputs = output_nodes[scope]
        order = query.args.get("order")
        keep_all = bool(
            query.args.get("distinct")
            or isinstance(query, (exp.Intersect, exp.Except))
            or _is_self_referencing_cte(scope)
            or not isinstance(query, (exp.Select, exp.SetOperation))
        )

        # The containing expression requires the subquery's scope node and all its outputs.
        for child in scope.subquery_scopes:
            anchor = child.expression.parent

            assert anchor is not None
            owner(anchor).dependencies.extend([scope_nodes[child], *output_nodes[child]])

        # A wrapper such as (SELECT ...) LIMIT 1 exposes its inner query's outputs.
        if isinstance(query, exp.Subquery):
            for child in scope.derived_table_scopes:
                scope_node.dependencies.extend([scope_nodes[child], *output_nodes[child]])

        if isinstance(query, exp.SetOperation):
            left, right = scope.set_operation_scopes
            scope_node.dependencies.extend([scope_nodes[left], scope_nodes[right]])
            by_name = query.args.get("by_name")

            # Special set-operation modes need different column matching; BY NAME alias
            # lists refer to merged output positions, which may differ from either operand.
            if query.kind or query.side or (by_name and scope.outer_columns):
                keep_all = True

            if not by_name and len(output_nodes[left]) != len(output_nodes[right]):
                raise OptimizeError(f"Invalid set operation due to column mismatch: {query.sql()}.")

            # Match outputs by name for BY NAME, otherwise by position. A retained output
            # requires its matching branch outputs. Reverse edges propagate columns required
            # within a branch (e.g. by ORDER BY) back to the set operation and matching outputs
            # in the other branch, preserving the combined schema.
            for branch in (left, right):
                for i, output_node in enumerate(output_nodes[branch]):
                    targets = (
                        outputs_by_name[scope].get(output_names[branch][i], [])
                        if by_name
                        else [outputs[i]]
                    )
                    for output in targets:
                        output.dependencies.append(output_node)
                        output_node.dependencies.append(output)

        if keep_all:
            scope_node.dependencies.extend(outputs)
        elif order:
            # Keeping the prefix preserves positions referenced by remaining ORDER BY ordinals.
            max_ordinal = max(
                (
                    int(ordered.this.to_py())
                    for ordered in order.expressions
                    if isinstance(ordered.this, exp.Literal) and ordered.this.is_int
                ),
                default=0,
            )
            scope_node.dependencies.extend(outputs[:max_ordinal])

        for name in _output_column_refs(query, scoped=not isinstance(query, exp.Select)):
            scope_node.dependencies.extend(outputs_by_name[scope].get(name, []))

        scope_node.dependencies.extend(outputs[: len(scope.outer_columns)])

        if isinstance(query, exp.Select):
            windows = query.args.get("windows")
            group_all = _is_implicit_group_by_all(query)
            group_by_ordinals[scope] = _group_by_ordinal_refs(query)
            ordinals = {id(s) for _, s in group_by_ordinals[scope]}
            first_aggregate: DependencyNode | None = None
            non_aggregates: list[DependencyNode] = []
            has_grouping_key = False

            for selection, output_node in zip(query.selects, outputs):
                aggregate, has_column, has_srf = _projection_properties(selection, windows)
                if aggregate:
                    first_aggregate = first_aggregate or output_node
                else:
                    non_aggregates.append(output_node)

                if group_all and not aggregate and has_column:
                    has_grouping_key = True

                if id(selection) in ordinals or (group_all and not aggregate) or has_srf:
                    scope_node.dependencies.append(output_node)

            is_agg[scope] = first_aggregate is not None
            if first_aggregate and (
                not query.args.get("group") or (group_all and not has_grouping_key)
            ):
                # Keeping a constant while dropping the last aggregate would turn one row into
                # one per input row, even with GROUP BY ALL (constants don't infer grouping keys).
                # An empty projection list uses MAX(1) when pruning projections instead.
                for output_node in non_aggregates:
                    output_node.dependencies.append(first_aggregate)

        for name, reference in scope.references:
            source = scope.sources.get(name)
            if not isinstance(source, Scope):
                continue

            # Recursive references can use a second Scope object for the same expression.
            source = scopes_by_expression[id(source.expression)]
            scope_node.dependencies.append(scope_nodes[source])
            source_outputs = output_nodes[source]
            first = (
                seq_get(source.expression.selects, 0)
                if isinstance(source.expression, exp.Selectable)
                else None
            )

            if (
                name in scope.semi_or_anti_join_tables
                or scope.scans_all_subscope_columns
                or scope.pivots
                or isinstance(first, exp.QueryTransform)
            ):
                scope_node.dependencies.extend(source_outputs)

            # Column alias lists bind by position, so keep every projection they name.
            scope_node.dependencies.extend(source_outputs[: len(reference.alias_column_names)])

        for col in scope.columns:
            source = scope.sources.get(col.table or col.name)
            if isinstance(source, Scope):
                source = scopes_by_expression[id(source.expression)]
                owner(col).dependencies.extend(
                    outputs_by_name[source].get(col.name, []) if col.table else output_nodes[source]
                )

        for table_column in scope.table_columns:
            source = scope.sources.get(table_column.name)
            if isinstance(source, Scope):
                source = scopes_by_expression[id(source.expression)]
                owner(table_column).dependencies.extend(output_nodes[source])

    # DML/DDL can have several independent queries and CTEs referenced outside any query.
    roots = (
        [scopes[-1]]
        if isinstance(expression, exp.Query)
        else [scope for scope in scopes if scope.parent not in scope_nodes]
    )
    pending: deque[DependencyNode] = deque()
    for root in roots:
        root_outputs = output_nodes[root]
        all_roots = 1 if whole_query else (1 << len(root_outputs)) - 1
        scope_nodes[root].required_by = all_roots

        for i, output_node in enumerate(root_outputs):
            output_node.required_by = all_roots if whole_query else 1 << i

        pending.extend([scope_nodes[root], *root_outputs])

    queued = set(pending)
    while pending:
        node = pending.popleft()
        queued.remove(node)

        for dependency in node.dependencies:
            required_by = dependency.required_by | node.required_by
            if required_by != dependency.required_by:
                dependency.required_by = required_by
                if dependency not in queued:
                    queued.add(dependency)
                    pending.append(dependency)

    live = {}
    selections = {}
    set_names = {}
    for scope in scopes:
        live[scope] = scope_nodes[scope].required_by
        selections[scope] = [output_node.required_by for output_node in output_nodes[scope]]
        if isinstance(scope.expression, exp.SetOperation):
            set_names[scope] = scope.expression.named_selects

    return ProjectionReachability(
        scopes=scopes,
        live=live,
        selections=selections,
        is_agg=is_agg,
        group_by_ordinals=group_by_ordinals,
        set_names=set_names,
    )


def prune_projections(
    reachability: ProjectionReachability,
    root: int,
    journal: Journal | None = None,
    remove_ctes: bool = True,
) -> None:
    """Prune the analyzed tree to the scopes and projections reachable from root output index root.

    Use root=0 for reachability computed with whole_query=True. Required companion outputs, such
    as the other projections of SELECT DISTINCT, are retained as well.

    This edits SELECT projection lists, renumbers bare GROUP BY ordinals, and removes
    unused CTEs unless remove_ctes is false. Other clauses remain in their containing
    expressions; their dependencies keep the outputs those clauses reference.

    Grouping ordinals and set-operation names were saved during analysis, before pruning.
    Mutations are recorded in journal, if given, so the original tree can be restored before
    pruning for another root output.
    """
    # Visit parents before their children when removing unused CTEs.
    bit = 1 << root
    for scope in reversed(reachability.scopes):
        if not reachability.live[scope] & bit:
            if remove_ctes and scope.is_cte:
                cte_node = scope.expression.parent
                if isinstance(cte_node, exp.CTE):
                    with_node = cte_node.parent
                    if journal is not None and with_node is not None:
                        record(journal, with_node, "expressions")

                    cte_node.pop()

                    if with_node is not None and not with_node.expressions:
                        if journal is not None and with_node.parent is not None:
                            record(journal, with_node.parent, "with_")

                        with_node.pop()
            continue

        expression = scope.expression
        if not isinstance(expression, exp.Select):
            continue

        subset = [
            selection
            for selection, roots in zip(expression.selects, reachability.selections[scope])
            if roots & bit
        ]
        if len(subset) == len(expression.selects):
            continue

        ordinal_refs = reachability.group_by_ordinals[scope]
        if not subset:
            placeholder = default_selection(reachability.is_agg[scope])
            ancestor = scope

            while ancestor.is_set_operation and ancestor.parent:
                ancestor = ancestor.parent
                retained_name = next(
                    (
                        name
                        for name, roots in zip(
                            reachability.set_names[ancestor], reachability.selections[ancestor]
                        )
                        if roots & bit
                    ),
                    None,
                )
                if retained_name is not None:
                    # An empty BY NAME arm must contribute NULL under an existing output name.
                    # A new `_` column would change the width of an enclosing positional union.
                    placeholder.set(
                        "this",
                        exp.Max(this=exp.Null()) if reachability.is_agg[scope] else exp.Null(),
                    )
                    placeholder.set("alias", exp.to_identifier(retained_name, quoted=True))
                    break

            subset = [placeholder]

        if journal is not None:
            record(journal, expression, "expressions")

        expression.set("expressions", subset)

        if ordinal_refs:
            new_pos = {id(selection): i + 1 for i, selection in enumerate(subset)}
            for node, old_selection in ordinal_refs:
                pos = new_pos.get(id(old_selection))
                if pos is not None and int(node.this) != pos:
                    if journal is not None:
                        record(journal, node, "this")

                    node.set("this", str(pos))


def _projection_properties(
    selection: exp.Expr, windows: list[exp.Window] | None = None
) -> tuple[bool, bool, bool]:
    """Whether a projection aggregates rows, reads columns, or contains a set-returning function."""
    has_aggregate_or_window = has_column = has_srf = False
    for node in find_all_in_scope(
        selection, exp.AggFunc, exp.Window, exp.Column, *exp.SET_RETURNING_FUNCTIONS
    ):
        if isinstance(node, (exp.AggFunc, exp.Window)):
            has_aggregate_or_window = True
        if isinstance(node, exp.Column):
            has_column = True
        if isinstance(node, exp.SET_RETURNING_FUNCTIONS):
            has_srf = True

    aggregate = has_aggregate_or_window and projection_has_aggregate(selection, windows)
    return aggregate, has_column, has_srf


def _output_column_refs(expression: exp.Expr, scoped: bool) -> set[str]:
    refs: set[str] = set()

    for arg in ("order", "sort", "distribute", "cluster"):
        node = expression.args.get(arg)
        if node:
            columns = find_all_in_scope(node, exp.Column) if scoped else node.find_all(exp.Column)
            refs.update(c.name for c in columns if not c.table)

    return refs


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


def _is_implicit_group_by_all(select: exp.Select) -> bool:
    """Bare GROUP BY ALL infers its keys from the SELECT list, unlike ALL as a
    grouping-sets modifier (e.g. GROUP BY ALL CUBE (...) or GROUP BY ALL a, b)."""
    group = select.args.get("group")
    if not group or not group.args.get("all"):
        return False

    return not (
        group.expressions
        or group.args.get("cube")
        or group.args.get("rollup")
        or group.args.get("grouping_sets")
    )


def _group_by_ordinal_refs(
    select: exp.Select,
) -> list[tuple[exp.Literal, exp.Expr]]:
    """Map each GROUP BY integer ordinal to its pre-prune projection, including the ordinals
    nested in a grouping construct such as GROUPING SETS / CUBE / ROLLUP."""
    group = select.args.get("group")
    if not group:
        return []

    selects = select.selects
    n = len(selects)
    refs: list[tuple[exp.Literal, exp.Expr]] = []

    def collect(nodes: t.Iterable[exp.Expr]) -> None:
        for node in nodes:
            if isinstance(node, GROUPING_CONSTRUCTS):
                collect(node.iter_expressions())
            elif node.is_int and isinstance(node, exp.Literal):
                pos = int(node.this)
                if 1 <= pos <= n:
                    refs.append((node, selects[pos - 1]))

    collect(group.iter_expressions())

    return refs
