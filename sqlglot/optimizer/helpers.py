from __future__ import annotations

from sqlglot import exp
from sqlglot.optimizer.scope import find_in_scope, walk_in_scope

WINDOW_HAS_AGGREGATE = "window_has_aggregate"


def projection_has_aggregate(
    projection: exp.Expr,
    windows: list[exp.Window] | None = None,
) -> bool:
    windowed_aggregates: set[int] = set()
    remaining_windows = {window.name: window for window in windows or []}

    for node in walk_in_scope(projection):
        if isinstance(node, exp.Window):
            target = node.this

            # parens, FILTER and IGNORE NULLS wrap that function without changing which one it is
            while isinstance(target, exp.Expr) and not isinstance(target, exp.Func):
                target = target.this

            if isinstance(target, exp.AggFunc):
                windowed_aggregates.add(id(target))

            # WINDOW w AS (...)
            #        ^ this is the node's alias
            name = node.alias

            # The loop below handles named window inheritance, e.g.:
            # WINDOW w1 AS (PARTITION BY SUM(x)), w2 AS (w1 ORDER BY 1), w3 AS (w2)
            while name:
                window = remaining_windows.pop(name, None)
                if window is None:
                    break

                has_aggregate = window.meta_get(WINDOW_HAS_AGGREGATE)
                if has_aggregate is None:
                    has_aggregate = bool(find_in_scope(window, exp.AggFunc))
                    window.meta[WINDOW_HAS_AGGREGATE] = has_aggregate
                if has_aggregate:
                    return True

                name = window.alias
        elif isinstance(node, exp.AggFunc) and id(node) not in windowed_aggregates:
            return True

    return False
