from __future__ import annotations

from sqlglot import exp
from sqlglot.optimizer.scope import walk_in_scope

WINDOW_HAS_AGGREGATE = "window_has_aggregate"


def projection_has_aggregate(
    projection: exp.Expr,
    windows: list[exp.Window] | None = None,
) -> bool:
    windowed_aggregates: set[int] = set()

    for node in walk_in_scope(projection):
        if isinstance(node, exp.Window):
            target: object = node.this

            # parens, FILTER and IGNORE NULLS wrap that function without changing which one it is
            while isinstance(target, exp.Expr) and not isinstance(target, exp.Func):
                target = target.this

            if isinstance(target, exp.AggFunc):
                windowed_aggregates.add(id(target))

            if (
                node.alias
                and windows is not None
                and _named_window_has_aggregate(node.alias, windows)
            ):
                return True
        elif isinstance(node, exp.AggFunc) and id(node) not in windowed_aggregates:
            return True

    return False


def _named_window_has_aggregate(
    name: str,
    windows: list[exp.Window],
) -> bool:
    # the referenced window's PARTITION BY / ORDER BY may contain an un-windowed aggregate
    # that forces the whole (ungrouped) scope to aggregate, e.g., WINDOW w AS (ORDER BY SUM(a)).
    visited_names: set[str] = set()
    visited_windows: list[exp.Window] = []
    has_aggregate = False

    while name and name not in visited_names:
        visited_names.add(name)
        window = next((window for window in windows if window.name == name), None)
        if window is None:
            break

        cached = window.meta_get(WINDOW_HAS_AGGREGATE)
        if cached is not None:
            has_aggregate = cached
            break
        visited_windows.append(window)

        if projection_has_aggregate(window):
            has_aggregate = True
            break

        name = window.alias

    for window in visited_windows:
        window.meta[WINDOW_HAS_AGGREGATE] = has_aggregate

    return has_aggregate
