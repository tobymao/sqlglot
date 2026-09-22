from sqlglot import exp
from sqlglot.optimizer.scope import find_all_in_scope


def _is_window_expression(agg_func: exp.AggFunc) -> bool:
    node: exp.Expr = agg_func
    parent = agg_func.parent

    # parens, FILTER and IGNORE NULLS wrap that function without changing which one it is
    while parent is not None and not isinstance(parent, exp.Func) and parent.this is node:
        if isinstance(parent, exp.Window):
            return True

        node, parent = parent, parent.parent

    return False


def _named_window_has_aggregate(
    name: str,
    named_windows: dict[str, exp.Window],
    cache: dict[str, bool],
) -> bool:
    # the referenced window's PARTITION BY / ORDER BY may contain an un-windowed aggregate
    # that forces the whole (ungrouped) scope to aggregate, e.g., WINDOW w AS (ORDER BY SUM(a)).
    visited: set[str] = set()
    has_aggregate = False

    while name and name not in visited:
        if name in cache:
            has_aggregate = cache[name]
            break

        visited.add(name)
        window = named_windows.get(name)
        if not window:
            break

        if any(
            not _is_window_expression(aggregate)
            for aggregate in find_all_in_scope(window, exp.AggFunc)
        ):
            has_aggregate = True
            break

        name = window.alias

    for name in visited:
        cache[name] = has_aggregate

    return has_aggregate


def projection_has_aggregate(
    projection: exp.Expr,
    named_windows: dict[str, exp.Window],
    cache: dict[str, bool],
) -> bool:
    if any(not _is_window_expression(agg) for agg in find_all_in_scope(projection, exp.AggFunc)):
        return True

    # this projection's aggregate(s) are windowed (e.g. COUNT(*) OVER w), but the
    # referenced named window may still contain an aggregate that isn't
    return any(
        window.alias and _named_window_has_aggregate(window.alias, named_windows, cache)
        for window in find_all_in_scope(projection, exp.Window)
    )
