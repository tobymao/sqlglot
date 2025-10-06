from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import tsort

JOIN_ATTRS = ("on", "side", "kind", "using", "method")


def optimize_joins(expression):
    """
    Removes cross joins if possible and reorder joins based on predicate dependencies.

    Example:
        >>> from sqlglot import parse_one
        >>> optimize_joins(parse_one("SELECT * FROM x CROSS JOIN y JOIN z ON x.a = z.a AND y.a = z.a")).sql()
        'SELECT * FROM x JOIN z ON x.a = z.a AND TRUE JOIN y ON y.a = z.a'
    """

    for select in expression.find_all(exp.Select):
        joins = select.args.get("joins", [])

        # Skip CROSS JOIN optimization if any join has a side (LEFT/RIGHT/FULL)
        # because it can change query semantics
        if any(join.side for join in joins):
            continue

        references = {}
        cross_joins = []

        for join in joins:
            tables = other_table_names(join)

            if tables:
                for table in tables:
                    references[table] = references.get(table, []) + [join]
            else:
                cross_joins.append((join.alias_or_name, join))

        for name, join in cross_joins:
            for dep in references.get(name, []):
                on = dep.args["on"]

                if isinstance(on, exp.Connector):
                    if len(other_table_names(dep)) < 2:
                        continue

                    operator = type(on)
                    for predicate in on.flatten():
                        if name in exp.column_table_names(predicate):
                            predicate.replace(exp.true())
                            predicate = exp._combine(
                                [join.args.get("on"), predicate], operator, copy=False
                            )
                            join.on(predicate, append=False, copy=False)

    expression = reorder_joins(expression)
    expression = normalize(expression)
    return expression


def reorder_joins(expression):
    """
    Reorder joins by topological sort order based on predicate references.
    """
    for from_ in expression.find_all(exp.From):
        parent = from_.parent
        joins = parent.args.get("joins", [])

        # Skip reordering if any join has a side (LEFT/RIGHT/FULL)
        if any(join.side for join in joins):
            continue

        joins_by_name = {join.alias_or_name: join for join in joins}
        dag = {name: other_table_names(join) for name, join in joins_by_name.items()}
        parent.set(
            "joins",
            [
                joins_by_name[name]
                for name in tsort(dag)
                if name != from_.alias_or_name and name in joins_by_name
            ],
        )
    return expression


def normalize(expression):
    """
    Remove INNER and OUTER from joins as they are optional.
    """
    for join in expression.find_all(exp.Join):
        if not any(join.args.get(k) for k in JOIN_ATTRS):
            join.set("kind", "CROSS")

        if join.kind == "CROSS":
            join.set("on", None)
        else:
            if join.kind in ("INNER", "OUTER"):
                join.set("kind", None)

            if not join.args.get("on") and not join.args.get("using"):
                join.set("on", exp.true())
    return expression


def other_table_names(join: exp.Join) -> t.Set[str]:
    on = join.args.get("on")
    return exp.column_table_names(on, join.alias_or_name) if on else set()
