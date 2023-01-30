from sqlglot import exp
from sqlglot.helper import tsort
from sqlglot.optimizer.simplify import simplify


def optimize_joins(expression):
    """
    Removes cross joins if possible and reorder joins based on predicate dependencies.

    Example:
        >>> from sqlglot import parse_one
        >>> optimize_joins(parse_one("SELECT * FROM x CROSS JOIN y JOIN z ON x.a = z.a AND y.a = z.a")).sql()
        'SELECT * FROM x JOIN z ON x.a = z.a AND TRUE JOIN y ON y.a = z.a'
    """
    for select in expression.find_all(exp.Select):
        references = {}
        cross_joins = []

        for join in select.args.get("joins", []):
            name = join.this.alias_or_name
            tables = other_table_names(join, name)

            if tables:
                for table in tables:
                    references[table] = references.get(table, []) + [join]
            else:
                cross_joins.append((name, join))

        for name, join in cross_joins:
            for dep in references.get(name, []):
                on = dep.args["on"]
                on = on.replace(simplify(on))

                if isinstance(on, exp.Connector):
                    for predicate in on.flatten():
                        if name in exp.column_table_names(predicate):
                            predicate.replace(exp.true())
                            join.on(predicate, copy=False)

    expression = reorder_joins(expression)
    expression = normalize(expression)
    return expression


def reorder_joins(expression):
    """
    Reorder joins by topological sort order based on predicate references.
    """
    for from_ in expression.find_all(exp.From):
        head = from_.expressions[0]
        parent = from_.parent
        joins = {join.this.alias_or_name: join for join in parent.args.get("joins", [])}
        dag = {head.alias_or_name: []}

        for name, join in joins.items():
            dag[name] = other_table_names(join, name)

        parent.set(
            "joins",
            [joins[name] for name in tsort(dag) if name != head.alias_or_name],
        )
    return expression


def normalize(expression):
    """
    Remove INNER and OUTER from joins as they are optional.
    """
    for join in expression.find_all(exp.Join):
        if join.kind != "CROSS":
            join.set("kind", None)
    return expression


def other_table_names(join, exclude):
    return [
        name
        for name in (exp.column_table_names(join.args.get("on") or exp.true()))
        if name != exclude
    ]
