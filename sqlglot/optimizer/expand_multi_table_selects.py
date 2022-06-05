import sqlglot.expressions as exp
from sqlglot.helper import tsort


def expand_multi_table_selects(expression):
    for from_ in expression.find_all(exp.From):
        parent = from_.parent
        where = parent.args.get("where")
        tail = from_.args["expressions"][1:]

        for query in tail:
            alias = query.alias_or_name

            predicates = []

            if where:
                condition = where.this.unnest()

                if isinstance(condition, exp.Connector):
                    condition = condition.flatten()
                else:
                    condition = [condition]

                for predicate in condition:
                    for column in predicate.find_all(exp.Column):
                        if column.text("table") == alias:
                            predicates.append(predicate)

            for predicate in predicates:
                predicate.replace(exp.TRUE)
            parent.join(
                query,
                on=predicates,
                join_type=None if predicates else "CROSS",
                copy=False,
            )
            from_.args["expressions"].remove(query)

        if tail:
            reorder_joins(from_)

    return expression


def reorder_joins(from_):
    head = from_.args["expressions"][0]
    parent = from_.parent
    joins = {join.this.alias_or_name: join for join in parent.args.get("joins", [])}
    dag = {head.alias_or_name: set()}

    for name, join in joins.items():
        on = join.args.get("on")
        dag[name] = []
        if on:
            for column in on.find_all(exp.Column):
                table = column.text("table")
                if table != name:
                    dag[name].append(table)

        parent.set(
            "joins",
            [joins[name] for name in tsort(dag) if name != head.alias_or_name],
        )
