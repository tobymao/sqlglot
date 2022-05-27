import sqlglot.expressions as exp


def expand_multi_table_selects(expression):
    for from_ in expression.find_all(exp.From):
        parent = from_.parent
        where = parent.args.get("where")
        for query in from_.args["expressions"][1:]:
            alias = query.alias_or_name

            predicates = [
                predicate
                for predicate in [
                    column.find_ancestor(exp.PREDICATES)
                    for column in (where.find_all(exp.Column) if where else [])
                    if column.text("table") == alias
                ]
                if predicate
            ]

            for predicate in predicates:
                predicate.replace(exp.TRUE)
            parent.join(query, on=predicates, copy=False)
            from_.args["expressions"].remove(query)

    return expression
