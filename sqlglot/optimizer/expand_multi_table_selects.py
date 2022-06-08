import sqlglot.expressions as exp


def expand_multi_table_selects(expression):
    for from_ in expression.find_all(exp.From):
        parent = from_.parent
        where = parent.args.get("where")

        for query in from_.args["expressions"][1:]:

            # for predicate in predicates:
            #    predicate.replace(exp.TRUE)
            parent.join(
                query,
                join_type="CROSS",
                copy=False,
            )
            from_.args["expressions"].remove(query)

    return expression
