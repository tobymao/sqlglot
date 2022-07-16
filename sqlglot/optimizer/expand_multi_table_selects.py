from sqlglot import exp


def expand_multi_table_selects(expression):
    for from_ in expression.find_all(exp.From):
        parent = from_.parent

        for query in from_.expressions[1:]:
            parent.join(
                query,
                join_type="CROSS",
                copy=False,
            )
            from_.expressions.remove(query)

    return expression
