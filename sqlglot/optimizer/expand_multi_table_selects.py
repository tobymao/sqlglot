import sqlglot.expressions as exp


def expand_multi_table_selects(expression):
    for from_ in expression.find_all(exp.From):
        parent = from_.parent
        where = parent.args.get("where")

        for query in from_.args["expressions"][1:]:
            alias = query.alias_or_name

            predicates = []

            if where:
                condition = where.this.unnest()

                if isinstance(condition, exp.Connector):
                    condition = condition.flatten()
                else:
                    condition = [condition]

                for predicate in condition:
                    if alias in exp.column_table_names(predicate):
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

    return expression
