from sqlglot import expressions as exp
from sqlglot.optimizer.scope import traverse_scope


def predicate_pushdown(expression):
    """
    Rewrite sqlglot AST to pushdown predicates in FROMS and JOINS

    Example:
        >>> import sqlglot
        >>> sql = "SELECT * FROM (SELECT * FROM x AS x) AS y WHERE y.a = 1"
        >>> expression = sqlglot.parse_one(sql)
        >>> predicate_pushdown(expression).sql()
        'SELECT * FROM (SELECT * FROM x AS x WHERE y.a = 1) AS y WHERE TRUE'

    Args:
        expression (sqlglot.Expression): expression to optimize
    Returns:
        sqlglot.Expression: optimized expression
    """
    for scope in reversed(traverse_scope(expression)):
        where = scope.expression.args.get("where")

        if not where or where.find(exp.Or):
            continue

        for predicate in where.find_all(exp.PREDICATES):
            selectables = {
                scope.selectables.get(column.text("table"))
                for column in predicate.find_all(exp.Column)
            }

            if len(selectables) != 1:
                continue

            selectable = selectables.pop()

            if isinstance(selectable, exp.Table):
                node = selectable.find_ancestor(exp.Join, exp.From)

                if isinstance(node, exp.Join):
                    predicate.replace(exp.TRUE)
            elif selectable:
                node = selectable.expression
                predicate.replace(exp.TRUE)

                aliases = {}

                for select in selectable.selects:
                    if isinstance(select, exp.Alias):
                        aliases[select.alias] = select.this
                    else:
                        aliases[select.name] = select

                def replace_alias(column):
                    # pylint: disable=cell-var-from-loop
                    if isinstance(column, exp.Column) and column.name in aliases:
                        return aliases[column.name]
                    return column

                predicate = predicate.transform(replace_alias)
            else:
                continue

            if isinstance(node, exp.Join):
                on = node.args.get("on")
                node.set("on", exp.and_(predicate, on) if on else predicate)
            elif isinstance(node, exp.Select):
                node.where(predicate, copy=False)

    return expression
