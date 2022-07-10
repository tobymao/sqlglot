from sqlglot import exp
from sqlglot.optimizer.normalize import normalized
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.optimizer.simplify import simplify


def pushdown_predicates(expression):
    """
    Rewrite sqlglot AST to pushdown predicates in FROMS and JOINS

    Example:
        >>> import sqlglot
        >>> sql = "SELECT * FROM (SELECT * FROM x AS x) AS y WHERE y.a = 1"
        >>> expression = sqlglot.parse_one(sql)
        >>> pushdown_predicates(expression).sql()
        'SELECT * FROM (SELECT * FROM x AS x WHERE y.a = 1) AS y WHERE TRUE'

    Args:
        expression (sqlglot.Expression): expression to optimize
    Returns:
        sqlglot.Expression: optimized expression
    """
    for scope in reversed(traverse_scope(expression)):
        select = scope.expression
        where = select.args.get("where")
        if where:
            pushdown(where.this, scope.selected_sources)

        # joins should only pushdown into itself, not to other joins
        # so we limit the selected sources to only itself
        for join in select.args.get("joins") or []:
            name = join.this.alias_or_name
            pushdown(join.args.get("on"), {name: scope.selected_sources[name]})

    return expression


def pushdown(condition, sources):
    if not condition:
        return

    condition = condition.replace(simplify(condition))
    cnf_like = normalized(condition) or not normalized(condition, dnf=True)

    predicates = list(
        condition.flatten()
        if isinstance(condition, exp.And if cnf_like else exp.Or)
        else [condition]
    )

    if cnf_like:
        pushdown_cnf(predicates, sources)
    else:
        pushdown_dnf(predicates, sources)


def pushdown_cnf(predicates, scope):
    """
    If the predicates are in CNF like form, we can simply replace each block in the parent.
    """
    for predicate in predicates:
        for node in nodes_for_predicate(predicate, scope).values():
            if isinstance(node, exp.Join):
                predicate.replace(exp.TRUE)
                node.on(predicate, copy=False)
                break
            if isinstance(node, exp.Select):
                predicate.replace(exp.TRUE)
                node.where(replace_aliases(node, predicate), copy=False)


def pushdown_dnf(predicates, scope):
    """
    If the predicates are in DNF form, we can only push down conditions that are in all blocks.
    Additionally, we can't remove predicates from their original form.
    """
    # find all the tables that can be pushdown too
    # these are tables that are referenced in all blocks of a DNF
    # (a.x AND b.x) OR (a.y AND c.y)
    # only table a can be push down
    pushdown_tables = set()

    for a in predicates:
        a_tables = set(exp.column_table_names(a))

        for b in predicates:
            a_tables &= set(exp.column_table_names(b))

        pushdown_tables.update(a_tables)

    conditions = {}

    # for every pushdown table, find all related conditions in all predicates
    # combine them with ORS
    # (a.x AND and a.y AND b.x) OR (a.z AND c.y) -> (a.x AND a.y) OR (a.z)
    for table in sorted(pushdown_tables):
        for predicate in predicates:
            nodes = nodes_for_predicate(predicate, scope)

            if table not in nodes:
                continue

            predicate_condition = None

            for column in predicate.find_all(exp.Column):
                if column.table == table:
                    condition = column.find_ancestor(exp.Condition)
                    predicate_condition = (
                        exp.and_(predicate_condition, condition)
                        if predicate_condition
                        else condition
                    )

            if predicate_condition:
                conditions[table] = (
                    exp.or_(conditions[table], predicate_condition)
                    if table in conditions
                    else predicate_condition
                )

        for name, node in nodes.items():
            if name not in conditions:
                continue

            predicate = conditions[name]

            if isinstance(node, exp.Join):
                node.on(predicate, copy=False)
            elif isinstance(node, exp.Select):
                node.where(replace_aliases(node, predicate), copy=False)


def nodes_for_predicate(predicate, sources):
    nodes = {}
    tables = exp.column_table_names(predicate)
    where_condition = isinstance(
        predicate.find_ancestor(exp.Join, exp.Where), exp.Where
    )

    for table in tables:
        node, source = sources.get(table) or (None, None)

        # if the predicate is in a where statement we can try to push it down
        # we want to find the root join or from statement
        if node and where_condition:
            node = node.find_ancestor(exp.Join, exp.From)

        # a node can reference a CTE which should be push down
        if isinstance(node, exp.From) and not isinstance(source, exp.Table):
            node = source.expression

        if isinstance(node, exp.Join):
            if node.side:
                return {}
            nodes[table] = node
        elif isinstance(node, exp.Select) and len(tables) == 1:
            if not node.args.get("group"):
                nodes[table] = node
    return nodes


def replace_aliases(source, predicate):
    aliases = {}

    for select in source.selects:
        if isinstance(select, exp.Alias):
            aliases[select.alias] = select.this
        else:
            aliases[select.name] = select

    def _replace_alias(column):
        # pylint: disable=cell-var-from-loop
        if isinstance(column, exp.Column) and column.name in aliases:
            return aliases[column.name]
        return column

    return predicate.transform(_replace_alias)
