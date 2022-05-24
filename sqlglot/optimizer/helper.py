import dataclasses
from sqlglot import expressions as exp


@dataclasses.dataclass
class SelectScope:
    """
    Collection of SELECT child nodes in the current SELECT scope.

    Attributes:
        ctes (list of exp.CTE)
        derived_tables (list of exp.Subquery): to illustrate:
            SELECT * FROM (SELECT ...) <- derived table
        subqueries (list of (exp.Select or exp.Union)): to illustrate:
            SELECT * FROM x WHERE a IN (SELECT ...) <- subquery
        tables (list of exp.Table)
        columns (list of exp.Column)
    """

    ctes: list = dataclasses.field(default_factory=list)
    derived_tables: list = dataclasses.field(default_factory=list)
    subqueries: list = dataclasses.field(default_factory=list)
    tables: list = dataclasses.field(default_factory=list)
    columns: list = dataclasses.field(default_factory=list)


def select_scope(select):
    """
    Collect some common arbitrarily-nested child nodes from a SELECT expression.

    For example, given the following statement:
        SELECT a + 1 AS b FROM (SELECT a FROM x)

    This will find and collect all the child nodes in the outer query, but it
    won't traverse into the inner query.

    Args:
         select (exp.Select): expression to search
    Returns:
        SelectScope: SelectScope instance
    """
    scope = SelectScope()

    for node, parent, _ in select.walk(stop_after=(exp.Select, exp.Union)):
        if node is select:
            continue  # Skip this node itself - we only care about children
        if isinstance(node, (exp.Select, exp.Union)):
            if isinstance(parent, exp.CTE):
                scope.ctes.append(parent)
            elif isinstance(parent, exp.Subquery):
                scope.derived_tables.append(parent)
            else:
                scope.subqueries.append(node)
        elif isinstance(node, exp.Table):
            scope.tables.append(node)
        elif isinstance(node, exp.Column):
            scope.columns.append(node)

    return scope
