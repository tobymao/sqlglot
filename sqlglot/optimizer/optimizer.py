from sqlglot.optimizer.decorrelate_subqueries import decorrelate_subqueries
from sqlglot.optimizer.expand_multi_table_selects import expand_multi_table_selects
from sqlglot.optimizer.projection_pushdown import projection_pushdown
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.quote_identities import quote_identities
from sqlglot.optimizer.simplify import simplify


def optimize(expression, schema=None, db=None, catalog=None):
    """
    Rewrite a sqlglot AST into an optimized form.
    """
    expression = qualify_tables(expression, db=db, catalog=catalog)
    expression = qualify_columns(expression, schema or {})
    expression = projection_pushdown(expression)
    expression = decorrelate_subqueries(expression)
    expression = expand_multi_table_selects(expression)
    expression = simplify(expression)
    expression = quote_identities(expression)
    return expression
