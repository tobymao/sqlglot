from sqlglot.optimizer.rules.qualify_tables import qualify_tables
from sqlglot.optimizer.rules.qualify_columns import qualify_columns


def optimize(expression, schema=None, db=None, catalog=None):
    """
    Rewrite a sqlglot AST into an optimized form.
    """
    expression = qualify_tables(expression, db=db, catalog=catalog)
    expression = qualify_columns(expression, schema)
    return expression
