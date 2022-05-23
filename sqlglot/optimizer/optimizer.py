from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.quote_identities import quote_identities


def optimize(expression, schema=None, db=None, catalog=None):
    """
    Rewrite a sqlglot AST into an optimized form.
    """
    expression = qualify_tables(expression, db=db, catalog=catalog)
    expression = qualify_columns(expression, schema or {})
    expression = quote_identities(expression)
    return expression
