from sqlglot.optimizer.conjunctive_normal_form import conjunctive_normal_form
from sqlglot.optimizer.decorrelate_subqueries import decorrelate_subqueries
from sqlglot.optimizer.expand_multi_table_selects import expand_multi_table_selects
from sqlglot.optimizer.predicate_pushdown import predicate_pushdown
from sqlglot.optimizer.projection_pushdown import projection_pushdown
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.quote_identities import quote_identities
from sqlglot.optimizer.simplify import simplify


def optimize(expression, schema=None, db=None, catalog=None):
    """
    Rewrite a sqlglot AST into an optimized form.

    Args:
        expression (sqlglot.Expression): expression to optimize
        schema (dict|sqlglot.optimizer.Schema): database schema.
            This can either be an instance of `sqlglot.optimizer.Schema` or a mapping in one of
            the following forms:
                1. {table: {col: type}}
                2. {db: {table: {col: type}}}
                3. {catalog: {db: {table: {col: type}}}}
        db (str): specify the default database, as might be set by a `USE DATABASE db` statement
        catalog (str): specify the default catalog, as might be set by a `USE CATALOG c` statement
    Returns:
        sqlglot.Expression: optimized expression
    """
    expression = expression.copy()
    expression = qualify_tables(expression, db=db, catalog=catalog)
    expression = qualify_columns(expression, schema)
    expression = projection_pushdown(expression)
    expression = conjunctive_normal_form(expression)
    expression = decorrelate_subqueries(expression)
    expression = expand_multi_table_selects(expression)
    expression = predicate_pushdown(expression)
    expression = simplify(expression)
    expression = quote_identities(expression)
    return expression
