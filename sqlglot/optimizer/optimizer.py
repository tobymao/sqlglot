from sqlglot.optimizer.normalize import normalize
from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries
from sqlglot.optimizer.expand_multi_table_selects import expand_multi_table_selects
from sqlglot.optimizer.isolate_table_selects import isolate_table_selects
from sqlglot.optimizer.optimize_joins import optimize_joins
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.quote_identities import quote_identities
from sqlglot.optimizer.unnest_subqueries import unnest_subqueries


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
    expression = isolate_table_selects(expression)
    expression = qualify_columns(expression, schema)
    expression = pushdown_projections(expression)
    expression = normalize(expression)
    expression = unnest_subqueries(expression)
    expression = expand_multi_table_selects(expression)
    expression = pushdown_predicates(expression)
    expression = optimize_joins(expression)
    expression = eliminate_subqueries(expression)
    expression = quote_identities(expression)
    return expression
