import sqlglot
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.canonicalize import canonicalize
from sqlglot.optimizer.eliminate_ctes import eliminate_ctes
from sqlglot.optimizer.eliminate_joins import eliminate_joins
from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries
from sqlglot.optimizer.expand_multi_table_selects import expand_multi_table_selects
from sqlglot.optimizer.isolate_table_selects import isolate_table_selects
from sqlglot.optimizer.lower_identities import lower_identities
from sqlglot.optimizer.merge_subqueries import merge_subqueries
from sqlglot.optimizer.normalize import normalize
from sqlglot.optimizer.optimize_joins import optimize_joins
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.quote_identities import quote_identities
from sqlglot.optimizer.unnest_subqueries import unnest_subqueries

RULES = (
    lower_identities,
    qualify_tables,
    isolate_table_selects,
    qualify_columns,
    pushdown_projections,
    normalize,
    unnest_subqueries,
    expand_multi_table_selects,
    pushdown_predicates,
    optimize_joins,
    eliminate_subqueries,
    merge_subqueries,
    eliminate_joins,
    eliminate_ctes,
    annotate_types,
    canonicalize,
    quote_identities,
)


def optimize(expression, schema=None, db=None, catalog=None, rules=RULES, **kwargs):
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
            If no schema is provided then the default schema defined at `sqlgot.schema` will be used
        db (str): specify the default database, as might be set by a `USE DATABASE db` statement
        catalog (str): specify the default catalog, as might be set by a `USE CATALOG c` statement
        rules (list): sequence of optimizer rules to use
        **kwargs: If a rule has a keyword argument with a same name in **kwargs, it will be passed in.
    Returns:
        sqlglot.Expression: optimized expression
    """
    possible_kwargs = {"db": db, "catalog": catalog, "schema": schema or sqlglot.schema, **kwargs}
    expression = expression.copy()
    for rule in rules:

        # Find any additional rule parameters, beyond `expression`
        rule_params = rule.__code__.co_varnames
        rule_kwargs = {
            param: possible_kwargs[param] for param in rule_params if param in possible_kwargs
        }
        expression = rule(expression, **rule_kwargs)
    return expression
