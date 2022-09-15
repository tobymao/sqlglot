import logging
import time

from sqlglot import parse_one
from sqlglot.executor.python import PythonExecutor
from sqlglot.optimizer import RULES, optimize
from sqlglot.optimizer.merge_derived_tables import merge_derived_tables
from sqlglot.planner import Plan

logger = logging.getLogger("sqlglot")

OPTIMIZER_RULES = list(RULES)

# The executor needs isolated table selects
OPTIMIZER_RULES.remove(merge_derived_tables)


def execute(sql, schema, read=None):
    """
    Run a sql query against data.

    Args:
        sql (str): a sql statement
        schema (dict|sqlglot.optimizer.Schema): database schema.
            This can either be an instance of `sqlglot.optimizer.Schema` or a mapping in one of
            the following forms:
                1. {table: {col: type}}
                2. {db: {table: {col: type}}}
                3. {catalog: {db: {table: {col: type}}}}
        read (str): the SQL dialect to apply during parsing
            (eg. "spark", "hive", "presto", "mysql").
    Returns:
        sqlglot.executor.Table: Simple columnar data structure.
    """
    expression = parse_one(sql, read=read)
    now = time.time()
    expression = optimize(expression, schema, rules=OPTIMIZER_RULES)
    logger.debug("Optimization finished: %f", time.time() - now)
    logger.debug("Optimized SQL: %s", expression.sql(pretty=True))
    plan = Plan(expression)
    logger.debug("Logical Plan: %s", plan)
    now = time.time()
    result = PythonExecutor().execute(plan)
    logger.debug("Query finished: %f", time.time() - now)
    return result
