import logging
import time

from sqlglot import parse_one
from sqlglot.executor.python import PythonExecutor
from sqlglot.executor.table import Table
from sqlglot.optimizer import optimize
from sqlglot.planner import Plan

logger = logging.getLogger("sqlglot")


def execute(sql, schema, read=None, tables=None):
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
        tables (dict[str, list[dict]|sqlglot.executor.Table]): additional tables to register
    Returns:
        sqlglot.executor.Table: Simple columnar data structure.
    """
    tables = _ensure_tables(tables)
    expression = parse_one(sql, read=read)
    now = time.time()
    expression = optimize(expression, schema, leave_tables_isolated=True)
    logger.debug("Optimization finished: %f", time.time() - now)
    logger.debug("Optimized SQL: %s", expression.sql(pretty=True))
    plan = Plan(expression)
    logger.debug("Logical Plan: %s", plan)
    now = time.time()
    result = PythonExecutor(tables=tables).execute(plan)
    logger.debug("Query finished: %f", time.time() - now)
    return result


def _ensure_tables(tables):
    result = {}
    if not tables:
        return result
    for name, table in tables.items():
        if isinstance(table, Table):
            result[name] = table
        else:
            columns = tuple(table[0]) if table else ()
            rows = [tuple(row[c] for c in columns) for row in table]
            result[name] = Table(columns=columns, rows=rows)
    return result
