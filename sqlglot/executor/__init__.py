import logging
import time

from sqlglot import maybe_parse
from sqlglot.errors import ExecuteError
from sqlglot.executor.python import PythonExecutor
from sqlglot.executor.table import Table, ensure_tables
from sqlglot.optimizer import optimize
from sqlglot.planner import Plan
from sqlglot.schema import ensure_schema

logger = logging.getLogger("sqlglot")


def execute(sql, schema=None, read=None, tables=None):
    """
    Run a sql query against data.

    Args:
        sql (str|sqlglot.Expression): a sql statement
        schema (dict|sqlglot.optimizer.Schema): database schema.
            This can either be an instance of `sqlglot.optimizer.Schema` or a mapping in one of
            the following forms:
                1. {table: {col: type}}
                2. {db: {table: {col: type}}}
                3. {catalog: {db: {table: {col: type}}}}
        read (str): the SQL dialect to apply during parsing
            (eg. "spark", "hive", "presto", "mysql").
        tables (dict): additional tables to register.
    Returns:
        sqlglot.executor.Table: Simple columnar data structure.
    """
    tables = ensure_tables(tables)
    if not schema:
        schema = {
            name: {column: type(table[0][column]).__name__ for column in table.columns}
            for name, table in tables.mapping.items()
        }
    schema = ensure_schema(schema)
    if tables.supported_table_args and tables.supported_table_args != schema.supported_table_args:
        raise ExecuteError("Tables must support the same table args as schema")
    expression = maybe_parse(sql, dialect=read)
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
