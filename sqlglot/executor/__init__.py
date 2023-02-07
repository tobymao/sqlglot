"""
.. include:: ../../posts/python_sql_engine.md

----
"""

from __future__ import annotations

import logging
import time
import typing as t

from sqlglot import maybe_parse
from sqlglot.errors import ExecuteError
from sqlglot.executor.python import PythonExecutor
from sqlglot.executor.table import Table, ensure_tables
from sqlglot.optimizer import optimize
from sqlglot.planner import Plan
from sqlglot.schema import ensure_schema

logger = logging.getLogger("sqlglot")

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.executor.table import Tables
    from sqlglot.expressions import Expression
    from sqlglot.schema import Schema


def execute(
    sql: str | Expression,
    schema: t.Optional[t.Dict | Schema] = None,
    read: DialectType = None,
    tables: t.Optional[t.Dict] = None,
) -> Table:
    """
    Run a sql query against data.

    Args:
        sql: a sql statement.
        schema: database schema.
            This can either be an instance of `Schema` or a mapping in one of the following forms:
            1. {table: {col: type}}
            2. {db: {table: {col: type}}}
            3. {catalog: {db: {table: {col: type}}}}
        read: the SQL dialect to apply during parsing (eg. "spark", "hive", "presto", "mysql").
        tables: additional tables to register.

    Returns:
        Simple columnar data structure.
    """
    tables_ = ensure_tables(tables)

    if not schema:
        schema = {
            name: {column: type(table[0][column]).__name__ for column in table.columns}
            for name, table in tables_.mapping.items()
        }

    schema = ensure_schema(schema)

    if tables_.supported_table_args and tables_.supported_table_args != schema.supported_table_args:
        raise ExecuteError("Tables must support the same table args as schema")

    expression = maybe_parse(sql, dialect=read)

    now = time.time()
    expression = optimize(expression, schema, leave_tables_isolated=True)

    logger.debug("Optimization finished: %f", time.time() - now)
    logger.debug("Optimized SQL: %s", expression.sql(pretty=True))

    plan = Plan(expression)

    logger.debug("Logical Plan: %s", plan)

    now = time.time()
    result = PythonExecutor(tables=tables_).execute(plan)

    logger.debug("Query finished: %f", time.time() - now)

    return result
