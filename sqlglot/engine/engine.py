import logging
import time

from sqlglot import parse_one
from sqlglot.engine import python
from sqlglot.optimizer import optimize
from sqlglot.planner import Plan


logger = logging.getLogger("sqlglot")


def execute(sql, schema, read=None):
    expression = parse_one(sql, read=read)
    expression = optimize(expression, schema)
    logger.debug("Optimized SQL: %s", expression.sql(pretty=True))
    plan = Plan(expression)
    logger.debug("Logical Plan: %s", plan)
    now = time.time()
    result = python.execute(plan)
    logger.debug("Query finished: %f", time.time() - now)
    return result
