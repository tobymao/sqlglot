import sys

import sqlglot


sql = """
SELECT AVG(X) OVER (PARTITION BY x RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW)
"""
for sql in sqlglot.transpile(sql, pretty=True):
    print(sql)
