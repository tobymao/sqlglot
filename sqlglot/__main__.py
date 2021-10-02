import sys

import sqlglot


for sql in sqlglot.transpile(sys.argv[1], read="presto", write="spark", pretty=True):
    print(sql)
