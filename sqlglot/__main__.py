import sys

import sqlglot


for sql in sqlglot.transpile(sys.argv[1]):
    print(f"{sql};")
