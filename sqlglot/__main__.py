import sys

import sqlglot


for sql in sqlglot.transpile(sys.argv[1], transpile_opts={'pretty': True}):
    print(f"{sql};")
