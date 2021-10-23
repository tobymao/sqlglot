import argparse

import sqlglot


parser = argparse.ArgumentParser(description="Transpile SQL")
parser.add_argument("sql", metavar="sql", type=str, help="SQL string to transpile")
parser.add_argument(
    "--read",
    dest="read",
    type=str,
    default=None,
    help="Dialect to read default is generic",
)
parser.add_argument(
    "--write",
    dest="write",
    type=str,
    default=None,
    help="Dialect to write default is generic",
)
parser.add_argument(
    "--no-identify",
    dest="identify",
    action="store_false",
    help="Don't auto identify fields",
)
parser.add_argument(
    "--no-pretty",
    dest="pretty",
    action="store_false",
    help="Compress sql",
)
parser.add_argument(
    "--parse",
    dest="parse",
    action="store_true",
    help="Parse and return the expression tree",
)

args = parser.parse_args()

if args.parse:
    sqls = sqlglot.parse(args.sql, read=args.read)
else:
    sqls = sqlglot.transpile(
        args.sql,
        read=args.read,
        write=args.write,
        identify=args.identify,
        pretty=args.pretty,
    )

for sql in sqls:
    print(sql)
