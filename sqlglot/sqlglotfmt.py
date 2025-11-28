from __future__ import annotations

import argparse
from pathlib import Path

from sqlglot import parse


def main(argv = None, dialect="postgres") -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('file_names', nargs='*')
    parser.add_argument('--dialect')
    args = parser.parse_args(argv)
    dialect = args.dialect or dialect

    retval = 0
    for file_name in args.file_names:
        file = Path(file_name)
        orig_sql_str = file.read_text()
        sql_str = ";\n".join(stmnt.sql(pretty=True) for stmnt in parse(orig_sql_str, dialect)) + ";"
        file.write_text(sql_str)
        if orig_sql_str != sql_str:
            retval = 1
    return retval


if __name__ == '__main__':
    raise SystemExit(main())
