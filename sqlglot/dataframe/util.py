from itertools import chain
import typing as t

import sqlglot
from sqlglot import expressions as exp
from sqlglot.dataframe.column import Column
from sqlglot.helper import ensure_list

flatten = chain.from_iterable


def ensure_strings(args: t.List[t.Union[str, Column]]) -> t.List[str]:
    return [x.expression if isinstance(x, Column) else x for x in args]


def ensure_columns(args: t.List[t.Union[str, Column]]) -> t.List[Column]:
    return [Column(x) if not isinstance(x, Column) else x for x in args]


def ensure_sqlglot_column(args: t.Union[t.List[t.Union[str, Column, exp.Column]], t.Union[str, Column, exp.Column]]):
    args = ensure_list(args)
    results = []
    for arg in args:
        if isinstance(arg, Column):
            results.append(arg.expression)
        elif isinstance(arg, str):
            results.append(sqlglot.parse_one(arg))
        # Going to assume if not string or column then it is some sqlglot expression
        # This could be a bad assumption
        else:
            results.append(arg)
    return results


def convert_join_type(join_type: str) -> str:
    return join_type.replace("_", " ")
