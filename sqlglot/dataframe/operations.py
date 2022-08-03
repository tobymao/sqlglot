import typing as t
import functools
from enum import IntEnum


if t.TYPE_CHECKING:
    from sqlglot.dataframe.dataframe import DataFrame


class Operation(IntEnum):
    NO_OP = -1
    FROM = 1
    WHERE = 2
    GROUP_BY = 3
    HAVING = 4
    SELECT = 5
    ORDER_BY = 6
    LIMIT = 7


def operation(op: Operation):
    def decorator(func: t.Callable):
        @functools.wraps(func)
        def wrapper(self: "DataFrame", *args, **kwargs):
            last_op = self.last_op
            new_op = op if op != Operation.NO_OP else last_op
            if new_op < last_op or (last_op == new_op and new_op == Operation.SELECT):
                self = self._convert_leaf_to_cte()
            df: "DataFrame" = func(self, *args, **kwargs)
            df.last_op = new_op
            return df
        wrapper.__wrapped__ = func
        return wrapper
    return decorator