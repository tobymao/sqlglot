import typing as t
import functools
from enum import IntEnum


if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql.dataframe import DataFrame
    from sqlglot.dataframe.sql.group import GroupedData


class Operation(IntEnum):
    INIT = -1
    NO_OP = 0
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
            if self.last_op == Operation.INIT:
                self = self._convert_leaf_to_cte()
                self.last_op = Operation.NO_OP
            last_op = self.last_op
            new_op = op if op != Operation.NO_OP else last_op
            if new_op < last_op or (last_op == new_op and new_op == Operation.SELECT):
                self = self._convert_leaf_to_cte()
            df: t.Union["DataFrame", "GroupedData"] = func(self, *args, **kwargs)
            df.last_op = new_op
            return df
        wrapper.__wrapped__ = func
        return wrapper
    return decorator