from __future__ import annotations

import functools
import typing as t
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
    """
    Decorator used around DataFrame methods to indicate what type of operation is being performed from the
    ordered Operation enums. This is used to determine which operations should be performed on a CTE vs.
    included with the previous operation.

    Ex: After a user does a join we want to allow them to select which columns for the different
    tables that they want to carry through to the following operation. If we put that join in
    a CTE preemptively then the user would not have a chance to select which column they want
    in cases where there is overlap in names.
    """

    def decorator(func: t.Callable):
        @functools.wraps(func)
        def wrapper(self: DataFrame, *args, **kwargs):
            if self.last_op == Operation.INIT:
                self = self._convert_leaf_to_cte()
                self.last_op = Operation.NO_OP
            last_op = self.last_op
            new_op = op if op != Operation.NO_OP else last_op
            if new_op < last_op or (last_op == new_op and new_op == Operation.SELECT):
                self = self._convert_leaf_to_cte()
            df: t.Union[DataFrame, GroupedData] = func(self, *args, **kwargs)
            df.last_op = new_op  # type: ignore
            return df

        wrapper.__wrapped__ = func  # type: ignore
        return wrapper

    return decorator
