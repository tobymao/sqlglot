from __future__ import annotations

import datetime
import typing as t

from sqlglot import expressions as exp

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql.column import Column
    from sqlglot.dataframe.sql.types import StructType

ColumnLiterals = t.TypeVar(
    "ColumnLiterals",
    bound=t.Union[str, float, int, bool, t.List, t.Tuple, datetime.date, datetime.datetime],
)
ColumnOrName = t.TypeVar("ColumnOrName", bound=t.Union[Column, str])
ColumnOrLiteral = t.TypeVar(
    "ColumnOrLiteral",
    bound=t.Union[Column, str, float, int, bool, t.List, t.Tuple, datetime.date, datetime.datetime],
)
SchemaInput = t.TypeVar(
    "SchemaInput", bound=t.Union[str, t.List[str], StructType, t.Dict[str, str]]
)
OutputExpressionContainer = t.TypeVar(
    "OutputExpressionContainer", bound=t.Union[exp.Select, exp.Create, exp.Insert]
)
