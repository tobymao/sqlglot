from __future__ import annotations

import datetime
import decimal
import typing as t

from sqlglot import expressions as exp

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql.column import Column

Primitives = t.TypeVar(Primitives, bound=t.Union[str, float, int, bool])
ColumnOrName = t.TypeVar("ColumnOrName", bound=t.Union[Column, str])
ColumnOrPrimitive = t.TypeVar("ColumnOrPrimitive", bound=t.Union[Column, str, float, int, bool])
DateTimeLiteral = t.Union[datetime.datetime, datetime.date]
Literals = Primitives
DecimalLiteral = decimal.Decimal
SchemaInput = t.TypeVar("SchemaInput", bound=t.Union[str, t.List[str], "StructType", t.Dict[str, str]])
OutputExpressionContainer = t.TypeVar("OutputExpressionContainer", bound=t.Union[exp.Select, exp.Create, exp.Insert])
