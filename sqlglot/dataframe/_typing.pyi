import datetime
import decimal
import typing as t

Primitives = t.TypeVar("Primitives", bound=t.Union[str, float, int, bool])
ColumnOrName = t.TypeVar("ColumnOrName", bound=t.Union["Column", str])
ColumnOrPrimitive = t.TypeVar("ColumnOrPrimitive", bound=t.Union["Column", str, float, int, bool])
DateTimeLiteral = t.Union[datetime.datetime, datetime.date]
Literals = Primitives
DecimalLiteral = decimal.Decimal
