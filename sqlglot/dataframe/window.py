from itertools import chain
import sys
import typing as t

from sqlglot import expressions as exp
from sqlglot.dataframe import functions as F

if t.TYPE_CHECKING:
    from sqlglot.dataframe._typing import ColumnOrName

flatten = chain.from_iterable


class Window:
    _JAVA_MIN_LONG = -(1 << 63)  # -9223372036854775808
    _JAVA_MAX_LONG = (1 << 63) - 1  # 9223372036854775807
    _PRECEDING_THRESHOLD = max(-sys.maxsize, _JAVA_MIN_LONG)
    _FOLLOWING_THRESHOLD = min(sys.maxsize, _JAVA_MAX_LONG)

    unboundedPreceding: int = _JAVA_MIN_LONG

    unboundedFollowing: int = _JAVA_MAX_LONG

    currentRow: int = 0

    @classmethod
    def partitionBy(cls, *cols: t.Union["ColumnOrName", t.List["ColumnOrName"]]) -> "WindowSpec":
        return WindowSpec().partitionBy(*cols)

    @classmethod
    def orderBy(cls, *cols: t.Union["ColumnOrName", t.List["ColumnOrName"]]) -> "WindowSpec":
        return WindowSpec().orderBy(*cols)

    @classmethod
    def rowsBetween(cls, start: int, end: int) -> "WindowSpec":
        return WindowSpec().rowsBetween(start, end)

    @classmethod
    def rangeBetween(cls, start: int, end: int) -> "WindowSpec":
        return WindowSpec().rangeBetween(start, end)


class WindowSpec:
    def __init__(self, expression: exp.Expression = exp.Window()):
        self.expression = expression

    def copy(self):
        return WindowSpec(self.expression.copy())

    def sql(self, **kwargs) -> str:
        return self.expression.sql(dialect="spark", **kwargs)

    def partitionBy(self, *cols: t.Union["ColumnOrName", t.List["ColumnOrName"]]) -> "WindowSpec":
        from sqlglot.dataframe.column import Column

        cols = flatten(cols) if isinstance(cols[0], (list, set, tuple)) else cols
        expressions = [Column.ensure_col(x).expression for x in cols]
        window_spec = self.copy()
        if window_spec.expression.args.get("partition_by") is None:
            window_spec.expression.args["partition_by"] = expressions
        else:
            window_spec.expression.args["partition_by"].extend(expressions)
        return window_spec

    def orderBy(self, *cols: t.Union["ColumnOrName", t.List["ColumnOrName"]]) -> "WindowSpec":
        from sqlglot.dataframe.column import Column

        cols = flatten(cols) if isinstance(cols[0], (list, set, tuple)) else cols
        expressions = [Column.ensure_col(x).expression for x in cols]
        window_spec = self.copy()
        if window_spec.expression.args.get("order") is None:
            window_spec.expression.args["order"] = exp.Order(expressions=[])
        window_spec.expression.args["order"].args['expressions'].extend(expressions)
        return window_spec

    def _calc_start_end(self, start: int, end: int) -> t.Dict[str, t.Union[str, int]]:
        kwargs = {
            "start_side": None,
            "end_side": None
        }
        if start == Window.currentRow:
            kwargs["start"] = "CURRENT ROW"
        else:
            kwargs = {**kwargs, **{
                "start_side": "PRECEDING",
                "start": "UNBOUNDED" if start <= Window.unboundedPreceding else F.lit(start).expression
            }}
        if end == Window.currentRow:
            kwargs["end"] = "CURRENT ROW"
        else:
            kwargs = {**kwargs, **{
                "end_side": "FOLLOWING",
                "end": "UNBOUNDED" if end >= Window.unboundedFollowing else F.lit(end).expression
            }}
        return kwargs

    def rowsBetween(self, start: int, end: int) -> "WindowSpec":
        window_spec = self.copy()
        spec = self._calc_start_end(start, end)
        spec["kind"] = "ROWS"
        window_spec.expression.args["spec"] = exp.WindowSpec(**{
            **window_spec.expression.args.get("spec", exp.WindowSpec()).args,
            **spec
        })
        return window_spec

    def rangeBetween(self, start: int, end: int) -> "WindowSpec":
        window_spec = self.copy()
        spec = self._calc_start_end(start, end)
        spec["kind"] = "RANGE"
        window_spec.expression.args["spec"] = exp.WindowSpec(**{
            **window_spec.expression.args.get("spec", exp.WindowSpec()).args,
            **spec
        })
        return window_spec
