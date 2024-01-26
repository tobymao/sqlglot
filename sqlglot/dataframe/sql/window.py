from __future__ import annotations

import sys
import typing as t

from sqlglot import expressions as exp
from sqlglot.dataframe.sql import functions as F
from sqlglot.helper import flatten

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql._typing import ColumnOrName


class Window:
    _JAVA_MIN_LONG = -(1 << 63)  # -9223372036854775808
    _JAVA_MAX_LONG = (1 << 63) - 1  # 9223372036854775807
    _PRECEDING_THRESHOLD = max(-sys.maxsize, _JAVA_MIN_LONG)
    _FOLLOWING_THRESHOLD = min(sys.maxsize, _JAVA_MAX_LONG)

    unboundedPreceding: int = _JAVA_MIN_LONG

    unboundedFollowing: int = _JAVA_MAX_LONG

    currentRow: int = 0

    @classmethod
    def partitionBy(cls, *cols: t.Union[ColumnOrName, t.List[ColumnOrName]]) -> WindowSpec:
        return WindowSpec().partitionBy(*cols)

    @classmethod
    def orderBy(cls, *cols: t.Union[ColumnOrName, t.List[ColumnOrName]]) -> WindowSpec:
        return WindowSpec().orderBy(*cols)

    @classmethod
    def rowsBetween(cls, start: int, end: int) -> WindowSpec:
        return WindowSpec().rowsBetween(start, end)

    @classmethod
    def rangeBetween(cls, start: int, end: int) -> WindowSpec:
        return WindowSpec().rangeBetween(start, end)


class WindowSpec:
    def __init__(self, expression: exp.Expression = exp.Window()):
        self.expression = expression

    def copy(self):
        return WindowSpec(self.expression.copy())

    def sql(self, **kwargs) -> str:
        from sqlglot.dataframe.sql.session import SparkSession

        return self.expression.sql(dialect=SparkSession().dialect, **kwargs)

    def partitionBy(self, *cols: t.Union[ColumnOrName, t.List[ColumnOrName]]) -> WindowSpec:
        from sqlglot.dataframe.sql.column import Column

        cols = flatten(cols) if isinstance(cols[0], (list, set, tuple)) else cols  # type: ignore
        expressions = [Column.ensure_col(x).expression for x in cols]
        window_spec = self.copy()
        partition_by_expressions = window_spec.expression.args.get("partition_by", [])
        partition_by_expressions.extend(expressions)
        window_spec.expression.set("partition_by", partition_by_expressions)
        return window_spec

    def orderBy(self, *cols: t.Union[ColumnOrName, t.List[ColumnOrName]]) -> WindowSpec:
        from sqlglot.dataframe.sql.column import Column

        cols = flatten(cols) if isinstance(cols[0], (list, set, tuple)) else cols  # type: ignore
        expressions = [Column.ensure_col(x).expression for x in cols]
        window_spec = self.copy()
        if window_spec.expression.args.get("order") is None:
            window_spec.expression.set("order", exp.Order(expressions=[]))
        order_by = window_spec.expression.args["order"].expressions
        order_by.extend(expressions)
        window_spec.expression.args["order"].set("expressions", order_by)
        return window_spec

    def _calc_start_end(
        self, start: int, end: int
    ) -> t.Dict[str, t.Optional[t.Union[str, exp.Expression]]]:
        kwargs: t.Dict[str, t.Optional[t.Union[str, exp.Expression]]] = {
            "start_side": None,
            "end_side": None,
        }
        if start == Window.currentRow:
            kwargs["start"] = "CURRENT ROW"
        else:
            kwargs = {
                **kwargs,
                **{
                    "start_side": "PRECEDING",
                    "start": (
                        "UNBOUNDED"
                        if start <= Window.unboundedPreceding
                        else F.lit(start).expression
                    ),
                },
            }
        if end == Window.currentRow:
            kwargs["end"] = "CURRENT ROW"
        else:
            kwargs = {
                **kwargs,
                **{
                    "end_side": "FOLLOWING",
                    "end": (
                        "UNBOUNDED" if end >= Window.unboundedFollowing else F.lit(end).expression
                    ),
                },
            }
        return kwargs

    def rowsBetween(self, start: int, end: int) -> WindowSpec:
        window_spec = self.copy()
        spec = self._calc_start_end(start, end)
        spec["kind"] = "ROWS"
        window_spec.expression.set(
            "spec",
            exp.WindowSpec(
                **{**window_spec.expression.args.get("spec", exp.WindowSpec()).args, **spec}
            ),
        )
        return window_spec

    def rangeBetween(self, start: int, end: int) -> WindowSpec:
        window_spec = self.copy()
        spec = self._calc_start_end(start, end)
        spec["kind"] = "RANGE"
        window_spec.expression.set(
            "spec",
            exp.WindowSpec(
                **{**window_spec.expression.args.get("spec", exp.WindowSpec()).args, **spec}
            ),
        )
        return window_spec
