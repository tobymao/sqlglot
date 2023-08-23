from sqlglot.dataframe.sql import functions as F
from sqlglot.dataframe.sql.window import Window, WindowSpec
from tests.dataframe.unit.dataframe_test_base import DataFrameTestBase


class TestDataframeWindow(DataFrameTestBase):
    def test_window_spec_partition_by(self):
        partition_by = WindowSpec().partitionBy(F.col("cola"), F.col("colb"))
        self.assertEqual("OVER (PARTITION BY cola, colb)", partition_by.sql())

    def test_window_spec_order_by(self):
        order_by = WindowSpec().orderBy("cola", "colb")
        self.assertEqual("OVER (ORDER BY cola, colb)", order_by.sql())

    def test_window_spec_rows_between(self):
        rows_between = WindowSpec().rowsBetween(3, 5)
        self.assertEqual("OVER (ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING)", rows_between.sql())

    def test_window_spec_range_between(self):
        range_between = WindowSpec().rangeBetween(3, 5)
        self.assertEqual("OVER (RANGE BETWEEN 3 PRECEDING AND 5 FOLLOWING)", range_between.sql())

    def test_window_partition_by(self):
        partition_by = Window.partitionBy(F.col("cola"), F.col("colb"))
        self.assertEqual("OVER (PARTITION BY cola, colb)", partition_by.sql())

    def test_window_order_by(self):
        order_by = Window.orderBy("cola", "colb")
        self.assertEqual("OVER (ORDER BY cola, colb)", order_by.sql())

    def test_window_rows_between(self):
        rows_between = Window.rowsBetween(3, 5)
        self.assertEqual("OVER (ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING)", rows_between.sql())

    def test_window_range_between(self):
        range_between = Window.rangeBetween(3, 5)
        self.assertEqual("OVER (RANGE BETWEEN 3 PRECEDING AND 5 FOLLOWING)", range_between.sql())

    def test_window_rows_unbounded(self):
        rows_between_unbounded_start = Window.rowsBetween(Window.unboundedPreceding, 2)
        self.assertEqual(
            "OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING)",
            rows_between_unbounded_start.sql(),
        )
        rows_between_unbounded_end = Window.rowsBetween(1, Window.unboundedFollowing)
        self.assertEqual(
            "OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)",
            rows_between_unbounded_end.sql(),
        )
        rows_between_unbounded_both = Window.rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )
        self.assertEqual(
            "OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
            rows_between_unbounded_both.sql(),
        )

    def test_window_range_unbounded(self):
        range_between_unbounded_start = Window.rangeBetween(Window.unboundedPreceding, 2)
        self.assertEqual(
            "OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING)",
            range_between_unbounded_start.sql(),
        )
        range_between_unbounded_end = Window.rangeBetween(1, Window.unboundedFollowing)
        self.assertEqual(
            "OVER (RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)",
            range_between_unbounded_end.sql(),
        )
        range_between_unbounded_both = Window.rangeBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )
        self.assertEqual(
            "OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
            range_between_unbounded_both.sql(),
        )
