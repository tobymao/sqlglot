import datetime
import unittest

from sqlglot.dataframe.sql import functions as F
from sqlglot.dataframe.sql.window import Window


class TestDataframeColumn(unittest.TestCase):
    def test_eq(self):
        self.assertEqual("cola = 1", (F.col("cola") == 1).sql())

    def test_neq(self):
        self.assertEqual("cola <> 1", (F.col("cola") != 1).sql())

    def test_gt(self):
        self.assertEqual("cola > 1", (F.col("cola") > 1).sql())

    def test_lt(self):
        self.assertEqual("cola < 1", (F.col("cola") < 1).sql())

    def test_le(self):
        self.assertEqual("cola <= 1", (F.col("cola") <= 1).sql())

    def test_ge(self):
        self.assertEqual("cola >= 1", (F.col("cola") >= 1).sql())

    def test_and(self):
        self.assertEqual(
            "cola = colb AND colc = cold",
            ((F.col("cola") == F.col("colb")) & (F.col("colc") == F.col("cold"))).sql(),
        )

    def test_or(self):
        self.assertEqual(
            "cola = colb OR colc = cold",
            ((F.col("cola") == F.col("colb")) | (F.col("colc") == F.col("cold"))).sql(),
        )

    def test_mod(self):
        self.assertEqual("cola % 2", (F.col("cola") % 2).sql())

    def test_add(self):
        self.assertEqual("cola + 1", (F.col("cola") + 1).sql())

    def test_sub(self):
        self.assertEqual("cola - 1", (F.col("cola") - 1).sql())

    def test_mul(self):
        self.assertEqual("cola * 2", (F.col("cola") * 2).sql())

    def test_div(self):
        self.assertEqual("cola / 2", (F.col("cola") / 2).sql())

    def test_radd(self):
        self.assertEqual("1 + cola", (1 + F.col("cola")).sql())

    def test_rsub(self):
        self.assertEqual("1 - cola", (1 - F.col("cola")).sql())

    def test_rmul(self):
        self.assertEqual("1 * cola", (1 * F.col("cola")).sql())

    def test_rdiv(self):
        self.assertEqual("1 / cola", (1 / F.col("cola")).sql())

    def test_pow(self):
        self.assertEqual("POWER(cola, 2)", (F.col("cola") ** 2).sql())

    def test_rpow(self):
        self.assertEqual("POWER(2, cola)", (2 ** F.col("cola")).sql())

    def test_invert(self):
        self.assertEqual("NOT cola", (~F.col("cola")).sql())

    def test_startswith(self):
        self.assertEqual("STARTSWITH(cola, 'test')", F.col("cola").startswith("test").sql())

    def test_endswith(self):
        self.assertEqual("ENDSWITH(cola, 'test')", F.col("cola").endswith("test").sql())

    def test_rlike(self):
        self.assertEqual("cola RLIKE 'foo'", F.col("cola").rlike("foo").sql())

    def test_like(self):
        self.assertEqual("cola LIKE 'foo%'", F.col("cola").like("foo%").sql())

    def test_ilike(self):
        self.assertEqual("cola ILIKE 'foo%'", F.col("cola").ilike("foo%").sql())

    def test_substring(self):
        self.assertEqual("SUBSTRING(cola, 2, 3)", F.col("cola").substr(2, 3).sql())

    def test_isin(self):
        self.assertEqual("cola IN (1, 2, 3)", F.col("cola").isin([1, 2, 3]).sql())
        self.assertEqual("cola IN (1, 2, 3)", F.col("cola").isin(1, 2, 3).sql())

    def test_asc(self):
        self.assertEqual("cola", F.col("cola").asc().sql())

    def test_desc(self):
        self.assertEqual("cola DESC", F.col("cola").desc().sql())

    def test_asc_nulls_first(self):
        self.assertEqual("cola", F.col("cola").asc_nulls_first().sql())

    def test_asc_nulls_last(self):
        self.assertEqual("cola NULLS LAST", F.col("cola").asc_nulls_last().sql())

    def test_desc_nulls_first(self):
        self.assertEqual("cola DESC NULLS FIRST", F.col("cola").desc_nulls_first().sql())

    def test_desc_nulls_last(self):
        self.assertEqual("cola DESC", F.col("cola").desc_nulls_last().sql())

    def test_when_otherwise(self):
        self.assertEqual("CASE WHEN cola = 1 THEN 2 END", F.when(F.col("cola") == 1, 2).sql())
        self.assertEqual(
            "CASE WHEN cola = 1 THEN 2 END", F.col("cola").when(F.col("cola") == 1, 2).sql()
        )
        self.assertEqual(
            "CASE WHEN cola = 1 THEN 2 WHEN colb = 2 THEN 3 END",
            (F.when(F.col("cola") == 1, 2).when(F.col("colb") == 2, 3)).sql(),
        )
        self.assertEqual(
            "CASE WHEN cola = 1 THEN 2 WHEN colb = 2 THEN 3 END",
            F.col("cola").when(F.col("cola") == 1, 2).when(F.col("colb") == 2, 3).sql(),
        )
        self.assertEqual(
            "CASE WHEN cola = 1 THEN 2 WHEN colb = 2 THEN 3 ELSE 4 END",
            F.when(F.col("cola") == 1, 2).when(F.col("colb") == 2, 3).otherwise(4).sql(),
        )

    def test_is_null(self):
        self.assertEqual("cola IS NULL", F.col("cola").isNull().sql())

    def test_is_not_null(self):
        self.assertEqual("NOT cola IS NULL", F.col("cola").isNotNull().sql())

    def test_cast(self):
        self.assertEqual("CAST(cola AS INT)", F.col("cola").cast("INT").sql())

    def test_alias(self):
        self.assertEqual("cola AS new_name", F.col("cola").alias("new_name").sql())

    def test_between(self):
        self.assertEqual("cola BETWEEN 1 AND 3", F.col("cola").between(1, 3).sql())
        self.assertEqual("cola BETWEEN 10.1 AND 12.1", F.col("cola").between(10.1, 12.1).sql())
        self.assertEqual(
            "cola BETWEEN TO_DATE('2022-01-01') AND TO_DATE('2022-03-01')",
            F.col("cola").between(datetime.date(2022, 1, 1), datetime.date(2022, 3, 1)).sql(),
        )
        self.assertEqual(
            "cola BETWEEN CAST('2022-01-01T01:01:01+00:00' AS TIMESTAMP) "
            "AND CAST('2022-03-01T01:01:01+00:00' AS TIMESTAMP)",
            F.col("cola")
            .between(datetime.datetime(2022, 1, 1, 1, 1, 1), datetime.datetime(2022, 3, 1, 1, 1, 1))
            .sql(),
        )

    def test_over(self):
        over_rows = F.sum("cola").over(
            Window.partitionBy("colb").orderBy("colc").rowsBetween(1, Window.unboundedFollowing)
        )
        self.assertEqual(
            "SUM(cola) OVER (PARTITION BY colb ORDER BY colc ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)",
            over_rows.sql(),
        )
        over_range = F.sum("cola").over(
            Window.partitionBy("colb").orderBy("colc").rangeBetween(1, Window.unboundedFollowing)
        )
        self.assertEqual(
            "SUM(cola) OVER (PARTITION BY colb ORDER BY colc RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)",
            over_range.sql(),
        )
