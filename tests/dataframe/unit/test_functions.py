import datetime
import inspect
import unittest

from sqlglot import expressions as exp, parse_one
from sqlglot.dataframe.sql import functions as SF
from sqlglot.errors import ErrorLevel


class TestFunctions(unittest.TestCase):
    def test_invoke_anonymous(self):
        for name, func in inspect.getmembers(SF, inspect.isfunction):
            with self.subTest(f"{name} should not invoke anonymous_function"):
                if "invoke_anonymous_function" in inspect.getsource(func):
                    func = parse_one(f"{name}()", read="spark", error_level=ErrorLevel.IGNORE)
                    self.assertIsInstance(func, exp.Anonymous)

    def test_lit(self):
        test_str = SF.lit("test")
        self.assertEqual("'test'", test_str.sql())
        test_int = SF.lit(30)
        self.assertEqual("30", test_int.sql())
        test_float = SF.lit(10.10)
        self.assertEqual("10.1", test_float.sql())
        test_bool = SF.lit(False)
        self.assertEqual("FALSE", test_bool.sql())
        test_null = SF.lit(None)
        self.assertEqual("NULL", test_null.sql())
        test_date = SF.lit(datetime.date(2022, 1, 1))
        self.assertEqual("CAST('2022-01-01' AS DATE)", test_date.sql())
        test_datetime = SF.lit(datetime.datetime(2022, 1, 1, 1, 1, 1))
        self.assertEqual("CAST('2022-01-01T01:01:01+00:00' AS TIMESTAMP)", test_datetime.sql())
        test_dict = SF.lit({"cola": 1, "colb": "test"})
        self.assertEqual("STRUCT(1 AS cola, 'test' AS colb)", test_dict.sql())

    def test_col(self):
        test_col = SF.col("cola")
        self.assertEqual("cola", test_col.sql())
        test_col_with_table = SF.col("table.cola")
        self.assertEqual("table.cola", test_col_with_table.sql())
        test_col_on_col = SF.col(test_col)
        self.assertEqual("cola", test_col_on_col.sql())
        test_int = SF.col(10)
        self.assertEqual("10", test_int.sql())
        test_float = SF.col(10.10)
        self.assertEqual("10.1", test_float.sql())
        test_bool = SF.col(True)
        self.assertEqual("TRUE", test_bool.sql())
        test_array = SF.col([1, 2, "3"])
        self.assertEqual("ARRAY(1, 2, '3')", test_array.sql())
        test_date = SF.col(datetime.date(2022, 1, 1))
        self.assertEqual("CAST('2022-01-01' AS DATE)", test_date.sql())
        test_datetime = SF.col(datetime.datetime(2022, 1, 1, 1, 1, 1))
        self.assertEqual("CAST('2022-01-01T01:01:01+00:00' AS TIMESTAMP)", test_datetime.sql())
        test_dict = SF.col({"cola": 1, "colb": "test"})
        self.assertEqual("STRUCT(1 AS cola, 'test' AS colb)", test_dict.sql())

    def test_asc(self):
        asc_str = SF.asc("cola")
        # ASC is removed from output since that is default so we can't check sql
        self.assertIsInstance(asc_str.expression, exp.Ordered)
        asc_col = SF.asc(SF.col("cola"))
        self.assertIsInstance(asc_col.expression, exp.Ordered)

    def test_desc(self):
        desc_str = SF.desc("cola")
        self.assertEqual("cola DESC", desc_str.sql())
        desc_col = SF.desc(SF.col("cola"))
        self.assertEqual("cola DESC", desc_col.sql())

    def test_sqrt(self):
        col_str = SF.sqrt("cola")
        self.assertEqual("SQRT(cola)", col_str.sql())
        col = SF.sqrt(SF.col("cola"))
        self.assertEqual("SQRT(cola)", col.sql())

    def test_abs(self):
        col_str = SF.abs("cola")
        self.assertEqual("ABS(cola)", col_str.sql())
        col = SF.abs(SF.col("cola"))
        self.assertEqual("ABS(cola)", col.sql())

    def test_max(self):
        col_str = SF.max("cola")
        self.assertEqual("MAX(cola)", col_str.sql())
        col = SF.max(SF.col("cola"))
        self.assertEqual("MAX(cola)", col.sql())

    def test_min(self):
        col_str = SF.min("cola")
        self.assertEqual("MIN(cola)", col_str.sql())
        col = SF.min(SF.col("cola"))
        self.assertEqual("MIN(cola)", col.sql())

    def test_max_by(self):
        col_str = SF.max_by("cola", "colb")
        self.assertEqual("MAX_BY(cola, colb)", col_str.sql())
        col = SF.max_by(SF.col("cola"), SF.col("colb"))
        self.assertEqual("MAX_BY(cola, colb)", col.sql())

    def test_min_by(self):
        col_str = SF.min_by("cola", "colb")
        self.assertEqual("MIN_BY(cola, colb)", col_str.sql())
        col = SF.min_by(SF.col("cola"), SF.col("colb"))
        self.assertEqual("MIN_BY(cola, colb)", col.sql())

    def test_count(self):
        col_str = SF.count("cola")
        self.assertEqual("COUNT(cola)", col_str.sql())
        col = SF.count(SF.col("cola"))
        self.assertEqual("COUNT(cola)", col.sql())

    def test_sum(self):
        col_str = SF.sum("cola")
        self.assertEqual("SUM(cola)", col_str.sql())
        col = SF.sum(SF.col("cola"))
        self.assertEqual("SUM(cola)", col.sql())

    def test_avg(self):
        col_str = SF.avg("cola")
        self.assertEqual("AVG(cola)", col_str.sql())
        col = SF.avg(SF.col("cola"))
        self.assertEqual("AVG(cola)", col.sql())

    def test_mean(self):
        col_str = SF.mean("cola")
        self.assertEqual("MEAN(cola)", col_str.sql())
        col = SF.mean(SF.col("cola"))
        self.assertEqual("MEAN(cola)", col.sql())

    def test_sum_distinct(self):
        with self.assertRaises(NotImplementedError):
            SF.sum_distinct("cola")
        with self.assertRaises(NotImplementedError):
            SF.sumDistinct("cola")

    def test_product(self):
        with self.assertRaises(NotImplementedError):
            SF.product("cola")
        with self.assertRaises(NotImplementedError):
            SF.product("cola")

    def test_acos(self):
        col_str = SF.acos("cola")
        self.assertEqual("ACOS(cola)", col_str.sql())
        col = SF.acos(SF.col("cola"))
        self.assertEqual("ACOS(cola)", col.sql())

    def test_acosh(self):
        col_str = SF.acosh("cola")
        self.assertEqual("ACOSH(cola)", col_str.sql())
        col = SF.acosh(SF.col("cola"))
        self.assertEqual("ACOSH(cola)", col.sql())

    def test_asin(self):
        col_str = SF.asin("cola")
        self.assertEqual("ASIN(cola)", col_str.sql())
        col = SF.asin(SF.col("cola"))
        self.assertEqual("ASIN(cola)", col.sql())

    def test_asinh(self):
        col_str = SF.asinh("cola")
        self.assertEqual("ASINH(cola)", col_str.sql())
        col = SF.asinh(SF.col("cola"))
        self.assertEqual("ASINH(cola)", col.sql())

    def test_atan(self):
        col_str = SF.atan("cola")
        self.assertEqual("ATAN(cola)", col_str.sql())
        col = SF.atan(SF.col("cola"))
        self.assertEqual("ATAN(cola)", col.sql())

    def test_atan2(self):
        col_str = SF.atan2("cola", "colb")
        self.assertEqual("ATAN2(cola, colb)", col_str.sql())
        col = SF.atan2(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ATAN2(cola, colb)", col.sql())
        col_float = SF.atan2(10.10, "colb")
        self.assertEqual("ATAN2(10.1, colb)", col_float.sql())
        col_float2 = SF.atan2("cola", 10.10)
        self.assertEqual("ATAN2(cola, 10.1)", col_float2.sql())

    def test_atanh(self):
        col_str = SF.atanh("cola")
        self.assertEqual("ATANH(cola)", col_str.sql())
        col = SF.atanh(SF.col("cola"))
        self.assertEqual("ATANH(cola)", col.sql())

    def test_cbrt(self):
        col_str = SF.cbrt("cola")
        self.assertEqual("CBRT(cola)", col_str.sql())
        col = SF.cbrt(SF.col("cola"))
        self.assertEqual("CBRT(cola)", col.sql())

    def test_ceil(self):
        col_str = SF.ceil("cola")
        self.assertEqual("CEIL(cola)", col_str.sql())
        col = SF.ceil(SF.col("cola"))
        self.assertEqual("CEIL(cola)", col.sql())

    def test_cos(self):
        col_str = SF.cos("cola")
        self.assertEqual("COS(cola)", col_str.sql())
        col = SF.cos(SF.col("cola"))
        self.assertEqual("COS(cola)", col.sql())

    def test_cosh(self):
        col_str = SF.cosh("cola")
        self.assertEqual("COSH(cola)", col_str.sql())
        col = SF.cosh(SF.col("cola"))
        self.assertEqual("COSH(cola)", col.sql())

    def test_cot(self):
        col_str = SF.cot("cola")
        self.assertEqual("COT(cola)", col_str.sql())
        col = SF.cot(SF.col("cola"))
        self.assertEqual("COT(cola)", col.sql())

    def test_csc(self):
        col_str = SF.csc("cola")
        self.assertEqual("CSC(cola)", col_str.sql())
        col = SF.csc(SF.col("cola"))
        self.assertEqual("CSC(cola)", col.sql())

    def test_exp(self):
        col_str = SF.exp("cola")
        self.assertEqual("EXP(cola)", col_str.sql())
        col = SF.exp(SF.col("cola"))
        self.assertEqual("EXP(cola)", col.sql())

    def test_expm1(self):
        col_str = SF.expm1("cola")
        self.assertEqual("EXPM1(cola)", col_str.sql())
        col = SF.expm1(SF.col("cola"))
        self.assertEqual("EXPM1(cola)", col.sql())

    def test_floor(self):
        col_str = SF.floor("cola")
        self.assertEqual("FLOOR(cola)", col_str.sql())
        col = SF.floor(SF.col("cola"))
        self.assertEqual("FLOOR(cola)", col.sql())

    def test_log(self):
        col_str = SF.log("cola")
        self.assertEqual("LN(cola)", col_str.sql())
        col = SF.log(SF.col("cola"))
        self.assertEqual("LN(cola)", col.sql())
        col_arg = SF.log(10.0, "age")
        self.assertEqual("LOG(10.0, age)", col_arg.sql())

    def test_log10(self):
        col_str = SF.log10("cola")
        self.assertEqual("LOG10(cola)", col_str.sql())
        col = SF.log10(SF.col("cola"))
        self.assertEqual("LOG10(cola)", col.sql())

    def test_log1p(self):
        col_str = SF.log1p("cola")
        self.assertEqual("LOG1P(cola)", col_str.sql())
        col = SF.log1p(SF.col("cola"))
        self.assertEqual("LOG1P(cola)", col.sql())

    def test_log2(self):
        col_str = SF.log2("cola")
        self.assertEqual("LOG2(cola)", col_str.sql())
        col = SF.log2(SF.col("cola"))
        self.assertEqual("LOG2(cola)", col.sql())

    def test_rint(self):
        col_str = SF.rint("cola")
        self.assertEqual("RINT(cola)", col_str.sql())
        col = SF.rint(SF.col("cola"))
        self.assertEqual("RINT(cola)", col.sql())

    def test_sec(self):
        col_str = SF.sec("cola")
        self.assertEqual("SEC(cola)", col_str.sql())
        col = SF.sec(SF.col("cola"))
        self.assertEqual("SEC(cola)", col.sql())

    def test_signum(self):
        col_str = SF.signum("cola")
        self.assertEqual("SIGN(cola)", col_str.sql())
        col = SF.signum(SF.col("cola"))
        self.assertEqual("SIGN(cola)", col.sql())

    def test_sin(self):
        col_str = SF.sin("cola")
        self.assertEqual("SIN(cola)", col_str.sql())
        col = SF.sin(SF.col("cola"))
        self.assertEqual("SIN(cola)", col.sql())

    def test_sinh(self):
        col_str = SF.sinh("cola")
        self.assertEqual("SINH(cola)", col_str.sql())
        col = SF.sinh(SF.col("cola"))
        self.assertEqual("SINH(cola)", col.sql())

    def test_tan(self):
        col_str = SF.tan("cola")
        self.assertEqual("TAN(cola)", col_str.sql())
        col = SF.tan(SF.col("cola"))
        self.assertEqual("TAN(cola)", col.sql())

    def test_tanh(self):
        col_str = SF.tanh("cola")
        self.assertEqual("TANH(cola)", col_str.sql())
        col = SF.tanh(SF.col("cola"))
        self.assertEqual("TANH(cola)", col.sql())

    def test_degrees(self):
        col_str = SF.degrees("cola")
        self.assertEqual("DEGREES(cola)", col_str.sql())
        col = SF.degrees(SF.col("cola"))
        self.assertEqual("DEGREES(cola)", col.sql())
        col_legacy = SF.toDegrees(SF.col("cola"))
        self.assertEqual("DEGREES(cola)", col_legacy.sql())

    def test_radians(self):
        col_str = SF.radians("cola")
        self.assertEqual("RADIANS(cola)", col_str.sql())
        col = SF.radians(SF.col("cola"))
        self.assertEqual("RADIANS(cola)", col.sql())
        col_legacy = SF.toRadians(SF.col("cola"))
        self.assertEqual("RADIANS(cola)", col_legacy.sql())

    def test_bitwise_not(self):
        col_str = SF.bitwise_not("cola")
        self.assertEqual("~cola", col_str.sql())
        col = SF.bitwise_not(SF.col("cola"))
        self.assertEqual("~cola", col.sql())
        col_legacy = SF.bitwiseNOT(SF.col("cola"))
        self.assertEqual("~cola", col_legacy.sql())

    def test_asc_nulls_first(self):
        col_str = SF.asc_nulls_first("cola")
        self.assertIsInstance(col_str.expression, exp.Ordered)
        self.assertEqual("cola ASC", col_str.sql())
        col = SF.asc_nulls_first(SF.col("cola"))
        self.assertIsInstance(col.expression, exp.Ordered)
        self.assertEqual("cola ASC", col.sql())

    def test_asc_nulls_last(self):
        col_str = SF.asc_nulls_last("cola")
        self.assertIsInstance(col_str.expression, exp.Ordered)
        self.assertEqual("cola ASC NULLS LAST", col_str.sql())
        col = SF.asc_nulls_last(SF.col("cola"))
        self.assertIsInstance(col.expression, exp.Ordered)
        self.assertEqual("cola ASC NULLS LAST", col.sql())

    def test_desc_nulls_first(self):
        col_str = SF.desc_nulls_first("cola")
        self.assertIsInstance(col_str.expression, exp.Ordered)
        self.assertEqual("cola DESC NULLS FIRST", col_str.sql())
        col = SF.desc_nulls_first(SF.col("cola"))
        self.assertIsInstance(col.expression, exp.Ordered)
        self.assertEqual("cola DESC NULLS FIRST", col.sql())

    def test_desc_nulls_last(self):
        col_str = SF.desc_nulls_last("cola")
        self.assertIsInstance(col_str.expression, exp.Ordered)
        self.assertEqual("cola DESC", col_str.sql())
        col = SF.desc_nulls_last(SF.col("cola"))
        self.assertIsInstance(col.expression, exp.Ordered)
        self.assertEqual("cola DESC", col.sql())

    def test_stddev(self):
        col_str = SF.stddev("cola")
        self.assertEqual("STDDEV(cola)", col_str.sql())
        col = SF.stddev(SF.col("cola"))
        self.assertEqual("STDDEV(cola)", col.sql())

    def test_stddev_samp(self):
        col_str = SF.stddev_samp("cola")
        self.assertEqual("STDDEV_SAMP(cola)", col_str.sql())
        col = SF.stddev_samp(SF.col("cola"))
        self.assertEqual("STDDEV_SAMP(cola)", col.sql())

    def test_stddev_pop(self):
        col_str = SF.stddev_pop("cola")
        self.assertEqual("STDDEV_POP(cola)", col_str.sql())
        col = SF.stddev_pop(SF.col("cola"))
        self.assertEqual("STDDEV_POP(cola)", col.sql())

    def test_variance(self):
        col_str = SF.variance("cola")
        self.assertEqual("VARIANCE(cola)", col_str.sql())
        col = SF.variance(SF.col("cola"))
        self.assertEqual("VARIANCE(cola)", col.sql())

    def test_var_samp(self):
        col_str = SF.var_samp("cola")
        self.assertEqual("VARIANCE(cola)", col_str.sql())
        col = SF.var_samp(SF.col("cola"))
        self.assertEqual("VARIANCE(cola)", col.sql())

    def test_var_pop(self):
        col_str = SF.var_pop("cola")
        self.assertEqual("VAR_POP(cola)", col_str.sql())
        col = SF.var_pop(SF.col("cola"))
        self.assertEqual("VAR_POP(cola)", col.sql())

    def test_skewness(self):
        col_str = SF.skewness("cola")
        self.assertEqual("SKEWNESS(cola)", col_str.sql())
        col = SF.skewness(SF.col("cola"))
        self.assertEqual("SKEWNESS(cola)", col.sql())

    def test_kurtosis(self):
        col_str = SF.kurtosis("cola")
        self.assertEqual("KURTOSIS(cola)", col_str.sql())
        col = SF.kurtosis(SF.col("cola"))
        self.assertEqual("KURTOSIS(cola)", col.sql())

    def test_collect_list(self):
        col_str = SF.collect_list("cola")
        self.assertEqual("COLLECT_LIST(cola)", col_str.sql())
        col = SF.collect_list(SF.col("cola"))
        self.assertEqual("COLLECT_LIST(cola)", col.sql())

    def test_collect_set(self):
        col_str = SF.collect_set("cola")
        self.assertEqual("COLLECT_SET(cola)", col_str.sql())
        col = SF.collect_set(SF.col("cola"))
        self.assertEqual("COLLECT_SET(cola)", col.sql())

    def test_hypot(self):
        col_str = SF.hypot("cola", "colb")
        self.assertEqual("HYPOT(cola, colb)", col_str.sql())
        col = SF.hypot(SF.col("cola"), SF.col("colb"))
        self.assertEqual("HYPOT(cola, colb)", col.sql())
        col_float = SF.hypot(10.10, "colb")
        self.assertEqual("HYPOT(10.1, colb)", col_float.sql())
        col_float2 = SF.hypot("cola", 10.10)
        self.assertEqual("HYPOT(cola, 10.1)", col_float2.sql())

    def test_pow(self):
        col_str = SF.pow("cola", "colb")
        self.assertEqual("POWER(cola, colb)", col_str.sql())
        col = SF.pow(SF.col("cola"), SF.col("colb"))
        self.assertEqual("POWER(cola, colb)", col.sql())
        col_float = SF.pow(10.10, "colb")
        self.assertEqual("POWER(10.1, colb)", col_float.sql())
        col_float2 = SF.pow("cola", 10.10)
        self.assertEqual("POWER(cola, 10.1)", col_float2.sql())

    def test_row_number(self):
        col_str = SF.row_number()
        self.assertEqual("ROW_NUMBER()", col_str.sql())
        col = SF.row_number()
        self.assertEqual("ROW_NUMBER()", col.sql())

    def test_dense_rank(self):
        col_str = SF.dense_rank()
        self.assertEqual("DENSE_RANK()", col_str.sql())
        col = SF.dense_rank()
        self.assertEqual("DENSE_RANK()", col.sql())

    def test_rank(self):
        col_str = SF.rank()
        self.assertEqual("RANK()", col_str.sql())
        col = SF.rank()
        self.assertEqual("RANK()", col.sql())

    def test_cume_dist(self):
        col_str = SF.cume_dist()
        self.assertEqual("CUME_DIST()", col_str.sql())
        col = SF.cume_dist()
        self.assertEqual("CUME_DIST()", col.sql())

    def test_percent_rank(self):
        col_str = SF.percent_rank()
        self.assertEqual("PERCENT_RANK()", col_str.sql())
        col = SF.percent_rank()
        self.assertEqual("PERCENT_RANK()", col.sql())

    def test_approx_count_distinct(self):
        col_str = SF.approx_count_distinct("cola")
        self.assertEqual("APPROX_COUNT_DISTINCT(cola)", col_str.sql())
        col_str_with_accuracy = SF.approx_count_distinct("cola", 0.05)
        self.assertEqual("APPROX_COUNT_DISTINCT(cola, 0.05)", col_str_with_accuracy.sql())
        col = SF.approx_count_distinct(SF.col("cola"))
        self.assertEqual("APPROX_COUNT_DISTINCT(cola)", col.sql())
        col_with_accuracy = SF.approx_count_distinct(SF.col("cola"), 0.05)
        self.assertEqual("APPROX_COUNT_DISTINCT(cola, 0.05)", col_with_accuracy.sql())
        col_legacy = SF.approxCountDistinct(SF.col("cola"))
        self.assertEqual("APPROX_COUNT_DISTINCT(cola)", col_legacy.sql())

    def test_coalesce(self):
        col_str = SF.coalesce("cola", "colb", "colc")
        self.assertEqual("COALESCE(cola, colb, colc)", col_str.sql())
        col = SF.coalesce(SF.col("cola"), "colb", SF.col("colc"))
        self.assertEqual("COALESCE(cola, colb, colc)", col.sql())
        col_single = SF.coalesce("cola")
        self.assertEqual("COALESCE(cola)", col_single.sql())

    def test_corr(self):
        col_str = SF.corr("cola", "colb")
        self.assertEqual("CORR(cola, colb)", col_str.sql())
        col = SF.corr(SF.col("cola"), "colb")
        self.assertEqual("CORR(cola, colb)", col.sql())

    def test_covar_pop(self):
        col_str = SF.covar_pop("cola", "colb")
        self.assertEqual("COVAR_POP(cola, colb)", col_str.sql())
        col = SF.covar_pop(SF.col("cola"), "colb")
        self.assertEqual("COVAR_POP(cola, colb)", col.sql())

    def test_covar_samp(self):
        col_str = SF.covar_samp("cola", "colb")
        self.assertEqual("COVAR_SAMP(cola, colb)", col_str.sql())
        col = SF.covar_samp(SF.col("cola"), "colb")
        self.assertEqual("COVAR_SAMP(cola, colb)", col.sql())

    def test_count_distinct(self):
        col_str = SF.count_distinct("cola")
        self.assertEqual("COUNT(DISTINCT cola)", col_str.sql())
        col = SF.count_distinct(SF.col("cola"))
        self.assertEqual("COUNT(DISTINCT cola)", col.sql())
        col_legacy = SF.countDistinct(SF.col("cola"))
        self.assertEqual("COUNT(DISTINCT cola)", col_legacy.sql())
        col_multiple = SF.count_distinct(SF.col("cola"), SF.col("colb"))
        self.assertEqual("COUNT(DISTINCT cola, colb)", col_multiple.sql())

    def test_first(self):
        col_str = SF.first("cola")
        self.assertEqual("FIRST(cola)", col_str.sql())
        col = SF.first(SF.col("cola"))
        self.assertEqual("FIRST(cola)", col.sql())
        ignore_nulls = SF.first("cola", True)
        self.assertEqual("FIRST(cola) IGNORE NULLS", ignore_nulls.sql())

    def test_grouping_id(self):
        col_str = SF.grouping_id("cola", "colb")
        self.assertEqual("GROUPING_ID(cola, colb)", col_str.sql())
        col = SF.grouping_id(SF.col("cola"), SF.col("colb"))
        self.assertEqual("GROUPING_ID(cola, colb)", col.sql())
        col_grouping_no_arg = SF.grouping_id()
        self.assertEqual("GROUPING_ID()", col_grouping_no_arg.sql())
        col_grouping_single_arg = SF.grouping_id("cola")
        self.assertEqual("GROUPING_ID(cola)", col_grouping_single_arg.sql())

    def test_input_file_name(self):
        col = SF.input_file_name()
        self.assertEqual("INPUT_FILE_NAME()", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnull(self):
        col_str = SF.isnull("cola")
        self.assertEqual("ISNULL(cola)", col_str.sql())
        col = SF.isnull(SF.col("cola"))
        self.assertEqual("ISNULL(cola)", col.sql())

    def test_last(self):
        col_str = SF.last("cola")
        self.assertEqual("LAST(cola)", col_str.sql())
        col = SF.last(SF.col("cola"))
        self.assertEqual("LAST(cola)", col.sql())
        ignore_nulls = SF.last("cola", True)
        self.assertEqual("LAST(cola) IGNORE NULLS", ignore_nulls.sql())

    def test_monotonically_increasing_id(self):
        col = SF.monotonically_increasing_id()
        self.assertEqual("MONOTONICALLY_INCREASING_ID()", col.sql())

    def test_nanvl(self):
        col_str = SF.nanvl("cola", "colb")
        self.assertEqual("NANVL(cola, colb)", col_str.sql())
        col = SF.nanvl(SF.col("cola"), SF.col("colb"))
        self.assertEqual("NANVL(cola, colb)", col.sql())

    def test_percentile_approx(self):
        col_str = SF.percentile_approx("cola", [0.5, 0.4, 0.1])
        self.assertEqual("PERCENTILE_APPROX(cola, ARRAY(0.5, 0.4, 0.1))", col_str.sql())
        col = SF.percentile_approx(SF.col("cola"), [0.5, 0.4, 0.1])
        self.assertEqual("PERCENTILE_APPROX(cola, ARRAY(0.5, 0.4, 0.1))", col.sql())
        col_accuracy = SF.percentile_approx("cola", 0.1, 100)
        self.assertEqual("PERCENTILE_APPROX(cola, 0.1, 100)", col_accuracy.sql())

    def test_rand(self):
        col_str = SF.rand(SF.lit(0))
        self.assertEqual("RAND(0)", col_str.sql())
        col = SF.rand(SF.lit(0))
        self.assertEqual("RAND(0)", col.sql())
        no_col = SF.rand()
        self.assertEqual("RAND()", no_col.sql())

    def test_randn(self):
        col_str = SF.randn(0)
        self.assertEqual("RANDN(0)", col_str.sql())
        col = SF.randn(0)
        self.assertEqual("RANDN(0)", col.sql())
        no_col = SF.randn()
        self.assertEqual("RANDN()", no_col.sql())

    def test_round(self):
        col_str = SF.round("cola", 0)
        self.assertEqual("ROUND(cola, 0)", col_str.sql())
        col = SF.round(SF.col("cola"), 0)
        self.assertEqual("ROUND(cola, 0)", col.sql())
        col_no_scale = SF.round("cola")
        self.assertEqual("ROUND(cola)", col_no_scale.sql())

    def test_bround(self):
        col_str = SF.bround("cola", 0)
        self.assertEqual("BROUND(cola, 0)", col_str.sql())
        col = SF.bround(SF.col("cola"), 0)
        self.assertEqual("BROUND(cola, 0)", col.sql())
        col_no_scale = SF.bround("cola")
        self.assertEqual("BROUND(cola)", col_no_scale.sql())

    def test_shiftleft(self):
        col_str = SF.shiftleft("cola", 1)
        self.assertEqual("SHIFTLEFT(cola, 1)", col_str.sql())
        col = SF.shiftleft(SF.col("cola"), 1)
        self.assertEqual("SHIFTLEFT(cola, 1)", col.sql())
        col_legacy = SF.shiftLeft(SF.col("cola"), 1)
        self.assertEqual("SHIFTLEFT(cola, 1)", col_legacy.sql())

    def test_shiftright(self):
        col_str = SF.shiftright("cola", 1)
        self.assertEqual("SHIFTRIGHT(cola, 1)", col_str.sql())
        col = SF.shiftright(SF.col("cola"), 1)
        self.assertEqual("SHIFTRIGHT(cola, 1)", col.sql())
        col_legacy = SF.shiftRight(SF.col("cola"), 1)
        self.assertEqual("SHIFTRIGHT(cola, 1)", col_legacy.sql())

    def test_shiftrightunsigned(self):
        col_str = SF.shiftrightunsigned("cola", 1)
        self.assertEqual("SHIFTRIGHTUNSIGNED(cola, 1)", col_str.sql())
        col = SF.shiftrightunsigned(SF.col("cola"), 1)
        self.assertEqual("SHIFTRIGHTUNSIGNED(cola, 1)", col.sql())
        col_legacy = SF.shiftRightUnsigned(SF.col("cola"), 1)
        self.assertEqual("SHIFTRIGHTUNSIGNED(cola, 1)", col_legacy.sql())

    def test_expr(self):
        col_str = SF.expr("LENGTH(name)")
        self.assertEqual("LENGTH(name)", col_str.sql())

    def test_struct(self):
        col_str = SF.struct("cola", "colb", "colc")
        self.assertEqual("STRUCT(cola, colb, colc)", col_str.sql())
        col = SF.struct(SF.col("cola"), SF.col("colb"), SF.col("colc"))
        self.assertEqual("STRUCT(cola, colb, colc)", col.sql())
        col_single = SF.struct("cola")
        self.assertEqual("STRUCT(cola)", col_single.sql())
        col_list = SF.struct(["cola", "colb", "colc"])
        self.assertEqual("STRUCT(cola, colb, colc)", col_list.sql())

    def test_greatest(self):
        single_str = SF.greatest("cola")
        self.assertEqual("GREATEST(cola)", single_str.sql())
        single_col = SF.greatest(SF.col("cola"))
        self.assertEqual("GREATEST(cola)", single_col.sql())
        multiple_mix = SF.greatest("col1", "col2", SF.col("col3"), SF.col("col4"))
        self.assertEqual("GREATEST(col1, col2, col3, col4)", multiple_mix.sql())

    def test_least(self):
        single_str = SF.least("cola")
        self.assertEqual("LEAST(cola)", single_str.sql())
        single_col = SF.least(SF.col("cola"))
        self.assertEqual("LEAST(cola)", single_col.sql())
        multiple_mix = SF.least("col1", "col2", SF.col("col3"), SF.col("col4"))
        self.assertEqual("LEAST(col1, col2, col3, col4)", multiple_mix.sql())

    def test_when(self):
        col_simple = SF.when(SF.col("cola") == 2, 1)
        self.assertEqual("CASE WHEN cola = 2 THEN 1 END", col_simple.sql())
        col_complex = SF.when(SF.col("cola") == 2, SF.col("colb") + 2)
        self.assertEqual("CASE WHEN cola = 2 THEN colb + 2 END", col_complex.sql())

    def test_conv(self):
        col_str = SF.conv("cola", 2, 16)
        self.assertEqual("CONV(cola, 2, 16)", col_str.sql())
        col = SF.conv(SF.col("cola"), 2, 16)
        self.assertEqual("CONV(cola, 2, 16)", col.sql())

    def test_factorial(self):
        col_str = SF.factorial("cola")
        self.assertEqual("FACTORIAL(cola)", col_str.sql())
        col = SF.factorial(SF.col("cola"))
        self.assertEqual("FACTORIAL(cola)", col.sql())

    def test_lag(self):
        col_str = SF.lag("cola", 3, "colc")
        self.assertEqual("LAG(cola, 3, colc)", col_str.sql())
        col = SF.lag(SF.col("cola"), 3, "colc")
        self.assertEqual("LAG(cola, 3, colc)", col.sql())
        col_no_default = SF.lag("cola", 3)
        self.assertEqual("LAG(cola, 3)", col_no_default.sql())
        col_no_offset = SF.lag("cola")
        self.assertEqual("LAG(cola)", col_no_offset.sql())

    def test_lead(self):
        col_str = SF.lead("cola", 3, "colc")
        self.assertEqual("LEAD(cola, 3, colc)", col_str.sql())
        col = SF.lead(SF.col("cola"), 3, "colc")
        self.assertEqual("LEAD(cola, 3, colc)", col.sql())
        col_no_default = SF.lead("cola", 3)
        self.assertEqual("LEAD(cola, 3)", col_no_default.sql())
        col_no_offset = SF.lead("cola")
        self.assertEqual("LEAD(cola)", col_no_offset.sql())

    def test_nth_value(self):
        col_str = SF.nth_value("cola", 3)
        self.assertEqual("NTH_VALUE(cola, 3)", col_str.sql())
        col = SF.nth_value(SF.col("cola"), 3)
        self.assertEqual("NTH_VALUE(cola, 3)", col.sql())
        col_no_offset = SF.nth_value("cola")
        self.assertEqual("NTH_VALUE(cola)", col_no_offset.sql())

        self.assertEqual(
            "NTH_VALUE(cola) IGNORE NULLS", SF.nth_value("cola", ignoreNulls=True).sql()
        )

    def test_ntile(self):
        col = SF.ntile(2)
        self.assertEqual("NTILE(2)", col.sql())

    def test_current_date(self):
        col = SF.current_date()
        self.assertEqual("CURRENT_DATE", col.sql())

    def test_current_timestamp(self):
        col = SF.current_timestamp()
        self.assertEqual("CURRENT_TIMESTAMP()", col.sql())

    def test_date_format(self):
        col_str = SF.date_format("cola", "MM/dd/yyy")
        self.assertEqual("DATE_FORMAT(cola, 'MM/dd/yyy')", col_str.sql())
        col = SF.date_format(SF.col("cola"), "MM/dd/yyy")
        self.assertEqual("DATE_FORMAT(cola, 'MM/dd/yyy')", col.sql())

    def test_year(self):
        col_str = SF.year("cola")
        self.assertEqual("YEAR(cola)", col_str.sql())
        col = SF.year(SF.col("cola"))
        self.assertEqual("YEAR(cola)", col.sql())

    def test_quarter(self):
        col_str = SF.quarter("cola")
        self.assertEqual("QUARTER(cola)", col_str.sql())
        col = SF.quarter(SF.col("cola"))
        self.assertEqual("QUARTER(cola)", col.sql())

    def test_month(self):
        col_str = SF.month("cola")
        self.assertEqual("MONTH(cola)", col_str.sql())
        col = SF.month(SF.col("cola"))
        self.assertEqual("MONTH(cola)", col.sql())

    def test_dayofweek(self):
        col_str = SF.dayofweek("cola")
        self.assertEqual("DAYOFWEEK(cola)", col_str.sql())
        col = SF.dayofweek(SF.col("cola"))
        self.assertEqual("DAYOFWEEK(cola)", col.sql())

    def test_dayofmonth(self):
        col_str = SF.dayofmonth("cola")
        self.assertEqual("DAYOFMONTH(cola)", col_str.sql())
        col = SF.dayofmonth(SF.col("cola"))
        self.assertEqual("DAYOFMONTH(cola)", col.sql())

    def test_dayofyear(self):
        col_str = SF.dayofyear("cola")
        self.assertEqual("DAYOFYEAR(cola)", col_str.sql())
        col = SF.dayofyear(SF.col("cola"))
        self.assertEqual("DAYOFYEAR(cola)", col.sql())

    def test_hour(self):
        col_str = SF.hour("cola")
        self.assertEqual("HOUR(cola)", col_str.sql())
        col = SF.hour(SF.col("cola"))
        self.assertEqual("HOUR(cola)", col.sql())

    def test_minute(self):
        col_str = SF.minute("cola")
        self.assertEqual("MINUTE(cola)", col_str.sql())
        col = SF.minute(SF.col("cola"))
        self.assertEqual("MINUTE(cola)", col.sql())

    def test_second(self):
        col_str = SF.second("cola")
        self.assertEqual("SECOND(cola)", col_str.sql())
        col = SF.second(SF.col("cola"))
        self.assertEqual("SECOND(cola)", col.sql())

    def test_weekofyear(self):
        col_str = SF.weekofyear("cola")
        self.assertEqual("WEEKOFYEAR(cola)", col_str.sql())
        col = SF.weekofyear(SF.col("cola"))
        self.assertEqual("WEEKOFYEAR(cola)", col.sql())

    def test_make_date(self):
        col_str = SF.make_date("cola", "colb", "colc")
        self.assertEqual("MAKE_DATE(cola, colb, colc)", col_str.sql())
        col = SF.make_date(SF.col("cola"), SF.col("colb"), "colc")
        self.assertEqual("MAKE_DATE(cola, colb, colc)", col.sql())

    def test_date_add(self):
        col_str = SF.date_add("cola", 2)
        self.assertEqual("DATE_ADD(cola, 2)", col_str.sql())
        col = SF.date_add(SF.col("cola"), 2)
        self.assertEqual("DATE_ADD(cola, 2)", col.sql())
        col_col_for_add = SF.date_add("cola", "colb")
        self.assertEqual("DATE_ADD(cola, colb)", col_col_for_add.sql())
        current_date_add = SF.date_add(SF.current_date(), 5)
        self.assertEqual("DATE_ADD(CURRENT_DATE, 5)", current_date_add.sql())
        self.assertEqual("DATEADD(DAY, 5, CURRENT_DATE)", current_date_add.sql(dialect="snowflake"))

    def test_date_sub(self):
        col_str = SF.date_sub("cola", 2)
        self.assertEqual("DATE_ADD(cola, -2)", col_str.sql())
        col = SF.date_sub(SF.col("cola"), 2)
        self.assertEqual("DATE_ADD(cola, -2)", col.sql())
        col_col_for_add = SF.date_sub("cola", "colb")
        self.assertEqual("DATE_ADD(cola, colb * -1)", col_col_for_add.sql())

    def test_date_diff(self):
        col_str = SF.date_diff("cola", "colb")
        self.assertEqual("DATEDIFF(cola, colb)", col_str.sql())
        col = SF.date_diff(SF.col("cola"), SF.col("colb"))
        self.assertEqual("DATEDIFF(cola, colb)", col.sql())

    def test_add_months(self):
        col_str = SF.add_months("cola", 2)
        self.assertEqual("ADD_MONTHS(cola, 2)", col_str.sql())
        col = SF.add_months(SF.col("cola"), 2)
        self.assertEqual("ADD_MONTHS(cola, 2)", col.sql())
        col_col_for_add = SF.add_months("cola", "colb")
        self.assertEqual("ADD_MONTHS(cola, colb)", col_col_for_add.sql())

    def test_months_between(self):
        col_str = SF.months_between("cola", "colb")
        self.assertEqual("MONTHS_BETWEEN(cola, colb)", col_str.sql())
        col = SF.months_between(SF.col("cola"), SF.col("colb"))
        self.assertEqual("MONTHS_BETWEEN(cola, colb)", col.sql())
        col_round_off = SF.months_between("cola", "colb", True)
        self.assertEqual("MONTHS_BETWEEN(cola, colb, TRUE)", col_round_off.sql())

    def test_to_date(self):
        col_str = SF.to_date("cola")
        self.assertEqual("TO_DATE(cola)", col_str.sql())
        col = SF.to_date(SF.col("cola"))
        self.assertEqual("TO_DATE(cola)", col.sql())
        col_with_format = SF.to_date("cola", "yy-MM-dd")
        self.assertEqual("TO_DATE(cola, 'yy-MM-dd')", col_with_format.sql())

    def test_to_timestamp(self):
        col_str = SF.to_timestamp("cola")
        self.assertEqual("CAST(cola AS TIMESTAMP)", col_str.sql())
        col = SF.to_timestamp(SF.col("cola"))
        self.assertEqual("CAST(cola AS TIMESTAMP)", col.sql())
        col_with_format = SF.to_timestamp("cola", "yyyy-MM-dd")
        self.assertEqual("TO_TIMESTAMP(cola, 'yyyy-MM-dd')", col_with_format.sql())

    def test_trunc(self):
        col_str = SF.trunc("cola", "year")
        self.assertEqual("TRUNC(cola, 'YEAR')", col_str.sql())
        col = SF.trunc(SF.col("cola"), "year")
        self.assertEqual("TRUNC(cola, 'YEAR')", col.sql())

    def test_date_trunc(self):
        col_str = SF.date_trunc("year", "cola")
        self.assertEqual("DATE_TRUNC('YEAR', cola)", col_str.sql())
        col = SF.date_trunc("YEAR", SF.col("cola"))
        self.assertEqual("DATE_TRUNC('YEAR', cola)", col.sql())

    def test_next_day(self):
        col_str = SF.next_day("cola", "Mon")
        self.assertEqual("NEXT_DAY(cola, 'Mon')", col_str.sql())
        col = SF.next_day(SF.col("cola"), "Mon")
        self.assertEqual("NEXT_DAY(cola, 'Mon')", col.sql())

    def test_last_day(self):
        col_str = SF.last_day("cola")
        self.assertEqual("LAST_DAY(cola)", col_str.sql())
        col = SF.last_day(SF.col("cola"))
        self.assertEqual("LAST_DAY(cola)", col.sql())

    def test_from_unixtime(self):
        col_str = SF.from_unixtime("cola")
        self.assertEqual("FROM_UNIXTIME(cola)", col_str.sql())
        col = SF.from_unixtime(SF.col("cola"))
        self.assertEqual("FROM_UNIXTIME(cola)", col.sql())
        col_format = SF.from_unixtime("cola", "yyyy-MM-dd HH:mm")
        self.assertEqual("FROM_UNIXTIME(cola, 'yyyy-MM-dd HH:mm')", col_format.sql())

    def test_unix_timestamp(self):
        col_str = SF.unix_timestamp("cola")
        self.assertEqual("UNIX_TIMESTAMP(cola)", col_str.sql())
        col = SF.unix_timestamp(SF.col("cola"))
        self.assertEqual("UNIX_TIMESTAMP(cola)", col.sql())
        col_format = SF.unix_timestamp("cola", "yyyy-MM-dd HH:mm")
        self.assertEqual("UNIX_TIMESTAMP(cola, 'yyyy-MM-dd HH:mm')", col_format.sql())
        col_current = SF.unix_timestamp()
        self.assertEqual("UNIX_TIMESTAMP()", col_current.sql())

    def test_from_utc_timestamp(self):
        col_str = SF.from_utc_timestamp("cola", "PST")
        self.assertEqual("FROM_UTC_TIMESTAMP(cola, 'PST')", col_str.sql())
        col = SF.from_utc_timestamp(SF.col("cola"), "PST")
        self.assertEqual("FROM_UTC_TIMESTAMP(cola, 'PST')", col.sql())
        col_col = SF.from_utc_timestamp("cola", SF.col("colb"))
        self.assertEqual("FROM_UTC_TIMESTAMP(cola, colb)", col_col.sql())

    def test_to_utc_timestamp(self):
        col_str = SF.to_utc_timestamp("cola", "PST")
        self.assertEqual("TO_UTC_TIMESTAMP(cola, 'PST')", col_str.sql())
        col = SF.to_utc_timestamp(SF.col("cola"), "PST")
        self.assertEqual("TO_UTC_TIMESTAMP(cola, 'PST')", col.sql())
        col_col = SF.to_utc_timestamp("cola", SF.col("colb"))
        self.assertEqual("TO_UTC_TIMESTAMP(cola, colb)", col_col.sql())

    def test_timestamp_seconds(self):
        col_str = SF.timestamp_seconds("cola")
        self.assertEqual("TIMESTAMP_SECONDS(cola)", col_str.sql())
        col = SF.timestamp_seconds(SF.col("cola"))
        self.assertEqual("TIMESTAMP_SECONDS(cola)", col.sql())

    def test_window(self):
        col_str = SF.window("cola", "10 minutes")
        self.assertEqual("WINDOW(cola, '10 minutes')", col_str.sql())
        col = SF.window(SF.col("cola"), "10 minutes")
        self.assertEqual("WINDOW(cola, '10 minutes')", col.sql())
        col_all_values = SF.window("cola", "2 minutes 30 seconds", "30 seconds", "15 seconds")
        self.assertEqual(
            "WINDOW(cola, '2 minutes 30 seconds', '30 seconds', '15 seconds')", col_all_values.sql()
        )
        col_no_start_time = SF.window("cola", "2 minutes 30 seconds", "30 seconds")
        self.assertEqual(
            "WINDOW(cola, '2 minutes 30 seconds', '30 seconds')", col_no_start_time.sql()
        )
        col_no_slide = SF.window("cola", "2 minutes 30 seconds", startTime="15 seconds")
        self.assertEqual(
            "WINDOW(cola, '2 minutes 30 seconds', '2 minutes 30 seconds', '15 seconds')",
            col_no_slide.sql(),
        )

    def test_session_window(self):
        col_str = SF.session_window("cola", "5 seconds")
        self.assertEqual("SESSION_WINDOW(cola, '5 seconds')", col_str.sql())
        col = SF.session_window(SF.col("cola"), SF.lit("5 seconds"))
        self.assertEqual("SESSION_WINDOW(cola, '5 seconds')", col.sql())

    def test_crc32(self):
        col_str = SF.crc32("Spark")
        self.assertEqual("CRC32('Spark')", col_str.sql())
        col = SF.crc32(SF.col("cola"))
        self.assertEqual("CRC32(cola)", col.sql())

    def test_md5(self):
        col_str = SF.md5("Spark")
        self.assertEqual("MD5('Spark')", col_str.sql())
        col = SF.md5(SF.col("cola"))
        self.assertEqual("MD5(cola)", col.sql())

    def test_sha1(self):
        col_str = SF.sha1("Spark")
        self.assertEqual("SHA('Spark')", col_str.sql())
        col = SF.sha1(SF.col("cola"))
        self.assertEqual("SHA(cola)", col.sql())

    def test_sha2(self):
        col_str = SF.sha2("Spark", 256)
        self.assertEqual("SHA2('Spark', 256)", col_str.sql())
        col = SF.sha2(SF.col("cola"), 256)
        self.assertEqual("SHA2(cola, 256)", col.sql())

    def test_hash(self):
        col_str = SF.hash("cola", "colb", "colc")
        self.assertEqual("HASH(cola, colb, colc)", col_str.sql())
        col = SF.hash(SF.col("cola"), SF.col("colb"), SF.col("colc"))
        self.assertEqual("HASH(cola, colb, colc)", col.sql())

    def test_xxhash64(self):
        col_str = SF.xxhash64("cola", "colb", "colc")
        self.assertEqual("XXHASH64(cola, colb, colc)", col_str.sql())
        col = SF.xxhash64(SF.col("cola"), SF.col("colb"), SF.col("colc"))
        self.assertEqual("XXHASH64(cola, colb, colc)", col.sql())

    def test_assert_true(self):
        col = SF.assert_true(SF.col("cola") < SF.col("colb"))
        self.assertEqual("ASSERT_TRUE(cola < colb)", col.sql())
        col_error_msg_col = SF.assert_true(SF.col("cola") < SF.col("colb"), SF.col("colc"))
        self.assertEqual("ASSERT_TRUE(cola < colb, colc)", col_error_msg_col.sql())
        col_error_msg_lit = SF.assert_true(SF.col("cola") < SF.col("colb"), "error")
        self.assertEqual("ASSERT_TRUE(cola < colb, 'error')", col_error_msg_lit.sql())

    def test_raise_error(self):
        col_str = SF.raise_error("custom error message")
        self.assertEqual("RAISE_ERROR('custom error message')", col_str.sql())
        col = SF.raise_error(SF.col("cola"))
        self.assertEqual("RAISE_ERROR(cola)", col.sql())

    def test_upper(self):
        col_str = SF.upper("cola")
        self.assertEqual("UPPER(cola)", col_str.sql())
        col = SF.upper(SF.col("cola"))
        self.assertEqual("UPPER(cola)", col.sql())

    def test_lower(self):
        col_str = SF.lower("cola")
        self.assertEqual("LOWER(cola)", col_str.sql())
        col = SF.lower(SF.col("cola"))
        self.assertEqual("LOWER(cola)", col.sql())

    def test_ascii(self):
        col_str = SF.ascii(SF.lit(2))
        self.assertEqual("ASCII(2)", col_str.sql())
        col = SF.ascii(SF.col("cola"))
        self.assertEqual("ASCII(cola)", col.sql())

    def test_base64(self):
        col_str = SF.base64(SF.lit(2))
        self.assertEqual("BASE64(2)", col_str.sql())
        col = SF.base64(SF.col("cola"))
        self.assertEqual("BASE64(cola)", col.sql())

    def test_unbase64(self):
        col_str = SF.unbase64(SF.lit(2))
        self.assertEqual("UNBASE64(2)", col_str.sql())
        col = SF.unbase64(SF.col("cola"))
        self.assertEqual("UNBASE64(cola)", col.sql())

    def test_ltrim(self):
        col_str = SF.ltrim(SF.lit("Spark"))
        self.assertEqual("LTRIM('Spark')", col_str.sql())
        col = SF.ltrim(SF.col("cola"))
        self.assertEqual("LTRIM(cola)", col.sql())

    def test_rtrim(self):
        col_str = SF.rtrim(SF.lit("Spark"))
        self.assertEqual("RTRIM('Spark')", col_str.sql())
        col = SF.rtrim(SF.col("cola"))
        self.assertEqual("RTRIM(cola)", col.sql())

    def test_trim(self):
        col_str = SF.trim(SF.lit("Spark"))
        self.assertEqual("TRIM('Spark')", col_str.sql())
        col = SF.trim(SF.col("cola"))
        self.assertEqual("TRIM(cola)", col.sql())

    def test_concat_ws(self):
        col_str = SF.concat_ws("-", "cola", "colb")
        self.assertEqual("CONCAT_WS('-', cola, colb)", col_str.sql())
        col = SF.concat_ws("-", SF.col("cola"), SF.col("colb"))
        self.assertEqual("CONCAT_WS('-', cola, colb)", col.sql())

    def test_decode(self):
        col_str = SF.decode("cola", "US-ASCII")
        self.assertEqual("DECODE(cola, 'US-ASCII')", col_str.sql())
        col = SF.decode(SF.col("cola"), "US-ASCII")
        self.assertEqual("DECODE(cola, 'US-ASCII')", col.sql())

    def test_encode(self):
        col_str = SF.encode("cola", "US-ASCII")
        self.assertEqual("ENCODE(cola, 'US-ASCII')", col_str.sql())
        col = SF.encode(SF.col("cola"), "US-ASCII")
        self.assertEqual("ENCODE(cola, 'US-ASCII')", col.sql())

    def test_format_number(self):
        col_str = SF.format_number("cola", 4)
        self.assertEqual("FORMAT_NUMBER(cola, 4)", col_str.sql())
        col = SF.format_number(SF.col("cola"), 4)
        self.assertEqual("FORMAT_NUMBER(cola, 4)", col.sql())

    def test_format_string(self):
        col_str = SF.format_string("%d %s", "cola", "colb", "colc")
        self.assertEqual("FORMAT_STRING('%d %s', cola, colb, colc)", col_str.sql())
        col = SF.format_string("%d %s", SF.col("cola"), SF.col("colb"), SF.col("colc"))
        self.assertEqual("FORMAT_STRING('%d %s', cola, colb, colc)", col.sql())

    def test_instr(self):
        col_str = SF.instr("cola", "test")
        self.assertEqual("INSTR(cola, 'test')", col_str.sql())
        col = SF.instr(SF.col("cola"), "test")
        self.assertEqual("INSTR(cola, 'test')", col.sql())

    def test_overlay(self):
        col_str = SF.overlay("cola", "colb", 3, 7)
        self.assertEqual("OVERLAY(cola, colb, 3, 7)", col_str.sql())
        col = SF.overlay(SF.col("cola"), SF.col("colb"), SF.lit(3), SF.lit(7))
        self.assertEqual("OVERLAY(cola, colb, 3, 7)", col.sql())
        col_no_length = SF.overlay("cola", "colb", 3)
        self.assertEqual("OVERLAY(cola, colb, 3)", col_no_length.sql())

    def test_sentences(self):
        col_str = SF.sentences("cola", SF.lit("en"), SF.lit("US"))
        self.assertEqual("SENTENCES(cola, 'en', 'US')", col_str.sql())
        col = SF.sentences(SF.col("cola"), SF.lit("en"), SF.lit("US"))
        self.assertEqual("SENTENCES(cola, 'en', 'US')", col.sql())
        col_no_country = SF.sentences("cola", SF.lit("en"))
        self.assertEqual("SENTENCES(cola, 'en')", col_no_country.sql())
        col_no_lang = SF.sentences(SF.col("cola"), country=SF.lit("US"))
        self.assertEqual("SENTENCES(cola, 'en', 'US')", col_no_lang.sql())
        col_defaults = SF.sentences(SF.col("cola"))
        self.assertEqual("SENTENCES(cola)", col_defaults.sql())

    def test_substring(self):
        col_str = SF.substring("cola", 2, 3)
        self.assertEqual("SUBSTRING(cola, 2, 3)", col_str.sql())
        col = SF.substring(SF.col("cola"), 2, 3)
        self.assertEqual("SUBSTRING(cola, 2, 3)", col.sql())

    def test_substring_index(self):
        col_str = SF.substring_index("cola", ".", 2)
        self.assertEqual("SUBSTRING_INDEX(cola, '.', 2)", col_str.sql())
        col = SF.substring_index(SF.col("cola"), ".", 2)
        self.assertEqual("SUBSTRING_INDEX(cola, '.', 2)", col.sql())

    def test_levenshtein(self):
        col_str = SF.levenshtein("cola", "colb")
        self.assertEqual("LEVENSHTEIN(cola, colb)", col_str.sql())
        col = SF.levenshtein(SF.col("cola"), SF.col("colb"))
        self.assertEqual("LEVENSHTEIN(cola, colb)", col.sql())

    def test_locate(self):
        col_str = SF.locate("test", "cola", 3)
        self.assertEqual("LOCATE('test', cola, 3)", col_str.sql())
        col = SF.locate("test", SF.col("cola"), 3)
        self.assertEqual("LOCATE('test', cola, 3)", col.sql())
        col_no_pos = SF.locate("test", "cola")
        self.assertEqual("LOCATE('test', cola)", col_no_pos.sql())

    def test_lpad(self):
        col_str = SF.lpad("cola", 3, "#")
        self.assertEqual("LPAD(cola, 3, '#')", col_str.sql())
        col = SF.lpad(SF.col("cola"), 3, "#")
        self.assertEqual("LPAD(cola, 3, '#')", col.sql())

    def test_rpad(self):
        col_str = SF.rpad("cola", 3, "#")
        self.assertEqual("RPAD(cola, 3, '#')", col_str.sql())
        col = SF.rpad(SF.col("cola"), 3, "#")
        self.assertEqual("RPAD(cola, 3, '#')", col.sql())

    def test_repeat(self):
        col_str = SF.repeat("cola", 3)
        self.assertEqual("REPEAT(cola, 3)", col_str.sql())
        col = SF.repeat(SF.col("cola"), 3)
        self.assertEqual("REPEAT(cola, 3)", col.sql())

    def test_split(self):
        col_str = SF.split("cola", "[ABC]", 3)
        self.assertEqual("SPLIT(cola, '[ABC]', 3)", col_str.sql())
        col = SF.split(SF.col("cola"), "[ABC]", 3)
        self.assertEqual("SPLIT(cola, '[ABC]', 3)", col.sql())
        col_no_limit = SF.split("cola", "[ABC]")
        self.assertEqual("SPLIT(cola, '[ABC]')", col_no_limit.sql())

    def test_regexp_extract(self):
        col_str = SF.regexp_extract("cola", r"(\d+)-(\d+)", 1)
        self.assertEqual("REGEXP_EXTRACT(cola, '(\\d+)-(\\d+)', 1)", col_str.sql())
        col = SF.regexp_extract(SF.col("cola"), r"(\d+)-(\d+)", 1)
        self.assertEqual("REGEXP_EXTRACT(cola, '(\\d+)-(\\d+)', 1)", col.sql())
        col_no_idx = SF.regexp_extract(SF.col("cola"), r"(\d+)-(\d+)")
        self.assertEqual("REGEXP_EXTRACT(cola, '(\\d+)-(\\d+)')", col_no_idx.sql())

    def test_regexp_replace(self):
        col_str = SF.regexp_replace("cola", r"(\d+)", "--")
        self.assertEqual("REGEXP_REPLACE(cola, '(\\d+)', '--')", col_str.sql())
        col = SF.regexp_replace(SF.col("cola"), r"(\d+)", "--")
        self.assertEqual("REGEXP_REPLACE(cola, '(\\d+)', '--')", col.sql())

    def test_initcap(self):
        col_str = SF.initcap("cola")
        self.assertEqual("INITCAP(cola)", col_str.sql())
        col = SF.initcap(SF.col("cola"))
        self.assertEqual("INITCAP(cola)", col.sql())

    def test_soundex(self):
        col_str = SF.soundex("cola")
        self.assertEqual("SOUNDEX(cola)", col_str.sql())
        col = SF.soundex(SF.col("cola"))
        self.assertEqual("SOUNDEX(cola)", col.sql())

    def test_bin(self):
        col_str = SF.bin("cola")
        self.assertEqual("BIN(cola)", col_str.sql())
        col = SF.bin(SF.col("cola"))
        self.assertEqual("BIN(cola)", col.sql())

    def test_hex(self):
        col_str = SF.hex("cola")
        self.assertEqual("HEX(cola)", col_str.sql())
        col = SF.hex(SF.col("cola"))
        self.assertEqual("HEX(cola)", col.sql())

    def test_unhex(self):
        col_str = SF.unhex("cola")
        self.assertEqual("UNHEX(cola)", col_str.sql())
        col = SF.unhex(SF.col("cola"))
        self.assertEqual("UNHEX(cola)", col.sql())

    def test_length(self):
        col_str = SF.length("cola")
        self.assertEqual("LENGTH(cola)", col_str.sql())
        col = SF.length(SF.col("cola"))
        self.assertEqual("LENGTH(cola)", col.sql())

    def test_octet_length(self):
        col_str = SF.octet_length("cola")
        self.assertEqual("OCTET_LENGTH(cola)", col_str.sql())
        col = SF.octet_length(SF.col("cola"))
        self.assertEqual("OCTET_LENGTH(cola)", col.sql())

    def test_bit_length(self):
        col_str = SF.bit_length("cola")
        self.assertEqual("BIT_LENGTH(cola)", col_str.sql())
        col = SF.bit_length(SF.col("cola"))
        self.assertEqual("BIT_LENGTH(cola)", col.sql())

    def test_translate(self):
        col_str = SF.translate("cola", "abc", "xyz")
        self.assertEqual("TRANSLATE(cola, 'abc', 'xyz')", col_str.sql())
        col = SF.translate(SF.col("cola"), "abc", "xyz")
        self.assertEqual("TRANSLATE(cola, 'abc', 'xyz')", col.sql())

    def test_array(self):
        col_str = SF.array("cola", "colb")
        self.assertEqual("ARRAY(cola, colb)", col_str.sql())
        col = SF.array(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ARRAY(cola, colb)", col.sql())
        col_array = SF.array(["cola", "colb"])
        self.assertEqual("ARRAY(cola, colb)", col_array.sql())

    def test_create_map(self):
        col_str = SF.create_map("keya", "valuea", "keyb", "valueb")
        self.assertEqual("MAP(keya, valuea, keyb, valueb)", col_str.sql())
        col = SF.create_map(SF.col("keya"), SF.col("valuea"), SF.col("keyb"), SF.col("valueb"))
        self.assertEqual("MAP(keya, valuea, keyb, valueb)", col.sql())
        col_array = SF.create_map(["keya", "valuea", "keyb", "valueb"])
        self.assertEqual("MAP(keya, valuea, keyb, valueb)", col_array.sql())

    def test_map_from_arrays(self):
        col_str = SF.map_from_arrays("cola", "colb")
        self.assertEqual("MAP_FROM_ARRAYS(cola, colb)", col_str.sql())
        col = SF.map_from_arrays(SF.col("cola"), SF.col("colb"))
        self.assertEqual("MAP_FROM_ARRAYS(cola, colb)", col.sql())

    def test_array_contains(self):
        col_str = SF.array_contains("cola", "test")
        self.assertEqual("ARRAY_CONTAINS(cola, 'test')", col_str.sql())
        col = SF.array_contains(SF.col("cola"), "test")
        self.assertEqual("ARRAY_CONTAINS(cola, 'test')", col.sql())
        col_as_value = SF.array_contains("cola", SF.col("colb"))
        self.assertEqual("ARRAY_CONTAINS(cola, colb)", col_as_value.sql())

    def test_arrays_overlap(self):
        col_str = SF.arrays_overlap("cola", "colb")
        self.assertEqual("ARRAYS_OVERLAP(cola, colb)", col_str.sql())
        col = SF.arrays_overlap(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ARRAYS_OVERLAP(cola, colb)", col.sql())

    def test_slice(self):
        col_str = SF.slice("cola", SF.col("colb"), SF.col("colc"))
        self.assertEqual("SLICE(cola, colb, colc)", col_str.sql())
        col = SF.slice(SF.col("cola"), SF.col("colb"), SF.col("colc"))
        self.assertEqual("SLICE(cola, colb, colc)", col.sql())
        col_ints = SF.slice("cola", 1, 10)
        self.assertEqual("SLICE(cola, 1, 10)", col_ints.sql())

    def test_array_join(self):
        col_str = SF.array_join("cola", "-", "NULL_REPLACEMENT")
        self.assertEqual("ARRAY_JOIN(cola, '-', 'NULL_REPLACEMENT')", col_str.sql())
        col = SF.array_join(SF.col("cola"), "-", "NULL_REPLACEMENT")
        self.assertEqual("ARRAY_JOIN(cola, '-', 'NULL_REPLACEMENT')", col.sql())
        col_no_replacement = SF.array_join("cola", "-")
        self.assertEqual("ARRAY_JOIN(cola, '-')", col_no_replacement.sql())

    def test_concat(self):
        col_str = SF.concat("cola", "colb")
        self.assertEqual("CONCAT(cola, colb)", col_str.sql())
        col = SF.concat(SF.col("cola"), SF.col("colb"))
        self.assertEqual("CONCAT(cola, colb)", col.sql())
        col_single = SF.concat("cola")
        self.assertEqual("CONCAT(cola)", col_single.sql())

    def test_array_position(self):
        col_str = SF.array_position("cola", SF.col("colb"))
        self.assertEqual("ARRAY_POSITION(cola, colb)", col_str.sql())
        col = SF.array_position(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ARRAY_POSITION(cola, colb)", col.sql())
        col_lit = SF.array_position("cola", "test")
        self.assertEqual("ARRAY_POSITION(cola, 'test')", col_lit)

    def test_element_at(self):
        col_str = SF.element_at("cola", SF.col("colb"))
        self.assertEqual("ELEMENT_AT(cola, colb)", col_str.sql())
        col = SF.element_at(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ELEMENT_AT(cola, colb)", col.sql())
        col_lit = SF.element_at("cola", "test")
        self.assertEqual("ELEMENT_AT(cola, 'test')", col_lit)

    def test_array_remove(self):
        col_str = SF.array_remove("cola", SF.col("colb"))
        self.assertEqual("ARRAY_REMOVE(cola, colb)", col_str.sql())
        col = SF.array_remove(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ARRAY_REMOVE(cola, colb)", col.sql())
        col_lit = SF.array_remove("cola", "test")
        self.assertEqual("ARRAY_REMOVE(cola, 'test')", col_lit)

    def test_array_distinct(self):
        col_str = SF.array_distinct("cola")
        self.assertEqual("ARRAY_DISTINCT(cola)", col_str.sql())
        col = SF.array_distinct(SF.col("cola"))
        self.assertEqual("ARRAY_DISTINCT(cola)", col.sql())

    def test_array_intersect(self):
        col_str = SF.array_intersect("cola", "colb")
        self.assertEqual("ARRAY_INTERSECT(cola, colb)", col_str.sql())
        col = SF.array_intersect(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ARRAY_INTERSECT(cola, colb)", col.sql())

    def test_array_union(self):
        col_str = SF.array_union("cola", "colb")
        self.assertEqual("ARRAY_UNION(cola, colb)", col_str.sql())
        col = SF.array_union(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ARRAY_UNION(cola, colb)", col.sql())

    def test_array_except(self):
        col_str = SF.array_except("cola", "colb")
        self.assertEqual("ARRAY_EXCEPT(cola, colb)", col_str.sql())
        col = SF.array_except(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ARRAY_EXCEPT(cola, colb)", col.sql())

    def test_explode(self):
        col_str = SF.explode("cola")
        self.assertEqual("EXPLODE(cola)", col_str.sql())
        col = SF.explode(SF.col("cola"))
        self.assertEqual("EXPLODE(cola)", col.sql())

    def test_pos_explode(self):
        col_str = SF.posexplode("cola")
        self.assertEqual("POSEXPLODE(cola)", col_str.sql())
        col = SF.posexplode(SF.col("cola"))
        self.assertEqual("POSEXPLODE(cola)", col.sql())

    def test_explode_outer(self):
        col_str = SF.explode_outer("cola")
        self.assertEqual("EXPLODE_OUTER(cola)", col_str.sql())
        col = SF.explode_outer(SF.col("cola"))
        self.assertEqual("EXPLODE_OUTER(cola)", col.sql())

    def test_posexplode_outer(self):
        col_str = SF.posexplode_outer("cola")
        self.assertEqual("POSEXPLODE_OUTER(cola)", col_str.sql())
        col = SF.posexplode_outer(SF.col("cola"))
        self.assertEqual("POSEXPLODE_OUTER(cola)", col.sql())

    def test_get_json_object(self):
        col_str = SF.get_json_object("cola", "$.f1")
        self.assertEqual("GET_JSON_OBJECT(cola, '$.f1')", col_str.sql())
        col = SF.get_json_object(SF.col("cola"), "$.f1")
        self.assertEqual("GET_JSON_OBJECT(cola, '$.f1')", col.sql())

    def test_json_tuple(self):
        col_str = SF.json_tuple("cola", "f1", "f2")
        self.assertEqual("JSON_TUPLE(cola, 'f1', 'f2')", col_str.sql())
        col = SF.json_tuple(SF.col("cola"), "f1", "f2")
        self.assertEqual("JSON_TUPLE(cola, 'f1', 'f2')", col.sql())

    def test_from_json(self):
        col_str = SF.from_json("cola", "cola INT", dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual(
            "FROM_JSON(cola, 'cola INT', MAP('timestampFormat', 'dd/MM/yyyy'))", col_str.sql()
        )
        col = SF.from_json(SF.col("cola"), "cola INT", dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual(
            "FROM_JSON(cola, 'cola INT', MAP('timestampFormat', 'dd/MM/yyyy'))", col.sql()
        )
        col_no_option = SF.from_json("cola", "cola INT")
        self.assertEqual("FROM_JSON(cola, 'cola INT')", col_no_option.sql())

    def test_to_json(self):
        col_str = SF.to_json("cola", dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual("TO_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))", col_str.sql())
        col = SF.to_json(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual("TO_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))", col.sql())
        col_no_option = SF.to_json("cola")
        self.assertEqual("TO_JSON(cola)", col_no_option.sql())

    def test_schema_of_json(self):
        col_str = SF.schema_of_json("cola", dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual(
            "SCHEMA_OF_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))", col_str.sql()
        )
        col = SF.schema_of_json(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual("SCHEMA_OF_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))", col.sql())
        col_no_option = SF.schema_of_json("cola")
        self.assertEqual("SCHEMA_OF_JSON(cola)", col_no_option.sql())

    def test_schema_of_csv(self):
        col_str = SF.schema_of_csv("cola", dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual("SCHEMA_OF_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))", col_str.sql())
        col = SF.schema_of_csv(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual("SCHEMA_OF_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))", col.sql())
        col_no_option = SF.schema_of_csv("cola")
        self.assertEqual("SCHEMA_OF_CSV(cola)", col_no_option.sql())

    def test_to_csv(self):
        col_str = SF.to_csv("cola", dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual("TO_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))", col_str.sql())
        col = SF.to_csv(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual("TO_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))", col.sql())
        col_no_option = SF.to_csv("cola")
        self.assertEqual("TO_CSV(cola)", col_no_option.sql())

    def test_size(self):
        col_str = SF.size("cola")
        self.assertEqual("SIZE(cola)", col_str.sql())
        col = SF.size(SF.col("cola"))
        self.assertEqual("SIZE(cola)", col.sql())

    def test_array_min(self):
        col_str = SF.array_min("cola")
        self.assertEqual("ARRAY_MIN(cola)", col_str.sql())
        col = SF.array_min(SF.col("cola"))
        self.assertEqual("ARRAY_MIN(cola)", col.sql())

    def test_array_max(self):
        col_str = SF.array_max("cola")
        self.assertEqual("ARRAY_MAX(cola)", col_str.sql())
        col = SF.array_max(SF.col("cola"))
        self.assertEqual("ARRAY_MAX(cola)", col.sql())

    def test_sort_array(self):
        col_str = SF.sort_array("cola", False)
        self.assertEqual("SORT_ARRAY(cola, FALSE)", col_str.sql())
        col = SF.sort_array(SF.col("cola"), False)
        self.assertEqual("SORT_ARRAY(cola, FALSE)", col.sql())
        col_no_sort = SF.sort_array("cola")
        self.assertEqual("SORT_ARRAY(cola)", col_no_sort.sql())

    def test_array_sort(self):
        col_str = SF.array_sort("cola")
        self.assertEqual("ARRAY_SORT(cola)", col_str.sql())
        col = SF.array_sort(SF.col("cola"))
        self.assertEqual("ARRAY_SORT(cola)", col.sql())
        col_comparator = SF.array_sort(
            "cola",
            lambda x, y: SF.when(x.isNull() | y.isNull(), SF.lit(0)).otherwise(
                SF.length(y) - SF.length(x)
            ),
        )
        self.assertEqual(
            "ARRAY_SORT(cola, (x, y) -> CASE WHEN x IS NULL OR y IS NULL THEN 0 ELSE LENGTH(y) - LENGTH(x) END)",
            col_comparator.sql(),
        )

    def test_reverse(self):
        col_str = SF.reverse("cola")
        self.assertEqual("REVERSE(cola)", col_str.sql())
        col = SF.reverse(SF.col("cola"))
        self.assertEqual("REVERSE(cola)", col.sql())

    def test_flatten(self):
        col_str = SF.flatten("cola")
        self.assertEqual("FLATTEN(cola)", col_str.sql())
        col = SF.flatten(SF.col("cola"))
        self.assertEqual("FLATTEN(cola)", col.sql())

    def test_map_keys(self):
        col_str = SF.map_keys("cola")
        self.assertEqual("MAP_KEYS(cola)", col_str.sql())
        col = SF.map_keys(SF.col("cola"))
        self.assertEqual("MAP_KEYS(cola)", col.sql())

    def test_map_values(self):
        col_str = SF.map_values("cola")
        self.assertEqual("MAP_VALUES(cola)", col_str.sql())
        col = SF.map_values(SF.col("cola"))
        self.assertEqual("MAP_VALUES(cola)", col.sql())

    def test_map_entries(self):
        col_str = SF.map_entries("cola")
        self.assertEqual("MAP_ENTRIES(cola)", col_str.sql())
        col = SF.map_entries(SF.col("cola"))
        self.assertEqual("MAP_ENTRIES(cola)", col.sql())

    def test_map_from_entries(self):
        col_str = SF.map_from_entries("cola")
        self.assertEqual("MAP_FROM_ENTRIES(cola)", col_str.sql())
        col = SF.map_from_entries(SF.col("cola"))
        self.assertEqual("MAP_FROM_ENTRIES(cola)", col.sql())

    def test_array_repeat(self):
        col_str = SF.array_repeat("cola", 2)
        self.assertEqual("ARRAY_REPEAT(cola, 2)", col_str.sql())
        col = SF.array_repeat(SF.col("cola"), 2)
        self.assertEqual("ARRAY_REPEAT(cola, 2)", col.sql())

    def test_array_zip(self):
        col_str = SF.array_zip("cola", "colb")
        self.assertEqual("ARRAY_ZIP(cola, colb)", col_str.sql())
        col = SF.array_zip(SF.col("cola"), SF.col("colb"))
        self.assertEqual("ARRAY_ZIP(cola, colb)", col.sql())
        col_single = SF.array_zip("cola")
        self.assertEqual("ARRAY_ZIP(cola)", col_single.sql())

    def test_map_concat(self):
        col_str = SF.map_concat("cola", "colb")
        self.assertEqual("MAP_CONCAT(cola, colb)", col_str.sql())
        col = SF.map_concat(SF.col("cola"), SF.col("colb"))
        self.assertEqual("MAP_CONCAT(cola, colb)", col.sql())
        col_single = SF.map_concat("cola")
        self.assertEqual("MAP_CONCAT(cola)", col_single.sql())

    def test_sequence(self):
        col_str = SF.sequence("cola", "colb", "colc")
        self.assertEqual("SEQUENCE(cola, colb, colc)", col_str.sql())
        col = SF.sequence(SF.col("cola"), SF.col("colb"), SF.col("colc"))
        self.assertEqual("SEQUENCE(cola, colb, colc)", col.sql())
        col_no_step = SF.sequence("cola", "colb")
        self.assertEqual("SEQUENCE(cola, colb)", col_no_step.sql())

    def test_from_csv(self):
        col_str = SF.from_csv("cola", "cola INT", dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual(
            "FROM_CSV(cola, 'cola INT', MAP('timestampFormat', 'dd/MM/yyyy'))", col_str.sql()
        )
        col = SF.from_csv(SF.col("cola"), "cola INT", dict(timestampFormat="dd/MM/yyyy"))
        self.assertEqual(
            "FROM_CSV(cola, 'cola INT', MAP('timestampFormat', 'dd/MM/yyyy'))", col.sql()
        )
        col_no_option = SF.from_csv("cola", "cola INT")
        self.assertEqual("FROM_CSV(cola, 'cola INT')", col_no_option.sql())

    def test_aggregate(self):
        col_str = SF.aggregate("cola", SF.lit(0), lambda acc, x: acc + x, lambda acc: acc * 2)
        self.assertEqual("AGGREGATE(cola, 0, (acc, x) -> acc + x, acc -> acc * 2)", col_str.sql())
        col = SF.aggregate(SF.col("cola"), SF.lit(0), lambda acc, x: acc + x, lambda acc: acc * 2)
        self.assertEqual("AGGREGATE(cola, 0, (acc, x) -> acc + x, acc -> acc * 2)", col.sql())
        col_no_finish = SF.aggregate("cola", SF.lit(0), lambda acc, x: acc + x)
        self.assertEqual("AGGREGATE(cola, 0, (acc, x) -> acc + x)", col_no_finish.sql())
        col_custom_names = SF.aggregate(
            "cola",
            SF.lit(0),
            lambda accumulator, target: accumulator + target,
            lambda accumulator: accumulator * 2,
        )
        self.assertEqual(
            "AGGREGATE(cola, 0, (accumulator, target) -> accumulator + target, accumulator -> accumulator * 2)",
            col_custom_names.sql(),
        )

    def test_transform(self):
        col_str = SF.transform("cola", lambda x: x * 2)
        self.assertEqual("TRANSFORM(cola, x -> x * 2)", col_str.sql())
        col = SF.transform(SF.col("cola"), lambda x, i: x * i)
        self.assertEqual("TRANSFORM(cola, (x, i) -> x * i)", col.sql())
        col_custom_names = SF.transform("cola", lambda target, row_count: target * row_count)

        self.assertEqual(
            "TRANSFORM(cola, (target, row_count) -> target * row_count)", col_custom_names.sql()
        )

    def test_exists(self):
        col_str = SF.exists("cola", lambda x: x % 2 == 0)
        self.assertEqual("EXISTS(cola, x -> x % 2 = 0)", col_str.sql())
        col = SF.exists(SF.col("cola"), lambda x: x % 2 == 0)
        self.assertEqual("EXISTS(cola, x -> x % 2 = 0)", col.sql())
        col_custom_name = SF.exists("cola", lambda target: target > 0)
        self.assertEqual("EXISTS(cola, target -> target > 0)", col_custom_name.sql())

    def test_forall(self):
        col_str = SF.forall("cola", lambda x: x.rlike("foo"))
        self.assertEqual("FORALL(cola, x -> x RLIKE 'foo')", col_str.sql())
        col = SF.forall(SF.col("cola"), lambda x: x.rlike("foo"))
        self.assertEqual("FORALL(cola, x -> x RLIKE 'foo')", col.sql())
        col_custom_name = SF.forall("cola", lambda target: target.rlike("foo"))
        self.assertEqual("FORALL(cola, target -> target RLIKE 'foo')", col_custom_name.sql())

    def test_filter(self):
        col_str = SF.filter("cola", lambda x: SF.month(SF.to_date(x)) > SF.lit(6))
        self.assertEqual("FILTER(cola, x -> MONTH(TO_DATE(x)) > 6)", col_str.sql())
        col = SF.filter(SF.col("cola"), lambda x, i: SF.month(SF.to_date(x)) > SF.lit(i))
        self.assertEqual("FILTER(cola, (x, i) -> MONTH(TO_DATE(x)) > i)", col.sql())
        col_custom_names = SF.filter(
            "cola", lambda target, row_count: SF.month(SF.to_date(target)) > SF.lit(row_count)
        )

        self.assertEqual(
            "FILTER(cola, (target, row_count) -> MONTH(TO_DATE(target)) > row_count)",
            col_custom_names.sql(),
        )

    def test_zip_with(self):
        col_str = SF.zip_with("cola", "colb", lambda x, y: SF.concat_ws("_", x, y))
        self.assertEqual("ZIP_WITH(cola, colb, (x, y) -> CONCAT_WS('_', x, y))", col_str.sql())
        col = SF.zip_with(SF.col("cola"), SF.col("colb"), lambda x, y: SF.concat_ws("_", x, y))
        self.assertEqual("ZIP_WITH(cola, colb, (x, y) -> CONCAT_WS('_', x, y))", col.sql())
        col_custom_names = SF.zip_with("cola", "colb", lambda l, r: SF.concat_ws("_", l, r))
        self.assertEqual(
            "ZIP_WITH(cola, colb, (l, r) -> CONCAT_WS('_', l, r))", col_custom_names.sql()
        )

    def test_transform_keys(self):
        col_str = SF.transform_keys("cola", lambda k, v: SF.upper(k))
        self.assertEqual("TRANSFORM_KEYS(cola, (k, v) -> UPPER(k))", col_str.sql())
        col = SF.transform_keys(SF.col("cola"), lambda k, v: SF.upper(k))
        self.assertEqual("TRANSFORM_KEYS(cola, (k, v) -> UPPER(k))", col.sql())
        col_custom_names = SF.transform_keys("cola", lambda key, _: SF.upper(key))
        self.assertEqual("TRANSFORM_KEYS(cola, (key, _) -> UPPER(key))", col_custom_names.sql())

    def test_transform_values(self):
        col_str = SF.transform_values("cola", lambda k, v: SF.upper(v))
        self.assertEqual("TRANSFORM_VALUES(cola, (k, v) -> UPPER(v))", col_str.sql())
        col = SF.transform_values(SF.col("cola"), lambda k, v: SF.upper(v))
        self.assertEqual("TRANSFORM_VALUES(cola, (k, v) -> UPPER(v))", col.sql())
        col_custom_names = SF.transform_values("cola", lambda _, value: SF.upper(value))
        self.assertEqual(
            "TRANSFORM_VALUES(cola, (_, value) -> UPPER(value))", col_custom_names.sql()
        )

    def test_map_filter(self):
        col_str = SF.map_filter("cola", lambda k, v: k > v)
        self.assertEqual("MAP_FILTER(cola, (k, v) -> k > v)", col_str.sql())
        col = SF.map_filter(SF.col("cola"), lambda k, v: k > v)
        self.assertEqual("MAP_FILTER(cola, (k, v) -> k > v)", col.sql())
        col_custom_names = SF.map_filter("cola", lambda key, value: key > value)
        self.assertEqual("MAP_FILTER(cola, (key, value) -> key > value)", col_custom_names.sql())

    def test_map_zip_with(self):
        col = SF.map_zip_with("base", "ratio", lambda k, v1, v2: SF.round(v1 * v2, 2))
        self.assertEqual("MAP_ZIP_WITH(base, ratio, (k, v1, v2) -> ROUND(v1 * v2, 2))", col.sql())
