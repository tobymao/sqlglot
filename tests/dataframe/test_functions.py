import unittest

from pyspark.sql import functions

from sqlglot import expressions as exp
from sqlglot.dataframe import functions as SF


class TestDataframeFunctions(unittest.TestCase):
    def test_lit(self):
        test_str = SF.lit("test")
        self.assertEqual("'test'", test_str.sql())
        test_int = SF.lit(30)
        self.assertEqual("30", test_int.sql())
        test_float = SF.lit(10.10)
        self.assertEqual("10.1", test_float.sql())
        test_bool = SF.lit(False)
        self.assertEqual("False", test_bool.sql())
        test_null = SF.lit(None)
        self.assertEqual("NULL AS `NULL`", test_null.sql())

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
        self.assertEqual("true", test_bool.sql())
        test_array = SF.col([1, 2, 3])
        self.assertEqual("ARRAY(1, 2, 3)", test_array.sql())

    def test_greatest(self):
        single_str = SF.greatest("cola")
        self.assertEqual("GREATEST(cola)", single_str.sql())
        single_col = SF.greatest(SF.col("cola"))
        self.assertEqual("GREATEST(cola)", single_col.sql())
        multiple_mix = SF.greatest("col1", "col2", SF.col("col3"), SF.col("col4"))
        self.assertEqual("GREATEST(col1, col2, col3, col4)", multiple_mix.sql())

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
        self.assertEqual("LOG(cola)", col_str.sql())
        col = SF.log(SF.col("cola"))
        self.assertEqual("LOG(cola)", col.sql())

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
        self.assertEqual("SIGNUM(cola)", col_str.sql())
        col = SF.signum(SF.col("cola"))
        self.assertEqual("SIGNUM(cola)", col.sql())

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
        self.assertEqual("cola", col_str.sql())
        col = SF.asc_nulls_first(SF.col("cola"))
        self.assertIsInstance(col.expression, exp.Ordered)
        self.assertEqual("cola", col.sql())

    def test_asc_nulls_last(self):
        col_str = SF.asc_nulls_last("cola")
        self.assertIsInstance(col_str.expression, exp.Ordered)
        self.assertEqual("cola NULLS LAST", col_str.sql())
        col = SF.asc_nulls_last(SF.col("cola"))
        self.assertIsInstance(col.expression, exp.Ordered)
        self.assertEqual("cola NULLS LAST", col.sql())

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
        self.assertEqual("VAR_SAMP(cola)", col_str.sql())
        col = SF.var_samp(SF.col("cola"))
        self.assertEqual("VAR_SAMP(cola)", col.sql())

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
        self.assertEqual("POW(cola, colb)", col_str.sql())
        col = SF.pow(SF.col("cola"), SF.col("colb"))
        self.assertEqual("POW(cola, colb)", col.sql())
        col_float = SF.pow(10.10, "colb")
        self.assertEqual("POW(10.1, colb)", col_float.sql())
        col_float2 = SF.pow("cola", 10.10)
        self.assertEqual("POW(cola, 10.1)", col_float2.sql())

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
        with self.assertRaises(NotImplementedError):
            SF.count_distinct(SF.col("cola"), SF.col("colb"))

    def test_first(self):
        col_str = SF.first("cola")
        self.assertEqual("FIRST(cola)", col_str.sql())
        col = SF.first(SF.col("cola"))
        self.assertEqual("FIRST(cola)", col.sql())
        ignore_nulls = SF.first("cola", True)
        self.assertEqual("FIRST(cola, true)", ignore_nulls.sql())

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
        self.assertEqual("LAST(cola, true)", ignore_nulls.sql())

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

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())

    def test_isnan(self):
        col_str = SF.isnan("cola")
        self.assertEqual("ISNAN(cola)", col_str.sql())
        col = SF.isnan(SF.col("cola"))
        self.assertEqual("ISNAN(cola)", col.sql())
