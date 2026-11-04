import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from conformance.cases import Case, CaseError, load_cases, parse_case_file
from conformance.compare import NAN, compare_results, get_zone, normalize, values_equal
from conformance.dataset import TABLES
from conformance.engines import ENGINES, Engine, EngineError
from conformance.result import Result
from conformance.runner import CaseResult, Leg, Runner, diff_baseline, load_baseline, write_baseline


def result(rows, families=None, types=None):
    width = len(rows[0]) if rows else len(families or [])
    families = families or ["integer"] * width
    return Result([f"c{i}" for i in range(width)], types or families, families, rows)


class FakeEngine(Engine):
    def __init__(self, name, dialect, results):
        super().__init__()
        self.name = name
        self.dialect = dialect
        self.results = results
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        value = self.results.get(sql)
        if not isinstance(value, Result):
            raise EngineError(value or f"unexpected query: {sql}")
        return value


class TestCompare(unittest.TestCase):
    def test_numbers(self):
        self.assertTrue(values_equal(2, 2.0))
        self.assertTrue(values_equal(0.1 + 0.2, 0.3))
        self.assertFalse(values_equal(2, 2.5))
        self.assertFalse(values_equal(306783378, 306783378.14285713))
        self.assertFalse(values_equal(True, 1))

        self.assertTrue(values_equal(Decimal("1.50"), Decimal("1.5")))
        self.assertTrue(values_equal(Decimal("25.5220058532573370"), Decimal("25.522006")))
        self.assertTrue(values_equal(Decimal("0.333333"), 1 / 3))
        self.assertFalse(values_equal(Decimal("3"), Decimal("2.5")))
        self.assertFalse(values_equal(Decimal("1.25"), Decimal("1.3")))
        self.assertFalse(values_equal(Decimal("11.609293"), 11.6093))

    def test_special_floats(self):
        self.assertIs(normalize(float("nan")), NAN)
        self.assertTrue(values_equal(normalize(float("nan")), normalize(float("nan"))))
        self.assertTrue(values_equal(normalize(-0.0), normalize(0.0)))
        self.assertTrue(values_equal(float("inf"), float("inf")))
        self.assertFalse(values_equal(float("inf"), 1e308))

        widened = 0.10000000149011612
        self.assertTrue(values_equal(normalize(0.1, "float32"), normalize(widened, "float32")))
        self.assertFalse(values_equal(normalize(0.1), normalize(widened)))

    def test_temporal(self):
        instant = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
        self.assertEqual(normalize(instant), datetime(2024, 1, 1, 12))
        self.assertEqual(
            normalize(instant, zone=get_zone("America/Los_Angeles")), datetime(2024, 1, 1, 4)
        )
        self.assertTrue(values_equal(date(2024, 1, 1), datetime(2024, 1, 1)))
        self.assertFalse(values_equal(date(2024, 1, 1), datetime(2024, 1, 1, 1)))

    def test_structured_values(self):
        self.assertEqual(
            normalize('{"b": 1, "a": [1, 2]}', "json"), normalize('{"a":[1,2],"b":1}', "json")
        )
        self.assertNotEqual(normalize('"x"', "json"), normalize("x", "json"))
        self.assertEqual(normalize({"b": 2, "a": 1}, "map"), normalize([("a", 1), ("b", 2)], "map"))
        self.assertEqual(normalize({"x": 1, "y": "a"}), (1, "a"))
        self.assertEqual(normalize([1, None, [b"\x00"]]), (1, None, (b"\x00",)))

    def test_unordered(self):
        expected = result([(1,), (2,), (2,)])
        self.assertTrue(compare_results(expected, result([(2,), (1,), (2,)]), ordered=False).equal)

        comparison = compare_results(expected, result([(2,), (1,)]), ordered=False)
        self.assertFalse(comparison.equal)
        self.assertEqual(comparison.reason, "row count differs: expected 3, got 2")
        self.assertEqual(comparison.details["missing_rows"], ["(2,)"])

        comparison = compare_results(expected, result([(1,), (2,), (3,)]), ordered=False)
        self.assertEqual(comparison.reason, "1 of 3 rows differ")
        self.assertEqual(comparison.details["unexpected_rows"], ["(3,)"])

    def test_ordered(self):
        expected = result([(1,), (None,)])
        self.assertTrue(compare_results(expected, result([(1,), (None,)]), ordered=True).equal)

        comparison = compare_results(expected, result([(None,), (1,)]), ordered=True)
        self.assertFalse(comparison.equal)
        self.assertEqual(comparison.reason, "same rows in a different order")

    def test_columns_and_types(self):
        comparison = compare_results(result([(1, 2)]), result([(1,)]), ordered=False)
        self.assertEqual(comparison.reason, "column count differs: expected 2, got 1")

        comparison = compare_results(
            result([(1,)], ["integer"], ["int4"]),
            result([(1.0,)], ["float"], ["double"]),
            ordered=False,
        )
        self.assertTrue(comparison.equal)
        self.assertEqual(
            comparison.warnings, ["column 1 type: expected int4 (integer), got double (float)"]
        )

        comparison = compare_results(
            result([("{}",)], ["json"]), result([("{ }",)], ["string"]), ordered=False
        )
        self.assertTrue(comparison.equal)
        self.assertEqual(comparison.warnings, [])

    def test_vacuous(self):
        self.assertTrue(result([], ["integer"]).vacuous)
        self.assertTrue(result([(None,)]).vacuous)
        self.assertFalse(result([(None,), (1,)]).vacuous)


class TestCases(unittest.TestCase):
    def parse(self, text):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "topic.sql"
            path.write_text(text, encoding="utf-8")
            return parse_case_file(path)

    def test_parse(self):
        first, second, third = self.parse(
            """
-- A comment
# read: postgres
# name: first
SELECT 1;

# read: spark
# write: none
# ordered: false
# rel_tol: 1e-6
SELECT a
FROM t
ORDER BY a;

# read: postgres
# write: spark, postgres
SELECT 2
"""
        )
        self.assertEqual(first.id, "topic:first")
        self.assertEqual(
            (first.sql, first.read, first.write, first.line), ("SELECT 1", "postgres", None, 5)
        )

        self.assertEqual(second.sql, "SELECT a\nFROM t\nORDER BY a")
        self.assertEqual(
            (second.read, second.write, second.ordered, second.rel_tol), ("spark", (), False, 1e-6)
        )

        self.assertTrue(third.id.startswith("topic#"))
        self.assertEqual((third.sql, third.write), ("SELECT 2", ("spark", "postgres")))

    def test_errors(self):
        with self.assertRaises(CaseError):
            self.parse("SELECT 1;")
        with self.assertRaises(CaseError):
            self.parse("# reed: postgres\nSELECT 1;")

    def test_bundled_cases(self):
        cases = load_cases()
        self.assertTrue(all(case.read in ENGINES for case in cases))
        self.assertEqual(len([case for case in cases if case.id.startswith("tpch/spark:")]), 22)


class TestDataset(unittest.TestCase):
    def test_tables(self):
        for table in TABLES:
            with self.subTest(table.name):
                self.assertTrue(table.engines)
                self.assertTrue(table.rows)
                self.assertEqual(len(table.fingerprint), 64)


class TestRunner(unittest.TestCase):
    def run_case(self, sql, postgres, spark):
        engines = {
            "postgres": FakeEngine("postgres", "postgres", postgres),
            "spark": FakeEngine("spark", "spark", spark),
        }
        case = Case("case", sql, "postgres")
        return Runner(engines).run_case(case), engines

    def statuses(self, case_result):
        return {leg.name: leg.status for leg in case_result.legs}

    def test_legs(self):
        case_result, engines = self.run_case(
            "SELECT 5 / 2",
            {"SELECT 5 / 2": result([(2,)])},
            {"SELECT 5 / 2": result([(2.5,)], ["float"])},
        )
        self.assertEqual(
            self.statuses(case_result),
            {
                "source:postgres": "pass",
                "roundtrip:postgres": "pass",
                "transpile:postgres->spark": "mismatch",
            },
        )
        # The roundtrip produced identical SQL, so it wasn't executed again
        self.assertEqual(engines["postgres"].queries, ["SELECT 5 / 2"])

        case_result, _ = self.run_case(
            "SELECT 1", {"SELECT 1": result([(1,)])}, {"SELECT 1": "boom"}
        )
        self.assertEqual(self.statuses(case_result)["transpile:postgres->spark"], "error")

        case_result, _ = self.run_case("SELECT 1", {"SELECT 1": "syntax error"}, {})
        self.assertEqual(self.statuses(case_result), {"source:postgres": "invalid_case"})

    def test_engine_specific_tables(self):
        sql = "SELECT id FROM pg_extras"
        case_result, _ = self.run_case(sql, {sql: result([(1,)])}, {})
        self.assertEqual(
            list(self.statuses(case_result)), ["source:postgres", "roundtrip:postgres"]
        )

    def test_baseline(self):
        case = Case("case", "SELECT 1", "postgres")
        failing = [
            CaseResult(
                case, [Leg("source:postgres", "pass"), Leg("transpile:postgres->spark", "error")]
            )
        ]
        passing = [
            CaseResult(
                case, [Leg("source:postgres", "pass"), Leg("transpile:postgres->spark", "pass")]
            )
        ]

        self.assertEqual(
            diff_baseline(failing, {}).new_failures, ["case transpile:postgres->spark"]
        )
        self.assertEqual(
            diff_baseline(passing, {"case transpile:postgres->spark": "error"}).fixed,
            ["case transpile:postgres->spark"],
        )
        self.assertEqual(
            diff_baseline(failing, {"case transpile:postgres->spark": "mismatch"}).changed,
            [("case transpile:postgres->spark", "mismatch", "error")],
        )

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "baseline.json"
            other = {"other transpile:spark->postgres": "mismatch"}

            write_baseline(path, failing, other, complete=False)
            self.assertEqual(
                load_baseline(path), {**other, "case transpile:postgres->spark": "error"}
            )

            write_baseline(path, passing, load_baseline(path), complete=True)
            self.assertEqual(load_baseline(path), {})
