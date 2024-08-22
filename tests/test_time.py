import unittest
import sys

from sqlglot.time import format_time, subsecond_precision


class TestTime(unittest.TestCase):
    def test_format_time(self):
        self.assertEqual(format_time("", {}), None)
        self.assertEqual(format_time(" ", {}), " ")
        mapping = {"a": "b", "aa": "c"}
        self.assertEqual(format_time("a", mapping), "b")
        self.assertEqual(format_time("aa", mapping), "c")
        self.assertEqual(format_time("aaada", mapping), "cbdb")
        self.assertEqual(format_time("da", mapping), "db")

    def test_subsecond_precision(self):
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.123456+00:00"))
        self.assertEqual(3, subsecond_precision("2023-01-01 12:13:14.123+00:00"))
        self.assertEqual(0, subsecond_precision("2023-01-01 12:13:14+00:00"))
        self.assertEqual(0, subsecond_precision("2023-01-01 12:13:14"))
        self.assertEqual(0, subsecond_precision("garbage"))

    @unittest.skipUnless(
        sys.version_info >= (3, 11),
        "Python 3.11 relaxed datetime.fromisoformat() parsing with regards to microseconds",
    )
    def test_subsecond_precision_python311(self):
        # ref: https://docs.python.org/3/whatsnew/3.11.html#datetime
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.123456789+00:00"))
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.12345+00:00"))
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.1234+00:00"))
        self.assertEqual(3, subsecond_precision("2023-01-01 12:13:14.12+00:00"))
        self.assertEqual(3, subsecond_precision("2023-01-01 12:13:14.1+00:00"))
