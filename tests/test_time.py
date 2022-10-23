import unittest

from sqlglot.time import format_time


class TestTime(unittest.TestCase):
    def test_format_time(self):
        self.assertEqual(format_time("", {}), None)
        self.assertEqual(format_time(" ", {}), " ")
        mapping = {"a": "b", "aa": "c"}
        self.assertEqual(format_time("a", mapping), "b")
        self.assertEqual(format_time("aa", mapping), "c")
        self.assertEqual(format_time("aaada", mapping), "cbdb")
        self.assertEqual(format_time("da", mapping), "db")
