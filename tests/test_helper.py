import unittest

from sqlglot.helper import tsort


class TestHelper(unittest.TestCase):
    def test_tsort(self):
        self.assertEqual(tsort({"a": []}), ["a"])
        self.assertEqual(tsort({"a": ["b", "b"]}), ["b", "a"])
        self.assertEqual(tsort({"a": ["b"]}), ["b", "a"])
        self.assertEqual(tsort({"a": ["c"], "b": [], "c": []}), ["c", "a", "b"])
        self.assertEqual(
            tsort(
                {
                    "a": ["b", "c"],
                    "b": ["c"],
                    "c": [],
                    "d": ["a"],
                }
            ),
            ["c", "b", "a", "d"],
        )

        with self.assertRaises(ValueError):
            tsort(
                {
                    "a": ["b", "c"],
                    "b": ["a"],
                    "c": [],
                }
            )
