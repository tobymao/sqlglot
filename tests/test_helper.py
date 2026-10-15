import unittest
from decimal import Decimal

from sqlglot.helper import merge_ranges, name_sequence, to_int, tsort


class TestHelper(unittest.TestCase):
    def test_tsort(self):
        self.assertEqual(tsort({"a": set()}), ["a"])
        self.assertEqual(tsort({"a": {"b"}}), ["b", "a"])
        self.assertEqual(tsort({"a": {"c"}, "b": set(), "c": set()}), ["b", "c", "a"])
        self.assertEqual(
            tsort(
                {
                    "a": {"b", "c"},
                    "b": {"c"},
                    "c": set(),
                    "d": {"a"},
                }
            ),
            ["c", "b", "a", "d"],
        )

        with self.assertRaises(ValueError):
            tsort(
                {
                    "a": {"b", "c"},
                    "b": {"a"},
                    "c": set(),
                }
            )

    def test_name_sequence(self):
        s1 = name_sequence("a")
        s2 = name_sequence("b")

        self.assertEqual(s1(), "a0")
        self.assertEqual(s1(), "a1")
        self.assertEqual(s2(), "b0")
        self.assertEqual(s1(), "a2")
        self.assertEqual(s2(), "b1")
        self.assertEqual(s2(), "b2")

    def test_to_int(self):
        for value, expected in (
            ("38", 38),
            ("-1e3", -1000),
            (Decimal("2.0"), 2),
            (5, 5),
            ("2.5", None),
            ("p", None),
            ("NaN", None),
            ("Infinity", None),
            ("1e1000000000", None),
            (Decimal("1e-1000000000"), None),
        ):
            with self.subTest(value):
                self.assertEqual(to_int(value), expected)

        self.assertEqual(to_int("999", max_digits=3), 999)
        self.assertIsNone(to_int("1000", max_digits=3))

    def test_merge_ranges(self):
        self.assertEqual([], merge_ranges([]))
        self.assertEqual([(0, 1)], merge_ranges([(0, 1)]))
        self.assertEqual([(0, 1), (2, 3)], merge_ranges([(0, 1), (2, 3)]))
        self.assertEqual([(0, 3)], merge_ranges([(0, 1), (1, 3)]))
        self.assertEqual([(0, 1), (2, 4)], merge_ranges([(2, 3), (0, 1), (3, 4)]))
