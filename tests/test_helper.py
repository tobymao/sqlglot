import unittest

from sqlglot.helper import merge_ranges, name_sequence, tsort, is_url_string


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

    def test_merge_ranges(self):
        self.assertEqual([], merge_ranges([]))
        self.assertEqual([(0, 1)], merge_ranges([(0, 1)]))
        self.assertEqual([(0, 1), (2, 3)], merge_ranges([(0, 1), (2, 3)]))
        self.assertEqual([(0, 3)], merge_ranges([(0, 1), (1, 3)]))
        self.assertEqual([(0, 1), (2, 4)], merge_ranges([(2, 3), (0, 1), (3, 4)]))

    def test_is_url_string(self):
        self.assertTrue(is_url_string("http://test.com"))
        self.assertTrue(is_url_string("file:///my.txt"))
        self.assertTrue(is_url_string("file:///my file.txt"))
        self.assertTrue(is_url_string("s3:///my.bucket/foo/bar"))

        # negative examples
        self.assertFalse(is_url_string("'http://test.com'"))
        self.assertFalse(is_url_string("  http://test.com"))
        self.assertFalse(is_url_string("foo bar"))
