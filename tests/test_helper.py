import unittest

from sqlglot.dialects import BigQuery, Dialect, Snowflake
from sqlglot.helper import name_sequence, tsort


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

    def test_compare_dialects(self):
        bigquery_class = Dialect["bigquery"]
        bigquery_object = BigQuery()
        bigquery_string = "bigquery"

        snowflake_class = Dialect["snowflake"]
        snowflake_object = Snowflake()
        snowflake_string = "snowflake"

        self.assertEqual(snowflake_class, snowflake_class)
        self.assertEqual(snowflake_class, snowflake_object)
        self.assertEqual(snowflake_class, snowflake_string)
        self.assertEqual(snowflake_object, snowflake_object)
        self.assertEqual(snowflake_object, snowflake_string)

        self.assertNotEqual(snowflake_class, bigquery_class)
        self.assertNotEqual(snowflake_class, bigquery_object)
        self.assertNotEqual(snowflake_class, bigquery_string)
        self.assertNotEqual(snowflake_object, bigquery_object)
        self.assertNotEqual(snowflake_object, bigquery_string)

        self.assertTrue(snowflake_class in {"snowflake", "bigquery"})
        self.assertTrue(snowflake_object in {"snowflake", "bigquery"})
        self.assertFalse(snowflake_class in {"bigquery", "redshift"})
        self.assertFalse(snowflake_object in {"bigquery", "redshift"})

    def test_name_sequence(self):
        s1 = name_sequence("a")
        s2 = name_sequence("b")

        self.assertEqual(s1(), "a0")
        self.assertEqual(s1(), "a1")
        self.assertEqual(s2(), "b0")
        self.assertEqual(s1(), "a2")
        self.assertEqual(s2(), "b1")
        self.assertEqual(s2(), "b2")
