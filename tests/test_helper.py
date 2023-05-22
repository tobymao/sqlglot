import unittest

from sqlglot.dialects import BigQuery, Dialect, Snowflake
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
