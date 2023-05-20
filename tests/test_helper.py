import unittest

from sqlglot.dialects import BigQuery, Dialect, Snowflake
from sqlglot.helper import dialects_match, tsort


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

    def test_dialects_match(self):
        bigquery_class = Dialect["bigquery"]
        bigquery_object = BigQuery()
        bigquery_string = "bigquery"

        snowflake_class = Dialect["snowflake"]
        snowflake_object = Snowflake()
        snowflake_string = "snowflake"

        assert dialects_match(snowflake_class, snowflake_class)
        assert dialects_match(snowflake_class, snowflake_object)
        assert dialects_match(snowflake_class, snowflake_string)
        assert dialects_match(snowflake_object, snowflake_object)
        assert dialects_match(snowflake_object, snowflake_string)
        assert dialects_match(snowflake_string, snowflake_string)

        assert not dialects_match(snowflake_class, bigquery_class)
        assert not dialects_match(snowflake_class, bigquery_object)
        assert not dialects_match(snowflake_class, bigquery_string)
        assert not dialects_match(snowflake_object, bigquery_object)
        assert not dialects_match(snowflake_object, bigquery_string)
        assert not dialects_match(snowflake_string, bigquery_string)
        assert not dialects_match(snowflake_string, None)
