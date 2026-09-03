import importlib
import unittest
from unittest.mock import patch

import sqlglot.dialects


class TestDialectImports(unittest.TestCase):
    def test_dialect_import_no_deadlock(self):
        """Athena imports Trino and Hive while being imported itself, so a
        non-reentrant lock in sqlglot.dialects.__getattr__ would deadlock."""
        from sqlglot.dialects import Athena

        self.assertTrue(hasattr(Athena, "Parser"))
        self.assertTrue(hasattr(Athena, "Generator"))

    def test_dialect_attribute_access_is_cached(self):
        """Repeated class-name lookups on the package must not re-import."""
        import_count = 0
        import_module = importlib.import_module

        def mock_import_module(name):
            nonlocal import_count
            import_count += 1
            return import_module(name)

        # Drop any cached entry so the first lookup goes through __getattr__.
        sqlglot.dialects.__dict__.pop("Snowflake", None)

        with patch("importlib.import_module", side_effect=mock_import_module):
            first = sqlglot.dialects.Snowflake
            second = sqlglot.dialects.Snowflake

        self.assertIs(first, second)
        self.assertEqual(import_count, 1)
