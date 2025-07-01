import unittest
from unittest.mock import patch

from sqlglot.dialects import __getattr__ as dialects_getattr


class TestDialectImports(unittest.TestCase):
    def test_athena_import_no_deadlock(self):
        """Test that importing Athena dialect doesn't cause a deadlock.

        Athena imports from other dialects during its module initialization,
        which could cause a deadlock with a non-reentrant lock.
        """
        # This should complete without hanging
        from sqlglot.dialects import Athena

        # Verify it imported successfully
        self.assertIsNotNone(Athena)
        self.assertTrue(hasattr(Athena, "Parser"))
        self.assertTrue(hasattr(Athena, "Generator"))

    def test_nested_dialect_import_with_rlock(self):
        """Test that nested dialect imports work with RLock."""
        import_count = 0

        def mock_import_module(name):
            nonlocal import_count
            import_count += 1

            # Simulate Athena importing Hive
            if name.endswith(".athena"):
                # This simulates athena.py trying to import Hive
                dialects_getattr("Hive")

            # Return a mock module with the expected attribute
            import types

            module = types.ModuleType(name)
            dialect_name = name.split(".")[-1].title()
            setattr(module, dialect_name, f"Mock{dialect_name}")
            return module

        with patch("importlib.import_module", side_effect=mock_import_module):
            # This should not deadlock
            result = dialects_getattr("Athena")
            self.assertEqual(result, "MockAthena")
            # Should have imported both Athena and Hive
            self.assertEqual(import_count, 2)
