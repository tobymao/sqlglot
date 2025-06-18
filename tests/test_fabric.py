import unittest
from sqlglot import transpile
from sqlglot.dialects.fabric import Fabric


class TestFabric(unittest.TestCase):
    def test_fabric_create_schema_with_exists(self):
        """Test that Fabric dialect uses uppercase INFORMATION_SCHEMA.SCHEMATA"""
        sql = "CREATE SCHEMA IF NOT EXISTS test_schema"
        result = transpile(sql, read="fabric", write="fabric")[0]

        # Should use uppercase INFORMATION_SCHEMA.SCHEMATA and SCHEMA_NAME
        self.assertIn("INFORMATION_SCHEMA.SCHEMATA", result)
        self.assertIn("SCHEMA_NAME", result)
        self.assertNotIn("information_schema.schemata", result)
        self.assertNotIn("schema_name", result)

    def test_fabric_create_table_with_exists(self):
        """Test that Fabric dialect uses uppercase INFORMATION_SCHEMA.TABLES"""
        sql = "CREATE TABLE IF NOT EXISTS test_table (id INT)"
        result = transpile(sql, read="fabric", write="fabric")[0]

        # Should use uppercase INFORMATION_SCHEMA.TABLES and column names
        self.assertIn("INFORMATION_SCHEMA.TABLES", result)
        self.assertIn("TABLE_NAME", result)
        self.assertNotIn("information_schema.tables", result)
        self.assertNotIn("table_name", result)

    def test_fabric_inherits_from_tsql(self):
        """Test that Fabric dialect inherits T-SQL functionality"""
        # Test a T-SQL specific feature like TOP
        sql = "SELECT TOP 10 * FROM users"
        result = transpile(sql, read="fabric", write="fabric")[0]

        # Should maintain T-SQL syntax
        self.assertIn("SELECT TOP 10", result)

    def test_fabric_dialect_instance(self):
        """Test that Fabric dialect is properly instantiated"""
        dialect = Fabric()
        self.assertIsInstance(dialect, Fabric)
        self.assertTrue(hasattr(dialect, "Generator"))

        # Should have the custom Generator that overrides create_sql
        generator = dialect.Generator()
        self.assertTrue(hasattr(generator, "create_sql"))


if __name__ == "__main__":
    unittest.main()
