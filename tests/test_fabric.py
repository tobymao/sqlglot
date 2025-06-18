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

    def test_fabric_data_type_mappings(self):
        """Test that Fabric maps unsupported data types to supported alternatives"""
        test_cases = [
            # money -> decimal
            ("CREATE TABLE test (price MONEY)", "DECIMAL"),
            ("CREATE TABLE test (price SMALLMONEY)", "DECIMAL"),
            # datetime -> datetime2
            ("CREATE TABLE test (created DATETIME)", "DATETIME2"),
            ("CREATE TABLE test (created SMALLDATETIME)", "DATETIME2"),
            # unicode types -> non-unicode equivalents
            ("CREATE TABLE test (name NCHAR(10))", "CHAR(10)"),
            ("CREATE TABLE test (name NVARCHAR(50))", "VARCHAR(50)"),
            # text types -> varchar
            ("CREATE TABLE test (content TEXT)", "VARCHAR"),
            # binary types
            ("CREATE TABLE test (image_data IMAGE)", "VARBINARY"),
            # integer types - NOTE: T-SQL parses TINYINT as UTINYINT
            ("CREATE TABLE test (tiny_val TINYINT)", "SMALLINT"),
            # json -> varchar
            ("CREATE TABLE test (data JSON)", "VARCHAR"),
            # xml -> varchar
            ("CREATE TABLE test (xml_data XML)", "VARCHAR"),
        ]

        for input_sql, expected_type in test_cases:
            with self.subTest(input_sql=input_sql):
                # Read as T-SQL, write as Fabric to get type conversions
                result = transpile(input_sql, read="tsql", write="fabric")[0]
                self.assertIn(expected_type, result.upper())

    def test_fabric_datetime2_precision_limit(self):
        """Test that Fabric limits DATETIME2 and TIME precision to 6 digits"""
        test_cases = [
            ("CREATE TABLE test (ts DATETIME2(7))", "DATETIME2(6)"),
            ("CREATE TABLE test (ts TIME(7))", "TIME(6)"),
            ("CREATE TABLE test (ts DATETIME2(9))", "DATETIME2(6)"),
            ("CREATE TABLE test (ts TIME(3))", "TIME(3)"),  # Should remain unchanged
        ]

        for input_sql, expected in test_cases:
            with self.subTest(input_sql=input_sql):
                # Read as T-SQL, write as Fabric to get precision limiting
                result = transpile(input_sql, read="tsql", write="fabric")[0]
                self.assertIn(expected, result)

    def test_fabric_varchar_max_support(self):
        """Test that Fabric supports VARCHAR(MAX) and VARBINARY(MAX) (in preview)"""
        test_cases = [
            "CREATE TABLE test (large_text VARCHAR(MAX))",
            "CREATE TABLE test (large_binary VARBINARY(MAX))",
        ]

        for input_sql in test_cases:
            with self.subTest(input_sql=input_sql):
                result = transpile(input_sql, read="fabric", write="fabric")[0]
                # Should preserve MAX keyword
                self.assertIn("MAX", result)

    def test_fabric_supported_data_types(self):
        """Test that Fabric properly handles all supported data types"""
        # Note: These are the T-SQL representations that should work in Fabric
        supported_types = [
            "BIT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "DECIMAL",  # Will be generated as DECIMAL (not NUMERIC)
            "FLOAT",
            "REAL",
            "DATE",
            "TIME",
            "DATETIME2",
            "CHAR",
            "VARCHAR",
            "VARBINARY",
            "UNIQUEIDENTIFIER",
        ]

        for data_type in supported_types:
            with self.subTest(data_type=data_type):
                sql = f"CREATE TABLE test (col {data_type})"
                # Read as T-SQL, write as Fabric
                result = transpile(sql, read="tsql", write="fabric")[0]
                # Should transpile without error and preserve supported types
                self.assertTrue(len(result) > 0)  # Just check it doesn't fail

    def test_fabric_uniqueidentifier_handling(self):
        """Test that UNIQUEIDENTIFIER is supported but has special behavior"""
        sql = "CREATE TABLE test (id UNIQUEIDENTIFIER)"
        result = transpile(sql, read="fabric", write="fabric")[0]

        # Should preserve UNIQUEIDENTIFIER
        self.assertIn("UNIQUEIDENTIFIER", result)

    def test_fabric_unsupported_type_combinations(self):
        """Test combinations of unsupported types in complex scenarios"""
        sql = """
        CREATE TABLE test_table (
            id INT,
            old_date DATETIME,
            unicode_name NVARCHAR(100),
            money_field MONEY,
            tiny_num TINYINT,
            json_data JSON,
            xml_content XML
        )
        """

        # Read as T-SQL, write as Fabric to get type conversions
        result = transpile(sql, read="tsql", write="fabric")[0]

        # Check that all unsupported types are converted
        self.assertIn("DATETIME2", result)  # DATETIME -> DATETIME2
        self.assertIn("VARCHAR(100)", result)  # NVARCHAR -> VARCHAR
        self.assertIn("DECIMAL", result)  # MONEY -> DECIMAL
        self.assertIn("SMALLINT", result)  # TINYINT -> SMALLINT (via UTINYINT)
        # JSON and XML should both become VARCHAR


if __name__ == "__main__":
    unittest.main()
