import unittest
from sqlglot import parse_one, ErrorLevel
from performance_analyzer import PerformanceAnalyzer, PerformanceWarning

class TestPerformanceAnalyzer(unittest.TestCase):
    def setUp(self):
        self.schema = {"users": ["id", "name", "email"]}
        self.indexes = {"users": {"id"}}  

    def test_select_star(self):
        analyzer = PerformanceAnalyzer(self.schema, self.indexes, error_level=ErrorLevel.RAISE)
        expr = parse_one("SELECT * FROM users WHERE email = 'test@example.com'")
        with self.assertRaises(PerformanceWarning) as cm:
            analyzer.analyze(expr)
        self.assertIn("SELECT *", str(cm.exception))

    def test_unindexed_column_in_where(self):
        analyzer = PerformanceAnalyzer(self.schema, self.indexes, error_level=ErrorLevel.RAISE)
        expr = parse_one("SELECT id FROM users WHERE email = 'test@example.com'")
        with self.assertRaises(PerformanceWarning) as cm:
            analyzer.analyze(expr)
        self.assertIn("not indexed", str(cm.exception))

if __name__ == "__main__":
    unittest.main()
