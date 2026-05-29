import unittest
from sqlglot import transpile


class TestMultilineComments(unittest.TestCase):
    def test_connector_multiline_comment_idempotency(self):
        scenarios = [
            "SELECT * FROM x WHERE a = 1 AND /*\n  hello\n  world\n*/ 1 = 0",
            "SELECT * FROM x WHERE a = 1 OR /*\n  hello\n*/ b = 2",
            "SELECT * FROM x WHERE a = 1 AND /*\n middle \n*/ b = 2 AND c = 3",
            "SELECT * FROM x WHERE a = 1 AND /*\nno_indent\n*/ b = 2",
            "SELECT * FROM x WHERE a = 1 AND /*\n  line1\n\n  line3\n*/ b = 2",
        ]
        for sql in scenarios:
            with self.subTest(sql=sql):
                formatted = transpile(sql, pretty=True)[0]
                reformatted = transpile(formatted, pretty=True)[0]
                self.assertEqual(formatted, reformatted)

    def test_binary_multiline_comment_idempotency(self):
        sql = "SELECT 1 + /*\n  add\n*/ 2"
        formatted = transpile(sql, pretty=True)[0]
        reformatted = transpile(formatted, pretty=True)[0]
        self.assertEqual(formatted, reformatted)
