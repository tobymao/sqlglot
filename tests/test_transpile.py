import os
import unittest

from sqlglot import transpile

class TestTranspile(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, 'fixtures')

    def test_comments(self):
        sql = transpile('SELECT 1 FROM foo -- comment')[0]
        self.assertEqual(sql, 'SELECT 1 FROM foo')

        sql = transpile('SELECT 1 /* inline */ FROM foo -- comment')[0]
        self.assertEqual(sql, 'SELECT 1 FROM foo')

        sql = transpile(
            """
            SELECT 1 -- comment
            FROM foo -- comment
            """
        )[0]
        self.assertEqual(sql, 'SELECT 1 FROM foo')

        sql = transpile(
            """
            SELECT 1 /* big comment
             like this */
            FROM foo -- comment
            """
        )[0]
        self.assertEqual(sql, 'SELECT 1 FROM foo')

    def test_if(self):
        sql = transpile('SELECT IF(a > 1, 1, 0) FROM foo')[0]
        self.assertEqual(sql, 'SELECT CASE WHEN a > 1 THEN 1 ELSE 0 END FROM foo')
        sql = transpile('SELECT IF(a > 1, 1) FROM foo')[0]
        self.assertEqual(sql, 'SELECT CASE WHEN a > 1 THEN 1 END FROM foo')

    def test_identity(self):
        with open(os.path.join(self.fixtures_dir, 'identity.sql')) as f:
            for sql in f:
                self.assertEqual(transpile(sql)[0], sql.strip())

    def test_pretty(self):
        with open(os.path.join(self.fixtures_dir, 'pretty.sql')) as f:
            lines = f.read().split(';')
            size = len(lines)

            for i in range(0, size, 2):
                if i + 1 < size:
                    sql = lines[i]
                    pretty = lines[i + 1].strip()
                    generated = transpile(sql, pretty=True)[0]
                    self.assertEqual(generated, pretty)
