import os
import unittest

import sqlglot

class TestTranspile(unittest.TestCase):
    file_dir = os.path.dirname(__file__)
    fixtures_dir = os.path.join(file_dir, 'fixtures')

    def test_identity(self):
        with open(os.path.join(self.fixtures_dir, 'identity.sql')) as f:
            for sql in f:
                self.assertEqual(sqlglot.transpile(sql)[0], sql.strip())

    def test_pretty(self):
        with open(os.path.join(self.fixtures_dir, 'pretty.sql')) as f:
            lines = f.read().split(';')
            size = len(lines)

            for i in range(0, size, 2):
                if i + 1 < size:
                    sql = lines[i]
                    pretty = lines[i + 1].strip()
                    generated = sqlglot.transpile(sql, pretty=True)[0]
                    self.assertEqual(generated, pretty)
