import unittest

from sqlglot import parse_one
from sqlglot.optimizer.qualify import qualify


class TestQualifyReusedTableAlias(unittest.TestCase):
    def test_qualify_reused_table_alias(self):
        sql = "SELECT 1 FROM dbo.a JOIN dbo.b ON dbo.b.id = dbo.a.id JOIN dbo.b AS x ON x.id = dbo.a.id"
        parsed = parse_one(sql)
        qualify(parsed)
        result = parsed.sql()
        first_on = result.split(" ON ")[1].split(" JOIN")[0]

        self.assertNotIn('"x"', first_on, f"qualify incorrectly resolved unaliased dbo.b column to alias x: {first_on}")
