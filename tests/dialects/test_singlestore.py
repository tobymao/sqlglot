from sqlglot import parse_one
from sqlglot.optimizer.qualify import qualify
from tests.dialects.test_dialect import Validator


class TestSingleStore(Validator):
    dialect = "singlestore"

    def test_basic(self):
        self.validate_identity("SELECT 1")
        self.validate_identity("SELECT * FROM `users` ORDER BY ALL")
        ast = parse_one(
            "SELECT id AS my_id FROM data WHERE my_id = 1 GROUP BY my_id HAVING my_id = 1",
            dialect=self.dialect,
        )
        ast = qualify(ast, dialect=self.dialect, schema={"data": {"id": "INT", "my_id": "INT"}})
        self.assertEqual(
            "SELECT `data`.`id` AS `my_id` FROM `data` AS `data` WHERE `data`.`my_id` = 1 GROUP BY `data`.`my_id` HAVING `data`.`id` = 1",
            ast.sql(dialect=self.dialect),
        )

    def test_restricted_keywords(self):
        self.validate_identity("SELECT * FROM abs", "SELECT * FROM `abs`")
        self.validate_identity("SELECT * FROM ABS", "SELECT * FROM `ABS`")
        self.validate_identity(
            "SELECT * FROM security_lists_intersect", "SELECT * FROM `security_lists_intersect`"
        )
        self.validate_identity("SELECT * FROM vacuum", "SELECT * FROM `vacuum`")
