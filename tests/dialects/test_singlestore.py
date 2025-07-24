from tests.dialects.test_dialect import Validator


class TestSingleStore(Validator):
    dialect = "singlestore"

    def test_basic(self):
        self.validate_identity("SELECT 1")
        self.validate_identity("SELECT * FROM users ORDER BY ALL")
        self.validate_identity(
            "WITH data AS (SELECT 1 AS id, 2 AS my_id) SELECT id AS my_id FROM data WHERE my_id = 1 GROUP BY my_id HAVING my_id = 1"
        )
