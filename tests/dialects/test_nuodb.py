from sqlglot import expressions as exp
from tests.dialects.test_dialect import Validator


class TestNuoDB(Validator):
    dialect = "nuodb"
    # def test_ddl