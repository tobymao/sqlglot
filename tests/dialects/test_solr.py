from sqlglot import exp
from tests.dialects.test_dialect import Validator


class TestSolr(Validator):
    dialect = "solr"

    def test_solr(self):
        self.validate_identity("SELECT `default`.column FROM t")
        self.failureException('SELECT "column" FROM t')
        self.validate_identity("SELECT column FROM t WHERE column = 'val'")
        self.validate_identity("a || b", "a OR b").assert_is(exp.Or)
