from tests.dialects.test_dialect import Validator


class TestFirebolt(Validator):
    dialect = "firebolt"

    def test_firebolt(self):
        # `NOT` has higher precedence than `IN`/equality, negated part should be in parentheses
        self.validate_all(
            "SELECT NOT (col IN (1, 2)) FROM tbl",
            read={
                "": "SELECT col NOT IN (1, 2) FROM tbl",
                "firebolt": "SELECT col NOT IN (1, 2) FROM tbl",
            },
        )
        self.validate_all(
            "SELECT NOT (col IN (1, 2)) FROM tbl",
            read={"": "SELECT NOT col IN (1, 2) FROM tbl"},
        )
        # for equality no parentheses are needed
        self.validate_all(
            "SELECT NOT col = 1 FROM tbl",
            read={"": "SELECT NOT col = 1 FROM tbl"},
        )

        self.validate_identity("SELECT NOT (col IN (1, 2)) FROM tbl")
        self.validate_identity("SELECT NOT col IN (TRUE) FROM tbl")
        self.validate_identity("SELECT (NOT col) IN (TRUE) FROM tbl")
