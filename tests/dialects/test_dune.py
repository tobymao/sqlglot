from tests.dialects.test_dialect import Validator


class TestDune(Validator):
    dialect = "dune"

    def test_dune(self):
        self.validate_identity("CAST(x AS INT256)")
        self.validate_identity("CAST(x AS UINT256)")

        self.validate_all(
            "SELECT 0xdeadbeef",
            read={
                "dune": "SELECT X'deadbeef'",
                "postgres": "SELECT x'deadbeef'",
                "trino": "SELECT X'deadbeef'",
            },
            write={
                "dune": "SELECT 0xdeadbeef",
                "postgres": "SELECT x'deadbeef'",
                "trino": "SELECT X'deadbeef'",
            },
        )
