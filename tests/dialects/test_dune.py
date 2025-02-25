from sqlglot import exp
from tests.dialects.test_dialect import Validator


class TestDune(Validator):
    dialect = "dune"

    def test_dune(self):
        self.validate_identity("CAST(x AS INT256)")
        self.validate_identity("CAST(x AS UINT256)")

        for hex_literal in (
            "deadbeef",
            "deadbeefdead",
            "deadbeefdeadbeef",
            "deadbeefdeadbeefde",
            "deadbeefdeadbeefdead",
            "deadbeefdeadbeefdeadbeef",
            "deadbeefdeadbeefdeadbeefdeadbeef",
        ):
            with self.subTest(f"Transpiling hex literal {hex_literal}"):
                self.parse_one(f"0x{hex_literal}").assert_is(exp.HexString)

                self.validate_all(
                    f"SELECT 0x{hex_literal}",
                    read={
                        "dune": f"SELECT X'{hex_literal}'",
                        "postgres": f"SELECT x'{hex_literal}'",
                        "trino": f"SELECT X'{hex_literal}'",
                    },
                    write={
                        "dune": f"SELECT 0x{hex_literal}",
                        "postgres": f"SELECT x'{hex_literal}'",
                        "trino": f"SELECT x'{hex_literal}'",
                    },
                )
