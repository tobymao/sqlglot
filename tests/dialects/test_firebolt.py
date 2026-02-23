from tests.dialects.test_dialect import Validator


class TestFirebolt(Validator):
    dialect = "firebolt"

    def test_firebolt(self):
        # Basic identity tests
        self.validate_identity("SELECT 1")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(x) FROM t")
        self.validate_identity("SELECT ARRAY_LENGTH(arr) FROM t")

        # Type mappings
        self.validate_identity("CAST(x AS TEXT)")
        self.validate_identity("CAST(x AS BOOLEAN)")
        self.validate_identity("CAST(x AS DOUBLE PRECISION)")
        self.validate_identity("CAST(x AS TIMESTAMPTZ)")
        self.validate_identity("CAST(x AS BYTEA)")

        # Firebolt functions
        self.validate_identity("SELECT STRPOS(a, b) FROM t")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(x) FROM t")
        self.validate_identity("SELECT STDDEV_POP(x) FROM t")
        self.validate_identity("SELECT VAR_SAMP(x) FROM t")
        self.validate_identity("SELECT MAX_BY(a, b) FROM t")
        self.validate_identity("SELECT MIN_BY(a, b) FROM t")
        self.validate_identity("SELECT BIT_AND(x) FROM t")
        self.validate_identity("SELECT BIT_OR(x) FROM t")
        self.validate_identity("SELECT BIT_XOR(x) FROM t")

        # Firebolt-specific custom functions
        self.validate_identity("SELECT GEN_RANDOM_UUID_TEXT()")
        self.validate_identity("SELECT CITY_HASH(x)")
        self.validate_identity("SELECT TYPEOF(x)")
        self.validate_identity("SELECT CURRENT_DATABASE()")
        self.validate_identity("SELECT CURRENT_ENGINE()")
        self.validate_identity("SELECT CURRENT_ACCOUNT()")

        # Geospatial functions
        self.validate_identity("SELECT ST_GEOGPOINT(lon, lat)")
        self.validate_identity("SELECT ST_DISTANCE(a, b)")
        self.validate_identity("SELECT ST_CONTAINS(a, b)")

        # Vector functions
        self.validate_identity("SELECT VECTOR_COSINE_DISTANCE(a, b)")
        self.validate_identity("SELECT VECTOR_EUCLIDEAN_DISTANCE(a, b)")

        # Array functions
        self.validate_identity("SELECT ARRAY_DISTINCT(arr)")
        self.validate_identity("SELECT INDEX_OF(arr, val)")
        self.validate_identity("SELECT ARRAY_COUNT(arr)")

    def test_firebolt_array_literals(self):
        # Firebolt uses bracket syntax for arrays
        self.validate_all(
            "SELECT ['a', 'b', 'c']",
            write={
                "firebolt": "SELECT ['a', 'b', 'c']",
                "postgres": "SELECT ARRAY['a', 'b', 'c']",
            },
        )

    def test_firebolt_type_mapping(self):
        self.validate_all(
            "SELECT CAST(x AS VARCHAR)",
            write={
                "firebolt": "SELECT CAST(x AS TEXT)",
            },
        )
        self.validate_all(
            "SELECT CAST(x AS TINYINT)",
            write={
                "firebolt": "SELECT CAST(x AS INT)",
            },
        )
        self.validate_all(
            "SELECT CAST(x AS FLOAT)",
            write={
                "firebolt": "SELECT CAST(x AS REAL)",
            },
        )

    def test_firebolt_aggregations(self):
        self.validate_all(
            "SELECT APPROX_DISTINCT(x) FROM t",
            write={
                "firebolt": "SELECT APPROX_COUNT_DISTINCT(x) FROM t",
            },
        )

    def test_firebolt_numeric_functions(self):
        self.validate_all(
            "SELECT POW(x, 2)",
            write={
                "firebolt": "SELECT POW(x, 2)",
            },
        )
        self.validate_all(
            "SELECT RANDOM()",
            write={
                "firebolt": "SELECT RANDOM()",
            },
        )
