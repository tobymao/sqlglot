from tests.dialects.test_dialect import Validator


class TestPostgres(Validator):
    dialect = "postgres"

    def test_postgres(self):
        self.validate_all(
            "CREATE TABLE x (a INT SERIAL)",
            read={"sqlite": "CREATE TABLE x (a INTEGER AUTOINCREMENT)"},
            write={"sqlite": "CREATE TABLE x (a INTEGER AUTOINCREMENT)"},
        )
        self.validate_all(
            "CREATE TABLE x (a UUID, b BYTEA)",
            write={
                "presto": "CREATE TABLE x (a UUID, b VARBINARY)",
                "hive": "CREATE TABLE x (a UUID, b BINARY)",
                "spark": "CREATE TABLE x (a UUID, b BINARY)",
            },
        )
        self.validate_all(
            "SELECT * FROM x FETCH 1 ROW",
            write={
                "postgres": "SELECT * FROM x FETCH FIRST 1 ROWS ONLY",
                "presto": "SELECT * FROM x FETCH FIRST 1 ROWS ONLY",
                "hive": "SELECT * FROM x FETCH FIRST 1 ROWS ONLY",
                "spark": "SELECT * FROM x FETCH FIRST 1 ROWS ONLY",
            },
        )
        self.validate_all(
            "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname",
            write={
                "postgres": "SELECT fname, lname, age FROM person ORDER BY age DESC, fname, lname",
                "presto": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname, lname",
                "hive": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
                "spark": "SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname NULLS LAST, lname NULLS LAST",
            },
        )
