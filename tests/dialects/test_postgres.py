from sqlglot import ParseError, transpile
from tests.dialects.test_dialect import Validator


class TestPostgres(Validator):
    dialect = "postgres"

    def test_ddl(self):
        self.validate_all(
            "CREATE TABLE products (product_no INT UNIQUE, name TEXT, price DECIMAL)",
            write={
                "postgres": "CREATE TABLE products (product_no INT UNIQUE, name TEXT, price DECIMAL)"
            },
        )
        self.validate_all(
            "CREATE TABLE products (product_no INT CONSTRAINT must_be_different UNIQUE, name TEXT CONSTRAINT present NOT NULL, price DECIMAL)",
            write={
                "postgres": "CREATE TABLE products (product_no INT CONSTRAINT must_be_different UNIQUE, name TEXT CONSTRAINT present NOT NULL, price DECIMAL)"
            },
        )
        self.validate_all(
            "CREATE TABLE products (product_no INT, name TEXT, price DECIMAL, UNIQUE (product_no, name))",
            write={
                "postgres": "CREATE TABLE products (product_no INT, name TEXT, price DECIMAL, UNIQUE (product_no, name))"
            },
        )
        self.validate_all(
            "CREATE TABLE products ("
            "product_no INT UNIQUE,"
            " name TEXT,"
            " price DECIMAL CHECK (price > 0),"
            " discounted_price DECIMAL CONSTRAINT positive_discount CHECK (discounted_price > 0),"
            " CHECK (product_no > 1),"
            " CONSTRAINT valid_discount CHECK (price > discounted_price))",
            write={
                "postgres": "CREATE TABLE products ("
                "product_no INT UNIQUE,"
                " name TEXT,"
                " price DECIMAL CHECK (price > 0),"
                " discounted_price DECIMAL CONSTRAINT positive_discount CHECK (discounted_price > 0),"
                " CHECK (product_no > 1),"
                " CONSTRAINT valid_discount CHECK (price > discounted_price))"
            },
        )

        with self.assertRaises(ParseError):
            transpile(
                "CREATE TABLE products (price DECIMAL CHECK price > 0)", read="postgres"
            )
        with self.assertRaises(ParseError):
            transpile(
                "CREATE TABLE products (price DECIMAL, CHECK price > 1)",
                read="postgres",
            )

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
            "SELECT SUM(x) OVER (PARTITION BY a ORDER BY d ROWS 1 PRECEDING)",
            write={
                "postgres": "SELECT SUM(x) OVER (PARTITION BY a ORDER BY d ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
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
