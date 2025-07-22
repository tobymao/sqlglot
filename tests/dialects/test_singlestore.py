import os
import typing as t
from sqlglot import exp, ErrorLevel, UnsupportedError
from tests.dialects.test_dialect import Validator

SINGLESTORE_HOST = os.environ.get("SINGLESTORE_HOST", "127.0.0.1")
SINGLESTORE_PORT = os.environ.get("SINGLESTORE_PORT", "3306")
SINGLESTORE_USER = os.environ.get("SINGLESTORE_USER", "root")
SINGLESTORE_PASSWORD = os.environ.get("SINGLESTORE_PASSWORD", "1")
INTEGRATION_TEST = os.environ.get("INTEGRATION_TEST", "0") == "1"


# Executes the query against the actual database
# This will only run when INTEGRATION_TEST is set to true
def execute_query(query: str):
    if not INTEGRATION_TEST:
        return

    import singlestoredb as s2

    if not hasattr(execute_query, "conn"):
        execute_query.conn = s2.connect(
            host=SINGLESTORE_HOST,
            port=SINGLESTORE_PORT,
            user=SINGLESTORE_USER,
            password=SINGLESTORE_PASSWORD,
            multi_statements=True,
        )

    with execute_query.conn.cursor() as cur:
        cur.execute(query)


class TestSingleStore(Validator):
    dialect = "singlestore"

    # Precreates all necessary database objects for future tests
    def setUp(self):
        execute_query("DROP DATABASE IF EXISTS db")
        execute_query("CREATE DATABASE db")
        execute_query("USE db")
        execute_query("DROP ROLE IF EXISTS r")
        execute_query("CREATE ROLE r")
        execute_query("""CREATE ROWSTORE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    age INT,
    signup_date DATE,
    is_active BOOLEAN
);
""")
        execute_query("""CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP SERIES TIMESTAMP,
    KEY(user_id)
);
""")
        execute_query("""CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2),
    category VARCHAR(50),
    stock_quantity INT,
    FULLTEXT USING VERSION 1 ind (name)
);
""")
        execute_query("""CREATE TABLE products2 (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10, 2),
                category VARCHAR(50),
                stock_quantity INT,
                FULLTEXT USING VERSION 2 ind (name)
            );
            """)
        execute_query("""CREATE TABLE order_items (
    id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    item_price DECIMAL(10, 2)
);
""")
        execute_query("""CREATE TABLE events (
    id INT PRIMARY KEY,
    user_id INT,
    event_type VARCHAR(50),
    metadata JSON,
    metadatab BSON,
    occurred_at TIMESTAMP,
    FULLTEXT USING VERSION 2 index (event_type)
);
    """)
        execute_query("""CREATE FUNCTION is_prime(n BIGINT NOT NULL) returns BIGINT AS
  BEGIN
    IF n <= 1 THEN
      RETURN FALSE;
    END IF;
    FOR i IN 2 .. (n-1) LOOP
      EXIT WHEN i * i > n;
      IF n % i != 0 THEN
        CONTINUE;
      END IF;
      RETURN FALSE;
    END LOOP;
    RETURN TRUE;
  END
""")
        execute_query(
            """CREATE OR REPLACE PROCEDURE proc() RETURNS void AS BEGIN ECHO SELECT 1; END"""
        )
        execute_query("""DROP VIEW IF EXISTS users_view""")
        execute_query("""CREATE VIEW users_view AS SELECT * FROM users""")

    # Extends the base validate_identity to support error checking, AST expression validation, and optional execution on the database
    def validate_identity(
        self,
        sql,
        write_sql=None,
        pretty=False,
        check_command_warning=False,
        identify=False,
        exp_type: t.Type[exp.Expression] = None,
        error_message: str = None,
        run=True,
    ):
        super().validate_identity(sql, write_sql)

        if exp_type:
            assert self.parse_one(sql).find(exp_type) is not None

        if error_message:
            with self.assertRaises(UnsupportedError) as ctx:
                self.parse_one(sql, unsupported_level=ErrorLevel.RAISE)
            self.assertIn(
                error_message,
                str(ctx.exception),
            )

        if run:
            execute_query(self.parse_one(sql).sql(dialect="singlestore"))

    def test_basic(self):
        self.validate_identity("SELECT 1")
