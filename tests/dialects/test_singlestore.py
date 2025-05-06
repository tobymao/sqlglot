from tests.dialects.test_dialect import Validator
from sqlglot import exp, parse_one, ErrorLevel, UnsupportedError
import typing as t
import singlestoredb as s2
import os

SINGELSTORE_HOST = os.environ.get("SINGELSTORE_HOST", "127.0.0.1")
SINGELSTORE_PORT = os.environ.get("SINGELSTORE_PORT", "3306")
SINGELSTORE_USER = os.environ.get("SINGELSTORE_USER", "root")
SINGELSTORE_PASSWORD = os.environ.get("SINGELSTORE_PASSWORD", "1")

conn = s2.connect(host=SINGELSTORE_HOST, port=SINGELSTORE_PORT,
                  user=SINGELSTORE_USER, password=SINGELSTORE_PASSWORD)


class TestSingleStore(Validator):
    dialect = "singlestore"

    def setUp(self):
        with conn.cursor() as cur:
            cur.execute("DROP DATABASE IF EXISTS db")
            cur.execute("CREATE DATABASE db")
            cur.execute("USE db")
            cur.execute("""CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    age INT,
    signup_date DATE,
    is_active BOOLEAN
);
""")
            cur.execute("""CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP
);
""")
            cur.execute("""CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2),
    category VARCHAR(50),
    stock_quantity INT
);
""")
            cur.execute("""CREATE TABLE order_items (
    id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    item_price DECIMAL(10, 2)
);
""")
            cur.execute("""CREATE TABLE events (
    id INT PRIMARY KEY,
    user_id INT,
    event_type VARCHAR(50),
    metadata JSON,
    occurred_at TIMESTAMP
);
    """)

    def validate_generation(self,
        sql: str, expected_sql: str = None, error_message: str = None,
        from_dialect="mysql", exp_type: t.Type[exp.Expression] = None,
        run: bool = True):
        query = parse_one(sql, read=from_dialect)

        # check that expression which is validated is somewhere in the query
        if exp_type is not None:
            assert query.find(exp_type) is not None

        if error_message is not None:
            with self.assertRaises(UnsupportedError) as ctx:
                query.sql(dialect="singlestore",
                          unsupported_level=ErrorLevel.RAISE)
            self.assertEqual(
                str(ctx.exception),
                error_message,
            )

        generated = query.sql(dialect="singlestore")

        if run:
            with conn.cursor() as cur:
                cur.execute(generated)

        if expected_sql is not None:
            self.assertEqual(expected_sql, generated)
        else:
            self.assertEqual(sql, generated)

    def test_predicate_generation(self):
        self.validate_generation(
            sql="SELECT name FROM users WHERE age > ALL (SELECT age FROM users WHERE is_active = FALSE)",
            error_message="ALL subquery predicate is not supported in SingleStore",
            exp_type=exp.All,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM orders WHERE amount > ANY (SELECT amount FROM orders WHERE status = 'pending')",
            error_message="ANY subquery predicate is not supported in SingleStore",
            exp_type=exp.Any,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM users AS u WHERE EXISTS(SELECT 1 FROM orders AS o WHERE o.user_id = u.id)",
            exp_type=exp.Exists
        )

        self.validate_generation(
            sql="SELECT * FROM users WHERE age = 30",
            exp_type=exp.EQ
        )
        self.validate_generation(
            sql="SELECT 1 <=> NULL",
            exp_type=exp.NullSafeEQ
        )
        self.validate_generation(
            sql="SELECT 1 IS DISTINCT FROM NULL",
            expected_sql="SELECT NOT 1 <=> NULL",
            from_dialect="postgres",
            exp_type=exp.NullSafeNEQ
        )
        self.validate_generation(
            sql="SELECT * FROM files WHERE name GLOB '*.csv'",
            error_message="GLOB predicate is not supported in SingleStore",
            exp_type=exp.Glob,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM orders WHERE amount > 100",
            exp_type=exp.GT
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE age >= 18",
            exp_type=exp.GTE
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE name ILIKE 'john%'",
            expected_sql="SELECT * FROM users WHERE LOWER(name) LIKE LOWER('john%')",
            exp_type=exp.ILike,
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE name ILIKE ANY ('A%', 'B%')",
            from_dialect="snowflake",
            error_message="ILIKE ANY predicate is not supported in SingleStore",
            exp_type=exp.ILikeAny,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE is_active IS TRUE",
            exp_type=exp.Is
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE name LIKE 'A%'",
            expected_sql="SELECT * FROM users WHERE name LIKE 'A%'",
            exp_type=exp.Like
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE name LIKE ANY ('A%', 'B%')",
            from_dialect="snowflake",
            error_message="LIKE ANY predicate is not supported in SingleStore",
            exp_type=exp.LikeAny,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM products WHERE price < 50",
            exp_type=exp.LT
        )
        self.validate_generation(
            sql="SELECT * FROM products WHERE price <= 99.99",
            exp_type=exp.LTE
        )
        self.validate_generation(
            sql="SELECT * FROM orders WHERE status <> 'inactive'",
            exp_type=exp.NEQ
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE name SIMILAR TO 'Jo.*'",
            error_message="SIMILAR TO predicate is not supported in SingleStore",
            exp_type=exp.SimilarTo,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM orders WHERE amount BETWEEN 100 AND 200",
            exp_type=exp.Between
        )
        self.validate_generation(
            sql="SELECT * FROM products WHERE category IN ('Books', 'Games')",
            exp_type=exp.In
        )
        self.validate_generation(
            sql="SELECT * FROM events WHERE '\"promo\"' MEMBER OF(metadata)",
            expected_sql="SELECT * FROM events WHERE JSON_ARRAY_CONTAINS_JSON(metadata, '\"promo\"')",
            exp_type=exp.JSONArrayContains
        )
