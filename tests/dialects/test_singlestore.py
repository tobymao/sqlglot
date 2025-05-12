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
    metadatab BSON,
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

    def test_condition_generation(self):
        self.validate_generation(
            sql="SELECT b'1010'",
            exp_type=exp.BitString)
        self.validate_generation(
            sql="SELECT x'1F'",
            exp_type=exp.HexString)
        self.validate_generation(
            sql="SELECT e'hello'",
            from_dialect="postgres",
            exp_type=exp.ByteString)
        self.validate_generation(
            sql="SELECT r'raw\\nstring'",
            expected_sql="SELECT 'raw\\\\nstring'",
            from_dialect="spark",
            exp_type=exp.RawString)
        self.validate_generation(
            sql="SELECT U&'d\\0061t\\0061'",
            expected_sql="SELECT 'data'",
            from_dialect="presto",
            exp_type=exp.UnicodeString)
        self.validate_generation(
            sql="SELECT name FROM users",
            exp_type=exp.Column)
        self.validate_generation(
            sql="SELECT 42",
            exp_type=exp.Literal)
        self.validate_generation(
            sql="SELECT RANK() OVER (PARTITION BY category ORDER BY price) FROM products",
            exp_type=exp.Window)
        self.validate_generation(
            sql="SELECT @a",
            exp_type=exp.Parameter,
            run=False
        )
        self.validate_generation(
            sql="SELECT @@session.time_zone",
            exp_type=exp.SessionParameter)
        self.validate_generation(
            sql="SELECT ?",
            exp_type=exp.Placeholder,
            run=False
        )
        self.validate_generation(
            sql="SELECT :name",
            from_dialect="oracle",
            exp_type=exp.Placeholder,
            error_message="Named placeholders are not supported in SingleStore",
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE name IS NULL",
            exp_type=exp.Null)
        self.validate_generation(
            sql="SELECT TRUE",
            exp_type=exp.Boolean)

    def test_binary_functions_generation(self):
        self.validate_generation(
            sql="SELECT 1 + 2",
            exp_type=exp.Add)
        self.validate_generation(
            sql="SELECT TRUE AND FALSE",
            exp_type=exp.And)
        self.validate_generation(
            sql="SELECT TRUE OR FALSE",
            exp_type=exp.Or)
        self.validate_generation(
            sql="SELECT TRUE XOR FALSE",
            expected_sql="SELECT (TRUE AND (NOT FALSE)) OR ((NOT TRUE) AND FALSE)",
            exp_type=exp.Xor)
        self.validate_generation(
            sql="SELECT 5 & 3",
            exp_type=exp.BitwiseAnd)
        self.validate_generation(
            sql="SELECT 1 << 2",
            exp_type=exp.BitwiseLeftShift)
        self.validate_generation(
            sql="SELECT 5 | 2",
            exp_type=exp.BitwiseOr)
        self.validate_generation(
            sql="SELECT 8 >> 1",
            exp_type=exp.BitwiseRightShift)
        self.validate_generation(
            sql="SELECT 5 ^ 2",
            exp_type=exp.BitwiseXor)
        self.validate_generation(
            sql="SELECT 10 / 2",
            exp_type=exp.Div)
        self.validate_generation(
            sql="SELECT DATERANGE('2023-01-01', '2023-02-01') OVERLAPS DATERANGE('2023-01-15', '2023-03-01')",
            error_message="OVERLAPS is not supported in SingleStore",
            exp_type=exp.Overlaps,
            run=False
        )
        self.validate_generation(
            sql="SELECT 'f.g'.'h.e' FROM users",
            error_message="Dot condition (.) is not supported in SingleStore",
            exp_type=exp.Dot,
            run=False
        )
        self.validate_generation(
            sql="SELECT 'a' || 'b'",
            expected_sql="SELECT CONCAT('a', 'b')",
            from_dialect="postgres",
            exp_type=exp.DPipe)
        self.validate_generation(
            sql="key1 := 'value1'",
            exp_type=exp.PropertyEQ,
            run=False
        )
        self.validate_generation(
            sql="SELECT UNHEX('AE47E13EF2D20D3F7B14AE3E52B81E3F') <-> UNHEX('AE47E13EF2D20D3F7B14AE3E52B81E3F')",
            exp_type=exp.Distance)
        self.validate_generation(
            sql="SELECT 'abc' LIKE 'ABC' ESCAPE 'a'",
            error_message="ESCAPE condition in LIKE is not supported in SingleStore",
            exp_type=exp.Escape,
            run=False
        )
        self.validate_generation(
            sql="SELECT 10 DIV 3",
            exp_type=exp.IntDiv)
        self.validate_generation(
            sql="SELECT FUNC(key => 'value')",
            error_message="Kwarg condition (=>) is not supported in SingleStore",
            exp_type=exp.Kwarg,
            run=False
        )
        self.validate_generation(
            sql="SELECT 10 % 3",
            exp_type=exp.Mod)
        self.validate_generation(
            sql="SELECT 2 * 3",
            exp_type=exp.Mul)
        self.validate_generation(
            sql="SELECT 1 OPERATOR(+) 2",
            error_message="Custom operators are not supported in SingleStore",
            from_dialect="postgres",
            exp_type=exp.Operator,
            run=False
        )
        self.validate_generation(
            sql="SELECT [2,3,4][2:3]",
            expected_sql="SELECT ARRAY(2, 3, 4)[2 : 3]",
            from_dialect="duckdb",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Slice,
            run=False
        )
        self.validate_generation(
            sql="SELECT 5 - 3",
            exp_type=exp.Sub)
        self.validate_generation(
            sql="SELECT ARRAY_CONTAINS('hello'::VARIANT, ARRAY_CONSTRUCT('hello', 'hi'))",
            expected_sql="SELECT ARRAY_CONTAINS(ARRAY('hello', 'hi'), CAST('hello' AS VARIANT))",
            from_dialect="snowflake",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayContains,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_CONTAINS_ALL(arr, ARRAY(1, 2))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayContainsAll,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_OVERLAPS(arr1, arr2)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayOverlaps,
            run=False
        )
        self.validate_generation(
            sql="SELECT name COLLATE 'utf8mb4_bin' FROM users",
            expected_sql="SELECT name :> LONGTEXT COLLATE 'utf8mb4_bin' FROM users",
            exp_type=exp.Collate)
        self.validate_generation(
            sql="SELECT POWER(2, 10)",
            exp_type=exp.Pow)
        self.validate_generation(
            sql="SELECT metadata->'arr'->1->'key' FROM events",
            expected_sql="SELECT JSON_EXTRACT_JSON(JSON_EXTRACT_JSON(JSON_EXTRACT_JSON(metadata, 'arr'), 1), 'key') FROM events",
            from_dialect="postgres",
            exp_type=exp.JSONExtract)
        self.validate_generation(
            sql="SELECT metadata->'arr'->1->>'key' FROM events",
            expected_sql="SELECT JSON_EXTRACT_STRING(JSON_EXTRACT_JSON(JSON_EXTRACT_JSON(metadata, 'arr'), 1), 'key') FROM events",
            from_dialect="postgres",
            exp_type=exp.JSONExtractScalar)
        self.validate_generation(
            sql="SELECT metadatab#>'arr'#>1#>'key' FROM events",
            expected_sql="SELECT BSON_EXTRACT_BSON(BSON_EXTRACT_BSON(BSON_EXTRACT_BSON(metadatab, 'arr'), 1), 'key') FROM events",
            from_dialect="postgres",
            exp_type=exp.JSONBExtract)
        self.validate_generation(
            sql="SELECT metadatab#>'arr'#>1#>>'key' FROM events",
            expected_sql="SELECT BSON_EXTRACT_STRING(BSON_EXTRACT_BSON(BSON_EXTRACT_BSON(metadatab, 'arr'), 1), 'key') FROM events",
            from_dialect="postgres",
            exp_type=exp.JSONBExtractScalar)
        self.validate_generation(
            sql="SELECT metadatab?'arr' FROM events",
            expected_sql="SELECT JSONB_CONTAINS(metadatab, 'arr') FROM events",
            error_message="JSONBContains is not supported in SingleStore",
            exp_type=exp.JSONBContains,
            run=False
        )
        self.validate_generation(
            sql="SELECT 'abc' ~ 'a.*'",
            expected_sql="SELECT 'abc' RLIKE 'a.*'",
            from_dialect="postgres",
            exp_type=exp.RegexpLike)
        self.validate_generation(
            sql="SELECT 'ABC' ~* 'a.*'",
            expected_sql="SELECT LOWER('ABC') RLIKE LOWER('a.*')",
            from_dialect="postgres",
            exp_type=exp.RegexpILike)

    def test_unary_functions_generation(self):
        self.validate_generation(
            sql="SELECT ~5",
            exp_type=exp.BitwiseNot)
        self.validate_generation(
            sql="SELECT NOT TRUE",
            exp_type=exp.Not)
        self.validate_generation(
            sql="SELECT (1 + 2)",
            exp_type=exp.Paren)
        self.validate_generation(
            sql="SELECT -42",
            exp_type=exp.Neg)
