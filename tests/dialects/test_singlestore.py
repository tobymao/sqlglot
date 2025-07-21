import math

from tests.dialects.test_dialect import Validator
from sqlglot import exp, parse_one, ErrorLevel, UnsupportedError
import typing as t
import os

SINGELSTORE_HOST = os.environ.get("SINGELSTORE_HOST", "127.0.0.1")
SINGELSTORE_PORT = os.environ.get("SINGELSTORE_PORT", "3306")
SINGELSTORE_USER = os.environ.get("SINGELSTORE_USER", "root")
SINGELSTORE_PASSWORD = os.environ.get("SINGELSTORE_PASSWORD", "1")
INTEGRATION_TEST = os.environ.get("INTEGRATION_TEST", "0") == "1"


# Executes the query against the actual database only if INTEGRATION_TEST is enabled
def execute_query(query: str):
    if not INTEGRATION_TEST:
        return

    import singlestoredb as s2

    if not hasattr(execute_query, "conn"):
        execute_query.conn = s2.connect(
            host=SINGELSTORE_HOST,
            port=SINGELSTORE_PORT,
            user=SINGELSTORE_USER,
            password=SINGELSTORE_PASSWORD,
            multi_statements=True,
        )

    with execute_query.conn.cursor() as cur:
        cur.execute(query)


class TestSingleStore(Validator):
    dialect = "singlestore"

    def setUp(self):
        execute_query("DROP DATABASE IF EXISTS db")
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

    def validate_generation(
        self,
        sql: str,
        expected_sql: str = None,
        error_message: str = None,
        from_dialect="mysql",
        exp_type: t.Type[exp.Expression] = None,
        run: bool = True,
    ):
        query = parse_one(sql, read=from_dialect)

        # check that expression which is validated is somewhere in the query
        if exp_type is not None:
            assert query.find(exp_type) is not None

        if error_message is not None:
            with self.assertRaises(UnsupportedError) as ctx:
                query.sql(dialect="singlestore", unsupported_level=ErrorLevel.RAISE)
            self.assertIn(
                error_message,
                str(ctx.exception),
            )

        generated = query.sql(dialect="singlestore")
        print(generated)

        if run:
            execute_query(generated)

        if expected_sql is not None:
            self.assertEqual(expected_sql, generated)
        else:
            self.assertEqual(sql, generated)

    def validate_parsing(self, sql: str, expected_expr: exp.Expression, run: bool = True):
        query = parse_one(sql, read="singlestore")
        expr = query.find(type(expected_expr))
        self.assertIsNotNone(expr, f"Expected {expected_expr.type} in {query}")
        self.assertEqual(expr, expected_expr)

        # Check that the query can be executed
        generated = query.sql(dialect="singlestore")
        print(generated)
        if run:
            execute_query(generated)

    def test_predicate_generation(self):
        self.validate_generation(
            sql="SELECT name FROM users WHERE age > ALL (SELECT age FROM users WHERE is_active = FALSE)",
            error_message="ALL subquery predicate is not supported in SingleStore",
            exp_type=exp.All,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM orders WHERE amount > ANY (SELECT amount FROM orders WHERE status = 'pending')",
            error_message="ANY subquery predicate is not supported in SingleStore",
            exp_type=exp.Any,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM users AS u WHERE EXISTS(SELECT 1 FROM orders AS o WHERE o.user_id = u.id)",
            exp_type=exp.Exists,
        )

        self.validate_generation(sql="SELECT * FROM users WHERE age = 30", exp_type=exp.EQ)
        self.validate_generation(sql="SELECT 1 <=> NULL", exp_type=exp.NullSafeEQ)
        self.validate_generation(
            sql="SELECT 1 IS DISTINCT FROM NULL",
            expected_sql="SELECT NOT 1 <=> NULL",
            from_dialect="postgres",
            exp_type=exp.NullSafeNEQ,
        )
        self.validate_generation(
            sql="SELECT * FROM files WHERE name GLOB '*.csv'",
            error_message="GLOB predicate is not supported in SingleStore",
            exp_type=exp.Glob,
            run=False,
        )
        self.validate_generation(sql="SELECT * FROM orders WHERE amount > 100", exp_type=exp.GT)
        self.validate_generation(sql="SELECT * FROM users WHERE age >= 18", exp_type=exp.GTE)
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
            run=False,
        )
        self.validate_generation(sql="SELECT * FROM users WHERE is_active IS TRUE", exp_type=exp.Is)
        self.validate_generation(
            sql="SELECT * FROM users WHERE name LIKE 'A%'",
            expected_sql="SELECT * FROM users WHERE name LIKE 'A%'",
            exp_type=exp.Like,
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE name LIKE ANY ('A%', 'B%')",
            from_dialect="snowflake",
            error_message="LIKE ANY predicate is not supported in SingleStore",
            exp_type=exp.LikeAny,
            run=False,
        )
        self.validate_generation(sql="SELECT * FROM products WHERE price < 50", exp_type=exp.LT)
        self.validate_generation(
            sql="SELECT * FROM products WHERE price <= 99.99", exp_type=exp.LTE
        )
        self.validate_generation(
            sql="SELECT * FROM orders WHERE status <> 'inactive'", exp_type=exp.NEQ
        )
        self.validate_generation(
            sql="SELECT * FROM users WHERE name SIMILAR TO 'Jo.*'",
            error_message="SIMILAR TO predicate is not supported in SingleStore",
            exp_type=exp.SimilarTo,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM orders WHERE amount BETWEEN 100 AND 200", exp_type=exp.Between
        )
        self.validate_generation(
            sql="SELECT * FROM products WHERE category IN ('Books', 'Games')", exp_type=exp.In
        )
        self.validate_generation(
            sql="SELECT * FROM events WHERE '\"promo\"' MEMBER OF(metadata)",
            expected_sql="SELECT * FROM events WHERE JSON_ARRAY_CONTAINS_JSON(metadata, '\"promo\"')",
            exp_type=exp.JSONArrayContains,
        )

    def test_condition_generation(self):
        self.validate_generation(sql="SELECT b'1010'", exp_type=exp.BitString)
        self.validate_generation(sql="SELECT x'1F'", exp_type=exp.HexString)
        self.validate_generation(
            sql="SELECT e'hello'", from_dialect="postgres", exp_type=exp.ByteString
        )
        self.validate_generation(
            sql="SELECT r'raw\\nstring'",
            expected_sql="SELECT 'raw\\\\nstring'",
            from_dialect="spark",
            exp_type=exp.RawString,
        )
        self.validate_generation(
            sql="SELECT U&'d\\0061t\\0061'",
            expected_sql="SELECT 'data'",
            from_dialect="presto",
            exp_type=exp.UnicodeString,
        )
        self.validate_generation(sql="SELECT name FROM users", exp_type=exp.Column)
        self.validate_generation(sql="SELECT 42", exp_type=exp.Literal)
        self.validate_generation(
            sql="SELECT RANK() OVER (PARTITION BY category ORDER BY price) FROM products",
            exp_type=exp.Window,
        )
        self.validate_generation(sql="SELECT @a", exp_type=exp.Parameter, run=False)
        self.validate_generation(sql="SELECT @@session.time_zone", exp_type=exp.SessionParameter)
        self.validate_generation(sql="SELECT ?", exp_type=exp.Placeholder, run=False)
        self.validate_generation(
            sql="SELECT :name",
            from_dialect="oracle",
            exp_type=exp.Placeholder,
            error_message="Named placeholders are not supported in SingleStore",
            run=False,
        )
        self.validate_generation(sql="SELECT * FROM users WHERE name IS NULL", exp_type=exp.Null)
        self.validate_generation(sql="SELECT TRUE", exp_type=exp.Boolean)

    def test_binary_functions_generation(self):
        self.validate_generation(sql="SELECT 1 + 2", exp_type=exp.Add)
        self.validate_generation(sql="SELECT TRUE AND FALSE", exp_type=exp.And)
        self.validate_generation(sql="SELECT TRUE OR FALSE", exp_type=exp.Or)
        self.validate_generation(
            sql="SELECT TRUE XOR FALSE",
            expected_sql="SELECT (TRUE AND (NOT FALSE)) OR ((NOT TRUE) AND FALSE)",
            exp_type=exp.Xor,
        )
        self.validate_generation(sql="SELECT 5 & 3", exp_type=exp.BitwiseAnd)
        self.validate_generation(sql="SELECT 1 << 2", exp_type=exp.BitwiseLeftShift)
        self.validate_generation(sql="SELECT 5 | 2", exp_type=exp.BitwiseOr)
        self.validate_generation(sql="SELECT 8 >> 1", exp_type=exp.BitwiseRightShift)
        self.validate_generation(sql="SELECT 5 ^ 2", exp_type=exp.BitwiseXor)
        self.validate_generation(sql="SELECT 10 / 2", exp_type=exp.Div)
        self.validate_generation(
            sql="SELECT DATERANGE('2023-01-01', '2023-02-01') OVERLAPS DATERANGE('2023-01-15', '2023-03-01')",
            error_message="OVERLAPS is not supported in SingleStore",
            exp_type=exp.Overlaps,
            run=False,
        )
        self.validate_generation(
            sql="SELECT 'f.g'.'h.e' FROM users",
            error_message="Dot condition (.) is not supported in SingleStore",
            exp_type=exp.Dot,
            run=False,
        )
        self.validate_generation(
            sql="SELECT 'a' || 'b'",
            expected_sql="SELECT CONCAT('a', 'b')",
            from_dialect="postgres",
            exp_type=exp.DPipe,
        )
        self.validate_generation(sql="key1 := 'value1'", exp_type=exp.PropertyEQ, run=False)
        self.validate_generation(
            sql="SELECT UNHEX('AE47E13EF2D20D3F7B14AE3E52B81E3F') <-> UNHEX('AE47E13EF2D20D3F7B14AE3E52B81E3F')",
            exp_type=exp.Distance,
        )
        self.validate_generation(
            sql="SELECT 'abc' LIKE 'ABC' ESCAPE 'a'",
            error_message="ESCAPE condition in LIKE is not supported in SingleStore",
            exp_type=exp.Escape,
            run=False,
        )
        self.validate_generation(sql="SELECT 10 DIV 3", exp_type=exp.IntDiv)
        self.validate_generation(
            sql="SELECT FUNC(key => 'value')",
            error_message="Kwarg condition (=>) is not supported in SingleStore",
            exp_type=exp.Kwarg,
            run=False,
        )
        self.validate_generation(sql="SELECT 10 % 3", exp_type=exp.Mod)
        self.validate_generation(sql="SELECT 2 * 3", exp_type=exp.Mul)
        self.validate_generation(
            sql="SELECT 1 OPERATOR(+) 2",
            error_message="Custom operators are not supported in SingleStore",
            from_dialect="postgres",
            exp_type=exp.Operator,
            run=False,
        )
        self.validate_generation(
            sql="SELECT [2,3,4][2:3]",
            expected_sql="SELECT ARRAY(2, 3, 4)[2 : 3]",
            from_dialect="duckdb",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Slice,
            run=False,
        )
        self.validate_generation(sql="SELECT 5 - 3", exp_type=exp.Sub)
        self.validate_generation(
            sql="SELECT ARRAY_CONTAINS('hello'::VARIANT, ARRAY_CONSTRUCT('hello', 'hi'))",
            expected_sql="SELECT ARRAY_CONTAINS(ARRAY('hello', 'hi'), 'hello' :> TEXT)",
            from_dialect="snowflake",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayContains,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_CONTAINS_ALL(arr, ARRAY(1, 2))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayContainsAll,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_OVERLAPS(arr1, arr2)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayOverlaps,
            run=False,
        )
        self.validate_generation(
            sql="SELECT name COLLATE 'utf8mb4_bin' FROM users",
            expected_sql="SELECT name :> LONGTEXT COLLATE 'utf8mb4_bin' FROM users",
            exp_type=exp.Collate,
        )
        self.validate_generation(sql="SELECT POWER(2, 10)", exp_type=exp.Pow)
        self.validate_generation(
            sql="SELECT metadata->'arr'->1->'key' FROM events",
            expected_sql="SELECT JSON_EXTRACT_JSON(JSON_EXTRACT_JSON(JSON_EXTRACT_JSON(metadata, 'arr'), 1), 'key') FROM events",
            from_dialect="postgres",
            exp_type=exp.JSONExtract,
        )
        self.validate_generation(
            sql="SELECT metadata->'arr'->1->>'key' FROM events",
            expected_sql="SELECT JSON_EXTRACT_STRING(JSON_EXTRACT_JSON(JSON_EXTRACT_JSON(metadata, 'arr'), 1), 'key') FROM events",
            from_dialect="postgres",
            exp_type=exp.JSONExtractScalar,
        )
        self.validate_generation(
            sql="SELECT metadatab#>'arr'#>1#>'key' FROM events",
            expected_sql="SELECT BSON_EXTRACT_BSON(BSON_EXTRACT_BSON(BSON_EXTRACT_BSON(metadatab, 'arr'), 1), 'key') FROM events",
            from_dialect="postgres",
            exp_type=exp.JSONBExtract,
        )
        self.validate_generation(
            sql="SELECT metadatab#>'arr'#>1#>>'key' FROM events",
            expected_sql="SELECT BSON_EXTRACT_STRING(BSON_EXTRACT_BSON(BSON_EXTRACT_BSON(metadatab, 'arr'), 1), 'key') FROM events",
            from_dialect="postgres",
            exp_type=exp.JSONBExtractScalar,
        )
        self.validate_generation(
            sql="SELECT metadatab?'arr' FROM events",
            expected_sql="SELECT JSONB_CONTAINS(metadatab, 'arr') FROM events",
            error_message="JSONBContains is not supported in SingleStore",
            exp_type=exp.JSONBContains,
            run=False,
        )
        self.validate_generation(
            sql="SELECT 'abc' ~ 'a.*'",
            expected_sql="SELECT 'abc' RLIKE 'a.*'",
            from_dialect="postgres",
            exp_type=exp.RegexpLike,
        )
        self.validate_generation(
            sql="SELECT 'ABC' ~* 'a.*'",
            expected_sql="SELECT LOWER('ABC') RLIKE LOWER('a.*')",
            from_dialect="postgres",
            exp_type=exp.RegexpILike,
        )

    def test_unary_functions_generation(self):
        self.validate_generation(sql="SELECT ~5", exp_type=exp.BitwiseNot)
        self.validate_generation(sql="SELECT NOT TRUE", exp_type=exp.Not)
        self.validate_generation(sql="SELECT (1 + 2)", exp_type=exp.Paren)
        self.validate_generation(sql="SELECT -42", exp_type=exp.Neg)

    def test_bracket(self):
        self.validate_generation(
            sql="SELECT arr[1] FROM events",
            error_message="Arrays are not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.Bracket,
            run=False,
        )

    def test_aggregate_functions_generation(self):
        self.validate_generation(
            sql="SELECT quantileGK(100, 0.95)(reading) OVER (PARTITION BY id) FROM table",
            expected_sql="SELECT QUANTILEGK(100, 0.95)(reading) OVER (PARTITION BY id) FROM table",
            from_dialect="clickhouse",
            error_message="Parametrized aggregate functions are not supported in SingleStore",
            exp_type=exp.ParameterizedAgg,
            run=False,
        )
        self.validate_generation(
            sql="select quantileGKIf(100, 0.95)(reading) OVER (PARTITION BY id) FROM table",
            expected_sql="SELECT QUANTILEGKIF(100, 0.95)(reading) OVER (PARTITION BY id) FROM table",
            from_dialect="clickhouse",
            error_message="Parametrized aggregate functions are not supported in SingleStore",
            exp_type=exp.CombinedParameterizedAgg,
            run=False,
        )
        self.validate_generation(
            sql="select quantileGK(100, 0.95) OVER (PARTITION BY id) FROM table",
            expected_sql="SELECT QUANTILEGK(100, 0.95) OVER (PARTITION BY id) FROM table",
            from_dialect="clickhouse",
            error_message="Anonymous aggregate functions are not supported in SingleStore",
            exp_type=exp.AnonymousAggFunc,
            run=False,
        )
        self.validate_generation(
            sql="select quantileGKIf(100, 0.95) OVER (PARTITION BY id) FROM table",
            expected_sql="SELECT QUANTILEGKIF(100, 0.95) OVER (PARTITION BY id) FROM table",
            from_dialect="clickhouse",
            error_message="Aggregate function combinators are not supported in SingleStore",
            exp_type=exp.CombinedAggFunc,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARG_MAX(name, age) FROM users",
            error_message="ARG_MAX function is not supported in SingleStore",
            exp_type=exp.ArgMax,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARG_MIN(name, age) FROM users",
            error_message="ARG_MIN function is not supported in SingleStore",
            exp_type=exp.ArgMin,
            run=False,
        )
        self.validate_generation(
            sql="SELECT APPROX_TOP_K(category, 10) FROM products",
            error_message="APPROX_TOP_K function is not supported in SingleStore",
            exp_type=exp.ApproxTopK,
            run=False,
        )
        self.validate_generation(
            sql="SELECT HLL(user_id) FROM orders",
            expected_sql="SELECT APPROX_COUNT_DISTINCT(user_id) FROM orders",
            exp_type=exp.Hll,
        )
        self.validate_generation(
            sql="SELECT APPROX_DISTINCT(email) FROM users",
            expected_sql="SELECT APPROX_COUNT_DISTINCT(email) FROM users",
            exp_type=exp.ApproxDistinct,
        )
        self.validate_generation(
            sql="SELECT ARRAY_AGG(name) FROM users",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayAgg,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_UNIQUE_AGG(category) FROM products",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayUniqueAgg,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_UNION_AGG(category) FROM products",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayUnionAgg,
            run=False,
        )
        self.validate_generation(sql="SELECT AVG(amount) FROM orders", exp_type=exp.Avg)
        self.validate_generation(sql="SELECT ANY_VALUE(email) FROM users", exp_type=exp.AnyValue)
        self.validate_generation(
            sql="SELECT LAG(amount) OVER (ORDER BY created_at) FROM orders", exp_type=exp.Lag
        )
        self.validate_generation(
            sql="SELECT LEAD(amount) OVER (ORDER BY created_at) FROM orders", exp_type=exp.Lead
        )
        self.validate_generation(sql="SELECT FIRST(id) FROM orders", exp_type=exp.First)
        self.validate_generation(sql="SELECT LAST(id) FROM orders", exp_type=exp.Last)
        self.validate_generation(
            sql="SELECT FIRST_VALUE(amount) OVER (ORDER BY created_at) FROM orders",
            exp_type=exp.FirstValue,
        )
        self.validate_generation(
            sql="SELECT LAST_VALUE(amount) OVER (ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM orders",
            exp_type=exp.LastValue,
        )
        self.validate_generation(
            sql="SELECT NTH_VALUE(amount, 2) OVER (ORDER BY created_at) FROM orders",
            exp_type=exp.NthValue,
        )
        self.validate_generation(sql="SELECT COUNT(*) FROM users", exp_type=exp.Count)
        self.validate_generation(
            sql="SELECT COUNT_IF(age > 18) FROM users",
            expected_sql="SELECT SUM(CASE WHEN age > 18 THEN 1 ELSE 0 END) FROM users",
            exp_type=exp.CountIf,
        )
        self.validate_generation(
            sql="SELECT GROUP_CONCAT(name) FROM users", exp_type=exp.GroupConcat
        )
        self.validate_generation(
            sql="SELECT JSON_OBJECTAGG(id: name) FROM users",
            expected_sql="SELECT JSON_BUILD_OBJECT(id, name) FROM users",
            from_dialect="postgres",
            error_message="JSON_OBJECT_AGG function is not supported in SingleStore",
            exp_type=exp.JSONObjectAgg,
            run=False,
        )
        self.validate_generation(
            sql="SELECT JSONB_OBJECT_AGG(id, name) FROM users",
            expected_sql="SELECT J_S_O_N_B_OBJECT_AGG(id, name) FROM users",
            from_dialect="postgres",
            error_message="JSONB_OBJECT_AGG function is not supported in SingleStore",
            exp_type=exp.JSONBObjectAgg,
            run=False,
        )
        self.validate_generation(
            sql="SELECT BOOL_OR(is_active) FROM users",
            expected_sql="SELECT MAX(ABS(is_active)) FROM users",
            exp_type=exp.LogicalOr,
        )
        self.validate_generation(
            sql="SELECT BOOL_AND(is_active) FROM users",
            expected_sql="SELECT MIN(ABS(is_active)) FROM users",
            exp_type=exp.LogicalAnd,
        )
        self.validate_generation(sql="SELECT MAX(price) FROM products", exp_type=exp.Max)
        self.validate_generation(sql="SELECT MEDIAN(age) FROM users", exp_type=exp.Median)
        self.validate_generation(sql="SELECT MIN(age) FROM users", exp_type=exp.Min)
        self.validate_generation(
            sql="SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) FROM orders",
            exp_type=exp.PercentileCont,
        )
        self.validate_generation(
            sql="SELECT PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY age) FROM users",
            exp_type=exp.PercentileDisc,
        )
        self.validate_generation(
            sql="SELECT QUANTILE(amount, 0.9) FROM orders",
            expected_sql="SELECT APPROX_PERCENTILE(amount, 0.9) FROM orders",
            error_message="QUANTILE function is not supported in SingleStore",
            exp_type=exp.Quantile,
        )
        self.validate_generation(
            sql="SELECT APPROX_QUANTILE(amount, 0.9) FROM orders",
            expected_sql="SELECT APPROX_PERCENTILE(amount, 0.9) FROM orders",
            exp_type=exp.ApproxQuantile,
        )
        self.validate_generation(sql="SELECT SUM(amount) FROM orders", exp_type=exp.Sum)
        self.validate_generation(sql="SELECT STDDEV(amount) FROM orders", exp_type=exp.Stddev)
        self.validate_generation(
            sql="SELECT STDDEV_POP(amount) FROM orders", exp_type=exp.StddevPop
        )
        self.validate_generation(
            sql="SELECT STDDEV_SAMP(amount) FROM orders", exp_type=exp.StddevSamp
        )
        self.validate_generation(
            sql="SELECT VARIANCE(amount) FROM orders",
            expected_sql="SELECT VAR_SAMP(amount) FROM orders",
            exp_type=exp.Variance,
        )
        self.validate_generation(
            sql="SELECT VAR_POP(amount) FROM orders",
            expected_sql="SELECT VAR_POP(amount) FROM orders",
            exp_type=exp.VariancePop,
        )
        self.validate_generation(
            sql="SELECT CORR(user_id, amount) FROM orders",
            error_message="CORR function is not supported in SingleStore",
            exp_type=exp.Corr,
            run=False,
        )
        self.validate_generation(
            sql="SELECT COVAR_SAMP(user_id, amount) FROM orders",
            error_message="COVAR_SAMP function is not supported in SingleStore",
            exp_type=exp.CovarSamp,
            run=False,
        )
        self.validate_generation(
            sql="SELECT COVAR_POP(user_id, amount) FROM orders",
            error_message="COVAR_POP function is not supported in SingleStore",
            exp_type=exp.CovarPop,
            run=False,
        )

    def test_functions_generation(self):
        self.validate_generation(sql="SELECT ABS(age) FROM users", exp_type=exp.Abs)
        self.validate_generation(
            sql="SELECT FLATTEN(ARRAY(ARRAY(1, 2), ARRAY(3, 4)))",
            error_message="Arrays are not supported in SingleStore",
            from_dialect="spark",
            exp_type=exp.Flatten,
            run=False,
        )
        self.validate_generation(
            sql="SELECT TRANSFORM(ARRAY(1, 2, 3), x -> x + 1)",
            from_dialect="spark",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Transform,
            run=False,
        )
        self.validate_generation(sql="SELECT db.is_prime(age) FROM users", exp_type=exp.Anonymous)
        self.validate_generation(
            sql="SELECT id, age APPLY(sum) FROM users",
            error_message="APPLY function is not supported in SingleStore",
            from_dialect="clickhouse",
            exp_type=exp.Apply,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY(1, 2, 3)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Array,
            run=False,
        )
        self.validate_generation(
            sql="SELECT TO_ARRAY(name) FROM users",
            expected_sql="SELECT CASE WHEN name IS NULL THEN NULL ELSE ARRAY(name) END FROM users",
            error_message="Arrays are not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.ToArray,
            run=False,
        )
        self.validate_generation(
            sql="SELECT LIST(id) FROM users",
            error_message="LIST function is not supported in SingleStore",
            exp_type=exp.List,
            run=False,
        )
        self.validate_generation(sql="SELECT RPAD('ohai', 10, '_') AS o", exp_type=exp.Pad)
        self.validate_generation(
            sql="SELECT TO_CHAR(created_at, 'YYYY-MM-DD') FROM orders", exp_type=exp.ToChar
        )
        self.validate_generation(
            sql="SELECT TO_NUMBER(price, '999,999,999.99999') FROM products", exp_type=exp.ToNumber
        )
        self.validate_generation(
            sql="SELECT TO_DOUBLE(amount) FROM orders",
            expected_sql="SELECT amount :> DOUBLE FROM orders",
            exp_type=exp.ToDouble,
        )
        self.validate_generation(
            sql="SELECT COLUMNS('.*_amount') FROM trips LIMIT 10",
            from_dialect="clickhouse",
            error_message="Dynamic column selection is not supported in SingleStore",
            exp_type=exp.Columns,
            run=False,
        )
        self.validate_generation(
            sql="SELECT CONVERT(INT, name) FROM users",
            from_dialect="tsql",
            expected_sql="SELECT name :> INT FROM users",
            exp_type=exp.Convert,
        )
        self.validate_generation(
            sql="SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', created_at) FROM orders",
            expected_sql="SELECT CONVERT_TZ(created_at, 'UTC', 'America/New_York') FROM orders",
            exp_type=exp.ConvertTimezone,
        )
        self.validate_generation(
            sql="SELECT GENERATE_SERIES(1, 5)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.GenerateSeries,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM EXPLODING_GENERATE_SERIES(1, 5)",
            expected_sql="SELECT * FROM GENERATE_SERIES(1, 5)",
            error_message="EXPLODING_GENERATE_SERIES function is not supported in SingleStore",
            exp_type=exp.ExplodingGenerateSeries,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_ALL(ARRAY(1, 2, 3), x -> x > 0)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayAll,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_ANY(ARRAY(0, 1, 0), x -> x = 1)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayAny,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_CONCAT(ARRAY(1, 2), ARRAY(3, 4))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayConcat,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_CONSTRUCT_COMPACT(NULL, 1, NULL, 2)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayConstructCompact,
            run=False,
        )
        self.validate_generation(
            sql="SELECT FILTER(ARRAY(1, 2, 3), x -> x > 1)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayFilter,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_TO_STRING(ARRAY('a', 'b', 'c'), ',')",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayToString,
            run=False,
        )
        self.validate_generation(
            sql="SELECT STRING(created_at, 'UTC') FROM orders",
            expected_sql="SELECT CONVERT_TZ(created_at, 'UTC', 'UTC') :> TEXT FROM orders",
            exp_type=exp.String,
        )
        self.validate_generation(
            sql="SELECT STRING_TO_ARRAY('a,b,c', ',')",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.StringToArray,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_LENGTH(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArraySize,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_SORT(ARRAY(3, 1, 2))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArraySort,
            run=False,
        )
        self.validate_generation(
            sql="SELECT ARRAY_SUM(ARRAY(amount, 2, 3)) FROM orders",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArraySum,
            run=False,
        )
        self.validate_generation(
            sql="SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM users",
            exp_type=exp.Case,
        )
        self.validate_generation(
            sql="SELECT CAST(age AS DOUBLE) FROM users",
            expected_sql="SELECT age :> DOUBLE FROM users",
            exp_type=exp.Cast,
        )
        self.validate_generation(
            sql="SELECT TRY_CAST(age AS INT) FROM users",
            expected_sql="SELECT age !:> INT FROM users",
            exp_type=exp.TryCast,
        )

        self.validate_generation(
            sql="SELECT metadata.:STRING FROM events",
            from_dialect="clickhouse",
            expected_sql="SELECT metadata :> TEXT FROM events",
            exp_type=exp.JSONCast,
        )
        self.validate_generation(
            sql="SELECT TRY(PARSE_JSON('{bad: json}') IS NULL)",
            expected_sql="SELECT PARSE_JSON('{bad: json}') IS NULL",
            error_message="Unsupported TRY function",
            exp_type=exp.Try,
            run=False,
        )
        self.validate_generation(
            sql="SELECT CAST(age, 'TEXT') FROM users",
            expected_sql="SELECT age :> TEXT FROM users",
            exp_type=exp.CastToStrType,
        )
        self.validate_generation(sql="SELECT CEIL(amount) FROM orders", exp_type=exp.Ceil)
        self.validate_generation(
            sql="SELECT COALESCE(email, 'none') FROM users", exp_type=exp.Coalesce
        )
        self.validate_generation(
            sql="SELECT CHR(65)", expected_sql="SELECT CHAR(65)", exp_type=exp.Chr
        )
        self.validate_generation(sql="SELECT CONCAT(name, 'a') FROM users", exp_type=exp.Concat)
        self.validate_generation(
            sql="SELECT CONCAT_WS('-', name, 'a') FROM users", exp_type=exp.ConcatWs
        )
        self.validate_generation(
            sql="SELECT CONTAINS(name, 'book') FROM products",
            expected_sql="SELECT INSTR(name, 'book') FROM products",
            exp_type=exp.Contains,
        )
        self.validate_generation(
            sql="SELECT CONNECT_BY_ROOT DEPTNAME AS ROOT, DEPTNAME FROM DEPARTMENT START WITH DEPTNO IN ('B01', 'C01', 'D01', 'E01') CONNECT BY PRIOR DEPTNO = ADMRDEPT",
            error_message="CONNECT_BY_ROOT function is not supported in SingleStore",
            exp_type=exp.ConnectByRoot,
            run=False,
        )
        self.validate_generation(
            sql="SELECT CBRT(id) FROM orders",
            expected_sql="SELECT POWER(id, 0.3333333333333333) FROM orders",
            exp_type=exp.Cbrt,
        )
        self.validate_generation(
            sql="SELECT CURRENT_DATE",
            expected_sql="SELECT CURRENT_DATE()",
            exp_type=exp.CurrentDate,
        )
        self.validate_generation(
            sql="SELECT CURRENT_DATETIME",
            expected_sql="SELECT CURRENT_TIMESTAMP() :> DATETIME",
            from_dialect="bigquery",
            exp_type=exp.CurrentDatetime,
        )
        self.validate_generation(sql="SELECT CURRENT_TIME()", exp_type=exp.CurrentTime)
        self.validate_generation(sql="SELECT CURRENT_TIMESTAMP()", exp_type=exp.CurrentTimestamp)
        self.validate_generation(
            sql="SELECT CURRENT_SCHEMA",
            expected_sql="SELECT SCHEMA()",
            from_dialect="postgres",
            exp_type=exp.CurrentSchema,
        )
        self.validate_generation(sql="SELECT CURRENT_USER()", exp_type=exp.CurrentUser)
        self.validate_generation(
            sql="SELECT DATE_ADD(created_at, INTERVAL '1' DAY) FROM orders", exp_type=exp.DateAdd
        )
        self.validate_generation(
            sql="SELECT DATE_BIN(INTERVAL 15 MINUTE, created_at, TIMESTAMP '2001-01-01 00:00:00') FROM orders",
            expected_sql="SELECT TIME_BUCKET(INTERVAL '15' MINUTE, created_at, '2001-01-01 00:00:00' :> TIMESTAMP) FROM orders",
            exp_type=exp.DateBin,
        )
        self.validate_generation(
            sql="SELECT DATE_SUB(created_at, INTERVAL '1' DAY) FROM orders", exp_type=exp.DateSub
        )
        self.validate_generation(
            sql="SELECT DATE_DIFF(created_at, NOW(), DAY) FROM orders",
            expected_sql="SELECT TIMESTAMPDIFF(DAY, created_at, NOW()) FROM orders",
            exp_type=exp.DateDiff,
        )
        self.validate_generation(
            sql="SELECT DATE_TRUNC('MONTH', created_at) FROM orders", exp_type=exp.DateTrunc
        )
        self.validate_generation(
            sql="SELECT DATETIME('2024-05-06 13:00:00')",
            expected_sql="SELECT '2024-05-06 13:00:00' :> DATETIME",
            exp_type=exp.Datetime,
        )
        self.validate_generation(
            sql="SELECT DATETIME_ADD(created_at, INTERVAL 2 HOUR) FROM orders",
            expected_sql="SELECT DATE_ADD(created_at, INTERVAL '2' HOUR) FROM orders",
            exp_type=exp.DatetimeAdd,
        )
        self.validate_generation(
            sql="SELECT DATETIME_SUB(created_at, INTERVAL 30 MINUTE) FROM orders",
            expected_sql="SELECT DATE_SUB(created_at, INTERVAL '30' MINUTE) FROM orders",
            exp_type=exp.DatetimeSub,
        )
        self.validate_generation(
            sql="SELECT DATETIME_DIFF(MINUTE, created_at, NOW()) FROM orders",
            expected_sql="SELECT TIMESTAMPDIFF(MINUTE, created_at, NOW()) FROM orders",
            exp_type=exp.DatetimeDiff,
        )
        self.validate_generation(
            sql="SELECT DATETIME_TRUNC(created_at, MINUTE) FROM orders",
            expected_sql="SELECT DATE_TRUNC('MINUTE', created_at) FROM orders",
            exp_type=exp.DatetimeTrunc,
        )
        self.validate_generation(
            sql="SELECT DAY_OF_WEEK(created_at) FROM orders",
            expected_sql="SELECT DAYOFWEEK(created_at) FROM orders",
            exp_type=exp.DayOfWeek,
        )
        self.validate_generation(
            sql="SELECT DAY_OF_WEEK(created_at) FROM orders",
            expected_sql="SELECT ((DAYOFWEEK(created_at) % 7) + 1) FROM orders",
            from_dialect="presto",
            exp_type=exp.DayOfWeekIso,
        )
        self.validate_generation(
            sql="SELECT DAY_OF_MONTH(created_at) FROM orders",
            expected_sql="SELECT DAY(created_at) FROM orders",
            exp_type=exp.DayOfMonth,
        )
        self.validate_generation(
            sql="SELECT DAY_OF_YEAR(created_at) FROM orders",
            expected_sql="SELECT DAYOFYEAR(created_at) FROM orders",
            exp_type=exp.DayOfYear,
        )
        self.validate_generation(
            sql="SELECT WEEK_OF_YEAR(created_at) FROM orders",
            expected_sql="SELECT WEEKOFYEAR(created_at) FROM orders",
            exp_type=exp.WeekOfYear,
        )
        self.validate_generation(
            sql="SELECT MONTHS_BETWEEN(NOW(), created_at) FROM orders", exp_type=exp.MonthsBetween
        )
        self.validate_generation(
            sql="SELECT MAKE_INTERVAL(1, 2, 3, 4)",
            error_message="INTERVAL data type is not supported in SingleStore",
            exp_type=exp.MakeInterval,
            run=False,
        )
        self.validate_generation(
            sql="SELECT LAST_DAY(created_at) FROM orders", exp_type=exp.LastDay
        )
        self.validate_generation(
            sql="SELECT EXTRACT(DAY FROM created_at) FROM orders", exp_type=exp.Extract
        )
        self.validate_generation(
            sql="SELECT TIMESTAMP('2024-01-01 12:00:00')", exp_type=exp.Timestamp
        )
        self.validate_generation(
            sql="SELECT TIMESTAMP_ADD(created_at, INTERVAL 2 DAY) FROM orders",
            expected_sql="SELECT DATE_ADD(created_at, INTERVAL '2' DAY) FROM orders",
            exp_type=exp.TimestampAdd,
        )
        self.validate_generation(
            sql="SELECT TIMESTAMP_SUB(created_at, INTERVAL 1 HOUR) FROM orders",
            expected_sql="SELECT DATE_SUB(created_at, INTERVAL '1' HOUR) FROM orders",
            exp_type=exp.TimestampSub,
        )
        self.validate_generation(
            sql="SELECT TIMESTAMP_DIFF(NOW(), created_at, DAY) FROM orders",
            expected_sql="SELECT TIMESTAMPDIFF(DAY, created_at, NOW()) FROM orders",
            from_dialect="bigquery",
            exp_type=exp.TimestampDiff,
        )
        self.validate_generation(
            sql="SELECT TIMESTAMP_TRUNC(created_at, HOUR) FROM orders",
            expected_sql="SELECT DATE_TRUNC('HOUR', created_at) FROM orders",
            exp_type=exp.TimestampTrunc,
        )
        self.validate_generation(
            sql="SELECT TIME_ADD(TIME '12:00:00', INTERVAL 1 HOUR)",
            expected_sql="SELECT DATE_ADD('12:00:00' :> TIME, INTERVAL '1' HOUR)",
            exp_type=exp.TimeAdd,
        )
        self.validate_generation(
            sql="SELECT TIME_SUB(TIME '12:00:00', INTERVAL 30 MINUTE)",
            expected_sql="SELECT DATE_SUB('12:00:00' :> TIME, INTERVAL '30' MINUTE)",
            exp_type=exp.TimeSub,
        )
        self.validate_generation(
            sql="SELECT TIME_DIFF(TIME '10:00:00', TIME '11:00:00', MINUTE)",
            expected_sql="SELECT TIMESTAMPDIFF(MINUTE, '10:00:00' :> TIME, '11:00:00' :> TIME)",
            exp_type=exp.TimeDiff,
        )
        self.validate_generation(
            sql="SELECT TIME_TRUNC(TIME '12:34:56', MINUTE)",
            expected_sql="SELECT DATE_TRUNC('MINUTE', '12:34:56' :> TIME)",
            exp_type=exp.TimeTrunc,
        )
        self.validate_generation(
            sql="SELECT DATE_FROM_PARTS(2024, 5, 6)",
            error_message="DATE_FROM_PARTS function is not supported in SingleStore",
            exp_type=exp.DateFromParts,
            run=False,
        )
        self.validate_generation(
            sql="SELECT TIME_FROM_PARTS(10, 30, 0)",
            error_message="TIME_FROM_PARTS function is not supported in SingleStore",
            exp_type=exp.TimeFromParts,
            run=False,
        )
        self.validate_generation(
            sql="SELECT DATE_STR_TO_DATE('2024-05-06')",
            expected_sql="SELECT '2024-05-06' :> DATE",
            exp_type=exp.DateStrToDate,
        )
        self.validate_generation(
            sql="SELECT DATE_TO_DI(created_at) FROM orders",
            expected_sql="SELECT (DATE_FORMAT(created_at, '%Y%m%d') :> INT) FROM orders",
            exp_type=exp.DateToDi,
        )
        self.validate_generation(
            sql="SELECT DATE('2024-05-06')",
            expected_sql="SELECT '2024-05-06' :> DATE",
            from_dialect="bigquery",
            exp_type=exp.Date,
        )
        self.validate_generation(
            sql="SELECT DAY(created_at) FROM orders",
            expected_sql="SELECT DAY(created_at :> DATE) FROM orders",
            exp_type=exp.Day,
        )
        self.validate_generation(
            sql="SELECT DECODE('aGVsbG8=', 'base64')",
            error_message="DECODE function is not supported in SingleStore",
            exp_type=exp.Decode,
            run=False,
        )
        self.validate_generation(
            sql="SELECT DI_TO_DATE(20240506)",
            expected_sql="SELECT STR_TO_DATE(20240506, '%Y%m%d')",
            exp_type=exp.DiToDate,
        )
        self.validate_generation(
            sql="SELECT ENCODE('hello', 'base64')",
            error_message="ENCODE function is not supported in SingleStore",
            exp_type=exp.Encode,
            run=False,
        )
        self.validate_generation(sql="SELECT EXP(price) FROM products", exp_type=exp.Exp)
        self.validate_generation(
            sql="SELECT * FROM EXPLODE(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Explode,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM EXPLODE_OUTER(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ExplodeOuter,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM POSEXPLODE_OUTER(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.PosexplodeOuter,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM POSEXPLODE(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Posexplode,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM INLINE(ARRAY((1, 'a'), (2, 'b')))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Inline,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM UNNEST(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Unnest,
            run=False,
        )
        self.validate_generation(sql="SELECT FLOOR(amount) FROM orders", exp_type=exp.Floor)
        self.validate_generation(sql="SELECT FROM_BASE64('aGVsbG8=')", exp_type=exp.FromBase64)
        self.validate_generation(
            sql="SELECT FEATURES_AT_TIME(state, '2024-01-01 00:00:00')",
            error_message="FEATURES_AT_TIME function is not supported in SingleStore",
            exp_type=exp.FeaturesAtTime,
            run=False,
        )
        self.validate_generation(sql="SELECT TO_BASE64('hello')", exp_type=exp.ToBase64)
        self.validate_generation(
            sql="SELECT FROM_ISO8601_TIMESTAMP('2024-05-06T12:00:00Z')",
            error_message="FROM_ISO8601_TIMESTAMP function is not supported in SingleStore",
            exp_type=exp.FromISO8601Timestamp,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM GAP_FILL(TABLE device_data, ts_column => 'time', bucket_width => INTERVAL '1' MINUTE, partitioning_columns => ARRAY('device_id'), value_columns => ARRAY(('signal', 'locf'))) ORDER BY device_id",
            error_message="GAP_FILL function is not supported in SingleStore",
            exp_type=exp.GapFill,
            run=False,
        )
        self.validate_generation(
            sql="SELECT GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-10', INTERVAL 1 DAY)",
            expected_sql="SELECT GENERATE_DATE_ARRAY('2024-01-01' :> DATE, '2024-01-10' :> DATE, INTERVAL '1' DAY)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.GenerateDateArray,
            run=False,
        )
        self.validate_generation(
            sql="SELECT GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 00:00:00', INTERVAL 1 HOUR)",
            expected_sql="SELECT GENERATE_TIMESTAMP_ARRAY('2024-01-01 00:00:00' :> TIMESTAMP, '2024-01-02 00:00:00' :> TIMESTAMP, INTERVAL '1' HOUR)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.GenerateTimestampArray,
            run=False,
        )
        self.validate_generation(sql="SELECT GREATEST(age, 30) FROM users", exp_type=exp.Greatest)
        self.validate_generation(sql="SELECT HEX(255)", exp_type=exp.Hex)
        self.validate_generation(
            sql="SELECT LOWER_HEX(255)",
            expected_sql="SELECT LOWER(HEX(255))",
            exp_type=exp.LowerHex,
        )
        self.validate_generation(
            sql="SELECT IF(age > 18, 'adult', 'minor') FROM users",
            expected_sql="SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users",
            exp_type=exp.If,
        )
        self.validate_generation(sql="SELECT NULLIF(age, 0) FROM users", exp_type=exp.Nullif)
        self.validate_generation(sql="SELECT INITCAP(name) FROM users", exp_type=exp.Initcap)
        self.validate_generation(
            sql="SELECT IS_ASCII(name) FROM users",
            expected_sql="SELECT (name RLIKE '^[\x00-\x7f]*$') FROM users",
            exp_type=exp.IsAscii,
        )
        self.validate_generation(
            sql="SELECT IS_NAN(0.0 / 0.0)",
            error_message="IS_NAN function is not supported in SingleStore",
            exp_type=exp.IsNan,
            run=False,
        )
        self.validate_generation(
            sql="SELECT INT64(age) FROM users",
            expected_sql="SELECT age :> BIGINT FROM users",
            exp_type=exp.Int64,
        )
        self.validate_generation(
            sql="SELECT IS_INF(1.0 / 0.0)",
            error_message="IS_INF function is not supported in SingleStore",
            exp_type=exp.IsInf,
            run=False,
        )
        self.validate_generation(
            sql="SELECT JSON_OBJECT('name', name) FROM users",
            expected_sql="SELECT JSON_BUILD_OBJECT('name', name) FROM users",
            exp_type=exp.JSONObject,
        )
        self.validate_generation(
            sql="SELECT JSON_ARRAY(id, age) FROM users",
            from_dialect="oracle",
            expected_sql="SELECT JSON_BUILD_ARRAY(id, age) FROM users",
            exp_type=exp.JSONArray,
        )
        self.validate_generation(
            sql="SELECT JSON_ARRAYAGG(name ORDER BY id ASC, name DESC) FROM users",
            from_dialect="oracle",
            expected_sql="SELECT JSON_AGG(name ORDER BY id ASC NULLS LAST, name DESC NULLS FIRST) FROM users",
            exp_type=exp.JSONArrayAgg,
        )
        self.validate_generation(
            sql="SELECT JSON_EXISTS('{\"a\":1}', '$.a')",
            expected_sql="SELECT JSON_MATCH_ANY_EXISTS('{\"a\":1}', 'a')",
            from_dialect="oracle",
            exp_type=exp.JSONExists,
        )
        self.validate_generation(
            sql="SELECT JSON_VALUE_ARRAY('[1,2,3]', '$')",
            expected_sql="SELECT J_S_O_N_VALUE_ARRAY('[1,2,3]', )",
            from_dialect="bigquery",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.JSONValueArray,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM JSON_TABLE('[{\"id\":1}]', '$[*]' COLUMNS(id INT PATH '$.id'))",
            expected_sql="SELECT * FROM J_S_O_N_TABLE('[{\"id\":1}]', COLUMNS(id INT PATH '$.id'), '$[*]')",
            error_message="JSON_TABLE function is not supported in SingleStore",
            exp_type=exp.JSONTable,
            run=False,
        )
        self.validate_generation(
            sql="SELECT OBJECT_INSERT('{\"a\":1}', 'b', 2)",
            expected_sql="SELECT JSON_SET_JSON('{\"a\":1}', 'b', 2)",
            exp_type=exp.ObjectInsert,
        )
        self.validate_generation(
            sql='SELECT * FROM OPENJSON(\'{"a":1,"b":2}\')',
            error_message="OPENJSON function is not supported in SingleStore",
            exp_type=exp.OpenJSON,
            run=False,
        )
        self.validate_generation(
            sql="SELECT JSONB_EXISTS('{\"x\":true}', 'x')",
            expected_sql="SELECT BSON_MATCH_ANY_EXISTS('{\"x\":true}', 'x')",
            exp_type=exp.JSONBExists,
        )
        self.validate_generation(
            sql="SELECT JSON_EXTRACT_ARRAY(json_col, '$.items') FROM users",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.JSONExtractArray,
            run=False,
        )
        self.validate_generation(
            sql="SELECT JSON_FORMAT('{\"a\":1}')",
            expected_sql="SELECT JSON_PRETTY('{\"a\":1}')",
            exp_type=exp.JSONFormat,
        )
        self.validate_generation(
            sql="SELECT PARSE_JSON('{\"foo\": 123}')",
            error_message="PARSE_JSON function is not supported in SingleStore",
            exp_type=exp.ParseJSON,
            run=False,
        )
        self.validate_generation(sql="SELECT LEAST(age, 30) FROM users", exp_type=exp.Least)
        self.validate_generation(sql="SELECT LEFT(name, 2) FROM users", exp_type=exp.Left)
        self.validate_generation(sql="SELECT RIGHT(name, 2) FROM users", exp_type=exp.Right)
        self.validate_generation(sql="SELECT LENGTH(name) FROM users", exp_type=exp.Length)
        self.validate_generation(
            sql="SELECT LEVENSHTEIN('kitten', 'sitting')",
            error_message="LEVENSHTEIN function is not supported in SingleStore",
            exp_type=exp.Levenshtein,
            run=False,
        )
        self.validate_generation(sql="SELECT LN(price) FROM products", exp_type=exp.Ln)
        self.validate_generation(sql="SELECT LOG(10, price) FROM products", exp_type=exp.Log)
        self.validate_generation(sql="SELECT LOWER(name) FROM users", exp_type=exp.Lower)
        self.validate_generation(
            sql="SELECT MAP(ARRAY('k1', 'k2'), ARRAY('v1', 'v2'))",
            error_message="Maps are not supported in SingleStore",
            exp_type=exp.Map,
            run=False,
        )
        self.validate_generation(
            sql="SELECT TO_MAP(ARRAY('k1', 'k2'))",
            expected_sql="SELECT MAP ARRAY('k1', 'k2')",
            error_message="Maps are not supported in SingleStore",
            exp_type=exp.ToMap,
            run=False,
        )
        self.validate_generation(
            sql="SELECT MAP_FROM_ENTRIES(ARRAY(('k1', 'v1'), ('k2', 'v2')))",
            error_message="Maps are not supported in SingleStore",
            exp_type=exp.MapFromEntries,
            run=False,
        )
        self.validate_generation(
            sql="SELECT STAR_MAP(ARRAY('a', 'b'))",
            error_message="Maps are not supported in SingleStore",
            exp_type=exp.StarMap,
            run=False,
        )
        self.validate_generation(
            sql="SELECT VAR_MAP(ARRAY('k1', 'k2'), ARRAY('v1', 'v2'))",
            expected_sql="SELECT MAP(ARRAY(ARRAY('k1', 'k2')), ARRAY(ARRAY('v1', 'v2')))",
            error_message="Maps are not supported in SingleStore",
            exp_type=exp.VarMap,
            run=False,
        )
        self.validate_generation(
            sql="SELECT MATCH(name, name) AGAINST('book') FROM products",
            exp_type=exp.MatchAgainst,
        )
        self.validate_generation(sql="SELECT MD5(email) FROM users", exp_type=exp.MD5)
        self.validate_generation(
            sql="SELECT MD5_DIGEST(email) FROM users",
            expected_sql="SELECT UNHEX(MD5(email)) FROM users",
            exp_type=exp.MD5Digest,
        )
        self.validate_generation(
            sql="SELECT MONTH(created_at) FROM orders",
            expected_sql="SELECT MONTH(created_at :> DATE) FROM orders",
            exp_type=exp.Month,
        )
        self.validate_generation(
            sql="SELECT ADD_MONTHS(created_at, 1) FROM orders",
            expected_sql="SELECT TIMESTAMPADD(MONTH, 1, created_at) FROM orders",
            exp_type=exp.AddMonths,
        )
        self.validate_generation(
            sql="SELECT NVL2(email, 'known', 'unknown') FROM users",
            expected_sql="SELECT CASE WHEN NOT email IS NULL THEN 'known' ELSE 'unknown' END FROM users",
            exp_type=exp.Nvl2,
        )
        self.validate_generation(
            sql="SELECT NORMALIZE(name) FROM users",
            error_message="NORMALIZE function is not supported in SingleStore",
            exp_type=exp.Normalize,
            run=False,
        )
        self.validate_generation(
            sql="SELECT OVERLAY('abcdef' PLACING '123' FROM 2)",
            error_message="OVERLAY function is not supported in SingleStore",
            exp_type=exp.Overlay,
            run=False,
        )
        self.validate_generation(
            sql="SELECT PREDICT(MODEL model_col, TABLE input_col) FROM users",
            error_message="PREDICT function is not supported in SingleStore",
            exp_type=exp.Predict,
            run=False,
        )
        self.validate_generation(sql="SELECT QUARTER(created_at) FROM orders", exp_type=exp.Quarter)
        self.validate_generation(sql="SELECT RAND()", exp_type=exp.Rand)
        self.validate_generation(
            sql="SELECT RANDN()",
            error_message="RANDN function is not supported in SingleStore",
            exp_type=exp.Randn,
            run=False,
        )
        self.validate_generation(
            sql="SELECT RANGE_N(1, 10, 2)",
            error_message="RANGE_N function is not supported in SingleStore",
            exp_type=exp.RangeN,
            run=False,
        )
        self.validate_generation(
            sql="SELECT READ_CSV('path/to/file.csv')",
            error_message="READ_CSV function is not supported in SingleStore",
            exp_type=exp.ReadCSV,
            run=False,
        )
        self.validate_generation(
            sql="SELECT REDUCE(0, JSON_TO_ARRAY('[1,2,3,4]'), REDUCE_ACC() + REDUCE_VALUE()) AS Result",
            exp_type=exp.Reduce,
        )
        self.validate_generation(
            sql="SELECT REGEXP_EXTRACT(name, '[a-z]+') FROM users",
            expected_sql="SELECT REGEXP_SUBSTR(name, '[a-z]+') FROM users",
            exp_type=exp.RegexpExtract,
        )
        self.validate_generation(
            sql="SELECT REGEXP_EXTRACT_ALL(name, '[a-z]+') FROM users",
            expected_sql="SELECT REGEXP_MATCH(name, '[a-z]+') FROM users",
            exp_type=exp.RegexpExtractAll,
        )
        self.validate_generation(
            sql="SELECT REGEXP_REPLACE(name, '[aeiou]', '*') FROM users", exp_type=exp.RegexpReplace
        )
        self.validate_generation(
            sql="SELECT REGEXP_SPLIT(name, ' ') FROM users",
            expected_sql="SELECT SPLIT(name, ' ') FROM users",
            error_message="REGEXP_SPLIT function is not supported in SingleStore",
            exp_type=exp.RegexpSplit,
            run=False,
        )
        self.validate_generation(
            sql="SELECT REPEAT(name, 2) FROM users",
            expected_sql="SELECT LPAD('', LENGTH(name) * 2, name) FROM users",
            exp_type=exp.Repeat,
        )
        self.validate_generation(sql="SELECT ROUND(price, 1) FROM products", exp_type=exp.Round)
        self.validate_generation(
            sql="SELECT ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) FROM orders",
            exp_type=exp.RowNumber,
        )
        self.validate_generation(
            sql="SELECT SAFE_DIVIDE(amount, 0) FROM orders",
            expected_sql="SELECT CASE WHEN 0 <> 0 THEN amount / 0 ELSE NULL END FROM orders",
            exp_type=exp.SafeDivide,
        )
        self.validate_generation(sql="SELECT SHA(email) FROM users", exp_type=exp.SHA)
        self.validate_generation(sql="SELECT SHA2(email, 256) FROM users", exp_type=exp.SHA2)
        self.validate_generation(sql="SELECT SIGN(age - 30) FROM users", exp_type=exp.Sign)
        self.validate_generation(
            sql="SELECT SORT_ARRAY(ARRAY(3, 1, 2))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.SortArray,
            run=False,
        )
        self.validate_generation(sql="SPLIT(name, ' ')", exp_type=exp.Split, run=False)
        self.validate_generation(
            sql="SELECT SPLIT_PART(email, '@', 1) FROM users",
            error_message="SPLIT_PART function is not supported in SingleStore",
            exp_type=exp.SplitPart,
            run=False,
        )
        self.validate_generation(
            sql="SELECT SUBSTRING(name, 2, 3) FROM users", exp_type=exp.Substring
        )
        self.validate_generation(
            sql="SELECT STANDARD_HASH(email) FROM users",
            expected_sql="SELECT SHA(email) FROM users",
            exp_type=exp.StandardHash,
        )
        self.validate_generation(
            sql="SELECT STANDARD_HASH(email, 'MD5') FROM users",
            expected_sql="SELECT MD5(email) FROM users",
            exp_type=exp.StandardHash,
        )
        self.validate_generation(
            sql="SELECT STANDARD_HASH(email, 'SHA512') FROM users",
            expected_sql="SELECT SHA(email) FROM users",
            error_message="SHA512 hash method is not supported in SingleStore",
            exp_type=exp.StandardHash,
            run=False,
        )
        self.validate_generation(
            sql="SELECT STARTS_WITH(name, 'A') FROM users",
            expected_sql="SELECT REGEXP_INSTR(name, CONCAT('^', 'A')) FROM users",
            exp_type=exp.StartsWith,
        )
        self.validate_generation(
            sql="SELECT STRPOS(name, 'a') FROM users",
            expected_sql="SELECT LOCATE('a', name) FROM users",
            exp_type=exp.StrPosition,
        )
        self.validate_generation(
            sql="SELECT STR_TO_DATE('2024-01-01', '%Y-%m-%d')", exp_type=exp.StrToDate
        )
        self.validate_generation(
            sql="SELECT STR_TO_TIME('12:00:00', '%H:%i:%s')",
            expected_sql="SELECT STR_TO_DATE('12:00:00', '%H:%i:%s')",
            exp_type=exp.StrToTime,
        )
        self.validate_generation(
            sql="SELECT STR_TO_UNIX('2024-01-01 00:00:00')",
            expected_sql="SELECT UNIX_TIMESTAMP('2024-01-01 00:00:00')",
            exp_type=exp.StrToUnix,
        )
        self.validate_generation(
            sql="SELECT STR_TO_MAP('k1=v1,k2=v2', ',', '=')",
            error_message="Maps are not supported in SingleStore",
            exp_type=exp.StrToMap,
            run=False,
        )
        self.validate_generation(
            sql="SELECT NUMBER_TO_STR(123.456, 2)",
            expected_sql="SELECT FORMAT(123.456, 2)",
            exp_type=exp.NumberToStr,
        )
        self.validate_generation(
            sql="SELECT FROM_BASE('1010', 2)",
            expected_sql="SELECT CONV('1010', 2, 10)",
            exp_type=exp.FromBase,
        )
        self.validate_generation(
            sql="SELECT STRUCT(id, name) FROM users",
            error_message="Structs are not supported in SingleStore",
            exp_type=exp.Struct,
            run=False,
        )
        self.validate_generation(
            sql="SELECT STRUCT_EXTRACT(my_struct, 'name')",
            error_message="Structs are not supported in SingleStore",
            exp_type=exp.StructExtract,
            run=False,
        )
        self.validate_generation(
            sql="SELECT STUFF('abcdef', 2, 3, 'xyz')",
            expected_sql="SELECT CONCAT(SUBSTRING('abcdef', 1, 2-1), 'xyz', SUBSTRING('abcdef', 2+3))",
            exp_type=exp.Stuff,
        )
        self.validate_generation(sql="SELECT SQRT(amount) FROM orders", exp_type=exp.Sqrt)
        self.validate_generation(
            sql="SELECT TIME('12:00:00')",
            expected_sql="SELECT '12:00:00' :> TIME",
            exp_type=exp.Time,
        )
        self.validate_generation(
            sql="SELECT TIME_TO_STR(NOW(), '%H:%i:%s')",
            expected_sql="SELECT DATE_FORMAT(NOW(), '%H:%i:%s')",
            exp_type=exp.TimeToStr,
        )
        self.validate_generation(
            sql="SELECT TIME_TO_UNIX(TIME '12:00:00')",
            expected_sql="SELECT UNIX_TIMESTAMP('12:00:00' :> TIME)",
            exp_type=exp.TimeToUnix,
        )
        self.validate_generation(
            sql="SELECT TIME_STR_TO_DATE('2020-01-01 12:13:14')",
            expected_sql="SELECT '2020-01-01 12:13:14' :> DATE",
            exp_type=exp.TimeStrToDate,
        )
        self.validate_generation(
            sql="SELECT TIME_STR_TO_TIME('2020-01-01 12:13:14')",
            expected_sql="SELECT '2020-01-01 12:13:14' :> TIMESTAMP(6)",
            exp_type=exp.TimeStrToTime,
        )
        self.validate_generation(
            sql="SELECT TIME_STR_TO_UNIX('2020-01-01 12:13:14')",
            expected_sql="SELECT UNIX_TIMESTAMP('2020-01-01 12:13:14')",
            exp_type=exp.TimeStrToUnix,
        )
        self.validate_generation(sql="SELECT TRIM(name) FROM users", exp_type=exp.Trim)
        self.validate_generation(
            sql="SELECT TS_OR_DS_ADD(created_at, INTERVAL 1 DAY) FROM orders",
            expected_sql="SELECT DATE_ADD(created_at, INTERVAL '1' DAY) FROM orders",
            exp_type=exp.TsOrDsAdd,
        )
        self.validate_generation(
            sql="SELECT TS_OR_DS_DIFF(DAY, created_at, NOW()) FROM orders",
            expected_sql="SELECT TIMESTAMPDIFF(DAY, created_at, NOW()) FROM orders",
            exp_type=exp.TsOrDsDiff,
        )
        self.validate_generation(
            sql="SELECT TS_OR_DS_TO_DATE(created_at) FROM orders",
            expected_sql="SELECT created_at :> DATE FROM orders",
            exp_type=exp.TsOrDsToDate,
        )
        self.validate_generation(
            sql="SELECT TS_OR_DS_TO_DATETIME(created_at) FROM orders",
            expected_sql="SELECT created_at :> DATETIME FROM orders",
            exp_type=exp.TsOrDsToDatetime,
        )
        self.validate_generation(
            sql="SELECT TS_OR_DS_TO_TIME(created_at) FROM orders",
            expected_sql="SELECT created_at :> TIME FROM orders",
            exp_type=exp.TsOrDsToTime,
        )
        self.validate_generation(
            sql="SELECT TS_OR_DS_TO_TIMESTAMP(created_at) FROM orders",
            expected_sql="SELECT created_at :> TIMESTAMP FROM orders",
            exp_type=exp.TsOrDsToTimestamp,
        )
        self.validate_generation(
            sql="SELECT TS_OR_DI_TO_DI(created_at) FROM orders",
            expected_sql="SELECT (DATE_FORMAT(created_at, '%Y%m%d') :> INT) FROM orders",
            exp_type=exp.TsOrDiToDi,
        )
        self.validate_generation(sql="SELECT UNHEX('4d2')", exp_type=exp.Unhex)
        self.validate_generation(
            sql="SELECT UNICODE('a')",
            expected_sql="SELECT ASCII('a')",
            exp_type=exp.Unicode,
        )
        self.validate_generation(
            sql="SELECT UNIX_DATE(DATE '2024-01-01')",
            expected_sql="SELECT TIMESTAMPDIFF(DAY, '2024-01-01' :> DATE, '1970-01-01' :> DATE)",
            exp_type=exp.UnixDate,
        )
        self.validate_generation(
            sql="SELECT UNIX_TO_STR(1704067200)",
            expected_sql="SELECT FROM_UNIXTIME(1704067200)",
            exp_type=exp.UnixToStr,
        )
        self.validate_generation(
            sql="SELECT UNIX_TO_TIME(1704067200)",
            expected_sql="SELECT FROM_UNIXTIME(1704067200)",
            exp_type=exp.UnixToTime,
        )
        self.validate_generation(
            sql="SELECT UNIX_TO_TIME_STR(1704067200)",
            expected_sql="SELECT FROM_UNIXTIME(1704067200) :> TEXT",
            exp_type=exp.UnixToTimeStr,
        )
        self.validate_generation(
            sql="SELECT UNIX_SECONDS(created_at) FROM orders",
            expected_sql="SELECT UNIX_TIMESTAMP(created_at) FROM orders",
            exp_type=exp.UnixSeconds,
        )
        self.validate_generation(sql="SELECT UUID()", exp_type=exp.Uuid)
        self.validate_generation(
            sql="SELECT TIMESTAMP_FROM_PARTS(2024, 5, 6, 12, 0, 0)",
            error_message="TIMESTAMP_FROM_PARTS function is not supported in SingleStore",
            exp_type=exp.TimestampFromParts,
            run=False,
        )
        self.validate_generation(sql="SELECT UPPER(name) FROM users", exp_type=exp.Upper)
        self.validate_generation(
            sql="SELECT WEEK(created_at) FROM orders",
            expected_sql="SELECT WEEK(created_at :> DATE) FROM orders",
            exp_type=exp.Week,
        )
        self.validate_generation(
            sql="SELECT XMLELEMENT(NAME foo, 'bar')",
            error_message="XMLELEMENT function is not supported in SingleStore",
            exp_type=exp.XMLElement,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM XMLTABLE('/items/item' PASSING xml_data COLUMNS id INT)",
            error_message="XMLTABLE function is not supported in SingleStore",
            exp_type=exp.XMLTable,
            run=False,
        )
        self.validate_generation(
            sql="SELECT YEAR(created_at) FROM orders",
            expected_sql="SELECT YEAR(created_at :> DATE) FROM orders",
            exp_type=exp.Year,
        )
        self.validate_generation(
            sql="SELECT NEXT VALUE FOR Test.CountBy1 AS FirstUse",
            from_dialect="tsql",
            error_message="NEXT_VALUE_FOR function is not supported in SingleStore",
            exp_type=exp.NextValueFor,
            run=False,
        )

    def test_set_operations_generation(self):
        self.validate_generation(
            sql="SELECT id FROM users UNION SELECT id FROM orders", exp_type=exp.Union
        )
        self.validate_generation(
            sql="SELECT id FROM users EXCEPT SELECT id FROM orders", exp_type=exp.Except
        )
        self.validate_generation(
            sql="SELECT id FROM users INTERSECT SELECT id FROM orders", exp_type=exp.Intersect
        )

    def test_select_generation(self):
        # Basic SELECT
        self.validate_generation(sql="SELECT id, name FROM users", exp_type=exp.Select)
        # SELECT with DISTINCT
        self.validate_generation(sql="SELECT DISTINCT name FROM users", exp_type=exp.Select)
        # SELECT with LIMIT
        self.validate_generation(sql="SELECT id FROM users LIMIT 10", exp_type=exp.Select)
        # SELECT with WHERE
        self.validate_generation(sql="SELECT * FROM users WHERE age > 18", exp_type=exp.Select)
        # SELECT with GROUP BY
        self.validate_generation(
            sql="SELECT name, COUNT(*) FROM users GROUP BY name", exp_type=exp.Select
        )
        # SELECT with HAVING
        self.validate_generation(
            sql="SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1",
            exp_type=exp.Select,
        )
        # SELECT with ORDER BY
        self.validate_generation(sql="SELECT id FROM users ORDER BY name", exp_type=exp.Select)
        # SELECT with LIMIT + OFFSET
        self.validate_generation(sql="SELECT id FROM users LIMIT 10 OFFSET 5", exp_type=exp.Select)
        # SELECT with HINT
        self.validate_generation(
            sql="SELECT /*+ BROADCAST(users) */ id FROM users",
            expected_sql="SELECT id FROM users",
            exp_type=exp.Select,
        )
        # SELECT with OPERATION MODIFIERS
        self.validate_generation(sql="SELECT HIGH_PRIORITY id FROM users", exp_type=exp.Select)
        # SELECT with CTE
        self.validate_generation(
            sql="WITH active_users AS (SELECT * FROM users WHERE is_active = TRUE) SELECT id FROM active_users",
            exp_type=exp.Select,
        )
        # SELECT INTO must become CREATE TABLE AS
        self.validate_generation(
            sql="SELECT * INTO archived_users FROM users",
            expected_sql="CREATE TABLE archived_users AS SELECT * FROM users",
            exp_type=exp.Select,
        )
        # SELECT with kind
        self.validate_generation(
            sql="SELECT AS STRUCT id, name FROM users",
            from_dialect="bigquery",
            expected_sql="SELECT STRUCT(id, name) FROM users",
            error_message="Argument 'kind' is not supported for expression 'Select' when targeting SingleStore.",
            exp_type=exp.Select,
            run=False,
        )

    def test_cache_generation(self):
        self.validate_generation(
            sql="CACHE TABLE users",
            error_message="CACHE query is not supported in SingleStore",
            exp_type=exp.Cache,
            run=False,
        )
        self.validate_generation(
            sql="UNCACHE TABLE users",
            error_message="UNCACHE query is not supported in SingleStore",
            exp_type=exp.Uncache,
            run=False,
        )
        self.validate_generation(
            sql="REFRESH TABLE users",
            from_dialect="spark2",
            error_message="REFRESH query is not supported in SingleStore",
            exp_type=exp.Refresh,
            run=False,
        )

    def test_show_generation(self):
        self.validate_generation(sql="SHOW TABLES", exp_type=exp.Show)
        self.validate_generation(sql="SHOW DATABASES", exp_type=exp.Show)
        self.validate_generation(sql="SHOW COLUMNS FROM users", exp_type=exp.Show)
        self.validate_generation(sql="SHOW INDEX FROM orders", exp_type=exp.Show)
        self.validate_generation(sql="SHOW GRANTS FOR root", exp_type=exp.Show)
        self.validate_generation(sql="SHOW FULL TABLES", exp_type=exp.Show)
        self.validate_generation(sql="SHOW GLOBAL STATUS", exp_type=exp.Show)
        self.validate_generation(sql="SHOW FULL COLUMNS FROM users", exp_type=exp.Show)
        self.validate_generation(sql="SHOW GLOBAL VARIABLES", exp_type=exp.Show)
        self.validate_generation(sql="SHOW TABLES LIKE 'u%'", exp_type=exp.Show)
        self.validate_generation(
            sql="SHOW COLUMNS FROM users WHERE Field = 'id'", exp_type=exp.Show
        )
        self.validate_generation(sql="SHOW COLUMNS FROM users LIKE 'id'", exp_type=exp.Show)
        self.validate_generation(sql="SHOW TABLES FROM db", exp_type=exp.Show)

    def test_expressions_generation(self):
        self.validate_generation(
            sql="TRUNCATE users, events",
            expected_sql="TRUNCATE users; TRUNCATE events",
            exp_type=exp.TruncateTable,
        )
        self.validate_generation(
            sql="CREATE SEQUENCE user_id_seq START WITH 42 INCREMENT BY 5 MINVALUE 1 MAXVALUE 1000 CACHE 10",
            expected_sql="CREATE SEQUENCE user_id_seq",
            error_message="Unsupported property sequenceproperties",
            exp_type=exp.SequenceProperties,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE new_events SHALLOW COPY events",
            expected_sql="CREATE TABLE new_events LIKE events WITH SHALLOW COPY",
            exp_type=exp.Clone,
        )
        self.validate_generation(sql="DESCRIBE users", exp_type=exp.Describe)
        self.validate_generation(
            sql="ATTACH DATABASE memsql_demo", from_dialect="duckdb", exp_type=exp.Attach, run=False
        )
        self.validate_generation(
            sql="DETACH DATABASE memsql_demo", from_dialect="duckdb", exp_type=exp.Detach, run=False
        )
        self.validate_generation(
            sql="SUMMARIZE users",
            from_dialect="duckdb",
            error_message="SUMMARIZE query is not supported in SingleStore",
            exp_type=exp.Summarize,
            run=False,
        )
        self.validate_generation(sql="KILL QUERY 123", exp_type=exp.Kill, run=False)
        self.validate_generation(
            sql="PRAGMA foreign_keys = OFF",
            error_message="PRAGMA query is not supported in SingleStore",
            exp_type=exp.Pragma,
            run=False,
        )
        self.validate_generation(
            sql="DECLARE @myVar INT",
            expected_sql="DECLARE myVar INT",
            from_dialect="tsql",
            exp_type=exp.Declare,
            run=False,
        )
        self.validate_generation(
            sql="DECLARE @myVar INT",
            expected_sql="DECLARE myVar INT",
            from_dialect="tsql",
            exp_type=exp.DeclareItem,
            run=False,
        )
        self.validate_generation(sql="SET @a = 1", exp_type=exp.Set)
        self.validate_generation(sql="SET @x = 42", exp_type=exp.SetItem)
        self.validate_generation(
            sql="SELECT $a$this is a heredoc string$a$",
            expected_sql="SELECT 'this is a heredoc string'",
            from_dialect="postgres",
        )
        self.validate_generation(
            sql="CREATE FUNCTION db.some_func() RETURNS INT AS BEGIN END",
            from_dialect="tsql",
            exp_type=exp.UserDefinedFunction,
            run=False,
        )
        self.validate_generation(
            sql="WITH RECURSIVE emp(id, manager) AS ( SELECT 1, NULL UNION ALL SELECT 2, 1 ) SEARCH DEPTH FIRST BY id SET ord SELECT * FROM emp",
            expected_sql="WITH RECURSIVE emp(id, manager) AS (SELECT 1, NULL UNION ALL SELECT 2, 1) SELECT * FROM emp",
            error_message="RecursiveWithSearch expression is not supported in SingleStore",
            exp_type=exp.RecursiveWithSearch,
        )
        self.validate_generation(
            sql="WITH cte AS (SELECT * FROM users) SELECT * FROM cte", exp_type=exp.With
        )
        self.validate_generation(
            sql="SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) FROM users",
            exp_type=exp.WithinGroup,
        )
        self.validate_generation(
            sql="CREATE TABLE orders ( order_date DATE, PROJECTION total_by_customer ( SELECT order_date ) )",
            expected_sql="CREATE TABLE orders (order_date DATE, )",
            error_message="PROJECTION definition is not supported in SingleStore",
            exp_type=exp.ProjectionDef,
            from_dialect="clickhouse",
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM users AS u(id, name)",
            expected_sql="SELECT * FROM users AS u",
            error_message="Named columns are not supported in table alias.",
            exp_type=exp.TableAlias,
        )
        self.validate_generation(
            sql="ALTER TABLE users ADD COLUMN new_name TEXT AFTER id", exp_type=exp.ColumnPosition
        )
        self.validate_generation(
            sql="CREATE TABLE t (id INT NOT NULL DEFAULT 10)", exp_type=exp.ColumnDef
        )
        self.validate_generation(
            sql="ALTER TABLE users ALTER COLUMN name TYPE TEXT COLLATE 'binary'",
            expected_sql="ALTER TABLE users MODIFY COLUMN name TEXT COLLATE 'binary'",
            exp_type=exp.AlterColumn,
        )
        self.validate_generation(
            sql="ALTER TABLE users ALTER INDEX idx_name VISIBLE",
            error_message="INVISIBLE INDEXES are not supported in SingleStore",
            exp_type=exp.AlterIndex,
            run=False,
        )
        self.validate_generation(
            sql="ALTER TABLE t ALTER DISTSTYLE ALL",
            error_message="ALTER DYSTSTILE is not supported in SingleStore",
            exp_type=exp.AlterDistStyle,
            run=False,
        )
        self.validate_generation(
            sql="ALTER TABLE t ALTER SORTKEY (id)",
            error_message="ALTER SORTKEY is not supported in SingleStore",
            exp_type=exp.AlterSortKey,
            run=False,
        )
        self.validate_generation(
            sql="ALTER TABLE orders RENAME COLUMN created_at TO created_at",
            expected_sql="ALTER TABLE orders CHANGE created_at created_at",
            exp_type=exp.RenameColumn,
        )
        self.validate_generation(sql="ALTER TABLE t RENAME TO t_new", exp_type=exp.AlterRename)
        self.validate_generation(
            sql="ALTER TABLE t SWAP WITH t_backup",
            error_message="ALTER TABLE SWAP is not supported in SingleStore",
            exp_type=exp.SwapTable,
            run=False,
        )
        self.validate_generation(
            sql="COMMENT ON TABLE users IS 'user data'",
            error_message="COMMENT query is not supported in SingleStore",
            exp_type=exp.Comment,
            run=False,
        )
        self.validate_generation(
            sql="x FOR x IN numbers",
            error_message="Comprehension is not supported in SingleStore",
            exp_type=exp.Comprehension,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE tab ( d DateTime, a Int ) TTL d + INTERVAL 1 MONTH DELETE, d + INTERVAL 1 WEEK TO VOLUME 'aaa', d + INTERVAL 2 WEEK TO DISK 'bbb'",
            expected_sql="CREATE TABLE tab (d DATETIME, a INT)",
            error_message="Unsupported property mergetreettl",
            exp_type=exp.MergeTreeTTLAction,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE tab ( d DateTime, a Int ) TTL d + INTERVAL 1 MONTH DELETE, d + INTERVAL 1 WEEK TO VOLUME 'aaa', d + INTERVAL 2 WEEK TO DISK 'bbb'",
            expected_sql="CREATE TABLE tab (d DATETIME, a INT)",
            error_message="Unsupported property mergetreettl",
            exp_type=exp.MergeTreeTTL,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE IndexConstraintOption (a INT, INDEX (a) KEY_BLOCK_SIZE = 10)",
            exp_type=exp.IndexConstraintOption,
        )
        self.validate_generation(
            sql="CREATE TABLE ColumnConstraint (id INT AUTO_INCREMENT PRIMARY KEY)",
            exp_type=exp.ColumnConstraint,
        )
        self.validate_generation(
            sql="ALTER DATABASE db SET SYNC REPLICATION",
            expected_sql="ALTER DATABASE db SET SYNC REPLICATION",
            exp_type=exp.AlterSet,
            from_dialect="singlestore",
        )
        self.validate_generation(
            sql="CREATE TABLE ConstraintTable (a INT, CONSTRAINT id PRIMARY KEY (a))",
            exp_type=exp.Constraint,
        )
        self.validate_generation(
            sql="EXPORT DATA OPTIONS( uri='gs://bucket/folder/*.csv', format='CSV', overwrite=true, header=true, field_delimiter=';') AS SELECT field1, field2 FROM mydataset.table1 ORDER BY field1 LIMIT 10",
            expected_sql="EXPORT DATA  AS SELECT field1, field2 FROM mydataset.table1 ORDER BY field1 LIMIT 10",
            exp_type=exp.Export,
            from_dialect="bigquery",
            run=False,
        )
        self.validate_generation(
            sql="SELECT COUNT(age) FILTER (WHERE age > 18) AS adult_count FROM users",
            expected_sql="SELECT COUNT(CASE WHEN age > 18 THEN age END) AS adult_count FROM users",
            exp_type=exp.Filter,
        )
        self.validate_generation(
            sql="SELECT * FROM t1 CHANGES (INFORMATION => DEFAULT) AT (TIMESTAMP => @ts1)",
            expected_sql="SELECT * FROM t1",
            from_dialect="snowflake",
            error_message="Argument 'changes' is not supported for expression 'Table' when targeting SingleStore.",
            exp_type=exp.Changes,
            run=False,
        )
        self.validate_generation(
            sql="SELECT employee_id, manager_id FROM employees START WITH manager_id IS NULL CONNECT BY NOCYCLE PRIOR employee_id = manager_id",
            error_message="CONNECT BY clause is not supported in SingleStore",
            exp_type=exp.Connect,
            run=False,
        )
        self.validate_generation(
            sql="COPY INTO users FROM 'file.csv' WITH (FORMAT 'CSV')",
            error_message="COPY query is not supported in SingleStore",
            exp_type=exp.CopyParameter,
            run=False,
        )
        self.validate_generation(
            sql="COPY INTO mytable FROM 's3://mybucket/data/files' CREDENTIALS = (AWS_KEY_ID='$AWS_ACCESS_KEY_ID' AWS_SECRET_KEY='$AWS_SECRET_ACCESS_KEY') ENCRYPTION = (MASTER_KEY='eSx...') WITH (FILE_FORMAT = (FORMAT_NAME=my_csv_format))",
            error_message="COPY query is not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.Credentials,
            run=False,
        )
        self.validate_generation(
            sql="SELECT employee_id, manager_id FROM employees START WITH manager_id IS NULL CONNECT BY NOCYCLE PRIOR employee_id = manager_id",
            error_message="CONNECT BY clause is not supported in SingleStore",
            exp_type=exp.Prior,
            run=False,
        )
        self.validate_generation(
            sql="INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM test_table",
            expected_sql="INSERT INTO LOCAL DIRECTORY '/tmp/destination' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM test_table",
            error_message="INSERT OVERWRITE DIRECTORY query is not supported in SingleStore",
            exp_type=exp.Directory,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE orders1 (order_id INT PRIMARY KEY, customer_id INT, product_id INT, order_date DATE, FOREIGN KEY (customer_id) REFERENCES customers (customer_id), FOREIGN KEY (product_id) REFERENCES products (product_id))",
            error_message="Foreign keys are not supported in SingleStore",
            exp_type=exp.ForeignKey,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE ColumnPrefix (a TEXT, PRIMARY KEY (a(3)))",
            error_message="Using column prefix for PK is not supported in SingleStore",
            exp_type=exp.ColumnPrefix,
        )
        self.validate_generation(
            sql="CREATE TABLE PrimaryKey (a INT, PRIMARY KEY (a))", exp_type=exp.PrimaryKey
        )
        self.validate_generation(
            sql="SELECT * INTO users_new FROM users WHERE age >= 19",
            expected_sql="CREATE TABLE users_new AS SELECT * FROM users WHERE age >= 19",
            exp_type=exp.Into,
        )
        self.validate_generation(sql="SELECT * FROM users", exp_type=exp.From)
        self.validate_generation(
            sql="SELECT COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1", exp_type=exp.Having
        )
        self.validate_generation(
            sql="SELECT /*+ NO_INDEX(users) */ * FROM users",
            expected_sql="SELECT * FROM users",
            error_message="Hints are not supported",
            exp_type=exp.Hint,
        )
        self.validate_generation(
            sql="SELECT /*+ MERGE(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key",
            expected_sql="SELECT * FROM t1 INNER JOIN t2 ON t1.key = t2.key",
            error_message="Hints are not supported",
            from_dialect="spark2",
            exp_type=exp.JoinHint,
            run=False,
        )
        self.validate_generation(sql="SELECT id FROM users", exp_type=exp.Identifier)
        self.validate_generation(
            sql="CREATE INDEX test_index ON users (id varchar_pattern_ops)",
            expected_sql="CREATE INDEX test_index ON users(id)",
            error_message="Operator classes are not supported in SingleStore",
            exp_type=exp.Opclass,
        )
        self.validate_generation(sql="CREATE INDEX idx_name ON users(name)", exp_type=exp.Index)
        self.validate_generation(
            sql="CREATE INDEX idx_fillfactor ON users (name WITH varchar_pattern_ops)",
            expected_sql="CREATE INDEX idx_fillfactor ON users(name)",
            error_message="Indexes with operator are not supported in SingleStore",
            exp_type=exp.WithOperator,
        )
        self.validate_generation(
            sql="CREATE INDEX test_index1 ON users USING BTREE (id)",
            expected_sql="CREATE INDEX test_index1 ON users(id) USING BTREE",
            exp_type=exp.IndexParameters,
        )
        self.validate_generation(
            sql="INSERT ALL WHEN dept = 'SALES' THEN INTO sales_employees (empId, name) VALUES (empId, name) WHEN dept = 'HR' THEN INTO hr_employees (empId, name) VALUES (empId, name) ELSE INTO other_employees (empId, name) VALUES (empId, name) SELECT empId, name, dept FROM EMPLOYEE;",
            expected_sql="INSERT ALL WHEN dept = 'SALES' THEN INTO sales_employees (empId, name) VALUES (empId, name) WHEN dept = 'HR' THEN INTO hr_employees (empId, name) VALUES (empId, name) ELSE INTO other_employees (empId, name) VALUES (empId, name) SELECT empId, name, dept FROM EMPLOYEE",
            error_message="Conditional insert is not supported in SingleStore",
            exp_type=exp.ConditionalInsert,
            run=False,
        )
        self.validate_generation(
            sql="INSERT ALL INTO MultitableInserts (empno, ename) VALUES (1001, 'John') INTO MultitableInserts (empno, ename) VALUES (1002, 'Jane') SELECT * FROM dual",
            error_message="Multitable insert is not supported in SingleStore",
            exp_type=exp.MultitableInserts,
            run=False,
        )
        self.validate_generation(
            sql="INSERT INTO users (id, name) VALUES (1, 'Alice') ON DUPLICATE KEY UPDATE name = 'Alice'",
            exp_type=exp.OnConflict,
        )

        self.validate_generation(
            sql="SELECT JSON_VALUE(JSON '{}', 'a' NULL ON ERROR)",
            expected_sql="SELECT JSON_EXTRACT_STRING(PARSE_JSON('{}'), 'a')",
            error_message="Argument 'on_condition' is not supported for expression 'JSONValue' when targeting SingleStore.",
            exp_type=exp.OnCondition,
            run=False,
        )
        self.validate_generation(
            sql="INSERT INTO users (id, name, email, age, signup_date, is_active) VALUES (2, 'Alice', '', 1, NOW(), 1) RETURNING id",
            expected_sql="INSERT INTO users (id, name, email, age, signup_date, is_active) VALUES (2, 'Alice', '', 1, NOW(), 1)",
            error_message="Argument 'returning' is not supported for expression 'Insert' when targeting SingleStore.",
            exp_type=exp.Returning,
        )
        self.validate_generation(
            sql="SELECT _utf8'abc'",
            expected_sql="SELECT 'abc'",
            error_message="Character set introducers are not supported in SingleStore",
            exp_type=exp.Introducer,
        )

        self.validate_generation(
            sql="SELECT N'national string'",
            expected_sql="SELECT 'national string'",
            exp_type=exp.National,
        )
        self.validate_generation(
            sql="LOAD DATA LOCAL INPATH 'data.csv' OVERWRITE INTO TABLE LoadData INPUTFORMAT 'JSON'",
            expected_sql="LOAD DATA LOCAL INFILE 'data.csv' REPLACE INTO TABLE LoadData FORMAT JSON",
            exp_type=exp.LoadData,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM users FETCH FIRST 10 ROWS ONLY",
            expected_sql="SELECT * FROM users LIMIT 10",
            exp_type=exp.Fetch,
        )
        self.validate_generation(
            sql="GRANT SELECT ON sales TO fred",
            from_dialect="redshift",
            exp_type=exp.Grant,
            run=False,
        )

        self.validate_generation(
            sql="SELECT status, COUNT(*) FROM orders GROUP BY status, user_id", exp_type=exp.Group
        )
        self.validate_generation(
            sql="SELECT status, COUNT(*) FROM orders GROUP BY ALL", exp_type=exp.Group
        )
        self.validate_generation(
            sql="SELECT status, COUNT(*) FROM orders GROUP BY",
            error_message="Empty GROUP BY is not supported in SingleStore",
            exp_type=exp.Group,
            run=False,
        )
        self.validate_generation(
            sql="SELECT status, COUNT(*) FROM orders GROUP BY CUBE (amount, status), ROLLUP (user_id, status)",
            expected_sql="SELECT status, COUNT(*) FROM orders GROUP BY CUBE (amount, status)",
            error_message="Multiple grouping sets are not supported in SingleStore",
            exp_type=exp.Group,
        )
        self.validate_generation(
            sql="SELECT amount, status, SUM(id) FROM orders GROUP BY CUBE (amount, status)",
            exp_type=exp.Cube,
        )
        self.validate_generation(
            sql="SELECT user_id, status, SUM(amount) FROM orders GROUP BY ROLLUP (user_id, status)",
            exp_type=exp.Rollup,
        )
        self.validate_generation(
            sql="SELECT user_id, event_type, COUNT(*) FROM events GROUP BY GROUPING SETS ((user_id, event_type), (event_type)), GROUPING SETS ((user_id))",
            expected_sql="SELECT user_id, event_type, COUNT(*) FROM events GROUP BY user_id, event_type",
            error_message="Multiple grouping sets are not supported in SingleStore",
            exp_type=exp.GroupingSets,
        )
        self.validate_generation(
            sql="SELECT user_id, event_type, COUNT(*) FROM events GROUP BY GROUPING SETS ((user_id, event_type))",
            expected_sql="SELECT user_id, event_type, COUNT(*) FROM events GROUP BY user_id, event_type",
            exp_type=exp.GroupingSets,
        )
        self.validate_generation(
            sql="SELECT ARRAY_MAP(x -> x + 1, ARRAY(1, 2, 3))",
            error_message="Lambda functions are not supported in SingleStore",
            exp_type=exp.Lambda,
            run=False,
        )
        self.validate_generation(sql="SELECT * FROM users ORDER BY id LIMIT 5", exp_type=exp.Limit)
        self.validate_generation(
            sql="SELECT TOP 10 * FROM users",
            expected_sql="SELECT * FROM users LIMIT 10",
            from_dialect="tsql",
            exp_type=exp.Limit,
        )
        self.validate_generation(
            sql="SELECT TOP 10 PERCENT * FROM users",
            expected_sql="SELECT * FROM users LIMIT 10",
            from_dialect="tsql",
            error_message="Argument 'limit_options' is not supported for expression 'Limit' when targeting SingleStore.",
            exp_type=exp.LimitOptions,
        )
        self.validate_generation(
            sql="SELECT * FROM orders JOIN users ON orders.user_id = users.id", exp_type=exp.Join
        )
        self.validate_generation(
            sql="SELECT * FROM stock_price_history MATCH_RECOGNIZE (PARTITION BY company ORDER BY price_date NULLS LAST MEASURES MATCH_NUMBER() AS match_number, FIRST(price_date) AS start_date, LAST(price_date) AS end_date, COUNT(*) AS rows_in_sequence, COUNT(row_with_price_decrease.*) AS num_decreases, COUNT(row_with_price_increase.*) AS num_increases ONE ROW PER MATCH AFTER MATCH SKIP TO LAST row_with_price_increase PATTERN (row_before_decrease row_with_price_decrease+ row_with_price_increase+) DEFINE row_with_price_decrease AS price < LAG(price), row_with_price_increase AS price > LAG(price))",
            error_message="MATCH_RECOGNIZE is not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.MatchRecognize,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM stock_price_history MATCH_RECOGNIZE (PARTITION BY company ORDER BY price_date NULLS LAST MEASURES MATCH_NUMBER() AS match_number, FIRST(price_date) AS start_date, LAST(price_date) AS end_date, COUNT(*) AS rows_in_sequence, COUNT(row_with_price_decrease.*) AS num_decreases, COUNT(row_with_price_increase.*) AS num_increases ONE ROW PER MATCH AFTER MATCH SKIP TO LAST row_with_price_increase PATTERN (row_before_decrease row_with_price_decrease+ row_with_price_increase+) DEFINE row_with_price_decrease AS price < LAG(price), row_with_price_increase AS price > LAG(price))",
            error_message="MATCH_RECOGNIZE is not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.MatchRecognizeMeasure,
            run=False,
        )
        self.validate_generation(
            sql="SELECT id, name FROM users FINAL WHERE id > 1",
            expected_sql="SELECT id, name FROM users WHERE id > 1",
            error_message="FINAL clause is not supported in SingleStore",
            from_dialect="clickhouse",
            exp_type=exp.Final,
        )
        self.validate_generation(sql="SELECT * FROM events LIMIT 2 OFFSET 3", exp_type=exp.Offset)
        self.validate_generation(
            sql="SELECT * FROM users ORDER BY signup_date DESC", exp_type=exp.Order
        )
        self.validate_generation(
            sql="SELECT * FROM orders ORDER BY created_at CLUSTER BY user_id",
            expected_sql="SELECT * FROM orders ORDER BY created_at",
            error_message="Argument 'cluster' is not supported for expression 'Select' when targeting SingleStore.",
            exp_type=exp.Cluster,
        )
        self.validate_generation(
            sql="SELECT * FROM events DISTRIBUTE BY user_id",
            expected_sql="SELECT * FROM events",
            error_message="Argument 'distribute' is not supported for expression 'Select' when targeting SingleStore.",
            exp_type=exp.Distribute,
        )
        self.validate_generation(
            sql="SELECT * FROM products SORT BY category",
            expected_sql="SELECT * FROM products",
            error_message="Argument 'sort' is not supported for expression 'Select' when targeting SingleStore.",
            exp_type=exp.Sort,
        )
        self.validate_generation(
            sql="SELECT occurred_at FROM events ORDER BY occurred_at WITH FILL",
            expected_sql="SELECT occurred_at FROM events ORDER BY occurred_at NULLS LAST",
            error_message="WITH FILL clause is not supported in SingleStore",
            from_dialect="clickhouse",
            exp_type=exp.WithFill,
        )
        self.validate_generation(
            sql="SELECT * FROM orders ORDER BY created_at NULLS LAST", exp_type=exp.Ordered
        )
        self.validate_generation(
            sql="GRANT SELECT(id, name) ON users TO `root`", exp_type=exp.GrantPrivilege
        )
        self.validate_generation(
            sql="GRANT SELECT(id, name) ON users TO ROLE r", exp_type=exp.GrantPrincipal
        )
        self.validate_generation(
            sql="CREATE TAG cost_center ALLOWED_VALUES 'a', 'b'",
            expected_sql="CREATE TAG cost_center",
            error_message="Unsupported property allowedvaluesproperty",
            exp_type=exp.AllowedValuesProperty,
            from_dialect="snowflake",
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE orders_2024_01 PARTITION OF orders FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
            expected_sql="CREATE TABLE orders_2024_01",
            from_dialect="postgres",
            exp_type=exp.PartitionBoundSpec,
            error_message="Unsupported property partitionedofproperty",
            run=False,
        )
        self.validate_generation(
            sql="SELECT TRANSFORM(a, b) USING 'cat' AS (x, y)",
            from_dialect="spark2",
            error_message="TRANSFORM clause is not supported in SingleStore",
            exp_type=exp.QueryTransform,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE Properties (a INT) COLLATE=utf8mb4_general_ci",
            exp_type=exp.Properties,
        )
        self.validate_generation(
            sql="SELECT * FROM users QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1",
            error_message="QUALIFY clause is not supported in SingleStore",
            exp_type=exp.Qualify,
            run=False,
        )
        self.validate_generation(
            sql="CREATE EXTERNAL TABLE family (id INT, name STRING) ROW FORMAT SERDE 'com.ly.spark.serde.SerDeExample' STORED AS INPUTFORMAT 'com.ly.spark.example.serde.io.SerDeExampleInputFormat' OUTPUTFORMAT 'com.ly.spark.example.serde.io.SerDeExampleOutputFormat' LOCATION '/tmp/family/'",
            expected_sql="CREATE EXTERNAL TABLE family (id INT, name TEXT)",
            from_dialect="spark2",
            error_message="Unsupported property fileformatproperty",
            exp_type=exp.InputOutputFormat,
            run=False,
        )
        self.validate_generation(
            sql="CREATE OR REPLACE FUNCTION tvf_1(a INT) RETURNS TABLE AS RETURN SELECT * FROM users LIMIT a",
            from_dialect="postgres",
            exp_type=exp.Return,
        )
        self.validate_generation(
            sql="CREATE TABLE Reference (OrderID INT NOT NULL, OrderNumber INT NOT NULL, PersonID INT, FOREIGN KEY (PersonID) REFERENCES Persons (PersonID))",
            error_message="Foreign keys are not supported in SingleStore",
            exp_type=exp.Reference,
            run=False,
        )
        self.validate_generation(
            sql="INSERT INTO users (id, name) VALUES (1001, 'John')", exp_type=exp.Tuple
        )
        self.validate_generation(
            sql="SELECT * FROM users OPTION (RECOMPILE=1)",
            expected_sql="SELECT * FROM users",
            from_dialect="tsql",
            error_message="Unsupported query option.",
            exp_type=exp.QueryOption,
        )
        self.validate_generation(
            sql="SELECT * FROM users WITH (BROADCAST)",
            expected_sql="SELECT * FROM users",
            error_message="Table hints are not supported in SingleStore",
            exp_type=exp.WithTableHint,
        )
        self.validate_generation(
            sql="SELECT * FROM users USE INDEX (PRIMARY)", exp_type=exp.IndexTableHint
        )
        self.validate_generation(
            sql="SELECT * FROM users USE INDEX (PRIMARY) IGNORE INDEX FOR ORDER BY (PRIMARY)",
            expected_sql="SELECT * FROM users USE INDEX (PRIMARY) IGNORE INDEX (PRIMARY)",
            exp_type=exp.IndexTableHint,
            error_message="Argument 'target' is not supported for expression 'IndexTableHint' when targeting SingleStore.",
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM users AT(TIMESTAMP => '2024-03-13 13:56:09.553')",
            expected_sql="SELECT * FROM users",
            error_message="Historical data is not supported in SingleStore",
            exp_type=exp.HistoricalData,
        )
        self.validate_generation(
            sql="PUT 'file.txt' @my_stage",
            error_message="PUT query is not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.Put,
            run=False,
        )
        self.validate_generation(sql="SELECT * FROM users", exp_type=exp.Table)
        self.validate_generation(sql="SET @a = 1", exp_type=exp.Var)
        self.validate_generation(
            sql="SELECT * FROM users FOR VERSION AS OF '2024-01-01'",
            expected_sql="SELECT * FROM users",
            error_message="Argument 'version' is not supported for expression 'Table' when targeting SingleStore.",
            exp_type=exp.Version,
        )
        self.validate_generation(sql="CREATE TABLE SchemaTable (a INT)", exp_type=exp.Schema)
        self.validate_generation(sql="SELECT * FROM users FOR UPDATE", exp_type=exp.Lock)
        self.validate_generation(
            sql="SELECT * FROM users TABLESAMPLE SYSTEM (10)",
            expected_sql="SELECT * FROM users",
            error_message="Argument 'sample' is not supported for expression 'Table' when targeting SingleStore.",
            exp_type=exp.TableSample,
        )
        self.validate_generation(
            sql="SELECT 'MAX ID', Alice, Bob FROM TABLE (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name) PIVOT (MAX(id) FOR name IN ('Alice', 'Bob')) AS p",
            expected_sql="SELECT 'MAX ID', Alice, Bob FROM (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob') AS users PIVOT(MAX(id) FOR name IN ('Alice', 'Bob')) AS p",
            exp_type=exp.Pivot,
            from_dialect="snowflake",
        )
        self.validate_generation(
            sql="SELECT 'MAX ID', Alice, Bob FROM TABLE (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name) PIVOT (MAX(id), MIN(id) FOR name IN ('Alice', 'Bob')) AS p",
            expected_sql="SELECT 'MAX ID', Alice, Bob FROM (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob') AS users PIVOT(MAX(id), MIN(id) FOR name IN ('Alice', 'Bob')) AS p",
            exp_type=exp.Pivot,
            error_message="Multiple aggregations in PIVOT are not supported in SingleStore",
            from_dialect="snowflake",
            run=False,
        )
        self.validate_generation(
            sql="PIVOT cities ON year USING SUM(population) AS total, MAX(population) AS max GROUP BY country",
            exp_type=exp.Pivot,
            error_message="Simplified PIVOT is not supported in SingleStore",
            from_dialect="duckdb",
            run=False,
        )
        self.validate_generation(
            sql="UNPIVOT monthly_sales ON jan, feb, mar, apr, may, jun INTO NAME month VALUE sales",
            exp_type=exp.UnpivotColumns,
            error_message="Argument 'unpivot' is not supported for expression 'Pivot' when targeting SingleStore.",
            from_dialect="duckdb",
            run=False,
        )
        self.validate_generation(
            sql="SELECT SUM(id) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM users",
            exp_type=exp.WindowSpec,
        )
        self.validate_generation(
            sql="SELECT * FROM users PREWHERE age > 30",
            expected_sql="SELECT * FROM users",
            from_dialect="clickhouse",
            exp_type=exp.PreWhere,
        )
        self.validate_generation(sql="SELECT * FROM users WHERE age > 30", exp_type=exp.Where)
        self.validate_generation(sql="SELECT * FROM users", exp_type=exp.Star)
        self.validate_generation(
            sql="CREATE TABLE DataTypeParam (x INT(10))",
            exp_type=exp.DataTypeParam,
        )
        self.validate_generation(
            sql="CREATE TABLE DataType1 (x VARCHAR)",
            expected_sql="CREATE TABLE DataType1 (x TEXT)",
            exp_type=exp.DataType,
        )
        self.validate_generation(
            sql="CREATE TABLE DataType2 (x VARCHAR(1))",
            expected_sql="CREATE TABLE DataType2 (x VARCHAR(1))",
            exp_type=exp.DataType,
        )
        self.validate_generation(
            sql="CREATE TABLE DataType3 (x VECTOR(5))",
            expected_sql="CREATE TABLE DataType3 (x VECTOR(5))",
            exp_type=exp.DataType,
        )
        self.validate_generation(
            sql="CREATE TABLE DataType4 (x ENUM('v1', 'v2'))",
            expected_sql="CREATE TABLE DataType4 (x ENUM('v1', 'v2'))",
            exp_type=exp.DataType,
        )
        self.validate_generation(
            sql="CREATE TABLE DataType5 (x UINT (1))",
            expected_sql="CREATE TABLE DataType5 (x INT(1) UNSIGNED)",
            exp_type=exp.DataType,
        )
        self.validate_generation(
            sql="CREATE TABLE DataType6 (x ARRAY)",
            expected_sql="CREATE TABLE DataType6 (x TEXT)",
            error_message="Data type ARRAY is not supported in SingleStore",
            exp_type=exp.DataType,
        )
        self.validate_generation(
            sql="CREATE FUNCTION PseudoType(x CSTRING)",
            expected_sql="CREATE FUNCTION PseudoType(x TEXT)",
            error_message="Pseudo-Types are not supported in SingleStore",
            from_dialect="postgres",
            exp_type=exp.PseudoType,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE ObjectIdentifier (c oid)",
            expected_sql="CREATE TABLE ObjectIdentifier (c INT)",
            from_dialect="postgres",
            exp_type=exp.ObjectIdentifier,
        )
        self.validate_generation(
            sql="SELECT INTERVAL '1' YEAR TO MONTHS",
            error_message="INTERVAL spans are not supported in SingleStore",
            exp_type=exp.IntervalSpan,
            run=False,
        )
        self.validate_generation(
            sql="CALL proc()",
            exp_type=exp.Command,
        )
        self.validate_generation(
            sql="BEGIN",
            exp_type=exp.Transaction,
        )
        self.validate_generation(
            sql="COMMIT",
            exp_type=exp.Commit,
        )
        self.validate_generation(
            sql="ROLLBACK",
            exp_type=exp.Rollback,
        )
        self.validate_generation(
            sql="ALTER TABLE users ADD COLUMNS (a INT, c DOUBLE)",
            expected_sql="ALTER TABLE users ADD COLUMN (a INT, c DOUBLE)",
            exp_type=exp.Alter,
        )
        self.validate_generation(
            sql="ALTER TABLE users ADD INDEX (id)",
            exp_type=exp.AddConstraint,
        )
        self.validate_generation(
            sql="ATTACH 'sqlite_file.db' AS sqlite_db (TYPE sqlite)",
            expected_sql="ATTACH DATABASE 'sqlite_file.db' AS sqlite_db",
            from_dialect="duckdb",
            exp_type=exp.AttachOption,
            error_message="ATTACH options are not supported in SingleStore",
            run=False,
        )
        self.validate_generation(
            sql="ALTER TABLE orders DROP PARTITION(dt = '2014-05-14', country = 'IN')",
            error_message="ALTER TABLE DROP PARTITION is not supported in SingleStore",
            exp_type=exp.DropPartition,
            from_dialect="athena",
            run=False,
        )
        self.validate_generation(
            sql="ALTER TABLE table2 REPLACE PARTITION '123' FROM table1",
            expected_sql="ALTER TABLE table2 REPLACE PARTITION('123') FROM table1",
            from_dialect="clickhouse",
            exp_type=exp.ReplacePartition,
            error_message="ALTER TABLE REPLACE PARTITION is not supported in SingleStore",
            run=False,
        )
        self.validate_generation(
            sql="SELECT name AS n FROM users",
            exp_type=exp.Alias,
        )
        self.validate_generation(
            sql="SELECT * FROM table_name UNPIVOT (value FOR category IN (`col1` AS 'c1', `col2` AS 'c2')) AS unpivoted",
            expected_sql="SELECT * FROM table_name UNPIVOT(value FOR category IN (`col1` AS c1, `col2` AS c2)) AS unpivoted",
            error_message="Argument 'unpivot' is not supported for expression 'Pivot' when targeting SingleStore.",
            from_dialect="bigquery",
            exp_type=exp.PivotAlias,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter)) ORDER BY empid",
            expected_sql="SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter))_t0 ORDER BY empid",
            error_message="PIVOT ANY [ ORDER BY ... ] is not supported in SingleStore",
            exp_type=exp.PivotAny,
            run=False,
        )
        self.validate_generation(
            sql="SELECT id, item AS (a, b) FROM orders",
            expected_sql="SELECT id, item AS (a, b) FROM orders",
            error_message="Specifying multiple aliases in parrents is not supported in SingleStore",
            from_dialect="postgres",
            exp_type=exp.Aliases,
            run=False,
        )
        self.validate_generation(
            sql="SELECT c_name, orders.o_orderkey AS orderkey, index AS orderkey_index FROM customer_orders_lineitem AS c, c.c_orders AS orders AT index ORDER BY orderkey_index NULLS LAST",
            error_message="Arrays are not supported in SingleStore",
            from_dialect="redshift",
            exp_type=exp.AtIndex,
            run=False,
        )
        self.validate_generation(
            sql="SELECT occurred_at AT TIME ZONE 'UTC' FROM events",
            expected_sql="SELECT occurred_at FROM events",
            error_message="AT TIME ZONE is not supported in SingleStore",
            exp_type=exp.AtTimeZone,
        )
        self.validate_generation(
            sql="SELECT TO_UTC_TIMESTAMP(occurred_at, 'PST') FROM events",
            expected_sql="SELECT CONVERT_TZ(occurred_at :> TIMESTAMP, 'PST', 'UTC') FROM events",
            from_dialect="spark2",
            exp_type=exp.FromTimeZone,
        )
        self.validate_generation(
            sql="SELECT DISTINCT name FROM users",
            exp_type=exp.Distinct,
        )
        self.validate_generation(
            sql="FOR record IN (SELECT word, word_count FROM bigquery-public-data.samples.shakespeare LIMIT 5) DO SELECT record.word, record.word_count",
            expected_sql="FOR record IN (SELECT word, word_count FROM bigquery-public-data.samples.shakespeare LIMIT 5) LOOP SELECT record.word, record.word_count",
            from_dialect="bigquery",
            exp_type=exp.ForIn,
            run=False,
        )
        self.validate_generation(
            sql="SELECT DATE_ADD(created_at, INTERVAL '1' DAY) FROM orders",
            exp_type=exp.TimeUnit,
        )
        self.validate_generation(
            sql="SELECT user_id, created_at, AVG(amount) IGNORE NULLS OVER ( PARTITION BY user_id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING ) AS avg_amount_7_rows, SUM(amount) OVER ( PARTITION BY user_id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING ) AS sum_amount_7_rows FROM orders;",
            expected_sql="SELECT user_id, created_at, AVG(amount) OVER (PARTITION BY user_id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS avg_amount_7_rows, SUM(amount) OVER (PARTITION BY user_id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS sum_amount_7_rows FROM orders",
            error_message="IGNORE NULLS clause is not supported in SingleStore",
            exp_type=exp.IgnoreNulls,
        )
        self.validate_generation(
            sql="SELECT user_id, created_at, AVG(amount) RESPECT NULLS OVER ( PARTITION BY user_id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING ) AS avg_amount_7_rows, SUM(amount) OVER ( PARTITION BY user_id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING ) AS sum_amount_7_rows FROM orders;",
            expected_sql="SELECT user_id, created_at, AVG(amount) OVER (PARTITION BY user_id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS avg_amount_7_rows, SUM(amount) OVER (PARTITION BY user_id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS sum_amount_7_rows FROM orders",
            error_message="RESPECT NULLS clause is not supported in SingleStore",
            exp_type=exp.RespectNulls,
        )
        self.validate_generation(
            sql="SELECT ANY_VALUE(name HAVING MAX id) FROM users",
            expected_sql="SELECT ANY_VALUE(name) FROM users",
            error_message="HAVING NULL clause is not supported in SingleStore",
            exp_type=exp.HavingMax,
        )
        self.validate_generation(
            sql="USE db",
            exp_type=exp.Use,
        )
        self.validate_generation(
            sql="SELECT listagg(name, ',' ON OVERFLOW TRUNCATE '.....' WITH COUNT) FROM users",
            expected_sql="SELECT GROUP_CONCAT(name, ',') FROM users",
            from_dialect="trino",
        )
        self.validate_generation(
            sql="SELECT name IS JSON ARRAY WITH UNIQUE KEYS FROM users",
            exp_type=exp.JSON,
            run=False,
        )
        self.validate_generation(
            sql="SELECT JSON_OBJECT('name': 'Alice', 'age': 30)",
            expected_sql="SELECT JSON_BUILD_OBJECT('name', 'Alice', 'age', 30)",
            exp_type=exp.JSONKeyValue,
        )
        self.validate_generation(
            sql="SELECT JSON_OBJECT('a': 1 FORMAT JSON)",
            expected_sql="SELECT JSON_BUILD_OBJECT('a', 1)",
            error_message="FORMAT JSON clause is not supported in SingleStore",
            exp_type=exp.FormatJson,
        )
        self.validate_generation(
            sql="SELECT * FROM JSON_TABLE('[1,2,[\"a\",\"b\"]]', '$' COLUMNS (outer_value_0 NUMBER PATH '$[0]', outer_value_1 NUMBER PATH '$[1]'));",
            expected_sql="SELECT * FROM J_S_O_N_TABLE('[1,2,[\"a\",\"b\"]]', COLUMNS(outer_value_0 DECIMAL PATH '$[0]', outer_value_1 DECIMAL PATH '$[1]'), '$')",
            from_dialect="oracle",
            error_message="JSON_TABLE function is not supported in SingleStore",
            exp_type=exp.JSONColumnDef,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM JSON_TABLE('[1,2,[\"a\",\"b\"]]', '$' COLUMNS (outer_value_0 NUMBER PATH '$[0]', outer_value_1 NUMBER PATH '$[1]'));",
            expected_sql="SELECT * FROM J_S_O_N_TABLE('[1,2,[\"a\",\"b\"]]', COLUMNS(outer_value_0 DECIMAL PATH '$[0]', outer_value_1 DECIMAL PATH '$[1]'), '$')",
            from_dialect="oracle",
            error_message="JSON_TABLE function is not supported in SingleStore",
            exp_type=exp.JSONSchema,
            run=False,
        )
        self.validate_generation(
            sql="SELECT value FROM OPENJSON(doc) WITH (id INT, name VARCHAR(100))",
            from_dialect="tsql",
            error_message="OPENJSON function is not supported in SingleStore",
            exp_type=exp.OpenJSONColumnDef,
            run=False,
        )
        self.validate_generation(
            sql="SELECT * FROM XMLTABLE(XMLNAMESPACES('uri' AS a), '/items/item' PASSING xml_data COLUMNS id INT)",
            error_message="XMLTABLE function is not supported in SingleStore",
            exp_type=exp.XMLNamespace,
            run=False,
        )
        self.validate_generation(
            sql="SELECT JSON_VALUE(metadata, '$.name') FROM events",
            expected_sql="SELECT JSON_EXTRACT_STRING(metadata, 'name') FROM events",
            exp_type=exp.JSONValue,
        )
        self.validate_generation(
            sql="SELECT JSON_VALUE(metadata, '$.name' RETURNING INT) FROM events",
            expected_sql="SELECT JSON_EXTRACT_STRING(metadata, 'name') :> INT FROM events",
            exp_type=exp.JSONValue,
        )
        self.validate_generation(
            sql="SELECT JSON_QUERY(metadata, 'comment' OMIT QUOTES ON SCALAR STRING)  FROM events",
            expected_sql="SELECT JSON_EXTRACT_JSON(metadata, 'comment') FROM events",
            error_message="Argument 'quote' is not supported for expression 'JSONExtract' when targeting SingleStore.",
            from_dialect="trino",
            exp_type=exp.JSONExtractQuote,
        )
        self.validate_generation(
            sql="SELECT SCOPE_RESOLUTION(INT, 123) FROM dual",
            exp_type=exp.ScopeResolution,
            error_message="SCOPE_RESOLUTION is not supported in SingleStore",
            run=False,
        )
        self.validate_generation(
            sql="MERGE INTO x AS z USING (WITH t(c) AS (SELECT 1) SELECT c FROM t) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b",
            error_message="WHEN MATCHED clause is not supported in SingleStore",
            exp_type=exp.Whens,
            run=False,
        )
        self.validate_generation(
            sql="MERGE INTO x AS z USING (WITH t(c) AS (SELECT 1) SELECT c FROM t) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b",
            error_message="WHEN MATCHED clause is not supported in SingleStore",
            exp_type=exp.When,
            run=False,
        )

    def test_drop_generation(self):
        execute_query("DROP DATABASE IF EXISTS dropDB")
        execute_query("CREATE DATABASE dropDB")
        execute_query("CREATE TABLE dropDB.dropTable(a INT, INDEX a (a))")
        execute_query("CREATE TEMPORARY TABLE dropDB.dropTableTemp(a INT, INDEX a (a))")
        execute_query("CREATE VIEW dropDB.dropView AS SELECT * FROM dropDB.dropTable")
        execute_query("USE dropDB")

        self.validate_generation(
            sql="DROP INDEX a ON dropTable",
            exp_type=exp.Drop,
        )
        self.validate_generation(
            sql="DROP TABLE IF EXISTS dropTable",
            exp_type=exp.Drop,
        )
        self.validate_generation(
            sql="DROP TEMPORARY TABLE IF EXISTS dropTableTemp",
            exp_type=exp.Drop,
        )
        self.validate_generation(
            sql="DROP VIEW IF EXISTS dropView",
            exp_type=exp.Drop,
        )
        self.validate_generation(
            sql="DROP DATABASE IF EXISTS dropDB",
            exp_type=exp.Drop,
        )

    execute_query("USE db")

    def test_column_constraints(self):
        self.validate_generation(
            sql="CREATE TABLE PeriodForSystemTimeConstraint (valid_from DATE PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo))",
            expected_sql="CREATE TABLE PeriodForSystemTimeConstraint (valid_from DATE)",
            from_dialect="tsql",
            error_message="PERIOD FOR SYSTEM TIME column constraint is not supported in SingleStore",
            exp_type=exp.PeriodForSystemTimeConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE CaseSpecificColumnConstraint (name VARCHAR(100) CASESPECIFIC)",
            expected_sql="CREATE TABLE CaseSpecificColumnConstraint (name VARCHAR(100))",
            error_message="CASE SPECIFIC column constraint is not supported in SingleStore",
            exp_type=exp.CaseSpecificColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE CheckColumnConstraint (age INT CHECK (age > 0))",
            expected_sql="CREATE TABLE CheckColumnConstraint (age INT)",
            error_message="CHECK column constraint is not supported in SingleStore",
            exp_type=exp.CheckColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE ClusteredColumnConstraint (id INT PRIMARY KEY CLUSTERED (a, b))",
            expected_sql="CREATE TABLE ClusteredColumnConstraint (id INT PRIMARY KEY)",
            error_message="CLUSTERED column constraint is not supported in SingleStore",
            exp_type=exp.ClusteredColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE CompressColumnConstraint (data VARBINARY(100) COMPRESS LZ4)",
            expected_sql="CREATE TABLE CompressColumnConstraint (data VARBINARY(100))",
            error_message="COMPRESS column constraint is not supported in SingleStore",
            exp_type=exp.CompressColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE DateFormatColumnConstraint (dob DATE FORMAT 'YYYY-MM-DD')",
            expected_sql="CREATE TABLE DateFormatColumnConstraint (dob DATE)",
            error_message="FORMAT column constraint is not supported in SingleStore",
            exp_type=exp.DateFormatColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE EncodeColumnConstraint (data TEXT ENCODE ZSTD)",
            expected_sql="CREATE TABLE EncodeColumnConstraint (data TEXT)",
            error_message="ENCODE column constraint is not supported in SingleStore",
            exp_type=exp.EncodeColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE ExcludeColumnConstraint (ssn CHAR(11) EXCLUDE)",
            expected_sql="CREATE TABLE ExcludeColumnConstraint (ssn CHAR(11))",
            error_message="EXCLUDE column constraint is not supported in SingleStore",
            exp_type=exp.ExcludeColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE EphemeralColumnConstraint (temp_data VARCHAR(100) EPHEMERAL)",
            expected_sql="CREATE TABLE EphemeralColumnConstraint (temp_data VARCHAR(100))",
            error_message="EPHEMERAL column constraint is not supported in SingleStore",
            exp_type=exp.EphemeralColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE GeneratedAsIdentityColumnConstraint (id INT GENERATED ALWAYS AS IDENTITY)",
            expected_sql="CREATE TABLE GeneratedAsIdentityColumnConstraint (id INT)",
            error_message="GENERATED AS column constraint is not supported in SingleStore",
            exp_type=exp.GeneratedAsIdentityColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE GeneratedAsRowColumnConstraint (rownum INT GENERATED ALWAYS AS ROW)",
            expected_sql="CREATE TABLE GeneratedAsRowColumnConstraint (rownum INT)",
            error_message="GENERATED AS column constraint is not supported in SingleStore",
            exp_type=exp.GeneratedAsRowColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE UppercaseColumnConstraint (code VARCHAR(10) UPPERCASE)",
            expected_sql="CREATE TABLE UppercaseColumnConstraint (code VARCHAR(10))",
            error_message="UPPERCASE column constraint is not supported in SingleStore",
            exp_type=exp.UppercaseColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE PathColumnConstraint (data JSON PATH '$.user.id')",
            expected_sql="CREATE TABLE PathColumnConstraint (data JSON)",
            error_message="PATH column constraint is not supported in SingleStore",
            exp_type=exp.PathColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE ProjectionPolicyColumnConstraint (col VARCHAR(100) PROJECTION POLICY p)",
            expected_sql="CREATE TABLE ProjectionPolicyColumnConstraint (col VARCHAR(100))",
            from_dialect="snowflake",
            error_message="PROJECTION POLICY constraint is not supported in SingleStore",
            exp_type=exp.ProjectionPolicyColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE InlineLengthColumnConstraint (data VARCHAR(100) INLINE LENGTH 32)",
            expected_sql="CREATE TABLE InlineLengthColumnConstraint (data VARCHAR(100))",
            error_message="INLINE LENGTH column constraint is not supported in SingleStore",
            exp_type=exp.InlineLengthColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE NonClusteredColumnConstraint (id INT PRIMARY KEY NONCLUSTERED (a, b))",
            expected_sql="CREATE TABLE NonClusteredColumnConstraint (id INT PRIMARY KEY)",
            error_message="NONCLUSTERED column constraint is not supported in SingleStore",
            exp_type=exp.NonClusteredColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE NotForReplicationColumnConstraint (col INT NOT FOR REPLICATION)",
            expected_sql="CREATE TABLE NotForReplicationColumnConstraint (col INT)",
            error_message="NOT FOR REPLICATION column constraint is not supported in SingleStore",
            exp_type=exp.NotForReplicationColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE MaskingPolicyColumnConstraint (ssn VARCHAR(11) MASKING POLICY mask_ssn)",
            expected_sql="CREATE TABLE MaskingPolicyColumnConstraint (ssn VARCHAR(11))",
            error_message="MASKING POLICY column constraint is not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.MaskingPolicyColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE OnUpdateColumnConstraint (updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
            expected_sql="CREATE TABLE OnUpdateColumnConstraint (updated_at TIMESTAMP)",
            error_message="ON UPDATE column constraint is not supported in SingleStore",
            exp_type=exp.OnUpdateColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE TitleColumnConstraint (title VARCHAR(100) TITLE 'Book Title')",
            expected_sql="CREATE TABLE TitleColumnConstraint (title VARCHAR(100))",
            error_message="TITLE column constraint is not supported in SingleStore",
            exp_type=exp.TitleColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE NotNullColumnConstraint (name VARCHAR(100) NOT NULL)",
            expected_sql="CREATE TABLE NotNullColumnConstraint (name VARCHAR(100) NOT NULL)",
            exp_type=exp.NotNullColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE AutoIncrementColumnConstraint (id INT AUTO_INCREMENT, INDEX (id))",
            exp_type=exp.AutoIncrementColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE CharacterSetColumnConstraint (name VARCHAR(100) CHARACTER SET utf8)",
            exp_type=exp.CharacterSetColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE CollateColumnConstraint (name VARCHAR(100) COLLATE utf8_general_ci)",
            exp_type=exp.CollateColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE CommentColumnConstraint (id INT COMMENT 'Primary key')",
            exp_type=exp.CommentColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE DefaultColumnConstraint (status VARCHAR(10) DEFAULT 'active')",
            exp_type=exp.DefaultColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE IndexColumnConstraint (name VARCHAR(100), INDEX a USING BTREE (name) KEY_BLOCK_SIZE = 10)",
            exp_type=exp.IndexColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) PERSISTED NOT NULL)",
            expected_sql="CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) PERSISTED AUTO NOT NULL)",
            exp_type=exp.ComputedColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE PrimaryKeyColumnConstraint (id INT PRIMARY KEY)",
            exp_type=exp.PrimaryKeyColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE UniqueColumnConstraint (email VARCHAR(100) UNIQUE, SHARD INDEX (email))",
            exp_type=exp.UniqueColumnConstraint,
        )
        self.validate_generation(
            sql="CREATE TABLE Tags (id INT WITH TAG (a='1'))",
            expected_sql="CREATE TABLE Tags (id INT)",
            error_message="TAG column constraint is not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.Tags,
        )
        self.validate_generation(
            sql="CREATE TABLE WatermarkColumnConstraint (ts TIMESTAMP WATERMARK FOR ts AS ts)",
            expected_sql="CREATE TABLE WatermarkColumnConstraint (ts TIMESTAMP)",
            error_message="WATERMARK column constraint is not supported in SingleStore",
            exp_type=exp.WatermarkColumnConstraint,
        )

    def test_derived_table_generation(self):
        self.validate_generation(
            sql="SELECT * FROM users OUTER APPLY (SELECT * FROM orders WHERE orders.user_id = users.id) AS o",
            expected_sql="SELECT * FROM users LEFT JOIN LATERAL (SELECT * FROM orders WHERE orders.user_id = users.id) AS o ON TRUE",
            from_dialect="tsql",
            exp_type=exp.Lateral,
        )
        self.validate_generation(
            sql="SELECT * FROM users CROSS APPLY (SELECT * FROM orders WHERE orders.user_id = users.id) AS o",
            expected_sql="SELECT * FROM users INNER JOIN LATERAL (SELECT * FROM orders WHERE orders.user_id = users.id) AS o",
            exp_type=exp.Lateral,
        )
        self.validate_generation(
            sql="SELECT * FROM users, LATERAL (SELECT * FROM orders WHERE orders.user_id = users.id) AS order_sub",
            exp_type=exp.Lateral,
        )
        self.validate_generation(
            sql="SELECT * FROM users LEFT JOIN LATERAL (SELECT * FROM orders WHERE orders.user_id = users.id) AS o ON o.user_id = users.id",
            exp_type=exp.Lateral,
        )
        self.validate_generation(
            sql="SELECT * FROM users, LATERAL (SELECT * FROM orders)", exp_type=exp.Lateral
        )
        self.validate_generation(
            sql="SELECT * FROM TABLE (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name)",
            expected_sql="SELECT * FROM (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob') AS users",
            from_dialect="snowflake",
            exp_type=exp.TableFromRows,
        )
        self.validate_generation(
            sql="SELECT 'MAX ID', Alice, Bob FROM TABLE (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name) PIVOT (MAX(id) FOR name IN ('Alice', 'Bob')) AS p",
            expected_sql="SELECT 'MAX ID', Alice, Bob FROM (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob') AS users PIVOT(MAX(id) FOR name IN ('Alice', 'Bob')) AS p",
            from_dialect="snowflake",
            exp_type=exp.TableFromRows,
        )
        self.validate_generation(
            sql="SELECT * FROM TABLE (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name)",
            expected_sql="SELECT * FROM (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob') AS users",
            from_dialect="snowflake",
            exp_type=exp.Values,
        )
        self.validate_generation(
            sql="SELECT 'MAX ID', Alice, Bob FROM TABLE (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name) PIVOT (MAX(id) FOR name IN ('Alice', 'Bob')) AS p",
            expected_sql="SELECT 'MAX ID', Alice, Bob FROM (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob') AS users PIVOT(MAX(id) FOR name IN ('Alice', 'Bob')) AS p",
            from_dialect="snowflake",
            exp_type=exp.Values,
        )
        self.validate_generation(
            sql="WITH user_names(id, name) AS (SELECT id, name FROM users) SELECT * FROM user_names",
            exp_type=exp.CTE,
        )
        self.validate_generation(
            sql="WITH recent_orders AS (SELECT * FROM orders WHERE created_at > '2024-01-01') SELECT 'a', `1` FROM (SELECT user_id, COUNT(*) AS order_count FROM recent_orders GROUP BY user_id) AS order_summary PIVOT(MAX(order_count) FOR user_id IN (1, 2, 3)) AS e",
            exp_type=exp.Subquery,
        )

    def test_dml_generation(self):
        self.validate_generation(sql="UPDATE users SET name = 'Alice'", exp_type=exp.Update)
        self.validate_generation(
            sql="UPDATE users SET email = n.email FROM new_users AS n WHERE users.id = n.id",
            expected_sql="UPDATE users SET email = n.email WHERE users.id = n.id",
            error_message="Argument 'from' is not supported for expression 'Update' when targeting SingleStore.",
            exp_type=exp.Update,
            run=False,
        )
        self.validate_generation(
            sql="UPDATE users SET name = 'Bob' WHERE signup_date < '2024-01-01' AND PARTITION_ID() = 0 LIMIT 3",
            exp_type=exp.Update,
        )
        self.validate_generation(
            sql="UPDATE users SET active = FALSE RETURNING id, name",
            expected_sql="UPDATE users SET active = FALSE",
            error_message="Argument 'returning' is not supported for expression 'Update' when targeting SingleStore.",
            exp_type=exp.Update,
            run=False,
        )
        self.validate_generation(
            sql="UPDATE users SET status = 'archived' ORDER BY last_login LIMIT 10",
            expected_sql="UPDATE users SET status = 'archived' LIMIT 10",
            error_message="Argument 'order' is not supported for expression 'Update' when targeting SingleStore.",
            exp_type=exp.Update,
            run=False,
        )
        self.validate_generation(
            sql="WITH recent_logins(id) AS (SELECT id FROM users) UPDATE users SET name = 'Bob' WHERE users.id IN (SELECT id FROM recent_logins)",
            exp_type=exp.Update,
        )
        self.validate_generation(sql="DELETE FROM users", exp_type=exp.Delete)
        self.validate_generation(
            sql="DELETE FROM users USING sessions WHERE users.id = sessions.user_id",
            expected_sql="DELETE FROM users WHERE users.id = sessions.user_id",
            error_message="Argument 'using' is not supported for expression 'Delete' when targeting SingleStore.",
            exp_type=exp.Delete,
            run=False,
        )
        self.validate_generation(
            sql="DELETE FROM users WHERE name = 'Bob' RETURNING id",
            expected_sql="DELETE FROM users WHERE name = 'Bob'",
            error_message="Argument 'returning' is not supported for expression 'Delete' when targeting SingleStore.",
            exp_type=exp.Delete,
            run=False,
        )
        self.validate_generation(sql="DELETE FROM users LIMIT 10", exp_type=exp.Delete)
        self.validate_generation(
            sql="DELETE users FROM users JOIN orders ON users.id = orders.user_id WHERE orders.id = 2",
            exp_type=exp.Delete,
        )
        self.validate_generation(
            sql="WITH inactive AS (SELECT id FROM users WHERE id = 3) DELETE users FROM users JOIN inactive ON users.id = inactive.id WHERE users.id = inactive.id LIMIT 5",
            exp_type=exp.Delete,
        )
        self.validate_generation(
            sql="INSERT INTO users (id, name) VALUES (1, 'Alice')", exp_type=exp.Insert
        )
        self.validate_generation(
            sql="INSERT OVERWRITE users (id, name) VALUES (2, 'Bob')",
            expected_sql="INSERT INTO users (id, name) VALUES (2, 'Bob')",
            error_message="Argument 'overwrite' is not supported for expression 'Insert' when targeting SingleStore.",
            exp_type=exp.Insert,
            run=False,
        )
        self.validate_generation(
            sql="INSERT OVERWRITE DIRECTORY '/tmp/out.csv' SELECT id, name FROM users",
            expected_sql="INSERT INTO DIRECTORY '/tmp/out.csv' SELECT id, name FROM users",
            error_message="Argument 'overwrite' is not supported for expression 'Insert' when targeting SingleStore.",
            exp_type=exp.Insert,
            run=False,
        )
        self.validate_generation(
            sql="INSERT OR REPLACE INTO users (id, name) VALUES (3, 'Charlie')",
            expected_sql="INSERT INTO users (id, name) VALUES (3, 'Charlie')",
            error_message="Argument 'alternative' is not supported for expression 'Insert' when targeting SingleStore.",
            exp_type=exp.Insert,
            run=False,
        )
        self.validate_generation(
            sql="INSERT IGNORE INTO users (id, name) VALUES (4, 'Diana')", exp_type=exp.Insert
        )
        self.validate_generation(
            sql="INSERT INTO FUNCTION process_users (id, name) VALUES (5, 'Eve')",
            expected_sql="INSERT INTO PROCESS_USERS(id, name) VALUES (5, 'Eve')",
            error_message="Argument 'is_function' is not supported for expression 'Insert' when targeting SingleStore.",
            exp_type=exp.Insert,
            run=False,
        )
        self.validate_generation(
            sql="INSERT INTO users (id, name) IF EXISTS VALUES (6, 'Frank')",
            expected_sql="INSERT INTO users (id, name) VALUES (6, 'Frank')",
            error_message="Argument 'exists' is not supported for expression 'Insert' when targeting SingleStore.",
            exp_type=exp.Insert,
            run=False,
        )
        self.validate_generation(
            sql="INSERT INTO users(id, name) BY NAME VALUES (7, 'George')",
            expected_sql="INSERT INTO users (id, name) VALUES (7, 'George')",
            error_message="Argument 'by_name' is not supported for expression 'Insert' when targeting SingleStore.",
            exp_type=exp.Insert,
            run=False,
        )
        self.validate_generation(
            sql="INSERT INTO users SELECT * FROM users ON DUPLICATE KEY UPDATE name = 'q'",
            exp_type=exp.Insert,
        )
        self.validate_generation(
            sql="INSERT INTO users (id, name) VALUES (7, 'George') ON DUPLICATE KEY UPDATE name = 'q'",
            exp_type=exp.Insert,
        )
        self.validate_generation(
            sql="INSERT INTO users (id, name) VALUES (7, 'George') ON DUPLICATE KEY UPDATE name = 'q'",
            exp_type=exp.Insert,
        )
        self.validate_generation(
            sql="COPY INTO users FROM '/data/users.csv' WITH (FORMAT csv, HEADER, DELIMITER ',')",
            error_message="COPY query is not supported in SingleStore",
            exp_type=exp.Copy,
            run=False,
        )
        self.validate_generation(
            sql="MERGE INTO users AS u USING new_users AS n ON u.id = n.id WHEN MATCHED THEN UPDATE SET name = n.name, email = n.email WHEN NOT MATCHED THEN INSERT (id, name, email) VALUES (n.id, n.name, n.email)",
            error_message="MERGE query is not supported in SingleStore",
            exp_type=exp.Merge,
            run=False,
        )

    def test_analyze_generation(self):
        self.validate_generation(
            sql="ANALYZE TABLE users",
            exp_type=exp.Analyze,
        )
        self.validate_generation(
            sql="ANALYZE TABLE users COMPUTE STATISTICS FOR COLUMNS name",
            expected_sql="ANALYZE TABLE users",
            exp_type=exp.AnalyzeStatistics,
            from_dialect="spark2",
        )
        self.validate_generation(
            sql="ANALYZE TABLE users UPDATE HISTOGRAM ON id, name WITH 10 BUCKETS",
            expected_sql="ANALYZE TABLE users COLUMNS id, name ENABLE",
            exp_type=exp.AnalyzeHistogram,
        )
        self.validate_generation(
            sql="ANALYZE TABLE users COMPUTE STATISTICS SAMPLE 10 PERCENT",
            expected_sql="ANALYZE TABLE users",
            exp_type=exp.AnalyzeSample,
        )
        self.validate_generation(
            sql="ANALYZE TABLE users LIST CHAINED ROWS",
            expected_sql="ANALYZE TABLE users",
            error_message="LIST CHAINED ROWS clause is not supported in SingleStore",
            exp_type=exp.AnalyzeListChainedRows,
        )
        self.validate_generation(
            sql="ANALYZE TABLE users DELETE SYSTEM STATISTICS",
            expected_sql="ANALYZE TABLE users DROP",
            exp_type=exp.AnalyzeDelete,
        )
        self.validate_generation(
            sql="ANALYZE TABLE users UPDATE HISTOGRAM ON id, name WITH 10 BUCKETS",
            expected_sql="ANALYZE TABLE users COLUMNS id, name ENABLE",
            exp_type=exp.AnalyzeWith,
        )
        self.validate_generation(
            sql="ANALYZE TABLE users VALIDATE STRUCTURE CASCADE FAST",
            error_message="VALIDATE STRUCTURE clause is not supported in SingleStore",
            expected_sql="ANALYZE TABLE users",
            exp_type=exp.AnalyzeValidate,
        )
        self.validate_generation(
            sql="ANALYZE TABLE users ALL COLUMNS",
            expected_sql="ANALYZE TABLE users COLUMNS ALL ENABLE",
            exp_type=exp.AnalyzeColumns,
        )

    def test_join_sql_generation(self):
        self.validate_generation(
            sql="SELECT * FROM orders JOIN order_items ON orders.id = order_items.order_id",
            exp_type=exp.Join,
        )
        self.validate_generation(
            sql="SELECT * FROM orders LEFT JOIN order_items ON orders.id = order_items.order_id",
            exp_type=exp.Join,
        )
        self.validate_generation(
            sql="SELECT * FROM order_items RIGHT JOIN orders ON order_items.order_id = orders.id",
            exp_type=exp.Join,
        )
        self.validate_generation(
            sql="SELECT * FROM orders FULL OUTER JOIN events ON orders.user_id = events.user_id",
            exp_type=exp.Join,
        )
        self.validate_generation(
            sql="SELECT * FROM orders CROSS JOIN products",
            exp_type=exp.Join,
        )
        self.validate_generation(
            sql="SELECT * FROM orders JOIN events USING (user_id)",
            exp_type=exp.Join,
        )
        self.validate_generation(
            sql="SELECT * FROM orders STRAIGHT_JOIN order_items ON orders.id = order_items.order_id",
            exp_type=exp.Join,
        )
        self.validate_generation(
            sql="SELECT * FROM order_items JOIN products ON order_items.product_id = products.id AND products.stock_quantity > 0",
            exp_type=exp.Join,
        )
        self.validate_generation(
            sql="SELECT * FROM orders, products",
            exp_type=exp.Join,
        )
        self.validate_generation(
            sql="SELECT * FROM orders, LATERAL (SELECT * FROM events WHERE events.user_id = orders.user_id)",
            exp_type=exp.Join,
        )

    def test_properties_generation(self):
        self.validate_generation(
            sql="CREATE ALGORITHM=MERGE TABLE AlgorithmProperty (id INT)",
            expected_sql="CREATE TABLE AlgorithmProperty (id INT)",
            error_message="Unsupported property algorithmproperty",
            exp_type=exp.AlgorithmProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE AutoIncrementProperty (id INT) AUTO_INCREMENT=2",
            exp_type=exp.AutoIncrementProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE AutoRefreshProperty (id INT) AUTO REFRESH YES",
            expected_sql="CREATE TABLE AutoRefreshProperty (id INT)",
            error_message="Unsupported property autorefresh",
            exp_type=exp.AutoRefreshProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE BackupProperty (id INT) BACKUP YES",
            expected_sql="CREATE TABLE BackupProperty (id INT)",
            error_message="Unsupported property backup",
            exp_type=exp.BackupProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE BlockCompressionProperty BLOCKCOMPRESSION=NEVER (id INT)",
            expected_sql="CREATE TABLE BlockCompressionProperty (id INT)",
            error_message="Unsupported property blockcompression",
            exp_type=exp.BlockCompressionProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE CharacterSetProperty (name VARCHAR(100)) CHARACTER SET=utf8mb4",
            exp_type=exp.CharacterSetProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE ChecksumProperty CHECKSUM=ON (id INT)",
            expected_sql="CREATE TABLE ChecksumProperty (id INT)",
            error_message="Unsupported property checksum",
            exp_type=exp.ChecksumProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE CollateProperty (name VARCHAR(100)) COLLATE=utf8mb4_general_ci",
            exp_type=exp.CollateProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE CopyGrantsProperty (id INT) COPY GRANTS",
            expected_sql="CREATE TABLE CopyGrantsProperty (id INT)",
            error_message="Unsupported property copygrants",
            exp_type=exp.CopyGrantsProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE Cluster (id INT) CLUSTER BY id",
            expected_sql="CREATE TABLE Cluster (id INT)",
            error_message="Unsupported property cluster",
            exp_type=exp.Cluster,
        )
        self.validate_generation(
            sql="CREATE TABLE ClusteredByProperty (id INT) CLUSTERED BY (id) INTO 10 BUCKETS",
            expected_sql="CREATE TABLE ClusteredByProperty (id INT)",
            error_message="Unsupported property clusteredby",
            exp_type=exp.ClusteredByProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE DistributedByProperty (id INT) DISTRIBUTED BY RANDOM BUCKETS 10",
            expected_sql="CREATE TABLE DistributedByProperty (id INT)",
            error_message="Unsupported property distributedby",
            exp_type=exp.DistributedByProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE DuplicateKeyProperty (id INT) DUPLICATE KEY (id)",
            expected_sql="CREATE TABLE DuplicateKeyProperty (id INT)",
            error_message="Unsupported property duplicatekey",
            exp_type=exp.DuplicateKeyProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE DataBlocksizeProperty MINIMUM DATABLOCKSIZE (id INT)",
            expected_sql="CREATE TABLE DataBlocksizeProperty (id INT)",
            error_message="Unsupported property datablocksize",
            exp_type=exp.DataBlocksizeProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE DataDeletionProperty (id INT) DATA_DELETION=ON",
            expected_sql="CREATE TABLE DataDeletionProperty (id INT)",
            error_message="Unsupported property datadeletion",
            exp_type=exp.DataDeletionProperty,
        )
        self.validate_generation(
            sql="CREATE DEFINER=admin@host PROCEDURE DefinerFunction(id INT)",
            expected_sql="CREATE PROCEDURE DefinerFunction(id INT) DEFINER=admin@host",
            exp_type=exp.DefinerProperty,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE DictRange (id INT) LIFETIME(MIN 10 MAX 20)",
            expected_sql="CREATE TABLE DictRange (id INT)",
            error_message="Unsupported property dictrange",
            exp_type=exp.DictRange,
        )
        self.validate_generation(
            sql="CREATE TABLE DictProperty (id INT) LAYOUT (a)",
            expected_sql="CREATE TABLE DictProperty (id INT)",
            error_message="Unsupported property dict",
            exp_type=exp.DictProperty,
        )
        self.validate_generation(
            sql="CREATE DYNAMIC TABLE DynamicProperty (id INT)",
            expected_sql="CREATE TABLE DynamicProperty (id INT)",
            error_message="Unsupported property dynamic",
            exp_type=exp.DynamicProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE DistKeyProperty (id INT) DISTKEY(id)",
            expected_sql="CREATE TABLE DistKeyProperty (id INT)",
            error_message="Unsupported property distkey",
            exp_type=exp.DistKeyProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE DistStyleProperty (id INT) DISTSTYLE EVEN",
            expected_sql="CREATE TABLE DistStyleProperty (id INT)",
            error_message="Unsupported property diststyle",
            exp_type=exp.DistStyleProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE EmptyProperty (id INT) EMPTY",
            expected_sql="CREATE TABLE EmptyProperty (id INT)",
            error_message="Unsupported property empty",
            exp_type=exp.EmptyProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE EncodeProperty (bar INT, gen_col INT) FORMAT upsert ENCODE AVRO ( schema.registry = 'http://message_queue:8081' )",
            expected_sql="CREATE TABLE EncodeProperty (bar INT, gen_col INT)",
            error_message="Unsupported property encode",
            from_dialect="risingwave",
            exp_type=exp.EncodeProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE EngineProperty (id INT) ENGINE = Columnstore",
            expected_sql="CREATE TABLE EngineProperty (id INT)",
            error_message="Unsupported property engine",
            exp_type=exp.EngineProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE ExecuteAsProperty (id INT) EXECUTE AS 'admin'",
            expected_sql="CREATE TABLE ExecuteAsProperty (id INT)",
            error_message="Unsupported property executeas",
            exp_type=exp.ExecuteAsProperty,
        )
        self.validate_generation(
            sql="CREATE EXTERNAL FUNCTION db.some_func(a INT)",
            exp_type=exp.ExternalProperty,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE FallbackProperty NO FALLBACK (id INT)",
            expected_sql="CREATE TABLE FallbackProperty (id INT)",
            error_message="Unsupported property fallback",
            exp_type=exp.FallbackProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE FileFormatProperty (id INT) FORMAT=PARQUET",
            expected_sql="CREATE TABLE FileFormatProperty (id INT)",
            error_message="Unsupported property fileformat",
            exp_type=exp.FileFormatProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE FreespaceProperty FREESPACE = 25 (id INT)",
            expected_sql="CREATE TABLE FreespaceProperty (id INT)",
            error_message="Unsupported property freespace",
            exp_type=exp.FreespaceProperty,
        )
        self.validate_generation(
            sql="CREATE GLOBAL TABLE GlobalProperty (id INT)",
            expected_sql="CREATE TABLE GlobalProperty (id INT)",
            error_message="Unsupported property global",
            exp_type=exp.GlobalProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE HeapProperty (id INT) HEAP",
            expected_sql="CREATE TABLE HeapProperty (id INT)",
            error_message="Unsupported property heap",
            exp_type=exp.HeapProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE InheritsProperty (id INT) INHERITS (base_table)",
            expected_sql="CREATE TABLE InheritsProperty (id INT)",
            error_message="Unsupported property inherits",
            exp_type=exp.InheritsProperty,
        )
        self.validate_generation(
            sql="CREATE ICEBERG TABLE IcebergProperty (id INT)",
            expected_sql="CREATE TABLE IcebergProperty (id INT)",
            error_message="Unsupported property iceberg",
            exp_type=exp.IcebergProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE IncludeProperty (id INT) INCLUDE extra_column",
            expected_sql="CREATE TABLE IncludeProperty (id INT)",
            from_dialect="risingwave",
            error_message="Unsupported property include",
            exp_type=exp.IncludeProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE InputModelProperty (id INT) INPUT(a INT)",
            expected_sql="CREATE TABLE InputModelProperty (id INT)",
            error_message="Unsupported property inputmodel",
            exp_type=exp.InputModelProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE IsolatedLoadingProperty WITH ISOLATED LOADING (id INT)",
            expected_sql="CREATE TABLE IsolatedLoadingProperty (id INT)",
            error_message="Unsupported property isolatedloading",
            exp_type=exp.IsolatedLoadingProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE JournalProperty NO JOURNAL (id INT)",
            expected_sql="CREATE TABLE JournalProperty (id INT)",
            error_message="Unsupported property journal",
            exp_type=exp.JournalProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE LanguageProperty (id INT) LANGUAGE SQL",
            expected_sql="CREATE TABLE LanguageProperty (id INT)",
            error_message="Unsupported property language",
            exp_type=exp.LanguageProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE LikeProperty LIKE users",
            expected_sql="CREATE TABLE LikeProperty LIKE users",
            exp_type=exp.LikeProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE LocationProperty (id INT) LOCATION = 's3://bucket'",
            expected_sql="CREATE TABLE LocationProperty (id INT)",
            error_message="Unsupported property location",
            exp_type=exp.LocationProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE LockProperty (id INT) LOCK = EXCLUSIVE",
            expected_sql="CREATE TABLE LockProperty (id INT)",
            error_message="Unsupported property lock",
            exp_type=exp.LockProperty,
        )
        self.validate_generation(
            sql="CREATE VIEW LockingProperty AS LOCKING ROW FOR ACCESS SELECT * FROM users",
            from_dialect="teradata",
            expected_sql="CREATE VIEW LockingProperty AS SELECT * FROM users",
            error_message="Unsupported property locking",
            exp_type=exp.LockingProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE LogProperty LOG (id INT)",
            expected_sql="CREATE TABLE LogProperty (id INT)",
            error_message="Unsupported property log",
            exp_type=exp.LogProperty,
        )
        self.validate_generation(
            sql="CREATE MATERIALIZED TABLE MaterializedProperty (id INT)",
            expected_sql="CREATE TABLE MaterializedProperty (id INT)",
            error_message="Unsupported property materialized",
            exp_type=exp.MaterializedProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE MergeBlockRatioProperty DEFAULT MERGEBLOCKRATIO (id INT)",
            expected_sql="CREATE TABLE MergeBlockRatioProperty (id INT)",
            error_message="Unsupported property mergeblockratio",
            exp_type=exp.MergeBlockRatioProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE OnCommitProperty AS SELECT * FROM users ON COMMIT DELETE ROWS",
            expected_sql="CREATE TABLE OnCommitProperty AS SELECT * FROM users",
            error_message="Unsupported property oncommit",
            exp_type=exp.OnCommitProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE OrderProperty (id INT) ORDER BY (id)",
            expected_sql="CREATE TABLE OrderProperty (id INT)",
            error_message="Unsupported property order",
            exp_type=exp.Order,
        )
        self.validate_generation(
            sql="CREATE TABLE NoPrimaryIndexProperty AS (SELECT * FROM users) NO PRIMARY INDEX",
            expected_sql="CREATE TABLE NoPrimaryIndexProperty AS SELECT * FROM users",
            error_message="Unsupported property noprimaryindexproperty",
            exp_type=exp.NoPrimaryIndexProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE OutputModelProperty (id INT) OUTPUT(a INT)",
            expected_sql="CREATE TABLE OutputModelProperty (id INT)",
            error_message="Unsupported property outputmodel",
            exp_type=exp.OutputModelProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE PartitionedByProperty (id INT) WITH (PARTITIONED BY (id))",
            expected_sql="CREATE TABLE PartitionedByProperty (id INT)",
            error_message="Unsupported property partitionedby",
            exp_type=exp.PartitionedByProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE PartitionedOfProperty (id INT) PARTITION OF base_table DEFAULT",
            expected_sql="CREATE TABLE PartitionedOfProperty (id INT)",
            error_message="Unsupported property partitionedof",
            exp_type=exp.PartitionedOfProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE PrimaryKey (id INT) PRIMARY KEY (id)",
            expected_sql="CREATE TABLE PrimaryKey (id INT)",
            error_message="Unsupported property primarykey",
            exp_type=exp.PrimaryKey,
        )
        self.validate_generation(
            sql="CREATE TABLE Property (id INT) WITH (FOO = 'bar')",
            expected_sql="CREATE TABLE Property (id INT)",
            error_message="Unsupported property property",
            exp_type=exp.Property,
        )
        self.validate_generation(
            sql="CREATE TABLE RemoteWithConnectionModelProperty (id INT) REMOTE WITH CONNECTION db.user",
            expected_sql="CREATE TABLE RemoteWithConnectionModelProperty (id INT)",
            error_message="Unsupported property remotewithconnectionmodel",
            exp_type=exp.RemoteWithConnectionModelProperty,
        )
        self.validate_generation(
            sql="CREATE FUNCTION ReturnsProperty(id INT) RETURNS INT",
            expected_sql="CREATE FUNCTION ReturnsProperty(id INT) RETURNS INT",
            exp_type=exp.ReturnsProperty,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE RowFormatProperty (id INT) ROW_FORMAT=COMPRESSED",
            expected_sql="CREATE TABLE RowFormatProperty (id INT)",
            error_message="Unsupported property rowformat",
            exp_type=exp.RowFormatProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE RowFormatDelimitedProperty (id INT) ROW FORMAT DELIMITED",
            expected_sql="CREATE TABLE RowFormatDelimitedProperty (id INT)",
            error_message="Unsupported property rowformatdelimited",
            exp_type=exp.RowFormatDelimitedProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE RowFormatSerdeProperty (id INT) ROW FORMAT SERDE 'serde.class'",
            expected_sql="CREATE TABLE RowFormatSerdeProperty (id INT)",
            error_message="Unsupported property rowformatserde",
            exp_type=exp.RowFormatSerdeProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE SampleProperty (id INT) SAMPLE BY (id)",
            expected_sql="CREATE TABLE SampleProperty (id INT)",
            error_message="Unsupported property sample",
            exp_type=exp.SampleProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE SchemaCommentProperty (id INT) COMMENT='table comment'",
            exp_type=exp.SchemaCommentProperty,
        )
        self.validate_generation(
            sql="CREATE SECURE TABLE SecureProperty (id INT)",
            expected_sql="CREATE TABLE SecureProperty (id INT)",
            error_message="Unsupported property secure",
            exp_type=exp.SecureProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE SecurityProperty (id INT) SECURITY INVOKER",
            expected_sql="CREATE TABLE SecurityProperty (id INT)",
            error_message="Unsupported property security",
            exp_type=exp.SecurityProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE SerdePropertiesTable (id INT) WITH SERDEPROPERTIES ('property' = 'value')",
            expected_sql="CREATE TABLE SerdePropertiesTable (id INT)",
            from_dialect="hive",
            error_message="Unsupported property serdeproperties",
            exp_type=exp.SerdeProperties,
        )
        self.validate_generation(sql="SET @a = 1", exp_type=exp.Set)
        self.validate_generation(
            sql="CREATE TABLE SettingsProperty (id INT) SETTINGS ('k' = 'v')",
            expected_sql="CREATE TABLE SettingsProperty (id INT)",
            error_message="Unsupported property settings",
            exp_type=exp.SettingsProperty,
        )
        self.validate_generation(
            sql="CREATE SET TABLE SetProperty (id INT)",
            expected_sql="CREATE TABLE SetProperty (id INT)",
            error_message="Unsupported property setproperty",
            exp_type=exp.SetProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE SetConfigProperty (id INT) SET NULL(id)",
            expected_sql="CREATE TABLE SetConfigProperty (id INT)",
            error_message="Unsupported property setconfig",
            from_dialect="postgres",
            exp_type=exp.SetConfigProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE SharingProperty AS (SELECT * FROM users) SHARING='PUBLIC'",
            expected_sql="CREATE TABLE SharingProperty AS SELECT * FROM users",
            error_message="Unsupported property sharing",
            exp_type=exp.SharingProperty,
        )
        self.validate_generation(
            sql="CREATE SEQUENCE user_id_seq START WITH 42 INCREMENT BY 5 MINVALUE 1 MAXVALUE 1000 CACHE 10",
            expected_sql="CREATE SEQUENCE user_id_seq",
            error_message="Unsupported property sequenceproperties",
            exp_type=exp.SequenceProperties,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TABLE SortKeyProperty (id INT) SORTKEY(id)",
            expected_sql="CREATE TABLE SortKeyProperty (id INT)",
            error_message="Unsupported property sortkey",
            exp_type=exp.SortKeyProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE SqlReadWriteProperty (id INT) NO SQL",
            expected_sql="CREATE TABLE SqlReadWriteProperty (id INT)",
            error_message="Unsupported property sqlreadwrite",
            exp_type=exp.SqlReadWriteProperty,
        )
        self.validate_generation(
            sql="CREATE SQL SECURITY DEFINER TABLE SqlSecurityProperty (id INT)",
            expected_sql="CREATE TABLE SqlSecurityProperty (id INT)",
            error_message="Unsupported property sqlsecurity",
            exp_type=exp.SqlSecurityProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE StabilityProperty (id INT) IMMUTABLE",
            expected_sql="CREATE TABLE StabilityProperty (id INT)",
            error_message="Unsupported property stability",
            exp_type=exp.StabilityProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE StorageHandlerProperty (id INT) STORED BY 'a'",
            expected_sql="CREATE TABLE StorageHandlerProperty (id INT)",
            error_message="Unsupported property storagehandler",
            exp_type=exp.StorageHandlerProperty,
        )
        self.validate_generation(
            sql="CREATE STREAMING TABLE StreamingTableProperty (id INT)",
            expected_sql="CREATE TABLE StreamingTableProperty (id INT)",
            error_message="Unsupported property streamingtable",
            exp_type=exp.StreamingTableProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE StrictProperty (id INT) STRICT",
            expected_sql="CREATE TABLE StrictProperty (id INT)",
            error_message="Unsupported property strict",
            exp_type=exp.StrictProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE Tags (id INT) TAG ('env' = 'prod')",
            expected_sql="CREATE TABLE Tags (id INT)",
            from_dialect="snowflake",
            error_message="Unsupported property tags",
            exp_type=exp.Tags,
        )
        self.validate_generation(
            sql="CREATE TEMPORARY TABLE TemporaryProperty (id INT)",
            expected_sql="CREATE TEMPORARY TABLE TemporaryProperty (id INT)",
            exp_type=exp.TemporaryProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE ToTableProperty (id INT) TO db.a",
            expected_sql="CREATE TABLE ToTableProperty (id INT)",
            error_message="Unsupported property totable",
            exp_type=exp.ToTableProperty,
        )
        self.validate_generation(
            sql="CREATE TRANSIENT TABLE TransientProperty (id INT)",
            expected_sql="CREATE TABLE TransientProperty (id INT)",
            error_message="Unsupported property transient",
            exp_type=exp.TransientProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE TransformModelProperty (id INT) TRANSFORM (a+1, b+2)",
            expected_sql="CREATE TABLE TransformModelProperty (id INT)",
            error_message="Unsupported property transformmodel",
            exp_type=exp.TransformModelProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE MergeTreeTTL (id INT) TTL id + INTERVAL 1 DAY",
            expected_sql="CREATE TABLE MergeTreeTTL (id INT)",
            error_message="Unsupported property mergetreettl",
            exp_type=exp.MergeTreeTTL,
        )
        self.validate_generation(
            sql="CREATE UNLOGGED TABLE UnloggedProperty (id INT)",
            expected_sql="CREATE TABLE UnloggedProperty (id INT)",
            error_message="Unsupported property unlogged",
            exp_type=exp.UnloggedProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE UsingTemplateProperty (id INT) USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY order_id) FROM TABLE( INFER_SCHEMA( LOCATION=>'@mystage', FILE_FORMAT=>'my_parquet_format' ) ))",
            expected_sql="CREATE TABLE UsingTemplateProperty (id INT)",
            error_message="Unsupported property usingtemplate",
            from_dialect="snowflake",
            exp_type=exp.UsingTemplateProperty,
        )
        self.validate_generation(
            sql="CREATE VOLATILE TABLE VolatileProperty (id INT)",
            expected_sql="CREATE TABLE VolatileProperty (id INT)",
            error_message="Unsupported property volatile",
            exp_type=exp.VolatileProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE WithDataProperty AS SELECT * FROM users WITH DATA",
            expected_sql="CREATE TABLE WithDataProperty AS SELECT * FROM users",
            error_message="Unsupported property withdata",
            exp_type=exp.WithDataProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE WithJournalTableProperty WITH JOURNAL = 'journal_1' (id INT)",
            expected_sql="CREATE TABLE WithJournalTableProperty (id INT)",
            error_message="Unsupported property withjournaltable",
            exp_type=exp.WithJournalTableProperty,
        )
        self.validate_generation(
            sql="CREATE FORCE TABLE ForceProperty (id INT)",
            expected_sql="CREATE TABLE ForceProperty (id INT)",
            from_dialect="oracle",
            error_message="Unsupported property force",
            exp_type=exp.ForceProperty,
        )
        self.validate_generation(
            sql="CREATE TABLE WithSystemVersioningProperty (id INT) WITH (SYSTEM_VERSIONING=)",
            expected_sql="CREATE TABLE WithSystemVersioningProperty (id INT)",
            error_message="Unsupported property withsystemversioning",
            exp_type=exp.WithSystemVersioningProperty,
        )
        self.validate_generation(
            sql="CREATE PROCEDURE HumanResources.uspEncryptThis WITH RECOMPILE",
            expected_sql="CREATE PROCEDURE HumanResources.uspEncryptThis()",
            error_message="Unsupported property withprocedureoptions",
            from_dialect="tsql",
            exp_type=exp.WithProcedureOptions,
            run=False,
        )
        self.validate_generation(
            sql="CREATE VIEW a WITH SCHEMA BINDING AS SELECT * FROM users",
            expected_sql="CREATE SCHEMA_BINDING=ON VIEW a AS SELECT * FROM users",
            exp_type=exp.WithSchemaBindingProperty,
        )
        self.validate_generation(
            sql="CREATE VIEW ViewAttributeProperty WITH SCHEMABINDING AS SELECT * FROM users",
            expected_sql="CREATE SCHEMA_BINDING=ON VIEW ViewAttributeProperty AS SELECT * FROM users",
            from_dialect="tsql",
            exp_type=exp.ViewAttributeProperty,
        )

    def test_create_generation(self):
        self.validate_generation(
            sql="CREATE TABLE users1 (id INT, name VARCHAR(100))",
            exp_type=exp.Create,
        )
        self.validate_generation(
            sql="CREATE TABLE IF NOT EXISTS users2 (id INT, name VARCHAR(100))",
            exp_type=exp.Create,
        )
        self.validate_generation(
            sql="CREATE VIEW user_view AS SELECT id, name FROM users",
            exp_type=exp.Create,
        )
        self.validate_generation(
            sql="CREATE FUNCTION is_active(u_id INT) RETURNS BOOLEAN AS BEGIN RETURN u_id > 0",
            exp_type=exp.Create,
            run=False,
        )
        self.validate_generation(
            sql="CREATE TEMPORARY TABLE logs (id INT, message TEXT)",
            exp_type=exp.Create,
        )
        self.validate_generation(
            sql="CREATE VIEW user_view2 AS SELECT id, name FROM users WITH NO SCHEMA BINDING",
            expected_sql="CREATE SCHEMA_BINDING=OFF VIEW user_view2 AS SELECT id, name FROM users",
            exp_type=exp.Create,
        )
        self.validate_generation(
            sql="CREATE TABLE events1 SHALLOW COPY events",
            expected_sql="CREATE TABLE events1 LIKE events WITH SHALLOW COPY",
            exp_type=exp.Create,
        )
        self.validate_generation(
            sql="CREATE TABLE IF NOT EXISTS products2 (id INT, name TEXT, price DECIMAL(10, 2), INDEX (name))",
            exp_type=exp.Create,
        )

    def test_tsql_conversion(self):
        self.validate_generation(
            sql="CREATE TABLE syb_unichar_example (id INT, uni_field UNICHAR(10))",
            expected_sql="CREATE TABLE syb_unichar_example (id INT, uni_field CHAR(10))",
            from_dialect="tsql",
        )
        self.validate_generation(
            sql="CREATE TABLE syb_univarchar_example (id INT, uni_name UNIVARCHAR(100))",
            expected_sql="CREATE TABLE syb_univarchar_example (id INT, uni_name VARCHAR(100))",
            from_dialect="tsql",
        )
        self.validate_generation(
            sql="CREATE TABLE syb_bit_example (id INT, is_active BIT)",
            expected_sql="CREATE TABLE syb_bit_example (id INT, is_active BOOLEAN)",
            from_dialect="tsql",
        )
        self.validate_generation(
            sql="CREATE TABLE syb_image_example (id INT, photo IMAGE)",
            expected_sql="CREATE TABLE syb_image_example (id INT, photo LONGBLOB)",
            from_dialect="tsql",
        )

    def test_functions_parsing(self):
        self.validate_parsing(
            "SELECT ABS(age) FROM users",
            exp.Abs(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT ACOS(age) FROM users",
            exp.func("ACOS", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        # TODO: convert ADDTIME to DATE_ADD
        self.validate_parsing(
            'SELECT ADDTIME(signup_date, "02:45:07") FROM users',
            exp.func(
                "ADDTIME",
                exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                exp.Literal.string("02:45:07"),
            ),
        )
        self.validate_parsing(
            "SELECT AES_DECRYPT(email, 'key') FROM users",
            exp.func(
                "AES_DECRYPT",
                exp.Column(this=exp.Identifier(this="email", quoted=False)),
                exp.Literal.string("key"),
            ),
        )
        self.validate_parsing(
            "SELECT AES_ENCRYPT(email, 'key') FROM users",
            exp.func(
                "AES_ENCRYPT",
                exp.Column(this=exp.Identifier(this="email", quoted=False)),
                exp.Literal.string("key"),
            ),
        )
        self.validate_parsing("SELECT AGGREGATOR_ID() FROM users", exp.func("AGGREGATOR_ID"))
        self.validate_parsing(
            "SELECT ANY_VALUE(age) FROM users",
            exp.AnyValue(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT APPROX_COUNT_DISTINCT(email) FROM users",
            exp.Hll(this=exp.Column(this=exp.Identifier(this="email", quoted=False))),
        )
        self.validate_parsing(
            "SELECT APPROX_COUNT_DISTINCT_ACCUMULATE(email) FROM users",
            exp.func(
                "APPROX_COUNT_DISTINCT_ACCUMULATE",
                exp.Column(this=exp.Identifier(this="email", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT APPROX_COUNT_DISTINCT_COMBINE(email) FROM users",
            exp.func(
                "APPROX_COUNT_DISTINCT_COMBINE",
                exp.Column(this=exp.Identifier(this="email", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT APPROX_COUNT_DISTINCT_ESTIMATE(email) FROM users",
            exp.func(
                "APPROX_COUNT_DISTINCT_ESTIMATE",
                exp.Column(this=exp.Identifier(this="email", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT APPROX_GEOGRAPHY_INTERSECTS(id, age) FROM users",
            exp.func(
                "APPROX_GEOGRAPHY_INTERSECTS",
                exp.Column(this=exp.Identifier(this="id", quoted=False)),
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT APPROX_PERCENTILE(age, 0.9) FROM users",
            exp.ApproxQuantile(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                quantile=exp.Literal.number(0.9),
            ),
        )
        self.validate_parsing(
            "SELECT ASCII(name) FROM users",
            exp.Unicode(this=exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT ASIN(age) FROM users",
            exp.func("ASIN", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT ATAN(age) FROM users",
            exp.func("ATAN", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT ATAN2(age, id) FROM users",
            exp.func(
                "ATAN2",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Column(this=exp.Identifier(this="id", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT AVG(age) FROM users",
            exp.Avg(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT age BETWEEN 18 AND 30 FROM users",
            exp.Between(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                low=exp.Literal.number(18),
                high=exp.Literal.number(30),
            ),
        )
        self.validate_parsing(
            "SELECT age NOT BETWEEN 18 AND 30 FROM users",
            exp.Not(
                this=exp.Between(
                    this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    low=exp.Literal.number(18),
                    high=exp.Literal.number(30),
                )
            ),
        )
        self.validate_parsing(
            "SELECT BIN(id) FROM users",
            exp.func("BIN", exp.Column(this=exp.Identifier(this="id", quoted=False))),
        )
        self.validate_parsing(
            "SELECT BIN_TO_UUID(id) FROM users",
            exp.func("BIN_TO_UUID", exp.Column(this=exp.Identifier(this="id", quoted=False))),
        )
        self.validate_parsing(
            "SELECT BINARY(name) FROM users",
            exp.Cast(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                to=exp.DataType(this=exp.DataType.Type.BINARY),
            ),
        )
        self.validate_parsing(
            "SELECT BIT_AND(age) FROM users",
            exp.func("BIT_AND", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT BIT_COUNT(id) FROM users",
            exp.func("BIT_COUNT", exp.Column(this=exp.Identifier(this="id", quoted=False))),
        )
        self.validate_parsing(
            "SELECT BIT_OR(age) FROM users",
            exp.func("BIT_OR", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT BIT_XOR(age) FROM users",
            exp.func("BIT_XOR", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT id & age FROM users",
            exp.BitwiseAnd(
                this=exp.Column(this=exp.Identifier(this="id", quoted=False)),
                expression=exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT id << 2 FROM users",
            exp.BitwiseLeftShift(
                this=exp.Column(this=exp.Identifier(this="id", quoted=False)),
                expression=exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT ~age FROM users",
            exp.BitwiseNot(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT id | age FROM users",
            exp.BitwiseOr(
                this=exp.Column(this=exp.Identifier(this="id", quoted=False)),
                expression=exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT id >> 2 FROM users",
            exp.BitwiseRightShift(
                this=exp.Column(this=exp.Identifier(this="id", quoted=False)),
                expression=exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT id ^ age FROM users",
            exp.BitwiseXor(
                this=exp.Column(this=exp.Identifier(this="id", quoted=False)),
                expression=exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT BM25(events, 'body:database') FROM events",
            exp.func(
                "BM25",
                exp.Column(this=exp.Identifier(this="events", quoted=False)),
                exp.Literal.string("body:database"),
            ),
        )
        self.validate_parsing(
            "SELECT BM25_GLOBAL(events, 'body:database') FROM events",
            exp.func(
                "BM25_GLOBAL",
                exp.Column(this=exp.Identifier(this="events", quoted=False)),
                exp.Literal.string("body:database"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_ARRAY_CONTAINS_BSON(metadatab, 'value') FROM events",
            exp.func(
                "BSON_ARRAY_CONTAINS_BSON",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("value"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_ARRAY_PUSH(metadatab, 'value') FROM events",
            exp.func(
                "BSON_ARRAY_PUSH",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("value"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_ARRAY_SLICE(metadatab, 1, 2) FROM events",
            exp.func(
                "BSON_ARRAY_SLICE",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.number(1),
                exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_BUILD_ARRAY('a', 'b') FROM events",
            exp.func("BSON_BUILD_ARRAY", exp.Literal.string("a"), exp.Literal.string("b")),
        )
        self.validate_parsing(
            "SELECT BSON_BUILD_OBJECT('k', 'v') FROM events",
            exp.func("BSON_BUILD_OBJECT", exp.Literal.string("k"), exp.Literal.string("v")),
        )
        self.validate_parsing(
            "SELECT BSON_COMPARE(metadatab, metadatab) FROM events",
            exp.func(
                "BSON_COMPARE",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_EXTRACT_BIGINT(metadatab, 'k') FROM events",
            exp.func(
                "BSON_EXTRACT_BIGINT",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("k"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_EXTRACT_BOOL(metadatab, 'k') FROM events",
            exp.func(
                "BSON_EXTRACT_BOOL",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("k"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_EXTRACT_BSON(metadatab, 'k') FROM events",
            exp.JSONBExtract(
                this=exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                expression=exp.JSONPath(
                    expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="k")]
                ),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_EXTRACT_BSON(metadatab, 'k', 1, 'a') FROM events",
            exp.JSONBExtract(
                this=exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                expression=exp.JSONPath(
                    expressions=[
                        exp.JSONPathRoot(),
                        exp.JSONPathKey(this="k"),
                        exp.JSONPathSubscript(this=str(1)),
                        exp.JSONPathKey(this="a"),
                    ]
                ),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_EXTRACT_STRING(metadatab, 'k') FROM events",
            exp.JSONBExtractScalar(
                this=exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                expression=exp.JSONPath(
                    expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="k")]
                ),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_EXTRACT_STRING(metadatab, 'k', 1, 'a') FROM events",
            exp.JSONBExtractScalar(
                this=exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                expression=exp.JSONPath(
                    expressions=[
                        exp.JSONPathRoot(),
                        exp.JSONPathKey(this="k"),
                        exp.JSONPathSubscript(this=str(1)),
                        exp.JSONPathKey(this="a"),
                    ]
                ),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_EXTRACT_DATETIME(metadatab, 'k') FROM events",
            exp.func(
                "BSON_EXTRACT_DATETIME",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("k"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_EXTRACT_DOUBLE(metadatab, 'k') FROM events",
            exp.func(
                "BSON_EXTRACT_DOUBLE",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("k"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_GET_TYPE(metadatab) FROM events",
            exp.func(
                "BSON_GET_TYPE", exp.Column(this=exp.Identifier(this="metadatab", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT BSON_INCLUDE_MASK(metadatab, '{}') FROM events",
            exp.func(
                "BSON_INCLUDE_MASK",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("{}"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_EXCLUDE_MASK(metadatab, '{}') FROM events",
            exp.func(
                "BSON_EXCLUDE_MASK",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("{}"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_LENGTH(metadatab) FROM events",
            exp.func(
                "BSON_LENGTH", exp.Column(this=exp.Identifier(this="metadatab", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT BSON_MATCH_ANY(metadatab, 'filter') FROM events",
            exp.func(
                "BSON_MATCH_ANY",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("filter"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_MATCH_ANY_EXISTS(metadatab, 'filter') FROM events",
            exp.func(
                "BSON_MATCH_ANY_EXISTS",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("filter"),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_MERGE(metadatab, metadatab) FROM events",
            exp.func(
                "BSON_MERGE",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT BSON_NORMALIZE(metadatab) FROM events",
            exp.func(
                "BSON_NORMALIZE", exp.Column(this=exp.Identifier(this="metadatab", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT BSON_SET_BSON(metadatab, 'k', 'v') FROM events",
            exp.func(
                "BSON_SET_BSON",
                exp.Column(this=exp.Identifier(this="metadatab", quoted=False)),
                exp.Literal.string("k"),
                exp.Literal.string("v"),
            ),
        )
        self.validate_parsing(
            "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users",
            exp.Case(
                ifs=[
                    exp.If(
                        this=exp.GT(
                            this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                            expression=exp.Literal.number(18),
                        ),
                        true=exp.Literal.string("adult"),
                    ),
                ],
                default=exp.Literal.string("minor"),
            ),
        )
        self.validate_parsing(
            "SELECT CEIL(amount) FROM orders",
            exp.Ceil(this=exp.Column(this=exp.Identifier(this="amount", quoted=False))),
        )
        self.validate_parsing(
            "SELECT CHAR(65, 66) FROM users",
            exp.func("CHAR", exp.Literal.number(65), exp.Literal.number(66)),
        )
        self.validate_parsing(
            "SELECT CHARACTER_LENGTH(name) FROM users",
            exp.Length(this=exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT CHARSET(name) FROM users",
            exp.func("CHARSET", exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT COALESCE(email, 'no_email@example.com') FROM users",
            exp.Coalesce(
                this=exp.Column(this=exp.Identifier(this="email", quoted=False)),
                expressions=[exp.Literal.string("no_email@example.com")],
            ),
        )
        self.validate_parsing(
            "SELECT CONCAT(name, email) FROM users",
            exp.Concat(
                expressions=[
                    exp.Column(this=exp.Identifier(this="name", quoted=False)),
                    exp.Column(this=exp.Identifier(this="email", quoted=False)),
                ],
                safe=True,
                coalesce=False,
            ),
        )
        self.validate_parsing(
            "SELECT CONCAT_WS('-', name, email) FROM users",
            exp.ConcatWs(
                expressions=[
                    exp.Literal.string("-"),
                    exp.Column(this=exp.Identifier(this="name", quoted=False)),
                    exp.Column(this=exp.Identifier(this="email", quoted=False)),
                ],
                safe=True,
                coalesce=False,
            ),
        )
        self.validate_parsing("SELECT CONNECTION_ID() FROM users", exp.func("CONNECTION_ID"))
        self.validate_parsing(
            "SELECT CONV(age, 10, 16) FROM users",
            exp.func(
                "CONV",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Literal.number(10),
                exp.Literal.number(16),
            ),
        )
        self.validate_parsing(
            "SELECT CAST(age AS CHAR) FROM users",
            exp.Cast(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                to=exp.DataType(this=exp.DataType.Type.CHAR),
            ),
        )
        self.validate_parsing(
            "SELECT CONVERT(age, CHAR) FROM users",
            exp.Cast(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                to=exp.DataType(this=exp.DataType.Type.CHAR),
            ),
        )
        self.validate_parsing(
            "SELECT CONVERT_TZ(signup_date, 'UTC', 'America/New_York') FROM users",
            exp.ConvertTimezone(
                timestamp=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                source_tz=exp.Literal.string("UTC"),
                target_tz=exp.Literal.string("America/New_York"),
            ),
        )
        self.validate_parsing(
            "SELECT COS(age) FROM users",
            exp.func("COS", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT COT(age) FROM users",
            exp.func("COT", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT COUNT(*) FROM users", exp.Count(this=exp.Star(), big_int=True)
        )
        self.validate_parsing(
            "SELECT CRC32(email) FROM users",
            exp.func("CRC32", exp.Column(this=exp.Identifier(this="email", quoted=False))),
        )
        self.validate_parsing("SELECT CURRENT_DATE FROM users", exp.CurrentDate())
        self.validate_parsing("SELECT CURRENT_TIME FROM users", exp.CurrentTime())
        self.validate_parsing("SELECT CURRENT_TIMESTAMP FROM users", exp.CurrentTimestamp())
        self.validate_parsing("SELECT DATABASE()", exp.CurrentSchema())
        self.validate_parsing(
            "SELECT DATE(signup_date) FROM users",
            exp.cast(
                exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                exp.DataType.Type.DATE,
            ),
        )
        self.validate_parsing(
            "SELECT DATE_ADD(signup_date, INTERVAL 1 DAY) FROM users",
            exp.DateAdd(
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                expression=exp.Interval(this=exp.Literal.string("1"), unit=exp.Var(this="DAY")),
            ),
        )
        self.validate_parsing(
            "SELECT DATE_FORMAT(signup_date, '%Y-%m-%d') FROM users",
            exp.TimeToStr(
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                format=exp.Literal.string("%Y-%m-%d"),
            ),
        )
        self.validate_parsing(
            "SELECT DATE_SUB(signup_date, INTERVAL 7 DAY) FROM users",
            exp.DateSub(
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                expression=exp.Interval(this=exp.Literal.string("7"), unit=exp.Var(this="DAY")),
            ),
        )
        self.validate_parsing(
            "SELECT DATE_TRUNC('month', signup_date) FROM users",
            exp.DateTrunc(
                unit=exp.Literal.string("MONTH"),
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT DATEDIFF(CURRENT_DATE, signup_date) FROM users",
            exp.DateDiff(
                this=exp.CurrentDate(),
                expression=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT DAY(signup_date) FROM users",
            exp.Day(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )
        self.validate_parsing(
            "SELECT DAYNAME(signup_date) FROM users",
            exp.TimeToStr(
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                format=exp.Literal.string("%A"),
            ),
        )
        self.validate_parsing(
            "SELECT DAYOFWEEK(signup_date) FROM users",
            exp.DayOfWeek(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )
        self.validate_parsing(
            "SELECT DAYOFYEAR(signup_date) FROM users",
            exp.DayOfYear(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )
        self.validate_parsing(
            "SELECT DECODE(age, 18, 'minor', 21, 'young adult', 'adult') FROM users",
            exp.DecodeCase(
                expressions=[
                    exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    exp.Literal.number(18),
                    exp.Literal.string("minor"),
                    exp.Literal.number(21),
                    exp.Literal.string("young adult"),
                    exp.Literal.string("adult"),
                ]
            ),
        )
        self.validate_parsing(
            "SELECT DEGREES(age) FROM users",
            exp.Mul(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(180 / math.pi),
            ),
        )
        self.validate_parsing(
            "SELECT DENSE_RANK() OVER (ORDER BY age) FROM users", exp.func("DENSE_RANK")
        )
        self.validate_parsing(
            "SELECT DOT_PRODUCT(name, name) FROM users",
            exp.func(
                "DOT_PRODUCT",
                exp.Column(this=exp.Identifier(this="name", quoted=False)),
                exp.Column(this=exp.Identifier(this="name", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT ELT(2, 'a', 'b', 'c') FROM users",
            exp.func(
                "ELT",
                exp.Literal.number(2),
                exp.Literal.string("a"),
                exp.Literal.string("b"),
                exp.Literal.string("c"),
            ),
        )
        self.validate_parsing(
            "SELECT ESTIMATED_QUERY_LEAF_MEMORY() FROM users",
            exp.func("ESTIMATED_QUERY_LEAF_MEMORY"),
        )
        self.validate_parsing(
            "SELECT ESTIMATED_QUERY_RUNTIME() FROM users", exp.func("ESTIMATED_QUERY_RUNTIME")
        )
        self.validate_parsing(
            "SELECT EUCLIDEAN_DISTANCE(name, name) FROM users",
            exp.Distance(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Column(this=exp.Identifier(this="name", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT EXP(age) FROM users",
            exp.Exp(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT EXTRACT(YEAR FROM signup_date) FROM users",
            exp.Extract(
                this=exp.Var(this="YEAR"),
                expression=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT FIELD(name, 'Alice', 'Bob') FROM users",
            exp.Case(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                ifs=[
                    exp.If(
                        this=exp.Literal.string("Alice"),
                        true=exp.Literal.number(1),
                    ),
                    exp.If(
                        this=exp.Literal.string("Bob"),
                        true=exp.Literal.number(2),
                    ),
                ],
                default=exp.Literal.number(0),
            ),
        )
        self.validate_parsing(
            "SELECT FIRST(id) FROM orders",
            exp.First(this=exp.Column(this=exp.Identifier(this="id", quoted=False))),
        )
        self.validate_parsing(
            "SELECT FIRST(age, signup_date) FROM users",
            exp.First(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT FIRST_VALUE(age) OVER (ORDER BY signup_date) FROM users",
            exp.FirstValue(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT FLOOR(age) FROM users",
            exp.Floor(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT FORMAT(age, 2) FROM users",
            exp.func(
                "FORMAT",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Literal.number(2),
            ),
        )
        self.validate_parsing("SELECT FOUND_ROWS()", exp.func("FOUND_ROWS"))
        self.validate_parsing(
            "SELECT FROM_BASE64('SGVsbG8=') FROM users",
            exp.FromBase64(this=exp.Literal.string("SGVsbG8=")),
        )
        self.validate_parsing(
            "SELECT FROM_DAYS(738000) FROM users", exp.func("FROM_DAYS", exp.Literal.number(738000))
        )
        self.validate_parsing(
            "SELECT FROM_UNIXTIME(1609459200) FROM users",
            exp.UnixToTime(this=exp.Literal.number(1609459200)),
        )
        self.validate_parsing(
            "SELECT GEOGRAPHY_AREA(age) FROM users",
            exp.func("GEOGRAPHY_AREA", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT GEOGRAPHY_CONTAINS(age, age) FROM users",
            exp.func(
                "GEOGRAPHY_CONTAINS",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT GEOGRAPHY_DISTANCE(age, age) FROM users",
            exp.func(
                "GEOGRAPHY_DISTANCE",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT GEOGRAPHY_INTERSECTS(age, age) FROM users",
            exp.func(
                "GEOGRAPHY_INTERSECTS",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT GEOGRAPHY_LATITUDE(age) FROM users",
            exp.func(
                "GEOGRAPHY_LATITUDE", exp.Column(this=exp.Identifier(this="age", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT GEOGRAPHY_LENGTH(age) FROM users",
            exp.func("GEOGRAPHY_LENGTH", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT GEOGRAPHY_LONGITUDE(age) FROM users",
            exp.func(
                "GEOGRAPHY_LONGITUDE", exp.Column(this=exp.Identifier(this="age", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT GEOGRAPHY_POINT(48.2, 16.4) FROM users",
            exp.func("GEOGRAPHY_POINT", exp.Literal.number(48.2), exp.Literal.number(16.4)),
        )
        self.validate_parsing(
            "SELECT GEOGRAPHY_WITHIN_DISTANCE(age, age, 100) FROM users",
            exp.func(
                "GEOGRAPHY_WITHIN_DISTANCE",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Literal.number(100),
            ),
        )
        self.validate_parsing(
            "SELECT GET_FORMAT(DATE, 'USA') FROM users",
            exp.func("GET_FORMAT", exp.Var(this="DATE"), exp.Literal.string("USA")),
        )
        self.validate_parsing(
            "SELECT GREATEST(age, 25, 30) FROM users",
            exp.Greatest(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expressions=[exp.Literal.number(25), exp.Literal.number(30)],
            ),
        )
        self.validate_parsing(
            "SELECT GROUP_CONCAT(name) FROM users",
            exp.GroupConcat(this=exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT HEX(email) FROM users",
            exp.Hex(this=exp.Column(this=exp.Identifier(this="email", quoted=False))),
        )
        self.validate_parsing(
            "SELECT HOUR(signup_date) FROM users",
            exp.cast(
                exp.TimeToStr(
                    this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                    format=exp.Literal.string("%-H"),
                ),
                exp.DataType.Type.INT,
            ),
        )
        self.validate_parsing(
            "SELECT IF(age > 18, 'adult', 'minor') FROM users",
            exp.If(
                this=exp.GT(
                    this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    expression=exp.Literal.number(18),
                ),
                true=exp.Literal.string("adult"),
                false=exp.Literal.string("minor"),
            ),
        )
        self.validate_parsing(
            "SELECT age IN (20, 30) FROM users",
            exp.In(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expressions=[exp.Literal.number(20), exp.Literal.number(30)],
            ),
        )
        self.validate_parsing(
            "SELECT age NOT IN (20, 30) FROM users",
            exp.Not(
                this=exp.In(
                    this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    expressions=[exp.Literal.number(20), exp.Literal.number(30)],
                )
            ),
        )
        self.validate_parsing(
            "SELECT INET_ATON('192.168.0.1') FROM users",
            exp.func("INET_ATON", exp.Literal.string("192.168.0.1")),
        )
        self.validate_parsing(
            "SELECT INET_NTOA(3232235521) FROM users",
            exp.func("INET_NTOA", exp.Literal.number(3232235521)),
        )
        self.validate_parsing(
            "SELECT INET6_ATON('2001:db8::1') FROM users",
            exp.func("INET6_ATON", exp.Literal.string("2001:db8::1")),
        )
        self.validate_parsing(
            "SELECT INET6_NTOA(x'20010DB8000000000000000000000001') FROM users",
            exp.func("INET6_NTOA", exp.HexString(this="20010DB8000000000000000000000001")),
        )
        self.validate_parsing(
            "SELECT INITCAP(name) FROM users",
            exp.Initcap(this=exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT INSTR(name, 'a') FROM users",
            exp.StrPosition(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                substr=exp.Literal.string("a"),
            ),
        )
        self.validate_parsing(
            "SELECT IS_BSON_NULL(metadatab) FROM events",
            exp.func(
                "IS_BSON_NULL", exp.Column(this=exp.Identifier(this="metadatab", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT IS_UUID(name) FROM users",
            exp.RegexpILike(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string(
                    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
                ),
            ),
        )
        self.validate_parsing(
            "SELECT ISNULL(email) FROM users",
            exp.Is(
                this=exp.Column(this=exp.Identifier(this="email", quoted=False)),
                expression=exp.Null(),
            ),
        )
        self.validate_parsing(
            "SELECT ISNUMERIC(age) FROM users",
            exp.func("ISNUMERIC", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT JSON_AGG(name) FROM users",
            exp.JSONArrayAgg(this=exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        # TODO: add support of the struct type
        # self.validate_parsing(
        #     "SELECT JSON_AGG(ROW(name, id):>RECORD(name TEXT, id INT)) FROM users",
        #     exp.JSONArrayAgg(
        #         this=exp.Column(this=exp.Identifier(this="name", quoted=False)))
        # )
        self.validate_parsing(
            "SELECT JSON_AGG(users.*) FROM users",
            exp.JSONArrayAgg(
                this=exp.Column(this=exp.Star(), table=exp.Identifier(this="users", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT JSON_AGG(name ORDER BY id) FROM users",
            exp.JSONArrayAgg(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                order=exp.Order(
                    expressions=[
                        exp.Ordered(
                            this=exp.Column(this=exp.Identifier(this="id", quoted=False)),
                            nulls_first=True,
                        )
                    ]
                ),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_ARRAY_CONTAINS_STRING(name, 'value') FROM users",
            exp.func(
                "JSON_ARRAY_CONTAINS_STRING",
                exp.Column(this=exp.Identifier(this="name", quoted=False)),
                exp.Literal.string("value"),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_ARRAY_PACK('[1.0, 2.0]') FROM users",
            exp.func("JSON_ARRAY_PACK", exp.Literal.string("[1.0, 2.0]")),
        )
        self.validate_parsing(
            "SELECT JSON_ARRAY_PUSH_STRING(metadata, 'new') FROM events",
            exp.func(
                "JSON_ARRAY_PUSH_STRING",
                exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                exp.Literal.string("new"),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_ARRAY_UNPACK(name) FROM users",
            exp.func(
                "JSON_ARRAY_UNPACK", exp.Column(this=exp.Identifier(this="name", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT JSON_BUILD_ARRAY(1, 'a', TRUE) FROM users",
            exp.JSONArray(
                expressions=[exp.Literal.number(1), exp.Literal.string("a"), exp.Boolean(this=True)]
            ),
        )
        self.validate_parsing(
            "SELECT JSON_BUILD_OBJECT('name', name, 'age', age) FROM users",
            exp.JSONObject(
                expressions=[
                    exp.JSONKeyValue(
                        this=exp.Literal(this="name", is_string=True),
                        expression=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                    ),
                    exp.JSONKeyValue(
                        this=exp.Literal(this="age", is_string=True),
                        expression=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    ),
                ]
            ),
        )
        self.validate_parsing(
            "SELECT JSON_DELETE_KEY(metadata, 'key') FROM events",
            exp.func(
                "JSON_DELETE_KEY",
                exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                exp.Literal.string("key"),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_EXTRACT_DOUBLE(metadata, '$.name') FROM events",
            exp.func(
                "JSON_EXTRACT_DOUBLE",
                exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                exp.Literal.string("$.name"),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_EXTRACT_STRING(metadata, 'name') FROM events",
            exp.JSONExtractScalar(
                this=exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                expression=exp.JSONPath(
                    expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="name")]
                ),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_EXTRACT_JSON(metadata, 'name') FROM events",
            exp.JSONExtract(
                this=exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                expression=exp.JSONPath(
                    expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="name")]
                ),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_EXTRACT_BIGINT(metadata, '$.name') FROM events",
            exp.func(
                "JSON_EXTRACT_BIGINT",
                exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                exp.Literal.string("$.name"),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_GET_TYPE(metadata) FROM events",
            exp.func(
                "JSON_GET_TYPE", exp.Column(this=exp.Identifier(this="metadata", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT JSON_KEYS(metadata) FROM events",
            exp.func("JSON_KEYS", exp.Column(this=exp.Identifier(this="metadata", quoted=False))),
        )
        self.validate_parsing(
            "SELECT JSON_LENGTH(metadata) FROM events",
            exp.func("JSON_LENGTH", exp.Column(this=exp.Identifier(this="metadata", quoted=False))),
        )
        self.validate_parsing(
            'SELECT JSON_MATCH_ANY(metadata, \'{"key": "value"}\') FROM events',
            exp.func(
                "JSON_MATCH_ANY",
                exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                exp.Literal.string('{"key": "value"}'),
            ),
        )
        self.validate_parsing(
            'SELECT JSON_MERGE_PATCH(metadata, \'{"key": "value"}\') FROM events',
            exp.func(
                "JSON_MERGE_PATCH",
                exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                exp.Literal.string('{"key": "value"}'),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_PRETTY(metadata) FROM events",
            exp.func("JSON_PRETTY", exp.Column(this=exp.Identifier(this="metadata", quoted=False))),
        )
        self.validate_parsing(
            "SELECT JSON_SET_STRING(metadata, '$.key', 'value') FROM events",
            exp.func(
                "JSON_SET_STRING",
                exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                exp.Literal.string("$.key"),
                exp.Literal.string("value"),
            ),
        )
        self.validate_parsing(
            "SELECT JSON_SPLICE_STRING(metadata, 1, 2, 'new') FROM events",
            exp.func(
                "JSON_SPLICE_STRING",
                exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                exp.Literal.number(1),
                exp.Literal.number(2),
                exp.Literal.string("new"),
            ),
        )
        self.validate_parsing(
            "JSON_TO_ARRAY(metadata)",
            exp.func(
                "JSON_TO_ARRAY", exp.Column(this=exp.Identifier(this="metadata", quoted=False))
            ),
            run=False,
        )
        self.validate_parsing(
            "SELECT LAG(age) OVER (ORDER BY signup_date) FROM users",
            exp.Lag(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT LAST(id) FROM orders",
            exp.Last(this=exp.Column(this=exp.Identifier(this="id", quoted=False))),
        )
        self.validate_parsing(
            "SELECT LAST(age, signup_date) FROM users",
            exp.Last(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT LAST_DAY(signup_date) FROM users",
            exp.LastDay(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )
        self.validate_parsing("SELECT LAST_INSERT_ID() FROM users", exp.func("LAST_INSERT_ID"))
        self.validate_parsing(
            "SELECT LAST_VALUE(age) OVER (ORDER BY signup_date) FROM users",
            exp.LastValue(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT LCASE(name) FROM users",
            exp.Lower(this=exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT LEAD(age) OVER (ORDER BY signup_date) FROM users",
            exp.Lead(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT LEAST(age, 18, 30) FROM users",
            exp.Least(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expressions=[exp.Literal.number(18), exp.Literal.number(30)],
            ),
        )
        self.validate_parsing(
            "SELECT LEFT(name, 3) FROM users",
            exp.Left(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.number(3),
            ),
        )
        self.validate_parsing(
            "SELECT LENGTH(name) FROM users",
            exp.Length(this=exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT name LIKE 'A%' FROM users",
            exp.Like(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string("A%"),
            ),
        )
        self.validate_parsing(
            "SELECT name NOT LIKE 'B%' FROM users",
            exp.Not(
                this=exp.Like(
                    this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                    expression=exp.Literal.string("B%"),
                )
            ),
        )
        self.validate_parsing(
            "SELECT LN(age) FROM users",
            exp.Ln(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing("SELECT LOCALTIMESTAMP() FROM users", exp.func("LOCALTIMESTAMP"))
        self.validate_parsing(
            "SELECT LOCATE('a', name, 1) FROM users",
            exp.StrPosition(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                substr=exp.Literal.string("a"),
                position=exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT LOG(10, age) FROM users",
            exp.Log(
                this=exp.Literal.number(10),
                expression=exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT LOG10(age) FROM users",
            exp.Log(
                this=exp.Literal.number(10),
                expression=exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT LOG2(age) FROM users",
            exp.Log(
                this=exp.Literal.number(2),
                expression=exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT LPAD(name, 10, '*') FROM users",
            exp.Pad(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.number(10),
                fill_pattern=exp.Literal.string("*"),
                is_left=True,
            ),
        )
        self.validate_parsing(
            "SELECT LTRIM(name) FROM users",
            exp.Trim(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)), position="LEADING"
            ),
        )
        self.validate_parsing(
            "SELECT MATCH(name) AGAINST ('search term') FROM products",
            exp.MatchAgainst(
                this=exp.Literal.string("search term"),
                expressions=[exp.Column(this=exp.Identifier(this="name", quoted=False))],
            ),
        )
        self.validate_parsing(
            "SELECT MAX(age) FROM users",
            exp.Max(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT MD5(email) FROM users",
            exp.MD5(this=exp.Column(this=exp.Identifier(this="email", quoted=False))),
        )
        self.validate_parsing(
            "SELECT MEDIAN(age) FROM users",
            exp.Median(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT MIN(age) FROM users",
            exp.Min(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT MOD(age, 5) FROM users",
            exp.Mod(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(5),
            ),
        )
        self.validate_parsing(
            "SELECT MONTHS_BETWEEN(CURRENT_DATE, signup_date) FROM users",
            exp.MonthsBetween(
                this=exp.CurrentDate(),
                expression=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT NOPARAM('SELECT * FROM users WHERE age = 25') FROM users",
            exp.func("NOPARAM", exp.Literal.string("SELECT * FROM users WHERE age = 25")),
        )
        self.validate_parsing(
            "SELECT MICROSECOND(signup_date) FROM users",
            exp.cast(
                exp.TimeToStr(
                    this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                    format=exp.Literal.string("%f"),
                ),
                exp.DataType.Type.INT,
            ),
        )
        self.validate_parsing(
            "SELECT MINUTE(signup_date) FROM users",
            exp.cast(
                exp.TimeToStr(
                    this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                    format=exp.Literal.string("%M"),
                ),
                exp.DataType.Type.INT,
            ),
        )
        self.validate_parsing(
            "SELECT MONTH(signup_date) FROM users",
            exp.Month(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )
        self.validate_parsing(
            "SELECT MONTHNAME(signup_date) FROM users",
            exp.TimeToStr(
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                format=exp.Literal.string("%B"),
            ),
        )
        self.validate_parsing(
            "SELECT MATCH(TABLE products2) AGAINST ('search term') FROM products2",
            exp.MatchAgainst(
                this=exp.Literal.string("search term"),
                expressions=[exp.Table(this=exp.Identifier(this="products2", quoted=False))],
            ),
        )
        self.validate_parsing("SELECT NOW() FROM users", exp.CurrentTimestamp())
        self.validate_parsing(
            "SELECT NOW(6) FROM users", exp.CurrentTimestamp(this=exp.Literal.number(6))
        )
        self.validate_parsing(
            "SELECT NTH_VALUE(age, 2) OVER (ORDER BY signup_date) FROM users",
            exp.NthValue(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                offset=exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT NTILE(4) OVER (ORDER BY age) FROM users",
            exp.func("NTILE", exp.Literal.number(4)),
        )
        self.validate_parsing(
            "SELECT NULLIF(age, 0) FROM users",
            exp.Nullif(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(0),
            ),
        )
        self.validate_parsing(
            "SELECT IFNULL(age, 18) FROM users",
            exp.Coalesce(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expressions=[exp.Literal.number(18)],
                is_nvl=True,
            ),
        )
        self.validate_parsing(
            "SELECT NVL(age, 18) FROM users",
            exp.Coalesce(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expressions=[exp.Literal.number(18)],
                is_nvl=True,
            ),
        )
        self.validate_parsing(
            "SELECT PERCENT_RANK() OVER (ORDER BY age) FROM users", exp.func("PERCENT_RANK")
        )
        self.validate_parsing(
            "SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY age) FROM users",
            exp.PercentileCont(this=exp.Literal.number(0.75)),
        )
        self.validate_parsing(
            "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY age) FROM users",
            exp.PercentileDisc(this=exp.Literal.number(0.9)),
        )
        self.validate_parsing("SELECT PI() FROM users", exp.func("PI"))
        self.validate_parsing(
            "SELECT POW(age, 2) FROM users",
            exp.Pow(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT QUARTER(signup_date) FROM users",
            exp.Quarter(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )
        self.validate_parsing(
            "SELECT QUOTE(name) FROM users",
            exp.func("QUOTE", exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT RADIANS(age) FROM users",
            exp.Mul(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(0.017453292519943295),
            ),
        )
        self.validate_parsing("SELECT RAND() FROM users", exp.Rand())
        self.validate_parsing("SELECT RAND(1) FROM users", exp.Rand(this=exp.Literal.number(1)))
        self.validate_parsing("SELECT RANK() OVER (ORDER BY age) FROM users", exp.func("RANK"))
        # TODO: make reduce supported
        self.validate_parsing(
            "SELECT REDUCE( 0, JSON_TO_ARRAY('[1,2,3,4]'), REDUCE_ACC() + REDUCE_VALUE() ) AS Result",
            exp.Reduce(
                this=exp.Literal.number(0),
                initial=exp.func("JSON_TO_ARRAY", exp.Literal(this="[1,2,3,4]", is_string=True)),
                merge=exp.Add(this=exp.func("REDUCE_ACC"), expression=exp.func("REDUCE_VALUE")),
            ),
        )
        self.validate_parsing(
            "SELECT REGEXP_INSTR(name, '^A') FROM users",
            exp.func(
                "REGEXP_INSTR",
                exp.Column(this=exp.Identifier(this="name", quoted=False)),
                exp.Literal.string("^A"),
            ),
        )
        self.validate_parsing(
            "SELECT REGEXP_MATCH(name, '[A-Za-z]+', 'c') FROM users",
            exp.RegexpExtractAll(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string("[A-Za-z]+"),
                parameters=exp.Literal.string("c"),
            ),
        )
        self.validate_parsing(
            "SELECT REGEXP_REPLACE(name, 'a', 'x', 'c') FROM users",
            exp.RegexpReplace(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string("a"),
                replacement=exp.Literal.string("x"),
                modifiers=exp.Literal.string("c"),
            ),
        )
        self.validate_parsing(
            "SELECT REGEXP_SUBSTR(name, '[A-Z][a-z]+', 1, 2, 'c') FROM users",
            exp.RegexpExtract(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string("[A-Z][a-z]+"),
                position=exp.Literal.number(1),
                occurrence=exp.Literal.number(2),
                parameters=exp.Literal.string("c"),
            ),
        )
        self.validate_parsing(
            "SELECT REPLACE(name, 'a', 'o') FROM users",
            exp.func(
                "REPLACE",
                exp.Column(this=exp.Identifier(this="name", quoted=False)),
                exp.Literal.string("a"),
                exp.Literal.string("o"),
            ),
        )
        self.validate_parsing(
            "SELECT REVERSE(name) FROM users",
            exp.func("REVERSE", exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT RIGHT(name, 3) FROM users",
            exp.Right(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.number(3),
            ),
        )
        self.validate_parsing(
            "SELECT name RLIKE '^[A-Z]' FROM users",
            exp.RegexpLike(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string("^[A-Z]"),
            ),
        )
        self.validate_parsing(
            "SELECT ROUND(age, 0) FROM users",
            exp.Round(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                decimals=exp.Literal.number(0),
            ),
        )
        self.validate_parsing("SELECT ROW_COUNT() FROM users", exp.func("ROW_COUNT"))
        self.validate_parsing("SELECT ROW_NUMBER() OVER (ORDER BY age) FROM users", exp.RowNumber())
        self.validate_parsing(
            "SELECT RPAD(name, 10, '*') FROM users",
            exp.Pad(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.number(10),
                fill_pattern=exp.Literal.string("*"),
                is_left=False,
            ),
        )
        self.validate_parsing(
            "SELECT RTRIM(name) FROM users",
            exp.Trim(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)), position="TRAILING"
            ),
        )
        self.validate_parsing(
            "SELECT SCALAR_VECTOR_MUL(2, JSON_ARRAY_PACK('[0.1, 0.8, 0.2, 0.555]')) FROM users",
            exp.func(
                "SCALAR_VECTOR_MUL",
                exp.Literal.number(2),
                exp.func("JSON_ARRAY_PACK", exp.Literal.string("[0.1, 0.8, 0.2, 0.555]")),
            ),
        )
        self.validate_parsing(
            "SELECT SEC_TO_TIME(3600) FROM users", exp.func("SEC_TO_TIME", exp.Literal.number(3600))
        )
        self.validate_parsing(
            "SELECT MONTHNAME(signup_date) FROM users",
            exp.TimeToStr(
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                format=exp.Literal.string("%B"),
            ),
        )
        self.validate_parsing(
            "SELECT SECOND(signup_date) FROM users",
            exp.cast(
                exp.TimeToStr(
                    this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                    format=exp.Literal.string("%S"),
                ),
                exp.DataType.Type.INT,
            ),
        )
        self.validate_parsing(
            "SELECT SECRET('password') FROM users",
            exp.func("SECRET", exp.Literal.string("password")),
        )
        self.validate_parsing(
            "SELECT SHA1(email) FROM users",
            exp.SHA(this=exp.Column(this=exp.Identifier(this="email", quoted=False))),
        )
        self.validate_parsing(
            "SELECT SHA2(email, 256) FROM users",
            exp.SHA2(
                this=exp.Column(this=exp.Identifier(this="email", quoted=False)),
                length=exp.Literal.number(256),
            ),
        )
        self.validate_parsing(
            "SELECT SIGMOID(age) FROM users",
            exp.Div(
                this=exp.Literal.number(1),
                expression=exp.Paren(
                    this=exp.Add(
                        this=exp.Literal.number(1),
                        expression=exp.Exp(
                            this=exp.Neg(
                                this=exp.Column(this=exp.Identifier(this="age", quoted=False))
                            )
                        ),
                    )
                ),
            ),
        )
        self.validate_parsing(
            "SELECT SIGN(age) FROM users",
            exp.Sign(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT SIN(age) FROM users",
            exp.func("SIN", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT SLEEP(2) FROM users", exp.func("SLEEP", exp.Literal.number(2))
        )
        self.validate_parsing(
            "SPLIT(name, '-')",
            exp.Split(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string("-"),
            ),
            run=False,
        )
        self.validate_parsing(
            "SELECT SQRT(age) FROM users",
            exp.Sqrt(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT STD(age) FROM users",
            exp.Stddev(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT STDDEV(age) FROM users",
            exp.Stddev(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT STDDEV_POP(age) FROM users",
            exp.StddevPop(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT STDDEV_SAMP(age) FROM users",
            exp.StddevSamp(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT STR_TO_DATE('2024-12-31', '%Y-%m-%d') FROM users",
            exp.StrToDate(
                this=exp.Literal.string("2024-12-31"), format=exp.Literal.string("%Y-%m-%d")
            ),
        )
        # TODO: think about rewriting this using CASE
        self.validate_parsing(
            "SELECT STRCMP(name, 'Alice') FROM users",
            exp.func(
                "STRCMP",
                exp.Column(this=exp.Identifier(this="name", quoted=False)),
                exp.Literal.string("Alice"),
            ),
        )
        self.validate_parsing(
            "STRING_BYTES(name)",
            exp.func("STRING_BYTES", exp.Column(this=exp.Identifier(this="name", quoted=False))),
            run=False,
        )
        self.validate_parsing(
            "SELECT SUBSTRING(name, 1, 3) FROM users",
            exp.Substring(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                start=exp.Literal.number(1),
                length=exp.Literal.number(3),
            ),
        )
        self.validate_parsing(
            "SELECT SUBSTRING(name, 1) FROM users",
            exp.Substring(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                start=exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT SUBSTRING(name FROM 1 FOR 3) FROM users",
            exp.Substring(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                start=exp.Literal.number(1),
                length=exp.Literal.number(3),
            ),
        )
        self.validate_parsing(
            "SELECT SUBSTRING(name FROM 1) FROM users",
            exp.Substring(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                start=exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT SUBSTRING_INDEX(name, '-', 2) FROM users",
            exp.func(
                "SUBSTRING_INDEX",
                exp.Column(this=exp.Identifier(this="name", quoted=False)),
                exp.Literal.string("-"),
                exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT SUM(age) FROM users",
            exp.Sum(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing("SELECT UUID() FROM users", exp.Uuid())
        self.validate_parsing(
            "SELECT TAN(age) FROM users",
            exp.func("TAN", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT TIME(signup_date) FROM users",
            exp.cast(
                exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                exp.DataType.Type.TIME,
            ),
        )
        self.validate_parsing(
            "SELECT TIME_BUCKET('1d') FROM orders", exp.DateBin(this=exp.Literal.string("1d"))
        )
        self.validate_parsing(
            "SELECT TIME_BUCKET('1d', '2019-03-14 06:04:12')",
            exp.DateBin(
                this=exp.Literal.string("1d"), expression=exp.Literal.string("2019-03-14 06:04:12")
            ),
        )
        self.validate_parsing(
            "SELECT TIME_BUCKET('1d', '2019-03-14 06:04:12', '2019-03-13 03:00:00')",
            exp.DateBin(
                this=exp.Literal.string("1d"),
                expression=exp.Literal.string("2019-03-14 06:04:12"),
                offset=exp.Literal.string("2019-03-13 03:00:00"),
            ),
        )
        self.validate_parsing(
            "SELECT TIME_FORMAT(signup_date, '%H:%i:%s') FROM users",
            exp.TimeToStr(
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                format=exp.Literal.string("%H:%M:%S"),
            ),
        )
        self.validate_parsing(
            "SELECT TIME_TO_SEC(signup_date) FROM users",
            exp.func(
                "TIME_TO_SEC", exp.Column(this=exp.Identifier(this="signup_date", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT TIMEDIFF(signup_date, CURRENT_TIMESTAMP) FROM users",
            exp.TimeDiff(
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                expression=exp.CurrentTimestamp(),
            ),
        )
        self.validate_parsing(
            "SELECT TIMESTAMP('2024-01-01 10:00:00') FROM users",
            exp.Timestamp(this=exp.Literal.string("2024-01-01 10:00:00")),
        )
        self.validate_parsing(
            "SELECT TIMESTAMPADD(DAY, 7, signup_date) FROM users",
            exp.TimestampAdd(
                unit=exp.Var(this="DAY"),
                expression=exp.Literal.number(7),
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT TIMESTAMPDIFF(DAY, signup_date, CURRENT_DATE) FROM users",
            exp.TimestampDiff(
                unit=exp.Var(this="DAY"),
                expression=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                this=exp.CurrentDate(),
            ),
        )
        self.validate_parsing(
            "SELECT TO_BASE64(email) FROM users",
            exp.ToBase64(this=exp.Column(this=exp.Identifier(this="email", quoted=False))),
        )
        self.validate_parsing(
            "SELECT TO_CHAR(signup_date, 'YYYY-MM-DD') FROM users",
            exp.TimeToStr(
                this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                format=exp.Literal.string("%Y-%m-%d"),
            ),
        )
        self.validate_parsing(
            "SELECT TO_DATE('2024-01-01', 'YYYY-MM-DD') FROM users",
            exp.StrToDate(
                this=exp.Literal.string("2024-01-01"), format=exp.Literal.string("%Y-%m-%d")
            ),
        )
        self.validate_parsing(
            "SELECT TO_DAYS(signup_date) FROM users",
            exp.func("TO_DAYS", exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )

        self.validate_parsing(
            "SELECT TO_JSON(name) FROM users",
            exp.func("TO_JSON", exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT TO_JSON(users.*) FROM users",
            exp.func(
                "TO_JSON",
                exp.Column(this=exp.Star(), table=exp.Identifier(this="users", quoted=False)),
            ),
        )
        # TODO: add support of the struct type
        # self.validate_parsing(
        #     "SELECT TO_JSON(ROW(1,2):>RECORD(a INT, b INT)) AS RowOutput",
        #     exp.func("TO_JSON",
        #              exp.Column(this=exp.Identifier(this="name", quoted=False)))
        # )
        self.validate_parsing(
            "SELECT TO_NUMBER(age, '999999.99') FROM users",
            exp.ToNumber(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                format=exp.Literal.string("999999.99"),
            ),
        )

        self.validate_parsing(
            "SELECT TO_SECONDS(signup_date) FROM users",
            exp.func(
                "TO_SECONDS", exp.Column(this=exp.Identifier(this="signup_date", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT TO_TIMESTAMP('The date and time are 01/01/2018 2:30:15.123456', 'The date and time are MM/DD/YYYY HH:MI:SS.FF6') AS result",
            exp.StrToTime(
                this=exp.Literal.string("The date and time are 01/01/2018 2:30:15.123456"),
                format=exp.Literal.string("The date and time are %m/%d/%Y %I:%M:%S.%f"),
            ),
        )
        self.validate_parsing(
            "SELECT TRIM(name) FROM users",
            exp.Trim(this=exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT TRUNC(signup_date, 'Y') FROM users",
            exp.func(
                "TRUNC",
                exp.Column(this=exp.Identifier(this="signup_date", quoted=False)),
                exp.Literal.string("Y"),
            ),
        )
        self.validate_parsing(
            "SELECT TRUNCATE(age, 1) FROM users",
            exp.func(
                "TRUNCATE",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT UCASE(name) FROM users",
            exp.Upper(this=exp.Column(this=exp.Identifier(this="name", quoted=False))),
        )
        self.validate_parsing(
            "SELECT UNHEX('4D2') FROM users", exp.Unhex(this=exp.Literal.string("4D2"))
        )
        self.validate_parsing(
            "SELECT UNIX_TIMESTAMP(signup_date) FROM users",
            exp.StrToUnix(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )
        self.validate_parsing("SELECT USER() FROM users", exp.CurrentUser())
        self.validate_parsing(
            "SELECT UTC_DATE() FROM users", exp.CurrentDate(this=exp.Literal.string("UTC"))
        )
        self.validate_parsing(
            "SELECT UTC_TIME() FROM users", exp.CurrentTime(this=exp.Literal.string("UTC"))
        )
        self.validate_parsing(
            "SELECT UTC_TIMESTAMP() FROM users",
            exp.CurrentTimestamp(this=exp.Literal.string("UTC")),
        )
        self.validate_parsing(
            "SELECT UUID_TO_BIN(UUID()) FROM users", exp.func("UUID_TO_BIN", exp.func("UUID"))
        )
        self.validate_parsing(
            "SELECT VARIANCE(age) FROM users",
            exp.VariancePop(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT VAR_SAMP(age) FROM users",
            exp.Variance(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT VECTOR_ADD(age, age) FROM users",
            exp.func(
                "VECTOR_ADD",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT VECTOR_ELEMENTS_SUM(age) FROM users",
            exp.func(
                "VECTOR_ELEMENTS_SUM", exp.Column(this=exp.Identifier(this="age", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT VECTOR_KTH_ELEMENT(age, 2) FROM users",
            exp.func(
                "VECTOR_KTH_ELEMENT",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT VECTOR_MUL(age, age) FROM users",
            exp.func(
                "VECTOR_MUL",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT VECTOR_NUM_ELEMENTS(age) FROM users",
            exp.func(
                "VECTOR_NUM_ELEMENTS", exp.Column(this=exp.Identifier(this="age", quoted=False))
            ),
        )
        self.validate_parsing(
            "SELECT VECTOR_SORT(age) FROM users",
            exp.func("VECTOR_SORT", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )

        self.validate_parsing(
            "SELECT VECTOR_SUB(age, age) FROM users",
            exp.func(
                "VECTOR_SUB",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT VECTOR_SUBVECTOR(age, 1, 2) FROM users",
            exp.func(
                "VECTOR_SUBVECTOR",
                exp.Column(this=exp.Identifier(this="age", quoted=False)),
                exp.Literal.number(1),
                exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT VECTOR_SUM(age) FROM users",
            exp.func("VECTOR_SUM", exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT WEEK(signup_date) FROM users",
            exp.Week(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )
        self.validate_parsing(
            "SELECT WEEKDAY(signup_date) FROM users",
            exp.DayOfWeek(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )
        self.validate_parsing(
            "SELECT YEAR(signup_date) FROM users",
            exp.Year(this=exp.Column(this=exp.Identifier(this="signup_date", quoted=False))),
        )

    def test_operators_parsing(self):
        self.validate_parsing(
            "SELECT age BETWEEN 18 AND 30 FROM users",
            exp.Between(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                low=exp.Literal.number(18),
                high=exp.Literal.number(30),
            ),
        )
        self.validate_parsing(
            "SELECT age NOT BETWEEN 18 AND 30 FROM users",
            exp.Not(
                this=exp.Between(
                    this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    low=exp.Literal.number(18),
                    high=exp.Literal.number(30),
                )
            ),
        )
        self.validate_parsing(
            "SELECT age = 25 FROM users",
            exp.EQ(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(25),
            ),
        )
        self.validate_parsing(
            "SELECT age <=> 25 FROM users",
            exp.NullSafeEQ(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(25),
            ),
        )
        self.validate_parsing(
            "SELECT age > 25 FROM users",
            exp.GT(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(25),
            ),
        )
        self.validate_parsing(
            "SELECT age >= 25 FROM users",
            exp.GTE(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(25),
            ),
        )
        self.validate_parsing(
            "SELECT age < 25 FROM users",
            exp.LT(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(25),
            ),
        )
        self.validate_parsing(
            "SELECT age <= 25 FROM users",
            exp.LTE(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(25),
            ),
        )
        self.validate_parsing(
            "SELECT age IN (20, 25, 30) FROM users",
            exp.In(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expressions=[
                    exp.Literal.number(20),
                    exp.Literal.number(25),
                    exp.Literal.number(30),
                ],
            ),
        )
        self.validate_parsing(
            "SELECT age NOT IN (20, 25, 30) FROM users",
            exp.Not(
                this=exp.In(
                    this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    expressions=[
                        exp.Literal.number(20),
                        exp.Literal.number(25),
                        exp.Literal.number(30),
                    ],
                )
            ),
        )
        self.validate_parsing(
            "SELECT is_active IS TRUE FROM users",
            exp.Is(
                this=exp.Column(this=exp.Identifier(this="is_active", quoted=False)),
                expression=exp.Boolean(this=True),
            ),
        )
        self.validate_parsing(
            "SELECT is_active IS NOT TRUE FROM users",
            exp.Not(
                this=exp.Is(
                    this=exp.Column(this=exp.Identifier(this="is_active", quoted=False)),
                    expression=exp.Boolean(this=True),
                )
            ),
        )
        self.validate_parsing(
            "SELECT age IS NULL FROM users",
            exp.Is(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Null(),
            ),
        )
        self.validate_parsing(
            "SELECT age IS NOT NULL FROM users",
            exp.Not(
                this=exp.Is(
                    this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    expression=exp.Null(),
                )
            ),
        )
        self.validate_parsing(
            "SELECT ISNULL(email) FROM users",
            exp.Is(
                this=exp.Column(this=exp.Identifier(this="email", quoted=False)),
                expression=exp.Null(),
            ),
        )
        self.validate_parsing(
            "SELECT name LIKE 'A%' FROM users",
            exp.Like(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string("A%"),
            ),
        )
        self.validate_parsing(
            "SELECT name NOT LIKE 'A%' FROM users",
            exp.Not(
                this=exp.Like(
                    this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                    expression=exp.Literal.string("A%"),
                )
            ),
        )
        self.validate_parsing(
            "SELECT name != 'John' FROM users",
            exp.NEQ(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string("John"),
            ),
        )
        self.validate_parsing(
            "SELECT name <> 'John' FROM users",
            exp.NEQ(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                expression=exp.Literal.string("John"),
            ),
        )
        self.validate_parsing(
            "SELECT STRCMP(name, 'Alice') FROM users",
            exp.func(
                "STRCMP",
                exp.Column(this=exp.Identifier(this="name", quoted=False)),
                exp.Literal.string("Alice"),
            ),
        )
        self.validate_parsing(
            "SELECT age + 1 FROM users",
            exp.Add(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT age - 1 FROM users",
            exp.Sub(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT age * 2 FROM users",
            exp.Mul(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT age / 2 FROM users",
            exp.Div(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(2),
                typed=False,
                safe=True,
            ),
        )
        self.validate_parsing(
            "SELECT age % 2 FROM users",
            exp.Mod(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SET @x = 5",
            exp.EQ(this=exp.Parameter(this=exp.Var(this="x")), expression=exp.Literal.number(5)),
        )
        self.validate_parsing(
            "SELECT age & 1 FROM users",
            exp.BitwiseAnd(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT age | 1 FROM users",
            exp.BitwiseOr(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT age ^ 1 FROM users",
            exp.BitwiseXor(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT ~age FROM users",
            exp.BitwiseNot(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
        )
        self.validate_parsing(
            "SELECT age << 2 FROM users",
            exp.BitwiseLeftShift(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(2),
            ),
        )
        self.validate_parsing(
            "SELECT age >> 1 FROM users",
            exp.BitwiseRightShift(
                this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                expression=exp.Literal.number(1),
            ),
        )
        self.validate_parsing(
            "SELECT age > 18 AND is_active FROM users",
            exp.And(
                this=exp.GT(
                    this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    expression=exp.Literal.number(18),
                ),
                expression=exp.Column(this=exp.Identifier(this="is_active", quoted=False)),
            ),
        )
        self.validate_parsing(
            "SELECT is_active OR age > 65 FROM users",
            exp.Or(
                this=exp.Column(this=exp.Identifier(this="is_active", quoted=False)),
                expression=exp.GT(
                    this=exp.Column(this=exp.Identifier(this="age", quoted=False)),
                    expression=exp.Literal.number(65),
                ),
            ),
        )
        self.validate_parsing(
            "SELECT NOT is_active FROM users",
            exp.Not(this=exp.Column(this=exp.Identifier(this="is_active", quoted=False))),
        )
        self.validate_parsing(
            "SELECT is_active && TRUE FROM users",
            exp.And(
                this=exp.Column(this=exp.Identifier(this="is_active", quoted=False)),
                expression=exp.Boolean(this=True),
            ),
        )
        self.validate_parsing(
            "SELECT is_active || FALSE FROM users",
            exp.Or(
                this=exp.Column(this=exp.Identifier(this="is_active", quoted=False)),
                expression=exp.Boolean(this=False),
            ),
        )
        self.validate_parsing(
            "SELECT !is_active FROM users",
            exp.Not(this=exp.Column(this=exp.Identifier(this="is_active", quoted=False))),
        )
        self.validate_parsing(
            "SELECT log(age):>TEXT FROM users",
            exp.Cast(
                this=exp.Log(this=exp.Column(this=exp.Identifier(this="age", quoted=False))),
                to=exp.DataType(this=exp.DataType.Type.TEXT, nested=False),
            ),
        )
        self.validate_parsing(
            "SELECT name!:>TEXT FROM users",
            exp.TryCast(
                this=exp.Column(this=exp.Identifier(this="name", quoted=False)),
                to=exp.DataType(this=exp.DataType.Type.TEXT, nested=False),
            ),
        )
        self.validate_parsing(
            "SELECT metadata::data::data1 FROM events",
            exp.JSONExtract(
                this=exp.JSONExtract(
                    this=exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                    expression=exp.JSONPath(
                        expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="data")]
                    ),
                    only_json_types=False,
                ),
                expression=exp.JSONPath(
                    expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="data1")]
                ),
                only_json_types=False,
            ),
        )
        self.validate_parsing(
            "SELECT metadata::data::`1` FROM events",
            exp.JSONExtract(
                this=exp.JSONExtract(
                    this=exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                    expression=exp.JSONPath(
                        expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="data")]
                    ),
                    only_json_types=False,
                ),
                expression=exp.JSONPath(
                    expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="1")]
                ),
                only_json_types=False,
            ),
        )
        # TODO: Distinguishing between extracting numeric and string values
        self.validate_parsing(
            "SELECT metadata::data::$`1` FROM events",
            exp.JSONExtractScalar(
                this=exp.JSONExtract(
                    this=exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                    expression=exp.JSONPath(
                        expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="data")]
                    ),
                    only_json_types=False,
                ),
                expression=exp.JSONPath(
                    expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="1")]
                ),
                only_json_types=False,
            ),
        )
        self.validate_parsing(
            "SELECT metadata::data::%`1` FROM events",
            exp.JSONExtractScalar(
                this=exp.JSONExtract(
                    this=exp.Column(this=exp.Identifier(this="metadata", quoted=False)),
                    expression=exp.JSONPath(
                        expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="data")]
                    ),
                    only_json_types=False,
                ),
                expression=exp.JSONPath(
                    expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this="1")]
                ),
                only_json_types=False,
            ),
        )
        # TODO: Handle assignment in SET command "SET @x := 5"
        self.validate_parsing(
            "@x := 5",
            exp.PropertyEQ(
                this=exp.Parameter(this=exp.Var(this="x")), expression=exp.Literal.number(5)
            ),
            run=False,
        )

    def test_alter_database_parsing(self):
        self.validate_parsing(
            "ALTER DATABASE db WITH SYNC REPLICATION",
            exp.Alter(
                this=exp.Table(this=exp.Identifier(this="db", kind="SCHEMA")),
                kind="DATABASE",
                actions=[exp.AlterSet(expressions=[exp.Var(this="SYNC REPLICATION")])],
            ),
        )
        self.validate_parsing(
            "ALTER DATABASE db WITH ASYNC REPLICATION",
            exp.Alter(
                this=exp.Table(this=exp.Identifier(this="db", kind="SCHEMA")),
                kind="DATABASE",
                actions=[exp.AlterSet(expressions=[exp.Var(this="ASYNC REPLICATION")])],
            ),
        )
        self.validate_parsing(
            "ALTER DATABASE db SET SYNC REPLICATION",
            exp.Alter(
                this=exp.Table(this=exp.Identifier(this="db", kind="SCHEMA")),
                kind="DATABASE",
                actions=[exp.AlterSet(expressions=[exp.Var(this="SYNC REPLICATION")])],
            ),
        )
        self.validate_parsing(
            "ALTER DATABASE db SET ASYNC REPLICATION",
            exp.Alter(
                this=exp.Table(this=exp.Identifier(this="db", kind="SCHEMA")),
                kind="DATABASE",
                actions=[exp.AlterSet(expressions=[exp.Var(this="ASYNC REPLICATION")])],
            ),
        )
