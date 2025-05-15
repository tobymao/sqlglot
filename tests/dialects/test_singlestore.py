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
    created_at TIMESTAMP SERIES TIMESTAMP
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
            cur.execute("""CREATE FUNCTION is_prime(n BIGINT NOT NULL) returns BIGINT AS
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
            self.assertIn(
                error_message,
                str(ctx.exception),
            )

        generated = query.sql(dialect="singlestore")
        print(generated)

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

    def test_bracket(self):
        self.validate_generation(
            sql="SELECT arr[1] FROM events",
            error_message="Arrays are not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.Bracket,
            run=False
        )

    def test_aggregate_functions_generation(self):
        self.validate_generation(
            sql="SELECT quantileGK(100, 0.95)(reading) OVER (PARTITION BY id) FROM table",
            expected_sql="SELECT QUANTILEGK(100, 0.95)(reading) OVER (PARTITION BY id) FROM table",
            from_dialect="clickhouse",
            error_message="Parametrized aggregate functions are not supported in SingleStore",
            exp_type=exp.ParameterizedAgg,
            run=False
        )
        self.validate_generation(
            sql="select quantileGKIf(100, 0.95)(reading) OVER (PARTITION BY id) FROM table",
            expected_sql="SELECT QUANTILEGKIF(100, 0.95)(reading) OVER (PARTITION BY id) FROM table",
            from_dialect="clickhouse",
            error_message="Parametrized aggregate functions are not supported in SingleStore",
            exp_type=exp.CombinedParameterizedAgg,
            run=False
        )
        self.validate_generation(
            sql="select quantileGK(100, 0.95) OVER (PARTITION BY id) FROM table",
            expected_sql="SELECT QUANTILEGK(100, 0.95) OVER (PARTITION BY id) FROM table",
            from_dialect="clickhouse",
            error_message="Anonymous aggregate functions are not supported in SingleStore",
            exp_type=exp.AnonymousAggFunc,
            run=False
        )
        self.validate_generation(
            sql="select quantileGKIf(100, 0.95) OVER (PARTITION BY id) FROM table",
            expected_sql="SELECT QUANTILEGKIF(100, 0.95) OVER (PARTITION BY id) FROM table",
            from_dialect="clickhouse",
            error_message="Aggregate function combinators are not supported in SingleStore",
            exp_type=exp.CombinedAggFunc,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARG_MAX(name, age) FROM users",
            error_message="ARG_MAX function is not supported in SingleStore",
            exp_type=exp.ArgMax,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARG_MIN(name, age) FROM users",
            error_message="ARG_MIN function is not supported in SingleStore",
            exp_type=exp.ArgMin,
            run=False
        )
        self.validate_generation(
            sql="SELECT APPROX_TOP_K(category, 10) FROM products",
            error_message="APPROX_TOP_K function is not supported in SingleStore",
            exp_type=exp.ApproxTopK,
            run=False
        )
        self.validate_generation(
            sql="SELECT HLL(user_id) FROM orders",
            expected_sql="SELECT APPROX_COUNT_DISTINCT(user_id) FROM orders",
            exp_type=exp.Hll)
        self.validate_generation(
            sql="SELECT APPROX_DISTINCT(email) FROM users",
            expected_sql="SELECT APPROX_COUNT_DISTINCT(email) FROM users",
            exp_type=exp.ApproxDistinct)
        self.validate_generation(
            sql="SELECT ARRAY_AGG(name) FROM users",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayAgg,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_UNIQUE_AGG(category) FROM products",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayUniqueAgg,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_UNION_AGG(category) FROM products",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayUnionAgg,
            run=False
        )
        self.validate_generation(
            sql="SELECT AVG(amount) FROM orders",
            exp_type=exp.Avg)
        self.validate_generation(
            sql="SELECT ANY_VALUE(email) FROM users",
            exp_type=exp.AnyValue)
        self.validate_generation(
            sql="SELECT LAG(amount) OVER (ORDER BY created_at) FROM orders",
            exp_type=exp.Lag)
        self.validate_generation(
            sql="SELECT LEAD(amount) OVER (ORDER BY created_at) FROM orders",
            exp_type=exp.Lead)
        self.validate_generation(
            sql="SELECT FIRST(id) FROM orders",
            exp_type=exp.First)
        self.validate_generation(
            sql="SELECT LAST(id) FROM orders",
            exp_type=exp.Last)
        self.validate_generation(
            sql="SELECT FIRST_VALUE(amount) OVER (ORDER BY created_at) FROM orders",
            exp_type=exp.FirstValue)
        self.validate_generation(
            sql="SELECT LAST_VALUE(amount) OVER (ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM orders",
            exp_type=exp.LastValue)
        self.validate_generation(
            sql="SELECT NTH_VALUE(amount, 2) OVER (ORDER BY created_at) FROM orders",
            exp_type=exp.NthValue)
        self.validate_generation(
            sql="SELECT COUNT(*) FROM users",
            exp_type=exp.Count)
        self.validate_generation(
            sql="SELECT COUNT_IF(age > 18) FROM users",
            expected_sql="SELECT SUM(CASE WHEN age > 18 THEN 1 ELSE 0 END) FROM users",
            exp_type=exp.CountIf)
        self.validate_generation(
            sql="SELECT GROUP_CONCAT(name) FROM users",
            exp_type=exp.GroupConcat)
        self.validate_generation(
            sql="SELECT JSON_OBJECTAGG(id: name) FROM users",
            from_dialect="postgres",
            error_message="JSON_OBJECT_AGG function is not supported in SingleStore",
            exp_type=exp.JSONObjectAgg,
            run=False
        )
        self.validate_generation(
            sql="SELECT JSONB_OBJECT_AGG(id, name) FROM users",
            expected_sql="SELECT J_S_O_N_B_OBJECT_AGG(id, name) FROM users",
            from_dialect="postgres",
            error_message="JSONB_OBJECT_AGG function is not supported in SingleStore",
            exp_type=exp.JSONBObjectAgg,
            run=False
        )
        self.validate_generation(
            sql="SELECT BOOL_OR(is_active) FROM users",
            expected_sql="SELECT MAX(ABS(is_active)) FROM users",
            exp_type=exp.LogicalOr)
        self.validate_generation(
            sql="SELECT BOOL_AND(is_active) FROM users",
            expected_sql="SELECT MIN(ABS(is_active)) FROM users",
            exp_type=exp.LogicalAnd)
        self.validate_generation(
            sql="SELECT MAX(price) FROM products",
            exp_type=exp.Max)
        self.validate_generation(
            sql="SELECT MEDIAN(age) FROM users",
            exp_type=exp.Median)
        self.validate_generation(
            sql="SELECT MIN(age) FROM users",
            exp_type=exp.Min)
        self.validate_generation(
            sql="SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) FROM orders",
            exp_type=exp.PercentileCont)
        self.validate_generation(
            sql="SELECT PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY age) FROM users",
            exp_type=exp.PercentileDisc)
        self.validate_generation(
            sql="SELECT QUANTILE(amount, 0.9) FROM orders",
            expected_sql="SELECT APPROX_PERCENTILE(amount, 0.9) FROM orders",
            error_message="QUANTILE function is not supported in SingleStore",
            exp_type=exp.Quantile)
        self.validate_generation(
            sql="SELECT APPROX_QUANTILE(amount, 0.9) FROM orders",
            expected_sql="SELECT APPROX_PERCENTILE(amount, 0.9) FROM orders",
            exp_type=exp.ApproxQuantile)
        self.validate_generation(
            sql="SELECT SUM(amount) FROM orders",
            exp_type=exp.Sum)
        self.validate_generation(
            sql="SELECT STDDEV(amount) FROM orders",
            exp_type=exp.Stddev)
        self.validate_generation(
            sql="SELECT STDDEV_POP(amount) FROM orders",
            exp_type=exp.StddevPop)
        self.validate_generation(
            sql="SELECT STDDEV_SAMP(amount) FROM orders",
            exp_type=exp.StddevSamp)
        self.validate_generation(
            sql="SELECT VARIANCE(amount) FROM orders",
            expected_sql="SELECT VAR_SAMP(amount) FROM orders",
            exp_type=exp.Variance)
        self.validate_generation(
            sql="SELECT VAR_POP(amount) FROM orders",
            expected_sql="SELECT VAR_POP(amount) FROM orders",
            exp_type=exp.VariancePop)
        self.validate_generation(
            sql="SELECT CORR(user_id, amount) FROM orders",
            error_message="CORR function is not supported in SingleStore",
            exp_type=exp.Corr,
            run=False
        )
        self.validate_generation(
            sql="SELECT COVAR_SAMP(user_id, amount) FROM orders",
            error_message="COVAR_SAMP function is not supported in SingleStore",
            exp_type=exp.CovarSamp,
            run=False
        )
        self.validate_generation(
            sql="SELECT COVAR_POP(user_id, amount) FROM orders",
            error_message="COVAR_POP function is not supported in SingleStore",
            exp_type=exp.CovarPop,
            run=False
        )

    def test_functions_generation(self):
        self.validate_generation(
            sql="SELECT ABS(age) FROM users",
            exp_type=exp.Abs)
        self.validate_generation(
            sql="SELECT FLATTEN(ARRAY(ARRAY(1, 2), ARRAY(3, 4)))",
            error_message="Arrays are not supported in SingleStore",
            from_dialect="spark",
            exp_type=exp.Flatten,
            run=False
        )
        self.validate_generation(
            sql="SELECT TRANSFORM(ARRAY(1, 2, 3), x -> x + 1)",
            from_dialect="spark",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Transform,
            run=False
        )
        self.validate_generation(
            sql="SELECT db.is_prime(age) FROM users",
            exp_type=exp.Anonymous)
        self.validate_generation(
            sql="SELECT id, age APPLY(sum) FROM users",
            error_message="APPLY function is not supported in SingleStore",
            from_dialect="clickhouse",
            exp_type=exp.Apply,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY(1, 2, 3)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Array,
            run=False
        )
        self.validate_generation(
            sql="SELECT TO_ARRAY(name) FROM users",
            expected_sql="SELECT CASE WHEN name IS NULL THEN NULL ELSE ARRAY(name) END FROM users",
            error_message="Arrays are not supported in SingleStore",
            from_dialect="snowflake",
            exp_type=exp.ToArray,
            run=False
        )
        self.validate_generation(
            sql="SELECT LIST(id) FROM users",
            error_message="LIST function is not supported in SingleStore",
            exp_type=exp.List,
            run=False
        )
        self.validate_generation(
            sql="SELECT RPAD('ohai', 10, '_') AS o",
            exp_type=exp.Pad)
        self.validate_generation(
            sql="SELECT TO_CHAR(created_at, 'YYYY-MM-DD') FROM orders",
            exp_type=exp.ToChar)
        self.validate_generation(
            sql="SELECT TO_NUMBER(price, '999,999,999.99999') FROM products",
            exp_type=exp.ToNumber)
        self.validate_generation(
            sql="SELECT TO_DOUBLE(amount) FROM orders",
            expected_sql="SELECT amount :> DOUBLE FROM orders",
            exp_type=exp.ToDouble)
        self.validate_generation(
            sql="SELECT COLUMNS('.*_amount') FROM trips LIMIT 10",
            from_dialect="clickhouse",
            error_message="Dynamic column selection is not supported in SingleStore",
            exp_type=exp.Columns,
            run=False
        )
        self.validate_generation(
            sql="SELECT CONVERT(INT, name) FROM users",
            from_dialect="tsql",
            expected_sql="SELECT name :> INT FROM users",
            exp_type=exp.Convert)
        self.validate_generation(
            sql="SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', created_at) FROM orders",
            expected_sql="SELECT CONVERT_TZ(created_at, 'UTC', 'America/New_York') FROM orders",
            exp_type=exp.ConvertTimezone)
        self.validate_generation(
            sql="SELECT GENERATE_SERIES(1, 5)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.GenerateSeries,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM EXPLODING_GENERATE_SERIES(1, 5)",
            expected_sql="SELECT * FROM GENERATE_SERIES(1, 5)",
            error_message="EXPLODING_GENERATE_SERIES function is not supported in SingleStore",
            exp_type=exp.ExplodingGenerateSeries,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_ALL(ARRAY(1, 2, 3), x -> x > 0)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayAll,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_ANY(ARRAY(0, 1, 0), x -> x = 1)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayAny,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_CONCAT(ARRAY(1, 2), ARRAY(3, 4))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayConcat,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_CONSTRUCT_COMPACT(NULL, 1, NULL, 2)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayConstructCompact,
            run=False
        )
        self.validate_generation(
            sql="SELECT FILTER(ARRAY(1, 2, 3), x -> x > 1)",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayFilter,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_TO_STRING(ARRAY('a', 'b', 'c'), ',')",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArrayToString,
            run=False
        )
        self.validate_generation(
            sql="SELECT STRING(created_at, 'UTC') FROM orders",
            expected_sql="SELECT CONVERT_TZ(created_at, 'UTC', 'UTC') :> TEXT FROM orders",
            exp_type=exp.String)
        self.validate_generation(
            sql="SELECT STRING_TO_ARRAY('a,b,c', ',')",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.StringToArray,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_LENGTH(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArraySize,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_SORT(ARRAY(3, 1, 2))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArraySort,
            run=False
        )
        self.validate_generation(
            sql="SELECT ARRAY_SUM(ARRAY(amount, 2, 3)) FROM orders",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ArraySum,
            run=False
        )
        self.validate_generation(
            sql="SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM users",
            exp_type=exp.Case)
        self.validate_generation(
            sql="SELECT CAST(age AS DOUBLE) FROM users",
            expected_sql="SELECT age :> DOUBLE FROM users",
            exp_type=exp.Cast)
        self.validate_generation(
            sql="SELECT TRY_CAST(age AS INT) FROM users",
            expected_sql="SELECT age !:> INT FROM users",
            exp_type=exp.TryCast)

        self.validate_generation(
            sql="SELECT metadata.:STRING FROM events",
            from_dialect="clickhouse",
            expected_sql="SELECT metadata :> TEXT FROM events",
            exp_type=exp.JSONCast)
        self.validate_generation(
            sql="SELECT TRY(PARSE_JSON('{bad: json}') IS NULL)",
            expected_sql="SELECT TO_JSON('{bad: json}') IS NULL",
            error_message="Unsupported TRY function",
            exp_type=exp.Try,
            run=False
        )
        self.validate_generation(
            sql="SELECT CAST(age, 'TEXT') FROM users",
            expected_sql="SELECT age :> TEXT FROM users",
            exp_type=exp.CastToStrType)
        self.validate_generation(
            sql="SELECT CEIL(amount) FROM orders",
            exp_type=exp.Ceil)
        self.validate_generation(
            sql="SELECT COALESCE(email, 'none') FROM users",
            exp_type=exp.Coalesce)
        self.validate_generation(
            sql="SELECT CHR(65)",
            expected_sql="SELECT CHAR(65)",
            exp_type=exp.Chr)
        self.validate_generation(
            sql="SELECT CONCAT(name, 'a') FROM users",
            exp_type=exp.Concat)
        self.validate_generation(
            sql="SELECT CONCAT_WS('-', name, 'a') FROM users",
            exp_type=exp.ConcatWs)
        self.validate_generation(
            sql="SELECT CONTAINS(name, 'book') FROM products",
            expected_sql="SELECT INSTR(name, 'book') FROM products",
            exp_type=exp.Contains)
        self.validate_generation(
            sql="SELECT CONNECT_BY_ROOT DEPTNAME AS ROOT, DEPTNAME FROM DEPARTMENT START WITH DEPTNO IN ('B01', 'C01', 'D01', 'E01') CONNECT BY PRIOR DEPTNO = ADMRDEPT",
            error_message="CONNECT_BY_ROOT function is not supported in SingleStore",
            exp_type=exp.ConnectByRoot,
            run=False
        )
        self.validate_generation(
            sql="SELECT CBRT(id) FROM orders",
            expected_sql="SELECT POWER(id, 0.3333333333333333) FROM orders",
            exp_type=exp.Cbrt)
        self.validate_generation(
            sql="SELECT CURRENT_DATE",
            exp_type=exp.CurrentDate)
        self.validate_generation(
            sql="SELECT CURRENT_DATETIME",
            expected_sql="SELECT CURRENT_TIMESTAMP() :> DATETIME",
            from_dialect="bigquery",
            exp_type=exp.CurrentDatetime)
        self.validate_generation(
            sql="SELECT CURRENT_TIME()",
            exp_type=exp.CurrentTime)
        self.validate_generation(
            sql="SELECT CURRENT_TIMESTAMP()",
            exp_type=exp.CurrentTimestamp)
        self.validate_generation(
            sql="SELECT CURRENT_SCHEMA",
            expected_sql="SELECT SCHEMA()",
            from_dialect="postgres",
            exp_type=exp.CurrentSchema)
        self.validate_generation(
            sql="SELECT CURRENT_USER()",
            exp_type=exp.CurrentUser)
        self.validate_generation(
            sql="SELECT DATE_ADD(created_at, INTERVAL '1' DAY) FROM orders",
            exp_type=exp.DateAdd)
        self.validate_generation(
            sql="SELECT DATE_BIN(INTERVAL 15 MINUTE, created_at, TIMESTAMP '2001-01-01 00:00:00') FROM orders",
            expected_sql="SELECT TIME_BUCKET(INTERVAL '15' MINUTE, created_at, '2001-01-01 00:00:00' :> TIMESTAMP) FROM orders",
            exp_type=exp.DateBin)
        self.validate_generation(
            sql="SELECT DATE_SUB(created_at, INTERVAL '1' DAY) FROM orders",
            exp_type=exp.DateSub)
        self.validate_generation(
            sql="SELECT DATE_DIFF(DAY, created_at, NOW()) FROM orders",
            expected_sql="SELECT TIMESTAMPDIFF(DAY, created_at, NOW()) FROM orders",
            exp_type=exp.DateDiff)
        self.validate_generation(
            sql="SELECT DATE_TRUNC('MONTH', created_at) FROM orders",
            exp_type=exp.DateTrunc)
        self.validate_generation(
            sql="SELECT DATETIME('2024-05-06 13:00:00')",
            expected_sql="SELECT '2024-05-06 13:00:00' :> DATETIME",
            exp_type=exp.Datetime
        )
        self.validate_generation(
            sql="SELECT DATETIME_ADD(created_at, INTERVAL 2 HOUR) FROM orders",
            expected_sql="SELECT DATE_ADD(created_at, INTERVAL '2' HOUR) FROM orders",
            exp_type=exp.DatetimeAdd)
        self.validate_generation(
            sql="SELECT DATETIME_SUB(created_at, INTERVAL 30 MINUTE) FROM orders",
            expected_sql="SELECT DATE_SUB(created_at, INTERVAL '30' MINUTE) FROM orders",
            exp_type=exp.DatetimeSub)
        self.validate_generation(
            sql="SELECT DATETIME_DIFF(MINUTE, created_at, NOW()) FROM orders",
            expected_sql="SELECT TIMESTAMPDIFF(MINUTE, created_at, NOW()) FROM orders",
            exp_type=exp.DatetimeDiff)
        self.validate_generation(
            sql="SELECT DATETIME_TRUNC(created_at, MINUTE) FROM orders",
            expected_sql="SELECT DATE_TRUNC('MINUTE', created_at) FROM orders",
            exp_type=exp.DatetimeTrunc)
        self.validate_generation(
            sql="SELECT DAY_OF_WEEK(created_at) FROM orders",
            expected_sql="SELECT DAYOFWEEK(created_at) FROM orders",
            exp_type=exp.DayOfWeek)
        self.validate_generation(
            sql="SELECT DAY_OF_WEEK(created_at) FROM orders",
            expected_sql="SELECT ((DAYOFWEEK(created_at) % 7) + 1) FROM orders",
            from_dialect="presto",
            exp_type=exp.DayOfWeekIso)
        self.validate_generation(
            sql="SELECT DAY_OF_MONTH(created_at) FROM orders",
            expected_sql="SELECT DAY(created_at) FROM orders",
            exp_type=exp.DayOfMonth)
        self.validate_generation(
            sql="SELECT DAY_OF_YEAR(created_at) FROM orders",
            expected_sql="SELECT DAYOFYEAR(created_at) FROM orders",
            exp_type=exp.DayOfYear)
        self.validate_generation(
            sql="SELECT WEEK_OF_YEAR(created_at) FROM orders",
            expected_sql="SELECT WEEKOFYEAR(created_at) FROM orders",
            exp_type=exp.WeekOfYear)
        self.validate_generation(
            sql="SELECT MONTHS_BETWEEN(NOW(), created_at) FROM orders",
            exp_type=exp.MonthsBetween)
        self.validate_generation(
            sql="SELECT MAKE_INTERVAL(1, 2, 3, 4)",
            error_message="INTERVAL data type is not supported in SingleStore",
            exp_type=exp.MakeInterval,
            run=False
        )
        self.validate_generation(
            sql="SELECT LAST_DAY(created_at) FROM orders",
            exp_type=exp.LastDay)
        self.validate_generation(
            sql="SELECT EXTRACT(DAY FROM created_at) FROM orders",
            exp_type=exp.Extract)
        self.validate_generation(
            sql="SELECT TIMESTAMP('2024-01-01 12:00:00')",
            exp_type=exp.Timestamp)
        self.validate_generation(
            sql="SELECT TIMESTAMP_ADD(created_at, INTERVAL 2 DAY) FROM orders",
            expected_sql="SELECT DATE_ADD(created_at, INTERVAL '2' DAY) FROM orders",
            exp_type=exp.TimestampAdd)
        self.validate_generation(
            sql="SELECT TIMESTAMP_SUB(created_at, INTERVAL 1 HOUR) FROM orders",
            expected_sql="SELECT DATE_SUB(created_at, INTERVAL '1' HOUR) FROM orders",
            exp_type=exp.TimestampSub)
        self.validate_generation(
            sql="SELECT TIMESTAMP_DIFF(DAY, created_at, NOW()) FROM orders",
            expected_sql="SELECT TIMESTAMPDIFF(DAY, created_at, NOW()) FROM orders",
            exp_type=exp.TimestampDiff)
        self.validate_generation(
            sql="SELECT TIMESTAMP_TRUNC(created_at, HOUR) FROM orders",
            expected_sql="SELECT DATE_TRUNC('HOUR', created_at) FROM orders",
            exp_type=exp.TimestampTrunc)
        self.validate_generation(
            sql="SELECT TIME_ADD(TIME '12:00:00', INTERVAL 1 HOUR)",
            expected_sql="SELECT DATE_ADD('12:00:00' :> TIME, INTERVAL '1' HOUR)",
            exp_type=exp.TimeAdd)
        self.validate_generation(
            sql="SELECT TIME_SUB(TIME '12:00:00', INTERVAL 30 MINUTE)",
            expected_sql="SELECT DATE_SUB('12:00:00' :> TIME, INTERVAL '30' MINUTE)",
            exp_type=exp.TimeSub)
        self.validate_generation(
            sql="SELECT TIME_DIFF(MINUTE, TIME '10:00:00', TIME '11:00:00')",
            expected_sql="SELECT TIMESTAMPDIFF(MINUTE, '10:00:00' :> TIME, '11:00:00' :> TIME)",
            exp_type=exp.TimeDiff)
        self.validate_generation(
            sql="SELECT TIME_TRUNC(TIME '12:34:56', MINUTE)",
            expected_sql="SELECT DATE_TRUNC('MINUTE', '12:34:56' :> TIME)",
            exp_type=exp.TimeTrunc)
        self.validate_generation(
            sql="SELECT DATE_FROM_PARTS(2024, 5, 6)",
            error_message="DATE_FROM_PARTS function is not supported in SingleStore",
            exp_type=exp.DateFromParts,
            run=False
        )
        self.validate_generation(
            sql="SELECT TIME_FROM_PARTS(10, 30, 0)",
            error_message="TIME_FROM_PARTS function is not supported in SingleStore",
            exp_type=exp.TimeFromParts,
            run=False
        )
        self.validate_generation(
            sql="SELECT DATE_STR_TO_DATE('2024-05-06')",
            expected_sql="SELECT '2024-05-06' :> DATE",
            exp_type=exp.DateStrToDate)
        self.validate_generation(
            sql="SELECT DATE_TO_DI(created_at) FROM orders",
            expected_sql="SELECT (DATE_FORMAT(created_at, '%Y%m%d') :> INT) FROM orders",
            exp_type=exp.DateToDi)
        self.validate_generation(
            sql="SELECT DATE('2024-05-06')",
            expected_sql="SELECT '2024-05-06' :> DATE",
            from_dialect="bigquery",
            exp_type=exp.Date)
        self.validate_generation(
            sql="SELECT DAY(created_at) FROM orders",
            expected_sql="SELECT DAY(created_at :> DATE) FROM orders",
            exp_type=exp.Day)
        self.validate_generation(
            sql="SELECT DECODE('aGVsbG8=', 'base64')",
            error_message="DECODE function is not supported in SingleStore",
            exp_type=exp.Decode,
            run=False
        )
        self.validate_generation(
            sql="SELECT DI_TO_DATE(20240506)",
            expected_sql="SELECT STR_TO_DATE(20240506, '%Y%m%d')",
            exp_type=exp.DiToDate)
        self.validate_generation(
            sql="SELECT ENCODE('hello', 'base64')",
            error_message="ENCODE function is not supported in SingleStore",
            exp_type=exp.Encode,
            run=False
        )
        self.validate_generation(
            sql="SELECT EXP(price) FROM products",
            exp_type=exp.Exp)
        self.validate_generation(
            sql="SELECT * FROM EXPLODE(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Explode,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM EXPLODE_OUTER(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.ExplodeOuter,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM POSEXPLODE_OUTER(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.PosexplodeOuter,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM POSEXPLODE(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Posexplode,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM INLINE(ARRAY((1, 'a'), (2, 'b')))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Inline,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM UNNEST(ARRAY(1, 2, 3))",
            error_message="Arrays are not supported in SingleStore",
            exp_type=exp.Unnest,
            run=False
        )
        self.validate_generation(
            sql="SELECT FLOOR(amount) FROM orders",
            exp_type=exp.Floor)
        self.validate_generation(
            sql="SELECT FROM_BASE64('aGVsbG8=')",
            exp_type=exp.FromBase64)
        self.validate_generation(
            sql="SELECT FEATURES_AT_TIME(state, '2024-01-01 00:00:00')",
            error_message="FEATURES_AT_TIME function is not supported in SingleStore",
            exp_type=exp.FeaturesAtTime,
            run=False
        )
        self.validate_generation(
            sql="SELECT TO_BASE64('hello')",
            exp_type=exp.ToBase64)
        self.validate_generation(
            sql="SELECT FROM_ISO8601_TIMESTAMP('2024-05-06T12:00:00Z')",
            error_message="FROM_ISO8601_TIMESTAMP function is not supported in SingleStore",
            exp_type=exp.FromISO8601Timestamp,
            run=False
        )
        self.validate_generation(
            sql="SELECT * FROM GAP_FILL(TABLE device_data, ts_column => 'time', bucket_width => INTERVAL '1' MINUTE, partitioning_columns => ARRAY('device_id'), value_columns => ARRAY(('signal', 'locf'))) ORDER BY device_id",
            error_message="GAP_FILL function is not supported in SingleStore",
            exp_type=exp.GapFill,
            run=False
        )
