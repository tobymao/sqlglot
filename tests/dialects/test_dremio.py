import sqlglot
from sqlglot.dialects.dremio import Dremio

try:
    dremio_dialect = sqlglot.dialects.Dialect.get("dremio")
    print("✅ Dremio dialect is registered:", dremio_dialect)
except KeyError:
    print("❌ Dremio dialect is NOT registered!")

# Register manually if needed
if not dremio_dialect:
    from tests.dialects.test_dremio import Dremio
    sqlglot.dialects.Dialect["dremio"] = Dremio
    print("🔄 Registered Dremio dialect manually.")


def diagnostics(sql, expression, generated_sql):
    parsed = sqlglot.parse_one(sql, dialect="dremio")
    print("Parsed Expression:")
    print(parsed.dump()) 
    print(f"Expression: {expression}")
    print(f"Generated SQL: {generated_sql}")

def test_select_with_where():
    sql = "SELECT name FROM employees WHERE age > 30"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_select_with_group_by():
    sql = "SELECT COUNT(city), city FROM locations GROUP BY city"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_insert_into():
    sql = "INSERT INTO customers VALUES (1, 'John Doe', 'New York')"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_update_set():
    sql = "UPDATE employees SET salary = 60000 WHERE id = 1"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_delete_from():
    sql = "DELETE FROM employees WHERE id = 1"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_create_table():
    sql = "CREATE TABLE employees (id INT, name VARCHAR, salary FLOAT)"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_drop_table():
    sql = "DROP TABLE employees"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_string_functions():
    sql = "SELECT UPPER(name), LOWER(name), LENGTH(name) FROM employees"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_coalesce():
    sql = "SELECT COALESCE(salary, 0) FROM employees"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"
    
def test_numeric_functions():
    sql = "SELECT ABS(-5), POWER(2, 3), SQRT(9), LOG(2), EXP(1), SIGN(-10)"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_date_add_with_integer():
    sql = "SELECT DATE_ADD('2022-01-01', 2)"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    correct_sql = """SELECT DATE_ADD('2022-01-01', CAST(2 AS INTERVAL DAY))"""
    assert generated_sql == correct_sql, f"Expected {sql}, but got {generated_sql}"


def test_date_add_with_negative_integer():
    sql = "SELECT DATE_ADD('2022-01-01', -2)"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    correct_sql = """SELECT DATE_ADD('2022-01-01', CAST(-2 AS INTERVAL DAY))"""
    assert generated_sql == correct_sql, f"Expected {correct_sql}, but got {generated_sql}"


def test_date_add_with_cast_interval():
    sql = "SELECT DATE_ADD('2022-01-01', CAST(30 AS INTERVAL DAY))"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"


def test_date_sub_with_integer():
    sql = "SELECT DATE_ADD('2022-01-01', -30)"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    correct_sql = """SELECT DATE_ADD('2022-01-01', CAST(-30 AS INTERVAL DAY))"""
    assert generated_sql == correct_sql, f"Expected {correct_sql}, but got {generated_sql}"


def test_date_sub_with_cast_interval():
    sql = "SELECT DATE_ADD('2022-01-01', CAST(-30 AS INTERVAL DAY))"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"


def test_date_add_with_timestamp():
    sql = "SELECT DATE_ADD(TIMESTAMP '2022-01-01 12:00:00', CAST(30 AS INTERVAL DAY))"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    diagnostics(sql, sql, generated_sql)
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"


def test_date_sub_with_timestamp():
    sql = "SELECT DATE_ADD(TIMESTAMP '2022-01-01 12:00:00', CAST(-30 AS INTERVAL DAY))"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"


def test_date_add_with_time():
    sql = "SELECT DATE_ADD(TIME '00:00:00', CAST(30 AS INTERVAL MINUTE))"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    diagnostics(sql, sql, generated_sql)
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"


def test_date_sub_with_time():
    sql = "SELECT DATE_ADD(TIME '00:00:00', CAST(-30 AS INTERVAL MINUTE))"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"


def test_aggregate_functions():
    sql = "SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary), VARIANCE(salary), STDDEV(salary)"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_conditional_functions():
    sql = "SELECT COALESCE(salary, 0), NULLIF(salary, 0)"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"



test_select_with_where()
test_select_with_group_by()
test_insert_into()
test_update_set()
test_delete_from()
test_create_table()
test_drop_table()
test_string_functions()
test_numeric_functions()
test_coalesce()
test_numeric_functions()
test_date_add_with_integer()
test_date_add_with_negative_integer()
test_date_add_with_cast_interval()
test_date_sub_with_integer()
test_date_sub_with_cast_interval()
test_date_add_with_timestamp()
test_date_sub_with_timestamp()
test_date_add_with_time()
test_date_sub_with_time()
test_aggregate_functions()
test_conditional_functions()




