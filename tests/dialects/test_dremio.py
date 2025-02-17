import sqlglot

def test_basic_select():
    sql = "SELECT name FROM employees WHERE age > 30 LIMIT 10"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

def test_simple_select():
    sql = "SELECT * FROM customers"
    expression = sqlglot.parse_one(sql, dialect="dremio")
    generated_sql = expression.sql(dialect="dremio")
    assert generated_sql == sql, f"Expected {sql}, but got {generated_sql}"

test_basic_select()
test_simple_select()


