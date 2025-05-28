from sqlglot import parse_one

sql = """
FROM x |> SELECT x1, x2 |> WHERE x1 > 0 AND x2 != 0
"""

expression = parse_one(sql)
print("======== AST ============")
print(repr(expression))
print("=========================")

output = expression.sql("")
print("======== SQL ============")
print(output)
print("=========================")


