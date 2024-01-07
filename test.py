import sqlglot
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot import parse_one

x = annotate_types(parse_one("SELECT struct(1 AS col, 2.5 AS row, struct(3 AS inner_col, 4.5 AS inner_row) AS nested_struct)", read="spark"))
print(repr(x))

# y = annotate_types(parse_one("select cast(struct(1 as col, 2 as row) as struct<col: int, row: int>)", read="spark"))
# print(repr(y))

# z = annotate_types(parse_one("select cast(struct(1 as col) as struct<col: int>)", read="spark"))
# print(repr(z))
