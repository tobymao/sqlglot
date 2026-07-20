from sqlglot import parse_one
from sqlglot.optimizer.annotate_types import annotate_types

dialect="duckdb"

sql = "encode('my_string_with_ü')"

ast = parse_one(sql,dialect=dialect)

annotated = annotate_types(ast,dialect=dialect)

print(repr(annotated))