# import sqlglot
# from sqlglot.dataframe.sql.session import SparkSession

# schema = dict()

# schema['"ID"'] = "STRING"
# schema['"OtherColumn"'] = "STRING"

# sqlglot.schema.add_table('"Test"', schema)

# df = SparkSession().table('"Test"')

# other_schema = dict()

# other_schema['"ID"'] = "STRING"
# other_schema['"JoinColumn"'] = "STRING"

# sqlglot.schema.add_table('"OtherTable"', other_schema)

# other_df = SparkSession().table('"OtherTable"')

# joined = df.join(
#     other_df, 
#     on=(
#         df['"ID"'] == other_df['"JoinColumn"']
#     ),
#     how='left'
# )

# schema = dict()

# schema['"ID"'] = "STRING"
# schema['"OtherColumn"'] = "STRING"

# sqlglot.schema.add_table('"third_column"', schema)

# third_df = SparkSession().table('"third_column"')

# doubly_joined = joined.join(
#     third_df, 
#     on=(
#         df['"OtherColumn"'] == third_df['"OtherColumn"']
#     ),
#     how='left'
# )

# doubly_joined.sql(dialect='snowflake')
