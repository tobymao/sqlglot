# PySpark DataFrame SQL Generator

This is a drop-in replacement for the PysPark DataFrame API that will generate SQL instead of executing DataFrame operations directly. This, when combined with the transpiling support in SQLGlot, allows one to write PySpark DataFrame code and execute it on other engines like [DuckDB](https://duckdb.org/), [Presto](https://prestodb.io/), [Spark](https://spark.apache.org/), [Snowflake](https://www.snowflake.com/en/), and [BigQuery](https://cloud.google.com/bigquery/). 

Currently many of the common operations are covered and more functionality will be added over time. Please open an issue or PR with your feedback to help influence what should be prioritized next and make sure your use case is properly supported.

# How to use

## Instructions
* [Install SQLGlot](https://github.com/tobymao/sqlglot/blob/main/README.md#install) and that is all that is required to just generate SQL. Some of the examples show generating SQL and then executing that SQL on a specific engine and that will require that engine's client library
* Find/replace all `from pyspark.sql` with `from sqlglot.dataframe`
* Prior to any `spark.read.table` or `spark.table` run `sqlglot.schema.add_table('<table_name>', <column_structure>)`
  * The column structure can be defined the following ways:
    * Dictionary where the keys are column names and values are string of the Spark SQL type name
      * Ex: {'cola': 'string', 'colb': 'int'}
    * PySpark DataFrame `StructType` similar to when using `createDataFrame`
      * Ex: `StructType([StructField('cola', StringType()), StructField('colb', IntegerType())])`
    * A string of names and types similar to what is supported in `createDataFrame`
      * Ex: `cola: STRING, colb: INT`
    * [Not Recommended] A list of string column names without type
      * Ex: ['cola', 'colb']
      * The lack of types may limit functionality in future releases
  * See [Registering Custom Schema](TODO) for information on how to skip this step if the information is stored externally
* Add `.sql()` to your final DataFrame command to return the SQL as a string
  * Spark is the default output dialect. See [dialects](https://github.com/tobymao/sqlglot/tree/main/sqlglot/dialects) for a full list of options
  * Ex: `.sql(dialect='bigquery')`

## Example

```python
import sqlglot
from sqlglot.dataframe.sql.session import SparkSession
from sqlglot.dataframe.sql import functions as F

sqlglot.schema.add_table('employee', {
  'employee_id': 'INT',
  'fname': 'STRING',
  'lname': 'STRING',
  'age': 'INT',
})  # Register the table structure

spark = SparkSession()

df = (
    spark
    .table('employee')
    .groupBy(F.col("age"))
    .agg(F.countDistinct(F.col("employee_id")).alias("num_employees")) 
)

print(df.sql())  # Spark will be the dialect used by default
```
Output:
```sparksql
SELECT
  `employee`.`age` AS `age`,
  COUNT(DISTINCT `employee`.`employee_id`) AS `num_employees`
FROM `employee` AS `employee`
GROUP BY
  `employee`.`age`
```

## Registering Custom Schema Class

The step of adding `sqlglot.schema.add_table` can be skipped if you have the column structure stored externally like in a file or from an external metadata table. This can be done by writting a class that implements the `sqlglot.schema.Schema` abstract class and then assigning that class to `sqlglot.schema`. 

```python
import sqlglot
from sqlglot.dataframe.sql.session import SparkSession
from sqlglot.dataframe.sql import functions as F
from sqlglot.schema import Schema


class ExternalSchema(Schema):
  ...

sqlglot.schema = ExternalSchema()

spark = SparkSession()

df = (
    spark
    .table('employee')
    .groupBy(F.col("age"))
    .agg(F.countDistinct(F.col("employee_id")).alias("num_employees")) 
)

print(df.sql())
```

## Example Implementations

### Bigquery
```python
from google.cloud import bigquery
from sqlglot.dataframe.sql.session import SparkSession
from sqlglot.dataframe.sql import types
from sqlglot.dataframe.sql import functions as F

client = bigquery.Client()

data = [
    (1, "Jack", "Shephard", 34),
    (2, "John", "Locke", 48),
    (3, "Kate", "Austen", 34),
    (4, "Claire", "Littleton", 22),
    (5, "Hugo", "Reyes", 26),
]
schema = types.StructType([
    types.StructField('employee_id', types.IntegerType(), False),
    types.StructField('fname', types.StringType(), False),
    types.StructField('lname', types.StringType(), False),
    types.StructField('age', types.IntegerType(), False),
])

query = (
    SparkSession()
    .createDataFrame(data, schema)
    .groupBy(F.col("age"))
    .agg(F.countDistinct(F.col("employee_id")).alias("num_employees"))
    .sql(dialect="bigquery")
)

for row in client.query(query):
    print(f"Age: {row['age']}, Num Employees: {row['num_employees']}")
```

### Snowflake
```python
import os

import snowflake.connector
from sqlglot.dataframe.session import SparkSession
from sqlglot.dataframe import types
from sqlglot.dataframe import functions as F

ctx = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASS"],
    account=os.environ["SNOWFLAKE_ACCOUNT"]
)
cs = ctx.cursor()

data = [
    (1, "Jack", "Shephard", 34),
    (2, "John", "Locke", 48),
    (3, "Kate", "Austen", 34),
    (4, "Claire", "Littleton", 22),
    (5, "Hugo", "Reyes", 26),
]
schema = types.StructType([
    types.StructField('employee_id', types.IntegerType(), False),
    types.StructField('fname', types.StringType(), False),
    types.StructField('lname', types.StringType(), False),
    types.StructField('age', types.IntegerType(), False),
])

query = (
    SparkSession()
    .createDataFrame(data, schema)
    .groupBy(F.col("age"))
    .agg(F.countDistinct(F.col("lname")).alias("num_employees"))
    .sql(dialect="snowflake")
)

try:
    cs.execute(query)
    results = cs.fetchall()
    for row in results:
        print(f"Age: {row[0]}, Num Employees: {row[1]}")
finally:
    cs.close()
ctx.close()
```
