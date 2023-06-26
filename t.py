import sqlglot 

# selectTest = sqlglot.transpile("SELECT EPOCH_MS(1618088028295)", read="duckdb", write="hive")[0]

sameToSameTest = sqlglot.transpile("""
-- Create a new table
CREATE TABLE employees (
  id INT,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  salary DECIMAL(10, 2)
);

-- Add a new column to the table
ALTER TABLE employees ADD COLUMN hire_date DATE;

-- Create a new index on the table
CREATE INDEX idx_last_name ON employees (last_name);

-- Create a new schema
CREATE SCHEMA sales;

-- Create a new table in the sales schema
CREATE TABLE sales.orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  total_amount DECIMAL(10, 2)
);

-- Rename a table
ALTER TABLE sales.orders RENAME TO sales.order_details;

-- Drop an index
-- DROP INDEX idx_last_name ON employees;

-- Drop a table
DROP TABLE employees;

-- Drop a schema
DROP SCHEMA sales;

""", read='oracle', write='mysql')[0]

print(sameToSameTest)
