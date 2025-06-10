from sqlglot import exp, UnsupportedError, ParseError, parse_one
from tests.dialects.test_dialect import Validator


class TestDB2(Validator):
    dialect = "db2"

    def test_db2(self):
        self.validate_identity("1 /* /* */")
        self.validate_all(
            "SELECT CONNECT_BY_ROOT x y",
            write={
                "": "SELECT CONNECT_BY_ROOT x AS y",
                "db2": "SELECT CONNECT_BY_ROOT x AS y",
            },
        )
        self.parse_one("ALTER TABLE tbl_name DROP FOREIGN KEY fk_symbol").assert_is(exp.Alter)

        self.validate_identity("CURRENT_TIMESTAMP")
        self.validate_identity("CREATE GLOBAL TEMPORARY TABLE t AS SELECT * FROM orders")
        self.validate_identity("CREATE PRIVATE TEMPORARY TABLE t AS SELECT * FROM orders")
        self.validate_identity("REGEXP_REPLACE('source', 'search')")
        self.validate_identity("TIMESTAMP(3) WITH TIME ZONE")
        self.validate_identity("CURRENT_TIMESTAMP(precision)")
        self.validate_identity("ALTER TABLE tbl_name DROP FOREIGN KEY fk_symbol")
        self.validate_identity("ALTER TABLE Payments ADD Stock DECIMAL NOT NULL")
        self.validate_identity("SELECT x FROM t WHERE cond FOR UPDATE")
        self.validate_identity("SELECT JSON_OBJECT(k1: v1 FORMAT JSON, k2: v2 FORMAT JSON)")
        self.validate_identity("SELECT JSON_OBJECT('name': first_name || ' ' || last_name) FROM t")
        self.validate_identity("COALESCE(c1, c2, c3)")
        self.validate_identity("SELECT * FROM TABLE(foo)")
        self.validate_identity("SELECT a$x#b")
        self.validate_identity("SELECT :OBJECT")
        self.validate_identity("SELECT * FROM t FOR UPDATE")
        self.validate_identity("SELECT * FROM t FOR UPDATE WAIT 5")
        self.validate_identity("SELECT * FROM t FOR UPDATE NOWAIT")
        self.validate_identity("SELECT * FROM t FOR UPDATE SKIP LOCKED")
        self.validate_identity("SELECT * FROM t FOR UPDATE OF s.t.c, s.t.v")
        self.validate_identity("SELECT * FROM t FOR UPDATE OF s.t.c, s.t.v NOWAIT")
        self.validate_identity("SELECT * FROM t FOR UPDATE OF s.t.c, s.t.v SKIP LOCKED")
        self.validate_identity("SELECT STANDARD_HASH('hello')")
        self.validate_identity("SELECT STANDARD_HASH('hello', 'MD5')")
        self.validate_identity("SELECT * FROM table_name@dblink_name.database_link_domain")
        self.validate_identity("SELECT * FROM table_name TABLESAMPLE SYSTEM (25)")
        self.validate_identity("SELECT COUNT(*) * 10 FROM orders TABLESAMPLE SYSTEM (25) SEED (1)")
        self.validate_identity("SELECT * FROM V$SESSION")
        self.validate_identity("SELECT TO_DATE('January 15, 1989, 11:00 A.M.')")
        self.validate_identity("SELECT INSTR(haystack, needle)")
        self.validate_identity(
            "SELECT * FROM consumer LEFT JOIN groceries ON consumer.groceries_id = consumer.id PIVOT(MAX(type_id) FOR consumer_type IN (1, 2, 3, 4))"
        )
        self.validate_identity(
            "SELECT * FROM test UNPIVOT INCLUDE NULLS (value FOR Description IN (col AS 'PREFIX ' || CHR(38) || ' SUFFIX'))"
        )
        self.validate_identity(
            "ALTER TABLE Payments ADD (Stock DECIMAL NOT NULL, dropid VARCHAR(500) NOT NULL)"
        )
        # @TODO
        #self.validate_identity(
        #    "SELECT JSON_ARRAYAGG(JSON_OBJECT(KEY 'RNK' VALUE RNK, KEY 'RATING_CODE' VALUE RATING_CODE, KEY 'DATE_VALUE' VALUE DATE_VALUE, KEY 'AGENT_ID' VALUE AGENT_ID) RETURNING CLOB) AS JSON_DATA FROM tablename"
        #)
        #self.validate_identity(
        #    "SELECT JSON_ARRAY(FOO() FORMAT JSON, BAR() NULL ON NULL RETURNING CLOB)"
        #)
        #self.validate_identity(
        #    "SELECT JSON_ARRAYAGG(FOO() FORMAT JSON ORDER BY bar NULL ON NULL RETURNING CLOB)"
        #)
        self.validate_identity(
            "SELECT COUNT(1) INTO V_Temp FROM TABLE(CAST(somelist AS data_list)) WHERE col LIKE '%contact'"
        )
        self.validate_identity(
            "SELECT department_id INTO v_department_id FROM departments LIMIT 1"
        )
        self.validate_identity(
            "SELECT MIN(column_name) KEEP (DENSE_RANK FIRST ORDER BY column_name DESC) FROM table_name"
        )
        self.validate_identity(
            "SELECT CAST('January 15, 1989, 11:00 A.M.' AS DATE DEFAULT NULL ON CONVERSION ERROR, 'Month dd, YYYY, HH:MI A.M.') FROM DUAL",
            "SELECT TO_DATE('January 15, 1989, 11:00 A.M.', 'Month dd, YYYY, HH12:MI P.M.') FROM DUAL",
        )
        self.validate_identity(
            "SELECT TRUNC(CURRENT_TIMESTAMP)",
            "SELECT TRUNC(CURRENT_TIMESTAMP, 'DD')",
        )
        self.validate_identity(
            """SELECT JSON_OBJECT(KEY 'key1' IS emp.column1, KEY 'key2' IS emp.column1) "emp_key" FROM emp""",
            """SELECT JSON_OBJECT('key1': emp.column1, 'key2': emp.column1) AS "emp_key" FROM emp""",
        )
        self.validate_identity(
            "SELECT JSON_OBJECTAGG(KEY department_name VALUE department_id) FROM dep WHERE id <= 30",
            "SELECT JSON_OBJECTAGG(department_name: department_id) FROM dep WHERE id <= 30",
        )
        self.validate_identity(
            "SELECT last_name, department_id, salary, MIN(salary) KEEP (DENSE_RANK FIRST ORDER BY commission_pct) "
            'OVER (PARTITION BY department_id) AS "Worst", MAX(salary) KEEP (DENSE_RANK LAST ORDER BY commission_pct) '
            'OVER (PARTITION BY department_id) AS "Best" FROM employees ORDER BY department_id, salary, last_name'
        )
        self.validate_identity(
            "SELECT UNIQUE col1, col2 FROM table",
            "SELECT DISTINCT col1, col2 FROM table",
        )
        #@TODO
        #self.validate_identity(
        #    "SELECT * FROM T ORDER BY I OFFSET NVL(:variable1, 10) ROWS FETCH NEXT NVL(:variable2, 10) ROWS ONLY",
        #)
        self.validate_identity(
            "SELECT * FROM t TABLESAMPLE (.25)",
            "SELECT * FROM t TABLESAMPLE (0.25)",
        )
        self.validate_identity("SELECT TO_CHAR(-100, 'L99', 'NL_CURRENCY = '' AusDollars '' ')")
        self.validate_identity(
            "SELECT * FROM t START WITH col CONNECT BY NOCYCLE PRIOR col1 = col2"
        )

        self.validate_all(
            "SELECT TRIM('|' FROM '||Hello ||| world||')",
            write={
                "clickhouse": "SELECT TRIM(BOTH '|' FROM '||Hello ||| world||')",
                "db2": "SELECT TRIM('|' FROM '||Hello ||| world||')",
            },
        )
        self.validate_all(
            "SELECT department_id, department_name INTO v_department_id, v_department_name FROM departments FETCH FIRST 1 ROWS ONLY",
            write={
                "db2": "SELECT department_id, department_name INTO v_department_id, v_department_name FROM departments LIMIT 1",
                "postgres": UnsupportedError,
                "tsql": UnsupportedError,
            },
        )
        self.validate_all(
            "SELECT * FROM test WHERE MOD(col1, 4) = 3",
            read={
                "duckdb": "SELECT * FROM test WHERE col1 % 4 = 3",
            },
            write={
                "duckdb": "SELECT * FROM test WHERE col1 % 4 = 3",
                "db2": "SELECT * FROM test WHERE MOD(col1, 4) = 3",
            },
        )
        self.validate_all(
            "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'YYYY/MM/DD') AND TO_DATE(f.C_EDATE, 'YYYY/MM/DD')",
            read={
                "postgres": "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'yyyy/mm/dd') AND TO_DATE(f.C_EDATE, 'yyyy/mm/dd')",
            },
            write={
                "db2": "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'YYYY/MM/DD') AND TO_DATE(f.C_EDATE, 'YYYY/MM/DD')",
                "postgres": "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'YYYY/MM/DD') AND TO_DATE(f.C_EDATE, 'YYYY/MM/DD')",
            },
        )
        self.validate_all(
            "TO_CHAR(x)",
            write={
                "doris": "CAST(x AS STRING)",
                "db2": "TO_CHAR(x)",
            },
        )
        self.validate_all(
            "TO_NUMBER(expr, fmt, nlsparam)",
            read={
                "teradata": "TO_NUMBER(expr, fmt, nlsparam)",
            },
            write={
                "db2": "TO_NUMBER(expr, fmt, nlsparam)",
                "teradata": "TO_NUMBER(expr, fmt, nlsparam)",
            },
        )
        self.validate_all(
            "TO_NUMBER(x)",
            write={
                "bigquery": "CAST(x AS FLOAT64)",
                "doris": "CAST(x AS DOUBLE)",
                "drill": "CAST(x AS DOUBLE)",
                "duckdb": "CAST(x AS DOUBLE)",
                "hive": "CAST(x AS DOUBLE)",
                "mysql": "CAST(x AS DOUBLE)",
                "db2": "TO_NUMBER(x)",
                "postgres": "CAST(x AS DOUBLE PRECISION)",
                "presto": "CAST(x AS DOUBLE)",
                "redshift": "CAST(x AS DOUBLE PRECISION)",
                "snowflake": "TO_NUMBER(x)",
                "spark": "CAST(x AS DOUBLE)",
                "spark2": "CAST(x AS DOUBLE)",
                "starrocks": "CAST(x AS DOUBLE)",
                "tableau": "CAST(x AS DOUBLE)",
                "teradata": "TO_NUMBER(x)",
            },
        )
        self.validate_all(
            "TO_NUMBER(x, fmt)",
            read={
                "databricks": "TO_NUMBER(x, fmt)",
                "drill": "TO_NUMBER(x, fmt)",
                "postgres": "TO_NUMBER(x, fmt)",
                "snowflake": "TO_NUMBER(x, fmt)",
                "spark": "TO_NUMBER(x, fmt)",
                "redshift": "TO_NUMBER(x, fmt)",
                "teradata": "TO_NUMBER(x, fmt)",
            },
            write={
                "databricks": "TO_NUMBER(x, fmt)",
                "drill": "TO_NUMBER(x, fmt)",
                "db2": "TO_NUMBER(x, fmt)",
                "postgres": "TO_NUMBER(x, fmt)",
                "snowflake": "TO_NUMBER(x, fmt)",
                "spark": "TO_NUMBER(x, fmt)",
                "redshift": "TO_NUMBER(x, fmt)",
                "teradata": "TO_NUMBER(x, fmt)",
            },
        )
        self.validate_all(
            "SELECT TO_CHAR(TIMESTAMP '1999-12-01 10:00:00')",
            write={
                "db2": "SELECT TO_CHAR(CAST('1999-12-01 10:00:00' AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS')",
                "postgres": "SELECT TO_CHAR(CAST('1999-12-01 10:00:00' AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS')",
            },
        )
        self.validate_all(
            "SELECT CAST(NULL AS VARCHAR(2328 CHAR)) AS COL1",
            write={
                "db2": "SELECT CAST(NULL AS VARCHAR(2328 CHAR)) AS COL1",
                "spark": "SELECT CAST(NULL AS VARCHAR(2328)) AS COL1",
            },
        )
        self.validate_all(
            "SELECT CAST(NULL AS VARCHAR(2328 BYTE)) AS COL1",
            write={
                "db2": "SELECT CAST(NULL AS VARCHAR(2328 BYTE)) AS COL1",
                "spark": "SELECT CAST(NULL AS VARCHAR(2328)) AS COL1",
            },
        )
        self.validate_all(
            "DATE '2022-01-01'",
            write={
                "": "DATE_STR_TO_DATE('2022-01-01')",
                "mysql": "CAST('2022-01-01' AS DATE)",
                "db2": "TO_DATE('2022-01-01', 'YYYY-MM-DD')",
                "postgres": "CAST('2022-01-01' AS DATE)",
            },
        )

        self.validate_all(
            "x::double",
            write={
                "db2": "CAST(x AS DOUBLE)",
                "": "CAST(x AS DOUBLE)",
            },
        )
        self.validate_all(
            "x::float",
            write={
                "db2": "CAST(x AS FLOAT)",
                "": "CAST(x AS FLOAT)",
            },
        )
        self.validate_all(
            "CAST(x AS sch.udt)",
            read={
                "postgres": "CAST(x AS sch.udt)",
            },
            write={
                "db2": "CAST(x AS sch.udt)",
                "postgres": "CAST(x AS sch.udt)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('2024-12-12 12:12:12.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')",
            write={
                "db2": "SELECT TO_TIMESTAMP('2024-12-12 12:12:12.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                "duckdb": "SELECT STRPTIME('2024-12-12 12:12:12.000000', '%Y-%m-%d %H:%M:%S.%f')",
            },
        )
        self.validate_all(
            "SELECT TO_DATE('2024-12-12', 'YYYY-MM-DD')",
            write={
                "db2": "SELECT TO_DATE('2024-12-12', 'YYYY-MM-DD')",
                "duckdb": "SELECT CAST(STRPTIME('2024-12-12', '%Y-%m-%d') AS DATE)",
            },
        )
        self.validate_identity(
            """SELECT * FROM t ORDER BY a ASC NULLS LAST, b ASC NULLS FIRST, c DESC NULLS LAST, d DESC NULLS FIRST""",
            """SELECT * FROM t ORDER BY a ASC, b ASC NULLS FIRST, c DESC NULLS LAST, d DESC""",
        )
        self.validate_all(
            "NVL(NULL, 1)",
            write={
                "db2": "NVL(NULL, 1)",
                "": "COALESCE(NULL, 1)",
                "clickhouse": "COALESCE(NULL, 1)",
            },
        )
        self.validate_all(
            "TRIM(BOTH 'h' FROM 'Hello World')",
            write={
                "db2": "TRIM(BOTH 'h' FROM 'Hello World')",
                "clickhouse": "TRIM(BOTH 'h' FROM 'Hello World')",
            },
        )
        self.validate_identity(
            "SELECT /*+ ORDERED */* FROM tbl", "SELECT /*+ ORDERED */ * FROM tbl"
        )
        self.validate_identity(
            "SELECT /* test */ /*+ ORDERED */* FROM tbl",
            "/* test */ SELECT /*+ ORDERED */ * FROM tbl",
        )
        self.validate_identity(
            "SELECT /*+ ORDERED */*/* test */ FROM tbl",
            "SELECT /*+ ORDERED */ * /* test */ FROM tbl",
        )

        self.validate_all(
            "SELECT * FROM t FETCH FIRST 10 ROWS ONLY",
            write={
                "db2": "SELECT * FROM t LIMIT 10",
                "tsql": "SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH FIRST 10 ROWS ONLY",
            },
        )
        self.validate_identity("CREATE OR REPLACE VIEW foo1.foo2")
        self.validate_identity("TO_TIMESTAMP('foo')")


    def test_xml_table(self):
        self.validate_identity("XMLTABLE('x')")
        self.validate_identity("XMLTABLE('x' RETURNING SEQUENCE BY REF)")
        self.validate_identity("XMLTABLE('x' PASSING y)")
        self.validate_identity("XMLTABLE('x' PASSING y RETURNING SEQUENCE BY REF)")
        self.validate_identity(
            "XMLTABLE('x' RETURNING SEQUENCE BY REF COLUMNS a VARCHAR, b FLOAT)"
        )
        self.validate_identity(
            "SELECT x.* FROM example t, XMLTABLE(XMLNAMESPACES(DEFAULT 'http://example.com/default', 'http://example.com/ns1' AS \"ns1\"), '/root/data' PASSING t.xml COLUMNS id DECIMAL PATH '@id', value VARCHAR(100) PATH 'ns1:value/text()') x"
        )

        self.validate_all(
            """SELECT warehouse_name warehouse,
   warehouse2."Water", warehouse2."Rail"
   FROM warehouses,
   XMLTABLE('/Warehouse'
      PASSING warehouses.warehouse_spec
      COLUMNS
         "Water" varchar2(6) PATH 'WaterAccess',
         "Rail" varchar2(6) PATH 'RailAccess')
      warehouse2""",
            write={
                "db2": """SELECT
  warehouse_name AS warehouse,
  warehouse2."Water",
  warehouse2."Rail"
FROM warehouses, XMLTABLE(
  '/Warehouse'
  PASSING
    warehouses.warehouse_spec
  COLUMNS
    "Water" VARCHAR(6) PATH 'WaterAccess',
    "Rail" VARCHAR(6) PATH 'RailAccess'
) warehouse2""",
            },
            pretty=True,
        )

        self.validate_all(
            """SELECT table_name, column_name, data_default FROM xmltable('ROWSET/ROW'
    passing dbms_xmlgen.getxmltype('SELECT table_name, column_name, data_default FROM user_tab_columns')
    columns table_name      VARCHAR(128)   PATH '*[1]'
            , column_name   VARCHAR(128)   PATH '*[2]'
            , data_default  VARCHAR(2000)  PATH '*[3]'
            );""",
            write={
                "db2": """SELECT
  table_name,
  column_name,
  data_default
FROM XMLTABLE(
  'ROWSET/ROW'
  PASSING
    dbms_xmlgen.getxmltype('SELECT table_name, column_name, data_default FROM user_tab_columns')
  COLUMNS
    table_name VARCHAR(128) PATH '*[1]',
    column_name VARCHAR(128) PATH '*[2]',
    data_default VARCHAR(2000) PATH '*[3]'
)""",
            },
            pretty=True,
        )

    def test_connect_by(self):
        start = "START WITH last_name = 'King'"
        connect = "CONNECT BY PRIOR employee_id = manager_id AND LEVEL <= 4"
        body = """
            SELECT last_name "Employee",
            LEVEL, SYS_CONNECT_BY_PATH(last_name, '/') "Path"
            FROM employees
            WHERE level <= 3 AND department_id = 80
        """
        pretty = """SELECT
  last_name AS "Employee",
  LEVEL,
  SYS_CONNECT_BY_PATH(last_name, '/') AS "Path"
FROM employees
WHERE
  level <= 3 AND department_id = 80
START WITH last_name = 'King'
CONNECT BY PRIOR employee_id = manager_id AND LEVEL <= 4"""

        for query in (f"{body}{start}{connect}", f"{body}{connect}{start}"):
            self.validate_identity(query, pretty, pretty=True)

    def test_query_restrictions(self):
        for restriction in ("UR", "RS"):
            for constraint_name in (" CONSTRAINT name", ""):
                with self.subTest(f"Restriction: {restriction}"):
                    self.validate_identity(f"SELECT * FROM tbl WITH {restriction}{constraint_name}")
                    self.validate_identity(
                        f"CREATE VIEW view AS SELECT * FROM tbl WITH {restriction}{constraint_name}"
                    )

    def test_multitable_inserts(self):
        self.maxDiff = None
        self.validate_identity(
            "INSERT ALL "
            "INTO dest_tab1 (id, description) VALUES (id, description) "
            "INTO dest_tab2 (id, description) VALUES (id, description) "
            "INTO dest_tab3 (id, description) VALUES (id, description) "
            "SELECT id, description FROM source_tab"
        )

        self.validate_identity(
            "INSERT ALL "
            "INTO pivot_dest (id, day, val) VALUES (id, 'mon', mon_val) "
            "INTO pivot_dest (id, day, val) VALUES (id, 'tue', tue_val) "
            "INTO pivot_dest (id, day, val) VALUES (id, 'wed', wed_val) "
            "INTO pivot_dest (id, day, val) VALUES (id, 'thu', thu_val) "
            "INTO pivot_dest (id, day, val) VALUES (id, 'fri', fri_val) "
            "SELECT * "
            "FROM pivot_source"
        )

        self.validate_identity(
            "INSERT ALL "
            "WHEN id <= 3 THEN "
            "INTO dest_tab1 (id, description) VALUES (id, description) "
            "WHEN id BETWEEN 4 AND 7 THEN "
            "INTO dest_tab2 (id, description) VALUES (id, description) "
            "WHEN id >= 8 THEN "
            "INTO dest_tab3 (id, description) VALUES (id, description) "
            "SELECT id, description "
            "FROM source_tab"
        )

        self.validate_identity(
            "INSERT ALL "
            "WHEN id <= 3 THEN "
            "INTO dest_tab1 (id, description) VALUES (id, description) "
            "WHEN id BETWEEN 4 AND 7 THEN "
            "INTO dest_tab2 (id, description) VALUES (id, description) "
            "WHEN 1 = 1 THEN "
            "INTO dest_tab3 (id, description) VALUES (id, description) "
            "SELECT id, description "
            "FROM source_tab"
        )

        self.validate_identity(
            "INSERT FIRST "
            "WHEN id <= 3 THEN "
            "INTO dest_tab1 (id, description) VALUES (id, description) "
            "WHEN id <= 5 THEN "
            "INTO dest_tab2 (id, description) VALUES (id, description) "
            "ELSE "
            "INTO dest_tab3 (id, description) VALUES (id, description) "
            "SELECT id, description "
            "FROM source_tab"
        )

        self.validate_identity(
            "INSERT FIRST "
            "WHEN id <= 3 THEN "
            "INTO dest_tab1 (id, description) VALUES (id, description) "
            "ELSE "
            "INTO dest_tab2 (id, description) VALUES (id, description) "
            "INTO dest_tab3 (id, description) VALUES (id, description) "
            "SELECT id, description "
            "FROM source_tab"
        )

        self.validate_identity(
            "/* COMMENT */ INSERT FIRST "
            "WHEN salary > 4000 THEN INTO emp2 "
            "WHEN salary > 5000 THEN INTO emp3 "
            "WHEN salary > 6000 THEN INTO emp4 "
            "SELECT salary FROM employees"
        )


    def test_grant(self):
        grant_cmds = [
            "GRANT purchases_reader_role TO george, maria",
            "GRANT USAGE ON TYPE price TO finance_role",
            "GRANT USAGE ON DERBY AGGREGATE types.maxPrice TO sales_role",
        ]

        for sql in grant_cmds:
            with self.subTest(f"Testing DB2s's GRANT command statement: {sql}"):
                self.validate_identity(sql, check_command_warning=True)

        self.validate_identity("GRANT SELECT ON TABLE t TO maria, harry")
        self.validate_identity("GRANT SELECT ON TABLE s.v TO PUBLIC")
        self.validate_identity("GRANT SELECT ON TABLE t TO purchases_reader_role")
        self.validate_identity("GRANT UPDATE, TRIGGER ON TABLE t TO anita, zhi")
        self.validate_identity("GRANT EXECUTE ON PROCEDURE p TO george")
        self.validate_identity("GRANT USAGE ON SEQUENCE order_id TO sales_role")

    def test_datetrunc(self):
        self.validate_all(
            "TRUNC(CURRENT_TIMESTAMP, 'YEAR')",
            write={
                "clickhouse": "DATE_TRUNC('YEAR', CURRENT_TIMESTAMP())",
                "db2": "TRUNC(CURRENT_TIMESTAMP, 'YEAR')",
            },
        )

        # Make sure units are not normalized e.g 'Q' -> 'QUARTER' and 'W' -> 'WEEK'
        # https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/ROUND-and-TRUNC-Date-Functions.html
        for unit in (
            "'Q'",
            "'W'",
        ):
            self.validate_identity(f"TRUNC(x, {unit})")

    def test_analyze(self):
        self.validate_identity("ANALYZE TABLE tbl")
        self.validate_identity("ANALYZE INDEX ndx")
        self.validate_identity("ANALYZE TABLE db.tbl PARTITION(foo = 'foo', bar = 'bar')")
        self.validate_identity("ANALYZE TABLE db.tbl SUBPARTITION(foo = 'foo', bar = 'bar')")
        self.validate_identity("ANALYZE INDEX db.ndx PARTITION(foo = 'foo', bar = 'bar')")
        self.validate_identity("ANALYZE INDEX db.ndx PARTITION(part1)")
        self.validate_identity("ANALYZE CLUSTER db.cluster")
        self.validate_identity("ANALYZE TABLE tbl VALIDATE REF UPDATE")
        self.validate_identity("ANALYZE LIST CHAINED ROWS")
        self.validate_identity("ANALYZE LIST CHAINED ROWS INTO tbl")
        self.validate_identity("ANALYZE DELETE STATISTICS")
        self.validate_identity("ANALYZE DELETE SYSTEM STATISTICS")
        self.validate_identity("ANALYZE VALIDATE REF UPDATE")
        self.validate_identity("ANALYZE VALIDATE REF UPDATE SET DANGLING TO NULL")
        self.validate_identity("ANALYZE VALIDATE STRUCTURE")
        self.validate_identity("ANALYZE VALIDATE STRUCTURE CASCADE FAST")
        self.validate_identity(
            "ANALYZE TABLE tbl VALIDATE STRUCTURE CASCADE COMPLETE ONLINE INTO db.tbl"
        )
        self.validate_identity(
            "ANALYZE TABLE tbl VALIDATE STRUCTURE CASCADE COMPLETE OFFLINE INTO db.tbl"
        )
