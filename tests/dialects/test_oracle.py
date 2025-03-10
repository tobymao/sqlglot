from sqlglot import exp, UnsupportedError
from tests.dialects.test_dialect import Validator


class TestOracle(Validator):
    dialect = "oracle"

    def test_oracle(self):
        self.validate_identity("1 /* /* */")
        self.validate_all(
            "SELECT CONNECT_BY_ROOT x y",
            write={
                "": "SELECT CONNECT_BY_ROOT x AS y",
                "oracle": "SELECT CONNECT_BY_ROOT x AS y",
            },
        )
        self.parse_one("ALTER TABLE tbl_name DROP FOREIGN KEY fk_symbol").assert_is(exp.Alter)

        self.validate_identity("CAST(value AS NUMBER DEFAULT 0 ON CONVERSION ERROR)")
        self.validate_identity("SYSDATE")
        self.validate_identity("CREATE GLOBAL TEMPORARY TABLE t AS SELECT * FROM orders")
        self.validate_identity("CREATE PRIVATE TEMPORARY TABLE t AS SELECT * FROM orders")
        self.validate_identity("REGEXP_REPLACE('source', 'search')")
        self.validate_identity("TIMESTAMP(3) WITH TIME ZONE")
        self.validate_identity("CURRENT_TIMESTAMP(precision)")
        self.validate_identity("ALTER TABLE tbl_name DROP FOREIGN KEY fk_symbol")
        self.validate_identity("ALTER TABLE Payments ADD Stock NUMBER NOT NULL")
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
        self.validate_identity("SELECT * FROM table_name SAMPLE (25) s")
        self.validate_identity("SELECT COUNT(*) * 10 FROM orders SAMPLE (10) SEED (1)")
        self.validate_identity("SELECT * FROM V$SESSION")
        self.validate_identity("SELECT TO_DATE('January 15, 1989, 11:00 A.M.')")
        self.validate_identity("SELECT INSTR(haystack, needle)")
        self.validate_identity(
            "SELECT * FROM test UNPIVOT INCLUDE NULLS (value FOR Description IN (col AS 'PREFIX ' || CHR(38) || ' SUFFIX'))"
        )
        self.validate_identity(
            "SELECT last_name, employee_id, manager_id, LEVEL FROM employees START WITH employee_id = 100 CONNECT BY PRIOR employee_id = manager_id ORDER SIBLINGS BY last_name"
        )
        self.validate_identity(
            "ALTER TABLE Payments ADD (Stock NUMBER NOT NULL, dropid VARCHAR2(500) NOT NULL)"
        )
        self.validate_identity(
            "SELECT JSON_ARRAYAGG(JSON_OBJECT('RNK': RNK, 'RATING_CODE': RATING_CODE, 'DATE_VALUE': DATE_VALUE, 'AGENT_ID': AGENT_ID RETURNING CLOB) RETURNING CLOB) AS JSON_DATA FROM tablename"
        )
        self.validate_identity(
            "SELECT JSON_ARRAY(FOO() FORMAT JSON, BAR() NULL ON NULL RETURNING CLOB STRICT)"
        )
        self.validate_identity(
            "SELECT JSON_ARRAYAGG(FOO() FORMAT JSON ORDER BY bar NULL ON NULL RETURNING CLOB STRICT)"
        )
        self.validate_identity(
            "SELECT COUNT(1) INTO V_Temp FROM TABLE(CAST(somelist AS data_list)) WHERE col LIKE '%contact'"
        )
        self.validate_identity(
            "SELECT department_id INTO v_department_id FROM departments FETCH FIRST 1 ROWS ONLY"
        )
        self.validate_identity(
            "SELECT department_id BULK COLLECT INTO v_department_ids FROM departments"
        )
        self.validate_identity(
            "SELECT department_id, department_name BULK COLLECT INTO v_department_ids, v_department_names FROM departments"
        )
        self.validate_identity(
            "SELECT MIN(column_name) KEEP (DENSE_RANK FIRST ORDER BY column_name DESC) FROM table_name"
        )
        self.validate_identity(
            "SELECT CAST('January 15, 1989, 11:00 A.M.' AS DATE DEFAULT NULL ON CONVERSION ERROR, 'Month dd, YYYY, HH:MI A.M.') FROM DUAL",
            "SELECT TO_DATE('January 15, 1989, 11:00 A.M.', 'Month dd, YYYY, HH12:MI P.M.') FROM DUAL",
        )
        self.validate_identity(
            "SELECT TRUNC(SYSDATE)",
            "SELECT TRUNC(SYSDATE, 'DD')",
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
        self.validate_identity(
            "SELECT * FROM T ORDER BY I OFFSET NVL(:variable1, 10) ROWS FETCH NEXT NVL(:variable2, 10) ROWS ONLY",
        )
        self.validate_identity(
            "SELECT * FROM t SAMPLE (.25)",
            "SELECT * FROM t SAMPLE (0.25)",
        )
        self.validate_identity("SELECT TO_CHAR(-100, 'L99', 'NL_CURRENCY = '' AusDollars '' ')")
        self.validate_identity(
            "SELECT * FROM t START WITH col CONNECT BY NOCYCLE PRIOR col1 = col2"
        )

        self.validate_all(
            "SELECT TRIM('|' FROM '||Hello ||| world||')",
            write={
                "clickhouse": "SELECT TRIM(BOTH '|' FROM '||Hello ||| world||')",
                "oracle": "SELECT TRIM('|' FROM '||Hello ||| world||')",
            },
        )
        self.validate_all(
            "SELECT department_id, department_name INTO v_department_id, v_department_name FROM departments FETCH FIRST 1 ROWS ONLY",
            write={
                "oracle": "SELECT department_id, department_name INTO v_department_id, v_department_name FROM departments FETCH FIRST 1 ROWS ONLY",
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
                "oracle": "SELECT * FROM test WHERE MOD(col1, 4) = 3",
            },
        )
        self.validate_all(
            "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'YYYY/MM/DD') AND TO_DATE(f.C_EDATE, 'YYYY/MM/DD')",
            read={
                "postgres": "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'yyyy/mm/dd') AND TO_DATE(f.C_EDATE, 'yyyy/mm/dd')",
            },
            write={
                "oracle": "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'YYYY/MM/DD') AND TO_DATE(f.C_EDATE, 'YYYY/MM/DD')",
                "postgres": "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'YYYY/MM/DD') AND TO_DATE(f.C_EDATE, 'YYYY/MM/DD')",
            },
        )
        self.validate_all(
            "TO_CHAR(x)",
            write={
                "doris": "CAST(x AS STRING)",
                "oracle": "TO_CHAR(x)",
            },
        )
        self.validate_all(
            "TO_NUMBER(expr, fmt, nlsparam)",
            read={
                "teradata": "TO_NUMBER(expr, fmt, nlsparam)",
            },
            write={
                "oracle": "TO_NUMBER(expr, fmt, nlsparam)",
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
                "oracle": "TO_NUMBER(x)",
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
                "oracle": "TO_NUMBER(x, fmt)",
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
                "oracle": "SELECT TO_CHAR(CAST('1999-12-01 10:00:00' AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS')",
                "postgres": "SELECT TO_CHAR(CAST('1999-12-01 10:00:00' AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS')",
            },
        )
        self.validate_all(
            "SELECT CAST(NULL AS VARCHAR2(2328 CHAR)) AS COL1",
            write={
                "oracle": "SELECT CAST(NULL AS VARCHAR2(2328 CHAR)) AS COL1",
                "spark": "SELECT CAST(NULL AS VARCHAR(2328)) AS COL1",
            },
        )
        self.validate_all(
            "SELECT CAST(NULL AS VARCHAR2(2328 BYTE)) AS COL1",
            write={
                "oracle": "SELECT CAST(NULL AS VARCHAR2(2328 BYTE)) AS COL1",
                "spark": "SELECT CAST(NULL AS VARCHAR(2328)) AS COL1",
            },
        )
        self.validate_all(
            "DATE '2022-01-01'",
            write={
                "": "DATE_STR_TO_DATE('2022-01-01')",
                "mysql": "CAST('2022-01-01' AS DATE)",
                "oracle": "TO_DATE('2022-01-01', 'YYYY-MM-DD')",
                "postgres": "CAST('2022-01-01' AS DATE)",
            },
        )

        self.validate_all(
            "x::binary_double",
            write={
                "oracle": "CAST(x AS DOUBLE PRECISION)",
                "": "CAST(x AS DOUBLE)",
            },
        )
        self.validate_all(
            "x::binary_float",
            write={
                "oracle": "CAST(x AS FLOAT)",
                "": "CAST(x AS FLOAT)",
            },
        )
        self.validate_all(
            "CAST(x AS sch.udt)",
            read={
                "postgres": "CAST(x AS sch.udt)",
            },
            write={
                "oracle": "CAST(x AS sch.udt)",
                "postgres": "CAST(x AS sch.udt)",
            },
        )
        self.validate_all(
            "SELECT TO_TIMESTAMP('2024-12-12 12:12:12.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')",
            write={
                "oracle": "SELECT TO_TIMESTAMP('2024-12-12 12:12:12.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                "duckdb": "SELECT STRPTIME('2024-12-12 12:12:12.000000', '%Y-%m-%d %H:%M:%S.%f')",
            },
        )
        self.validate_all(
            "SELECT TO_DATE('2024-12-12', 'YYYY-MM-DD')",
            write={
                "oracle": "SELECT TO_DATE('2024-12-12', 'YYYY-MM-DD')",
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
                "oracle": "NVL(NULL, 1)",
                "": "COALESCE(NULL, 1)",
                "clickhouse": "COALESCE(NULL, 1)",
            },
        )
        self.validate_all(
            "TRIM(BOTH 'h' FROM 'Hello World')",
            write={
                "oracle": "TRIM(BOTH 'h' FROM 'Hello World')",
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
                "oracle": "SELECT * FROM t FETCH FIRST 10 ROWS ONLY",
                "tsql": "SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH FIRST 10 ROWS ONLY",
            },
        )
        self.validate_identity("CREATE OR REPLACE FORCE VIEW foo1.foo2")

    def test_join_marker(self):
        self.validate_identity("SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y (+) = e2.y")

        self.validate_all(
            "SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y = e2.y (+)",
            write={"": UnsupportedError},
        )
        self.validate_all(
            "SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y = e2.y (+)",
            write={
                "": "SELECT e1.x, e2.x FROM e AS e1, e AS e2 WHERE e1.y = e2.y",
                "oracle": "SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y = e2.y (+)",
            },
        )

    def test_hints(self):
        self.validate_identity("SELECT /*+ USE_NL(A B) */ A.COL_TEST FROM TABLE_A A, TABLE_B B")
        self.validate_identity(
            "SELECT /*+ INDEX(v.j jhist_employee_ix (employee_id start_date)) */ * FROM v"
        )
        self.validate_identity(
            "SELECT /*+ USE_NL(A B C) */ A.COL_TEST FROM TABLE_A A, TABLE_B B, TABLE_C C"
        )
        self.validate_identity(
            "SELECT /*+ NO_INDEX(employees emp_empid) */ employee_id FROM employees WHERE employee_id > 200"
        )
        self.validate_identity(
            "SELECT /*+ NO_INDEX_FFS(items item_order_ix) */ order_id FROM order_items items"
        )
        self.validate_identity(
            "SELECT /*+ LEADING(e j) */ * FROM employees e, departments d, job_history j WHERE e.department_id = d.department_id AND e.hire_date = j.start_date"
        )
        self.validate_identity("INSERT /*+ APPEND */ INTO IAP_TBL (id, col1) VALUES (2, 'test2')")
        self.validate_identity("INSERT /*+ APPEND_VALUES */ INTO dest_table VALUES (i, 'Value')")
        self.validate_identity(
            "SELECT /*+ LEADING(departments employees) USE_NL(employees) */ * FROM employees JOIN departments ON employees.department_id = departments.department_id",
            """SELECT /*+ LEADING(departments employees)
  USE_NL(employees) */
  *
FROM employees
JOIN departments
  ON employees.department_id = departments.department_id""",
            pretty=True,
        )
        self.validate_identity(
            "SELECT /*+ USE_NL(bbbbbbbbbbbbbbbbbbbbbbbb) LEADING(aaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbb cccccccccccccccccccccccc dddddddddddddddddddddddd) INDEX(cccccccccccccccccccccccc) */ * FROM aaaaaaaaaaaaaaaaaaaaaaaa JOIN bbbbbbbbbbbbbbbbbbbbbbbb ON aaaaaaaaaaaaaaaaaaaaaaaa.id = bbbbbbbbbbbbbbbbbbbbbbbb.a_id JOIN cccccccccccccccccccccccc ON bbbbbbbbbbbbbbbbbbbbbbbb.id = cccccccccccccccccccccccc.b_id JOIN dddddddddddddddddddddddd ON cccccccccccccccccccccccc.id = dddddddddddddddddddddddd.c_id",
        )
        self.validate_identity(
            "SELECT /*+ USE_NL(bbbbbbbbbbbbbbbbbbbbbbbb) LEADING(aaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbb cccccccccccccccccccccccc dddddddddddddddddddddddd) INDEX(cccccccccccccccccccccccc) */ * FROM aaaaaaaaaaaaaaaaaaaaaaaa JOIN bbbbbbbbbbbbbbbbbbbbbbbb ON aaaaaaaaaaaaaaaaaaaaaaaa.id = bbbbbbbbbbbbbbbbbbbbbbbb.a_id JOIN cccccccccccccccccccccccc ON bbbbbbbbbbbbbbbbbbbbbbbb.id = cccccccccccccccccccccccc.b_id JOIN dddddddddddddddddddddddd ON cccccccccccccccccccccccc.id = dddddddddddddddddddddddd.c_id",
            """SELECT /*+ USE_NL(bbbbbbbbbbbbbbbbbbbbbbbb)
  LEADING(
    aaaaaaaaaaaaaaaaaaaaaaaa
    bbbbbbbbbbbbbbbbbbbbbbbb
    cccccccccccccccccccccccc
    dddddddddddddddddddddddd
  )
  INDEX(cccccccccccccccccccccccc) */
  *
FROM aaaaaaaaaaaaaaaaaaaaaaaa
JOIN bbbbbbbbbbbbbbbbbbbbbbbb
  ON aaaaaaaaaaaaaaaaaaaaaaaa.id = bbbbbbbbbbbbbbbbbbbbbbbb.a_id
JOIN cccccccccccccccccccccccc
  ON bbbbbbbbbbbbbbbbbbbbbbbb.id = cccccccccccccccccccccccc.b_id
JOIN dddddddddddddddddddddddd
  ON cccccccccccccccccccccccc.id = dddddddddddddddddddddddd.c_id""",
            pretty=True,
        )
        # Test that parsing error with keywords like select where etc falls back
        self.validate_identity(
            "SELECT /*+ LEADING(departments employees) USE_NL(employees) select where group by is order by */ * FROM employees JOIN departments ON employees.department_id = departments.department_id",
            """SELECT /*+ LEADING(departments employees) USE_NL(employees) select where group by is order by */
  *
FROM employees
JOIN departments
  ON employees.department_id = departments.department_id""",
            pretty=True,
        )
        # Test that parsing error with , inside hint function falls back
        self.validate_identity(
            "SELECT /*+ LEADING(departments, employees) */ * FROM employees JOIN departments ON employees.department_id = departments.department_id"
        )
        # Test that parsing error with keyword inside hint function falls back
        self.validate_identity(
            "SELECT /*+ LEADING(departments select) */ * FROM employees JOIN departments ON employees.department_id = departments.department_id"
        )

    def test_xml_table(self):
        self.validate_identity("XMLTABLE('x')")
        self.validate_identity("XMLTABLE('x' RETURNING SEQUENCE BY REF)")
        self.validate_identity("XMLTABLE('x' PASSING y)")
        self.validate_identity("XMLTABLE('x' PASSING y RETURNING SEQUENCE BY REF)")
        self.validate_identity(
            "XMLTABLE('x' RETURNING SEQUENCE BY REF COLUMNS a VARCHAR2, b FLOAT)"
        )
        self.validate_identity(
            "SELECT x.* FROM example t, XMLTABLE(XMLNAMESPACES(DEFAULT 'http://example.com/default', 'http://example.com/ns1' AS \"ns1\"), '/root/data' PASSING t.xml COLUMNS id NUMBER PATH '@id', value VARCHAR2(100) PATH 'ns1:value/text()') x"
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
                "oracle": """SELECT
  warehouse_name AS warehouse,
  warehouse2."Water",
  warehouse2."Rail"
FROM warehouses, XMLTABLE(
  '/Warehouse'
  PASSING
    warehouses.warehouse_spec
  COLUMNS
    "Water" VARCHAR2(6) PATH 'WaterAccess',
    "Rail" VARCHAR2(6) PATH 'RailAccess'
) warehouse2""",
            },
            pretty=True,
        )

        self.validate_all(
            """SELECT table_name, column_name, data_default FROM xmltable('ROWSET/ROW'
    passing dbms_xmlgen.getxmltype('SELECT table_name, column_name, data_default FROM user_tab_columns')
    columns table_name      VARCHAR2(128)   PATH '*[1]'
            , column_name   VARCHAR2(128)   PATH '*[2]'
            , data_default  VARCHAR2(2000)  PATH '*[3]'
            );""",
            write={
                "oracle": """SELECT
  table_name,
  column_name,
  data_default
FROM XMLTABLE(
  'ROWSET/ROW'
  PASSING
    dbms_xmlgen.getxmltype('SELECT table_name, column_name, data_default FROM user_tab_columns')
  COLUMNS
    table_name VARCHAR2(128) PATH '*[1]',
    column_name VARCHAR2(128) PATH '*[2]',
    data_default VARCHAR2(2000) PATH '*[3]'
)""",
            },
            pretty=True,
        )

    def test_match_recognize(self):
        self.validate_identity(
            """SELECT
  *
FROM sales_history
MATCH_RECOGNIZE (
  PARTITION BY product
  ORDER BY
    tstamp
  MEASURES
    STRT.tstamp AS start_tstamp,
    LAST(UP.tstamp) AS peak_tstamp,
    LAST(DOWN.tstamp) AS end_tstamp,
    MATCH_NUMBER() AS mno
  ONE ROW PER MATCH
  AFTER MATCH SKIP TO LAST DOWN
  PATTERN (STRT UP+ FLAT* DOWN+)
  DEFINE
    UP AS UP.units_sold > PREV(UP.units_sold),
    FLAT AS FLAT.units_sold = PREV(FLAT.units_sold),
    DOWN AS DOWN.units_sold < PREV(DOWN.units_sold)
) MR""",
            pretty=True,
        )

    def test_json_table(self):
        self.validate_identity(
            "SELECT * FROM JSON_TABLE(foo FORMAT JSON, 'bla' ERROR ON ERROR NULL ON EMPTY COLUMNS(foo PATH 'bar'))"
        )
        self.validate_identity(
            "SELECT * FROM JSON_TABLE(foo FORMAT JSON, 'bla' ERROR ON ERROR NULL ON EMPTY COLUMNS foo PATH 'bar')",
            "SELECT * FROM JSON_TABLE(foo FORMAT JSON, 'bla' ERROR ON ERROR NULL ON EMPTY COLUMNS(foo PATH 'bar'))",
        )
        self.validate_identity(
            """SELECT
  CASE WHEN DBMS_LOB.GETLENGTH(info) < 32000 THEN DBMS_LOB.SUBSTR(info) END AS info_txt,
  info AS info_clob
FROM schemaname.tablename ar
INNER JOIN JSON_TABLE(:emps, '$[*]' COLUMNS(empno NUMBER PATH '$')) jt
  ON ar.empno = jt.empno""",
            pretty=True,
        )
        self.validate_identity(
            """SELECT
  *
FROM JSON_TABLE(res, '$.info[*]' COLUMNS(
  tempid NUMBER PATH '$.tempid',
  NESTED PATH '$.calid[*]' COLUMNS(last_dt PATH '$.last_dt ')
)) src""",
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
        for restriction in ("READ ONLY", "CHECK OPTION"):
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

    def test_json_functions(self):
        for format_json in ("", " FORMAT JSON"):
            for on_cond in (
                "",
                " TRUE ON ERROR",
                " NULL ON EMPTY",
                " DEFAULT 1 ON ERROR TRUE ON EMPTY",
            ):
                for passing in ("", " PASSING 'name1' AS \"var1\", 'name2' AS \"var2\""):
                    with self.subTest("Testing JSON_EXISTS()"):
                        self.validate_identity(
                            f"SELECT * FROM t WHERE JSON_EXISTS(name{format_json}, '$[1].middle'{passing}{on_cond})"
                        )

    def test_grant(self):
        grant_cmds = [
            "GRANT purchases_reader_role TO george, maria",
            "GRANT USAGE ON TYPE price TO finance_role",
            "GRANT USAGE ON DERBY AGGREGATE types.maxPrice TO sales_role",
        ]

        for sql in grant_cmds:
            with self.subTest(f"Testing Oracles's GRANT command statement: {sql}"):
                self.validate_identity(sql, check_command_warning=True)

        self.validate_identity("GRANT SELECT ON TABLE t TO maria, harry")
        self.validate_identity("GRANT SELECT ON TABLE s.v TO PUBLIC")
        self.validate_identity("GRANT SELECT ON TABLE t TO purchases_reader_role")
        self.validate_identity("GRANT UPDATE, TRIGGER ON TABLE t TO anita, zhi")
        self.validate_identity("GRANT EXECUTE ON PROCEDURE p TO george")
        self.validate_identity("GRANT USAGE ON SEQUENCE order_id TO sales_role")

    def test_datetrunc(self):
        self.validate_all(
            "TRUNC(SYSDATE, 'YEAR')",
            write={
                "clickhouse": "DATE_TRUNC('YEAR', CURRENT_TIMESTAMP())",
                "oracle": "TRUNC(SYSDATE, 'YEAR')",
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
