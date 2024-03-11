from sqlglot import exp
from sqlglot.errors import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestOracle(Validator):
    dialect = "oracle"

    def test_oracle(self):
        self.validate_all(
            "SELECT CONNECT_BY_ROOT x y",
            write={
                "": "SELECT CONNECT_BY_ROOT(x) AS y",
                "oracle": "SELECT CONNECT_BY_ROOT x AS y",
            },
        )
        self.parse_one("ALTER TABLE tbl_name DROP FOREIGN KEY fk_symbol").assert_is(exp.AlterTable)

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
            "SELECT MIN(column_name) KEEP (DENSE_RANK FIRST ORDER BY column_name DESC) FROM table_name"
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
            "SYSDATE",
            "CURRENT_TIMESTAMP",
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
            "SELECT * FROM T ORDER BY I OFFSET nvl(:variable1, 10) ROWS FETCH NEXT nvl(:variable2, 10) ROWS ONLY",
            "SELECT * FROM T ORDER BY I OFFSET COALESCE(:variable1, 10) ROWS FETCH NEXT COALESCE(:variable2, 10) ROWS ONLY",
        )
        self.validate_identity(
            "SELECT * FROM t SAMPLE (.25)",
            "SELECT * FROM t SAMPLE (0.25)",
        )
        self.validate_identity("SELECT TO_CHAR(-100, 'L99', 'NL_CURRENCY = '' AusDollars '' ')")

        self.validate_all(
            "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'yyyy/mm/dd') AND TO_DATE(f.C_EDATE, 'yyyy/mm/dd')",
            read={
                "postgres": "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'yyyy/mm/dd') AND TO_DATE(f.C_EDATE, 'yyyy/mm/dd')",
            },
            write={
                "oracle": "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'yyyy/mm/dd') AND TO_DATE(f.C_EDATE, 'yyyy/mm/dd')",
                "postgres": "CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, 'yyyy/mm/dd') AND TO_DATE(f.C_EDATE, 'yyyy/mm/dd')",
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
            "NVL(NULL, 1)",
            write={
                "": "COALESCE(NULL, 1)",
                "oracle": "COALESCE(NULL, 1)",
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

    def test_join_marker(self):
        self.validate_identity("SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y (+) = e2.y")

        self.validate_all(
            "SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y = e2.y (+)", write={"": UnsupportedError}
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

    def test_xml_table(self):
        self.validate_identity("XMLTABLE('x')")
        self.validate_identity("XMLTABLE('x' RETURNING SEQUENCE BY REF)")
        self.validate_identity("XMLTABLE('x' PASSING y)")
        self.validate_identity("XMLTABLE('x' PASSING y RETURNING SEQUENCE BY REF)")
        self.validate_identity(
            "XMLTABLE('x' RETURNING SEQUENCE BY REF COLUMNS a VARCHAR2, b FLOAT)"
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
    dbms_xmlgen.GETXMLTYPE('SELECT table_name, column_name, data_default FROM user_tab_columns')
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
START WITH last_name = 'King'
CONNECT BY PRIOR employee_id = manager_id AND LEVEL <= 4
WHERE
  level <= 3 AND department_id = 80"""

        for query in (f"{body}{start}{connect}", f"{body}{connect}{start}"):
            self.validate_identity(query, pretty, pretty=True)
