from sqlglot.errors import UnsupportedError
from tests.dialects.test_dialect import Validator


class TestOracle(Validator):
    dialect = "oracle"

    def test_oracle(self):
        self.validate_identity("SELECT * FROM t FOR UPDATE")
        self.validate_identity("SELECT * FROM t FOR UPDATE WAIT 5")
        self.validate_identity("SELECT * FROM t FOR UPDATE NOWAIT")
        self.validate_identity("SELECT * FROM t FOR UPDATE SKIP LOCKED")
        self.validate_identity("SELECT * FROM t FOR UPDATE OF s.t.c, s.t.v")
        self.validate_identity("SELECT * FROM t FOR UPDATE OF s.t.c, s.t.v NOWAIT")
        self.validate_identity("SELECT * FROM t FOR UPDATE OF s.t.c, s.t.v SKIP LOCKED")
        self.validate_identity("SELECT STANDARD_HASH('hello')")
        self.validate_identity("SELECT STANDARD_HASH('hello', 'MD5')")
        self.validate_identity("SELECT CAST(NULL AS VARCHAR2(2328 CHAR)) AS COL1")
        self.validate_identity("SELECT CAST(NULL AS VARCHAR2(2328 BYTE)) AS COL1")
        self.validate_identity("SELECT * FROM table_name@dblink_name.database_link_domain")
        self.validate_identity("SELECT * FROM table_name SAMPLE (25) s")
        self.validate_identity("SELECT * FROM V$SESSION")
        self.validate_identity(
            "SELECT MIN(column_name) KEEP (DENSE_RANK FIRST ORDER BY column_name DESC) FROM table_name"
        )
        self.validate_identity(
            "SELECT last_name, department_id, salary, MIN(salary) KEEP (DENSE_RANK FIRST ORDER BY commission_pct) "
            'OVER (PARTITION BY department_id) AS "Worst", MAX(salary) KEEP (DENSE_RANK LAST ORDER BY commission_pct) '
            'OVER (PARTITION BY department_id) AS "Best" FROM employees ORDER BY department_id, salary, last_name'
        )

        self.validate_all(
            "NVL(NULL, 1)",
            write={
                "": "COALESCE(NULL, 1)",
                "oracle": "NVL(NULL, 1)",
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
