from tests.dialects.test_dialect import Validator


class TestOracle(Validator):
    dialect = "oracle"

    def test_oracle(self):
        self.validate_identity("SELECT * FROM V$SESSION")

    def test_functions(self):
        self.validate_identity("SELECT NVL(NULL, 1) FROM dual")

    def test_join_marker(self):
        self.validate_identity("SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y (+) = e2.y")
        self.validate_identity("SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y = e2.y (+)")
        self.validate_identity("SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y (+) = e2.y (+)")

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
    dbms_xmlgen.getxmltype ("SELECT table_name, column_name, data_default FROM user_tab_columns")
  COLUMNS
    table_name VARCHAR2(128) PATH '*[1]',
    column_name VARCHAR2(128) PATH '*[2]',
    data_default VARCHAR2(2000) PATH '*[3]'
)""",
            },
            pretty=True,
        )
