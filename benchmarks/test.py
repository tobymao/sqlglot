import timeit
import sqlglot
import sqlparse

n = 100
sql = """
SELECT
  "e"."employee_id" AS "Employee #",
  "e"."first_name" || ' ' || "e"."last_name" AS "Name",
  "e"."email" AS "Email",
  "e"."phone_number" AS "Phone",
  TO_CHAR("e"."hire_date", 'MM/DD/YYYY') AS "Hire Date",
  TO_CHAR("e"."salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS "Salary",
  "e"."commission_pct" AS "Comission %",
  'works as ' || "j"."job_title" || ' in ' || "d"."department_name" || ' department (manager: ' || "dm"."first_name" || ' ' || "dm"."last_name" || ') and immediate supervisor: ' || "m"."first_name" || ' ' || "m"."last_name" AS "Current Job",
  TO_CHAR("j"."min_salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') || ' - ' || TO_CHAR("j"."max_salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS "Current Salary",
  "l"."street_address" || ', ' || "l"."postal_code" || ', ' || "l"."city" || ', ' || "l"."state_province" || ', ' || "c"."country_name" || ' (' || "r"."region_name" || ')' AS "Location",
  "jh"."job_id" AS "History Job ID",
  'worked from ' || TO_CHAR("jh"."start_date", 'MM/DD/YYYY') || ' to ' || TO_CHAR("jh"."end_date", 'MM/DD/YYYY') || ' as ' || "jj"."job_title" || ' in ' || "dd"."department_name" || ' department' AS "History Job Title"
FROM "employees" AS e
JOIN "jobs" AS j
  ON "e"."job_id" = "j"."job_id"
LEFT JOIN "employees" AS m
  ON "e"."manager_id" = "m"."employee_id"
LEFT JOIN "departments" AS d
  ON "d"."department_id" = "e"."department_id"
LEFT JOIN "employees" AS dm
  ON "d"."manager_id" = "dm"."employee_id"
LEFT JOIN "locations" AS l
  ON "d"."location_id" = "l"."location_id"
LEFT JOIN "countries" AS c
  ON "l"."country_id" = "c"."country_id"
LEFT JOIN "regions" AS r
  ON "c"."region_id" = "r"."region_id"
LEFT JOIN "job_history" AS jh
  ON "e"."employee_id" = "jh"."employee_id"
LEFT JOIN "jobs" AS jj
  ON "jj"."job_id" = "jh"."job_id"
LEFT JOIN "departments" AS dd
  ON "dd"."department_id" = "jh"."department_id"
ORDER BY
  "e"."employee_id"
"""

def test_sqlglot():
    sqlglot.parse(sql)


def test_sqlparse():
    sqlparse.parse(sql)

print('sqlglot', timeit.repeat(lambda: test_sqlglot(), number=n))
print('sqlparse', timeit.repeat(lambda: test_sqlparse(), number=n))
