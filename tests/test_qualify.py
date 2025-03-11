import unittest

from sqlglot import maybe_parse
from sqlglot.dialects.dialect import DialectType
from sqlglot.optimizer import qualify


def qualify_without_validate_qualify_columns(sql: str, dialect: DialectType = None, **kwargs):
    """
    A helper function to qualify SQL queries without validating qualified columns.

    This function parses the provided SQL string and applies the qualify transformation
    with certain default options. The qualify process typically adds table aliases to
    column references to make them fully qualified.

    Args:
        sql (str): The SQL query string to qualify
        dialect (DialectType, optional): The SQL dialect to use for parsing
        **kwargs: Additional arguments to pass to the qualify function

    Returns:
        A qualified SQL expression tree
    """
    expression = maybe_parse(sql, dialect=dialect)
    expression = qualify.qualify(
        expression,
        dialect=dialect,
        # Override defaults to disable column validation and identifier normalization
        **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
    )
    return expression


class TestQualify(unittest.TestCase):
    def test_qualify_without_table_alias(self) -> None:
        """
        Tests SQL qualification for UNPIVOT queries without explicit table aliases.

        This test verifies that when an UNPIVOT is used without an explicit alias,
        the qualify process correctly generates an automatic alias (e.g., _q_0)
        and qualifies all column references appropriately.

        The test checks that columns like EMPLOYEE_ID, QUARTER, and SALES are
        properly qualified, and the CASE expressions are handled correctly.
        """
        sql = """
        SELECT 
          EMPLOYEE_ID,
          QUARTER,
          SALES,
          CASE 
            WHEN QUARTER = 'Q1_SALES' THEN 'First Quarter'
            WHEN QUARTER = 'Q2_SALES' THEN 'Second Quarter'
            WHEN QUARTER = 'Q3_SALES' THEN 'Third Quarter'
            WHEN QUARTER = 'Q4_SALES' THEN 'Fourth Quarter'
          END AS QUARTER_NAME
        FROM ZETA_LAB.DATAHUB.EMPLOYEE_SALES
        UNPIVOT (
          SALES FOR QUARTER IN (
            Q1_SALES,
            Q2_SALES,
            Q3_SALES,
            Q4_SALES
          )
        )
        ORDER BY EMPLOYEE_ID, 
          CASE 
            WHEN QUARTER = 'Q1_SALES' THEN 1
            WHEN QUARTER = 'Q2_SALES' THEN 2
            WHEN QUARTER = 'Q3_SALES' THEN 3
            WHEN QUARTER = 'Q4_SALES' THEN 4
          END;
        """

        expect = maybe_parse("""
        select
            _q_0.employee_id as employee_id,
            _q_0.quarter as quarter,
            _q_0.sales as sales,
            case
                when _q_0.quarter = 'Q1_SALES' then 'First Quarter'
                when _q_0.quarter = 'Q2_SALES' then 'Second Quarter'
                when _q_0.quarter = 'Q3_SALES' then 'Third Quarter'
                when _q_0.quarter = 'Q4_SALES' then 'Fourth Quarter'
            end as quarter_name
        from
            zeta_lab.datahub.employee_sales as employee_sales 
            UNPIVOT(
                _q_0.sales for _q_0.quarter in (q1_sales, q2_sales, q3_sales, q4_sales)
            ) as _q_0
        order by
            employee_id,
            case
                when quarter = 'Q1_SALES' then 1
                when quarter = 'Q2_SALES' then 2
                when quarter = 'Q3_SALES' then 3
                when quarter = 'Q4_SALES' then 4
            end
        """)
        actual = qualify_without_validate_qualify_columns(sql)
        self.assertEqual(actual, expect)

    def test_qualify_with_table_alias(self) -> None:
        """
        Tests SQL qualification for UNPIVOT queries with explicit table aliases.

        This test verifies that when an UNPIVOT is used with an explicit alias (UNPVT),
        the qualify process correctly uses that alias to qualify all column references.

        The test checks that columns prefixed with UNPVT (e.g., UNPVT.EMPLOYEE_ID)
        are properly qualified, and that the CASE expressions are handled correctly.
        """
        sql = """
        SELECT 
          UNPVT.EMPLOYEE_ID,
          UNPVT.QUARTER,
          UNPVT.SALES,
          CASE 
            WHEN UNPVT.QUARTER = 'Q1_SALES' THEN 'First Quarter'
            WHEN UNPVT.QUARTER = 'Q2_SALES' THEN 'Second Quarter'
            WHEN UNPVT.QUARTER = 'Q3_SALES' THEN 'Third Quarter'
            WHEN UNPVT.QUARTER = 'Q4_SALES' THEN 'Fourth Quarter'
          END AS QUARTER_NAME
        FROM ZETA_LAB.DATAHUB.EMPLOYEE_SALES
        UNPIVOT (
          SALES FOR QUARTER IN (
            Q1_SALES,
            Q2_SALES,
            Q3_SALES,
            Q4_SALES
          )
        ) UNPVT
        ORDER BY UNPVT.EMPLOYEE_ID, 
          CASE 
            WHEN UNPVT.QUARTER = 'Q1_SALES' THEN 1
            WHEN UNPVT.QUARTER = 'Q2_SALES' THEN 2
            WHEN UNPVT.QUARTER = 'Q3_SALES' THEN 3
            WHEN UNPVT.QUARTER = 'Q4_SALES' THEN 4
          END;
        """
        expect = maybe_parse("""
        select
            unpvt.employee_id as employee_id,
            unpvt.quarter as quarter,
            unpvt.sales as sales,
            case
                when unpvt.quarter = 'Q1_SALES' then 'First Quarter'
                when unpvt.quarter = 'Q2_SALES' then 'Second Quarter'
                when unpvt.quarter = 'Q3_SALES' then 'Third Quarter'
                when unpvt.quarter = 'Q4_SALES' then 'Fourth Quarter'
            end as quarter_name
        from
            zeta_lab.datahub.employee_sales as employee_sales 
            UNPIVOT(
                unpvt.sales for unpvt.quarter in (q1_sales, q2_sales, q3_sales, q4_sales)
            ) as unpvt
        order by
            unpvt.employee_id,
            case
                when unpvt.quarter = 'Q1_SALES' then 1
                when unpvt.quarter = 'Q2_SALES' then 2
                when unpvt.quarter = 'Q3_SALES' then 3
                when unpvt.quarter = 'Q4_SALES' then 4
            end
        """)
        actual = qualify_without_validate_qualify_columns(sql)
        self.assertEqual(actual, expect)

    def test_qualify_with_table_alias_join(self) -> None:
        """
        Tests SQL qualification for UNPIVOT queries with table alias and JOIN operations.

        This test verifies the qualify process for a more complex query that includes:
        - An UNPIVOT operation with explicit alias (unpvt)
        - A JOIN with another table (product_info with alias pi)
        - Type casting operations (::INT, ::string)
        - A derived table (subquery in the FROM clause)

        The test ensures that all column references are properly qualified across
        the entire query structure, including within the subquery and join conditions.
        """
        sql = """
        select
            *
        from
            (
            select
                unpvt.id ::INT,
                unpvt.product_id ::INT,
                pi.product_name ::string,
                pi.category ::string,
                unpvt.month ::string,
                unpvt.sales ::INT
            from
                zeta_lab.datahub.sales_data
            UNPIVOT (
                sales for month in (jan_sales, feb_sales, mar_sales)
            ) unpvt
            join zeta_lab.zetahub.product_info pi
            on
                unpvt.product_id = pi.product_id
        )
        """

        expect = maybe_parse("""
        select
            _q_0.id as id,
            _q_0.product_id as product_id,
            _q_0.product_name as product_name,
            _q_0.category as category,
            _q_0.month as month,
            _q_0.sales as sales
        from
            (
            select
                cast(unpvt.id as INT) as id,
                cast(unpvt.product_id as INT) as product_id,
                cast(pi.product_name as TEXT) as product_name,
                cast(pi.category as TEXT) as category,
                cast(unpvt.month as TEXT) as month,
                cast(unpvt.sales as INT) as sales
            from
                zeta_lab.datahub.sales_data as sales_data 
                UNPIVOT(
                    unpvt.sales for unpvt.month in (jan_sales, feb_sales, mar_sales)
                ) as unpvt
            join zeta_lab.zetahub.product_info as pi on
                unpvt.product_id = pi.product_id) as _q_0        
        """)
        actual = qualify_without_validate_qualify_columns(sql)
        self.assertEqual(actual, expect)

    def test_qualify_nested_unpivot(self) -> None:
        """
        Tests SQL qualification for queries with nested UNPIVOT operations.

        This test verifies that the qualify process correctly handles a query with:
        - Multiple UNPIVOT operations (one nested within another)
        - Automatic alias generation for each level (_q_0, _q_1, _q_2)
        - Column references across different UNPIVOT levels
        - WHERE clause conditions that reference columns from different UNPIVOTs

        The test ensures that all column references are properly qualified and
        that the nesting structure is preserved during qualification.
        """
        sql = """
        select
            sales_period,
            sales,
            profit
        from
            (
            select
                *
            from
                my_table
            UNPIVOT(sales for sales_period in (sales_Q1, sales_Q2))
        )
        UNPIVOT(profit for profit_period in (profit_Q1, profit_Q2))
        where
            sales_period = profit_period;
        """

        expect = maybe_parse("""
        select
            _q_2.sales_period as sales_period,
            _q_2.sales as sales,
            _q_2.profit as profit
        from
            (
            select
                *
            from
                my_table as my_table 
                UNPIVOT(
                    _q_0.sales for _q_0.sales_period in (sales_q1, sales_q2)
                ) as _q_0
            ) as _q_1 
            UNPIVOT(
                _q_2.profit for _q_2.profit_period in (profit_q1, profit_q2)
            ) as _q_2
        where
            _q_2.sales_period = _q_2.profit_period       
        """)
        actual = qualify_without_validate_qualify_columns(sql)
        self.assertEqual(actual, expect)

    def test_qualify_unpivot_with_subquery_join(self) -> None:
        """
        Tests SQL qualification for UNPIVOT with subquery and complex join conditions.

        This test verifies the qualify process for a complex query that includes:
        - An UNPIVOT operation with explicit alias (UNPVT)
        - A subquery in an EXISTS clause
        - Complex join conditions with multiple OR clauses
        - Multiple references to the same columns in different contexts

        The test ensures that all column references are properly qualified in both
        the main query and the subquery, and that the relationships between the
        tables are maintained during qualification.
        """
        sql = """
        SELECT 
          UNPVT.EMPLOYEE_ID,
          UNPVT.QUARTER,
          UNPVT.SALES,
          CASE 
            WHEN UNPVT.QUARTER = 'Q1_SALES' THEN 'First Quarter'
            WHEN UNPVT.QUARTER = 'Q2_SALES' THEN 'Second Quarter'
            WHEN UNPVT.QUARTER = 'Q3_SALES' THEN 'Third Quarter'
            WHEN UNPVT.QUARTER = 'Q4_SALES' THEN 'Fourth Quarter'
          END AS QUARTER_NAME
        FROM ZETA_LAB.DATAHUB.EMPLOYEE_SALES
        UNPIVOT (
          SALES FOR QUARTER IN (
            Q1_SALES,
            Q2_SALES,
            Q3_SALES,
            Q4_SALES
          )
        ) UNPVT
        WHERE EXISTS (
          SELECT 1
          FROM ZETA_LAB.DATAHUB.SALES S
          WHERE S.EMPLOYEE_ID = UNPVT.EMPLOYEE_ID
          AND (
            (UNPVT.QUARTER = 'Q1_SALES' AND S.QUARTER = 'Q1') OR
            (UNPVT.QUARTER = 'Q2_SALES' AND S.QUARTER = 'Q2') OR
            (UNPVT.QUARTER = 'Q3_SALES' AND S.QUARTER = 'Q3') OR
            (UNPVT.QUARTER = 'Q4_SALES' AND S.QUARTER = 'Q4')
          )
        )
        ORDER BY UNPVT.EMPLOYEE_ID, 
          CASE 
            WHEN UNPVT.QUARTER = 'Q1_SALES' THEN 1
            WHEN UNPVT.QUARTER = 'Q2_SALES' THEN 2
            WHEN UNPVT.QUARTER = 'Q3_SALES' THEN 3
            WHEN UNPVT.QUARTER = 'Q4_SALES' THEN 4
          END;
        """
        expect = maybe_parse("""
        select
            unpvt.employee_id as employee_id,
            unpvt.quarter as quarter,
            unpvt.sales as sales,
            case
                when unpvt.quarter = 'Q1_SALES' then 'First Quarter'
                when unpvt.quarter = 'Q2_SALES' then 'Second Quarter'
                when unpvt.quarter = 'Q3_SALES' then 'Third Quarter'
                when unpvt.quarter = 'Q4_SALES' then 'Fourth Quarter'
            end as quarter_name
        from
            zeta_lab.datahub.employee_sales as employee_sales 
            UNPIVOT(
                unpvt.sales for unpvt.quarter in (q1_sales, q2_sales, q3_sales, q4_sales)
            ) as unpvt
        where
            exists(
            select
                1 as "1"
            from
                zeta_lab.datahub.sales as s
            where
                s.employee_id = unpvt.employee_id
                and ((unpvt.quarter = 'Q1_SALES'
                    and s.quarter = 'Q1')
                or (unpvt.quarter = 'Q2_SALES'
                    and s.quarter = 'Q2')
                or (unpvt.quarter = 'Q3_SALES'
                    and s.quarter = 'Q3')
                or (unpvt.quarter = 'Q4_SALES'
                    and s.quarter = 'Q4')))
        order by
            unpvt.employee_id,
            case
                when unpvt.quarter = 'Q1_SALES' then 1
                when unpvt.quarter = 'Q2_SALES' then 2
                when unpvt.quarter = 'Q3_SALES' then 3
                when unpvt.quarter = 'Q4_SALES' then 4
            end
        """)
        actual = qualify_without_validate_qualify_columns(sql)
        self.assertEqual(actual, expect)
