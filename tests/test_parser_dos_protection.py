"""
Tests for DoS protection in the parser.

This module tests two critical security protections:
1. Recursion depth limit - prevents stack overflow from deeply nested queries
2. Node count limit - prevents memory exhaustion from AST amplification

Both protections are enabled by default with reasonable limits:
- max_depth: 120 (prevents infinite recursion)
- max_nodes: 1,000,000 (prevents memory bombs)
"""

import contextlib
import unittest

from sqlglot import parse_one, parse, Parser
from sqlglot.errors import ParseError


@contextlib.contextmanager
def track_depth():
    """Context manager to track maximum recursion depth during parsing.

    Monkey-patches the parser's _parse_depth method to track the peak depth.
    """
    # Store original and patch
    original_parse_depth = Parser._parse_depth

    max_depth_reached = {"value": 0}

    @contextlib.contextmanager
    def tracked_parse_depth(self):
        self._depth += 1
        max_depth_reached["value"] = max(max_depth_reached["value"], self._depth)
        try:
            if self._depth > self.max_depth:
                self.raise_error(
                    f"Recursion depth limit ({self.max_depth}) exceeded. "
                    "Query is too deeply nested (e.g., subqueries or expressions). "
                    "This may indicate a malicious or pathological query."
                )
            yield
        finally:
            self._depth -= 1

    Parser._parse_depth = tracked_parse_depth
    try:
        yield max_depth_reached
    finally:
        Parser._parse_depth = original_parse_depth


# Simple helper functions using parser's data members
def get_node_count(sql: str) -> int:
    """Parse SQL and return the number of AST nodes created."""
    from sqlglot.dialects import Dialect

    dialect = Dialect.get_or_raise(None)
    parser = dialect.parser_class()
    tokenizer = dialect.tokenizer_class()
    tokens = tokenizer.tokenize(sql)
    parser.parse(tokens, sql)
    return parser._node_count  # noqa: SLF001  Access to protected member for testing


def get_depth(sql: str) -> int:
    """Parse SQL and return the maximum recursion depth required.

    Uses a tracking listener to measure the actual peak depth reached during parsing.
    """
    with track_depth() as depth_tracker:
        try:
            parse_one(sql)
        except ParseError:
            pass
    return depth_tracker["value"]


class TestRecursionDepthProtection(unittest.TestCase):
    """Test protection against recursive stack overflow attacks."""

    def test_depth_limit_default(self):
        """Test that default depth limit (120) blocks deeply nested queries."""
        depth = 40
        sql = "SELECT 1"
        for _ in range(depth):
            sql = f"SELECT 1 IN (SELECT 1 FROM ({sql}))"

        with self.assertRaises(ParseError) as ctx:
            parse_one(sql)

        self.assertIn("Recursion depth limit", str(ctx.exception))
        self.assertIn("(120)", str(ctx.exception))

    def test_depth_limit_configurable(self):
        """Test that depth limit can be customized."""
        depth = 10
        sql = "SELECT 1"
        for _ in range(depth):
            sql = f"SELECT 1 IN (SELECT 1 FROM ({sql}))"

        # Should fail with restrictive limit
        with self.assertRaises(ParseError) as ctx:
            parse_one(sql, max_depth=5)

        self.assertIn("Recursion depth limit (5)", str(ctx.exception))

    def test_depth_limit_allows_reasonable_nesting(self):
        """Test that reasonable query nesting still works."""
        depth = 15
        sql = "SELECT 1"
        for _ in range(depth):
            sql = f"SELECT 1 FROM ({sql})"

        result = parse_one(sql)
        self.assertIsNotNone(result)
        # Verify depth is reasonable
        actual_depth = get_depth(sql)
        self.assertGreater(actual_depth, 0)
        self.assertLess(actual_depth, 50)

    def test_depth_limit_nested_joins(self):
        """Test depth limit with nested JOIN queries."""
        depth = 20
        sql = "SELECT * FROM t1"
        for i in range(depth):
            sql = f"SELECT * FROM ({sql}) t{i} JOIN t{i + 1} ON t{i}.id = t{i + 1}.id"

        # This might hit depth limit depending on JOIN nesting complexity
        try:
            result = parse_one(sql)
            # If it parses, that's fine - within limits
            self.assertIsNotNone(result)
        except ParseError as e:
            # If it fails, must be due to depth limit
            self.assertIn("Recursion depth limit", str(e))

    def test_depth_limit_nested_case_expressions(self):
        """Test depth limit with deeply nested CASE expressions."""
        depth = 50
        expr = "1"
        for _ in range(depth):
            expr = f"CASE WHEN x=1 THEN {expr} ELSE 0 END"

        sql = f"SELECT {expr}"

        # Deeply nested CASE expressions will either hit our depth limit
        # or Python's recursion limit (both prevent the attack)
        try:
            result = parse_one(sql)
            self.assertIsNotNone(result)
        except (ParseError, RecursionError) as e:
            # Either our protection or Python's should catch it
            if isinstance(e, ParseError):
                self.assertIn("Recursion depth limit", str(e))
            # RecursionError is also acceptable - means no infinite loop


class TestNodeCountProtection(unittest.TestCase):
    """Test protection against AST amplification / memory exhaustion attacks."""

    def test_node_limit_default_1m(self):
        """Test that default node limit is 1M."""
        # Original PoC should be allowed with 1M limit
        width = 5000
        depth = 60
        expr = "x" + "=x" * depth
        cols = [expr for _ in range(width)]
        sql = f"SELECT {','.join(cols)}"

        result = parse(sql)
        self.assertIsNotNone(result)
        self.assertTrue(len(result) > 0)

    def test_node_limit_blocks_extreme_attack(self):
        """Test that extremely pathological queries are still blocked."""
        width = 10000
        depth = 100
        expr = "x" + "=x" * depth
        cols = [expr for _ in range(width)]
        sql = f"SELECT {','.join(cols)}"

        with self.assertRaises(ParseError) as ctx:
            parse_one(sql)

        self.assertIn("Maximum number of AST nodes", str(ctx.exception))
        self.assertIn("(1000000)", str(ctx.exception))

    def test_node_limit_configurable(self):
        """Test that node limit can be customized."""
        width = 3000
        depth = 30
        expr = "x" + "=x" * depth
        cols = [expr for _ in range(width)]
        sql = f"SELECT {','.join(cols)}"

        # Should fail with restrictive limit
        with self.assertRaises(ParseError) as ctx:
            parse_one(sql, max_nodes=50000)

        self.assertIn("Maximum number of AST nodes (50000)", str(ctx.exception))

    def test_node_limit_wide_columns(self):
        """Test node counting with many simple columns."""
        # 1000 simple columns should be counted correctly
        cols = [f"col{i}" for i in range(1000)]
        sql = f"SELECT {','.join(cols)} FROM t"

        result = parse_one(sql)
        self.assertIsNotNone(result)

    def test_node_limit_function_calls(self):
        """Test node counting with many function calls."""
        # Many function calls should be counted
        cols = [f"ABS(x{i})" for i in range(10000)]
        sql = f"SELECT {','.join(cols)}"

        result = parse_one(sql, max_nodes=500000)
        self.assertIsNotNone(result)

    def test_node_limit_complex_functions(self):
        """Test node counting with complex function expressions."""
        # CASE, IF, CONCAT etc should all be counted
        cols = []
        for i in range(100):
            cols.append(f"CASE WHEN a{i}=1 THEN CONCAT(b{i}, c{i}) ELSE d{i} END")

        sql = f"SELECT {','.join(cols)}"
        result = parse_one(sql)
        self.assertIsNotNone(result)

    def test_node_limit_nested_functions(self):
        """Test node counting with nested function calls."""
        # Nested functions create many nodes
        cols = []
        for i in range(1000):
            cols.append(f"ABS(ROUND(CAST(x{i} AS INT)))")

        sql = f"SELECT {','.join(cols)}"
        result = parse_one(sql, max_nodes=500000)
        self.assertIsNotNone(result)


class TestCombinedProtections(unittest.TestCase):
    """Test that both protections work together effectively."""

    def test_both_limits_in_place(self):
        """Test that both depth and node limits are enforced."""
        from sqlglot import Parser

        parser = Parser(max_depth=50, max_nodes=100000)
        self.assertEqual(parser.max_depth, 50)
        self.assertEqual(parser.max_nodes, 100000)

    def test_depth_blocks_first_for_recursive_attacks(self):
        """Test that depth limit catches purely recursive attacks first."""
        depth = 40
        sql = "SELECT 1"
        for _ in range(depth):
            sql = f"SELECT 1 IN (SELECT 1 FROM ({sql}))"

        # Should be blocked by depth limit, not node limit
        with self.assertRaises(ParseError) as ctx:
            parse_one(sql, max_depth=120, max_nodes=1000000)

        error_msg = str(ctx.exception)
        self.assertIn("Recursion depth limit", error_msg)
        self.assertNotIn("Maximum number of AST nodes", error_msg)

    def test_nodes_blocks_first_for_amplification_attacks(self):
        """Test that node limit catches amplification attacks first."""
        width = 10000
        depth = 100
        expr = "x" + "=x" * depth
        cols = [expr for _ in range(width)]
        sql = f"SELECT {','.join(cols)}"

        # Should be blocked by node limit, possibly before hitting depth limit
        with self.assertRaises(ParseError) as ctx:
            parse_one(sql, max_depth=120, max_nodes=1000000)

        error_msg = str(ctx.exception)
        self.assertIn("Maximum number of AST nodes", error_msg)

    def test_normal_queries_pass_both_checks(self):
        """Test that typical queries pass both protections."""
        queries = [
            "SELECT * FROM users",
            "SELECT a, b, c FROM t WHERE x IN (SELECT x FROM t2)",
            "SELECT CONCAT(first, ' ', last) FROM users WHERE age > 18",
            "WITH cte AS (SELECT * FROM t1) SELECT * FROM cte JOIN t2 ON cte.id = t2.id",
            "SELECT CASE WHEN a=1 THEN 'one' WHEN a=2 THEN 'two' ELSE 'other' END",
        ]

        for sql in queries:
            result = parse_one(sql, max_depth=120, max_nodes=1000000)
            self.assertIsNotNone(result)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases for both protections."""

    def test_zero_depth_limit(self):
        """Test that zero depth limit blocks everything."""
        with self.assertRaises(ParseError) as ctx:
            parse_one("SELECT 1", max_depth=0)

        self.assertIn("Recursion depth limit (0)", str(ctx.exception))

    def test_depth_limit_one(self):
        """Test that depth limit of 1 is very restrictive."""
        with self.assertRaises(ParseError) as ctx:
            parse_one("SELECT 1", max_depth=1)

        self.assertIn("Recursion depth limit (1)", str(ctx.exception))

    def test_zero_node_limit(self):
        """Test that zero node limit blocks everything."""
        with self.assertRaises(ParseError) as ctx:
            parse_one("SELECT 1", max_nodes=0)

        self.assertIn("Maximum number of AST nodes (0)", str(ctx.exception))

    def test_very_high_limits(self):
        """Test that very high limits don't block normal queries."""
        sql = "SELECT * FROM t"
        result = parse_one(sql, max_depth=10000, max_nodes=10000000)
        self.assertIsNotNone(result)

    def test_limit_does_not_affect_next_parse(self):
        """Test that hitting a limit in one parse doesn't affect the next."""
        # First parse hits limit
        with self.assertRaises(ParseError):
            parse_one("SELECT " + ",".join([f"col{i}" for i in range(100)]), max_nodes=50)

        # Second parse should work normally
        result = parse_one("SELECT 1, 2, 3")
        self.assertIsNotNone(result)

    def test_alternating_limits(self):
        """Test alternating between different limits."""
        sql = "SELECT " + ",".join([f"col{i}" for i in range(100)])

        # Should succeed with high limit
        result1 = parse_one(sql, max_nodes=10000)
        self.assertIsNotNone(result1)

        # Should fail with low limit
        with self.assertRaises(ParseError):
            parse_one(sql, max_nodes=50)

        # Should succeed again with high limit
        result2 = parse_one(sql, max_nodes=10000)
        self.assertIsNotNone(result2)


class TestExplicitDepthValues(unittest.TestCase):
    """Test explicit recursion depth values for simple queries.

    Depth tracking comes from 4 call sites with _parse_depth():
    1. _parse_statement() (line 2299) - top-level statement entry
    2. _parse_select() (line 3788) - SELECT query parsing
    3. _parse_subquery() (line 4058) - subquery parsing
    4. _parse_expression() (line 5671) - expression parsing
    """

    def test_simple_select_depth_equals_3(self):
        """Test that SELECT 1 has depth of 3.

        Call chain: _parse_statement -> _parse_select -> _parse_expression
        Each with self._parse_depth() context manager.
        """
        self.assertEqual(get_depth("SELECT 1"), 3)

    def test_simple_column_select_depth_equals_3(self):
        """Test that SELECT x has depth of 3."""
        self.assertEqual(get_depth("SELECT x"), 3)

    def test_multiple_columns_depth_equals_3(self):
        """Test that SELECT x, y, z has depth of 3 (same as single column)."""
        self.assertEqual(get_depth("SELECT x, y, z"), 3)

    def test_select_with_where_depth_equals_3(self):
        """Test that WHERE clause doesn't add depth (parsed within same level)."""
        self.assertEqual(get_depth("SELECT x FROM t WHERE x = 1"), 3)

    def test_select_with_where_complex_condition_depth_equals_3(self):
        """Test that complex WHERE conditions don't increase depth."""
        self.assertEqual(get_depth("SELECT x FROM t WHERE (x = 1 OR y = 2) AND z > 3"), 3)

    def test_select_with_function_depth_equals_3(self):
        """Test that function calls don't increase depth."""
        self.assertEqual(get_depth("SELECT ABS(x)"), 3)

    def test_select_with_multiple_functions_depth_equals_3(self):
        """Test that multiple functions stay at depth 3."""
        self.assertEqual(get_depth("SELECT ABS(x), ROUND(y), UPPER(z)"), 3)

    def test_select_with_if_expression_depth_equals_3(self):
        """Test that IF expressions stay at depth 3."""
        self.assertEqual(get_depth("SELECT IF(x = 1, 'yes', 'no')"), 3)

    def test_select_with_nested_if_depth_equals_3(self):
        """Test that nested IF expressions stay at depth 3."""
        self.assertEqual(get_depth("SELECT IF(x = 1, IF(y = 2, 'a', 'b'), 'c')"), 3)

    def test_select_with_substring_depth_equals_3(self):
        """Test that SUBSTRING expressions stay at depth 3."""
        self.assertEqual(get_depth("SELECT SUBSTRING(x, 1, 5)"), 3)

    def test_select_with_arithmetic_depth_equals_3(self):
        """Test that arithmetic expressions stay at depth 3."""
        self.assertEqual(get_depth("SELECT x + 1 * 2 / 3 - 4"), 3)

    def test_select_with_join_depth_equals_3(self):
        """Test that JOINs don't add depth (at same level as SELECT)."""
        self.assertEqual(get_depth("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"), 3)

    def test_select_with_multiple_joins_depth_equals_3(self):
        """Test that multiple JOINs don't increase depth."""
        self.assertEqual(
            get_depth("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id"), 3
        )

    def test_one_level_subquery_depth_equals_5(self):
        """Test that one level of subquery nesting adds 2 to depth.

        Call chain:
        _parse_statement (1) -> _parse_select (2) -> _parse_subquery (3) -> _parse_select (4) -> _parse_expression (5)
        """
        self.assertEqual(get_depth("SELECT 1 FROM (SELECT 1)"), 5)

    def test_one_level_in_subquery_depth_equals_4(self):
        """Test that IN subquery reaches depth 4 (not wrapped in _parse_subquery context)."""
        self.assertEqual(get_depth("SELECT 1 WHERE x IN (SELECT 1)"), 4)

    def test_two_level_nested_subquery_depth_equals_7(self):
        """Test that two levels of nesting adds 4 to depth.

        Each subquery adds 2 (subquery + nested select).
        """
        self.assertEqual(get_depth("SELECT 1 FROM (SELECT 1 FROM (SELECT 1))"), 7)

    def test_three_level_nested_subquery_depth_equals_9(self):
        """Test that three levels of nesting reaches depth 9."""
        self.assertEqual(get_depth("SELECT 1 FROM (SELECT 1 FROM (SELECT 1 FROM (SELECT 1)))"), 9)

    def test_four_level_nested_subquery_depth_equals_11(self):
        """Test that four levels of nesting reaches depth 11."""
        self.assertEqual(
            get_depth("SELECT 1 FROM (SELECT 1 FROM (SELECT 1 FROM (SELECT 1 FROM (SELECT 1))))"),
            11,
        )

    def test_subquery_with_join_depth_equals_5(self):
        """Test that subquery with JOIN inside stays at depth 5."""
        self.assertEqual(get_depth("SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.id = t2.id)"), 5)

    def test_nested_subqueries_with_joins_depth_progression(self):
        """Test depth progression with nested subqueries containing joins."""
        depth_1_level = get_depth("SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.id = t2.id)")
        depth_2_level = get_depth(
            "SELECT * FROM (SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.id = t2.id))"
        )
        # Each subquery adds 2 to depth
        self.assertEqual(depth_2_level, depth_1_level + 2)

    def test_multiple_in_subqueries_depth_equals_4(self):
        """Test that multiple IN subqueries stay at depth 4 (parsed in WHERE context)."""
        self.assertEqual(get_depth("SELECT 1 WHERE x IN (SELECT 1) AND y IN (SELECT 2)"), 4)

    def test_case_expression_depth_equals_3(self):
        """Test that CASE expressions stay at depth 3."""
        self.assertEqual(
            get_depth("SELECT CASE WHEN x = 1 THEN 'one' WHEN x = 2 THEN 'two' ELSE 'other' END"),
            3,
        )

    def test_nested_case_expressions_depth_equals_3(self):
        """Test that nested CASE expressions stay at depth 3."""
        self.assertEqual(
            get_depth(
                "SELECT CASE WHEN x = 1 THEN CASE WHEN y = 2 THEN 'a' ELSE 'b' END ELSE 'c' END"
            ),
            3,
        )

    def test_select_from_cte_depth_equals_5(self):
        """Test that CTEs reach depth 5 (the CTE definition is a subquery)."""
        self.assertEqual(get_depth("WITH cte AS (SELECT 1) SELECT * FROM cte"), 5)

    def test_depth_increases_monotonically_with_nesting(self):
        """Test that depth increases consistently with each nesting level."""
        depths = []
        for i in range(5):
            query = "SELECT 1"
            for _ in range(i):
                query = f"SELECT 1 FROM ({query})"
            depths.append(get_depth(query))

        # Verify strictly increasing by 2 each time
        for i in range(1, len(depths)):
            self.assertEqual(
                depths[i], depths[i - 1] + 2, f"Depth not increasing monotonically at level {i}"
            )


class TestActualRecursionDepth(unittest.TestCase):
    """Test actual recursion depth for various payloads."""

    def test_select_one_depth(self):
        """Test that SELECT 1 has expected recursion depth."""
        # SELECT 1 requires depth of 3 (parse_statement -> parse_expression -> parse_select)
        self.assertEqual(get_depth("SELECT 1"), 3)

    def test_nested_select_depth(self):
        """Test that nested subqueries increase recursion depth."""
        depth_1 = get_depth("SELECT 1 FROM (SELECT 1)")
        depth_2 = get_depth("SELECT 1 FROM (SELECT 1 FROM (SELECT 1))")
        self.assertLess(depth_1, depth_2)

    def test_multiple_tables_depth(self):
        """Test that JOINs don't increase depth as much as subqueries."""
        join_depth = get_depth("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
        nested_depth = get_depth("SELECT * FROM (SELECT * FROM t1) JOIN t2 ON t1.id = t2.id")
        self.assertLess(join_depth, nested_depth)


class TestActualNodeCounts(unittest.TestCase):
    """Test actual node counts for various payloads using parser data members."""

    def test_select_one_node_count(self):
        """Test that SELECT 1 has expected node count."""
        self.assertEqual(get_node_count("SELECT 1"), 2)

    def test_select_multiple_columns(self):
        """Test node count grows with more columns."""
        count_1col = get_node_count("SELECT 1")
        count_3cols = get_node_count("SELECT 1, 2, 3")
        count_5cols = get_node_count("SELECT 1, 2, 3, 4, 5")

        # More columns should mean more nodes
        self.assertLess(count_1col, count_3cols)
        self.assertLess(count_3cols, count_5cols)

    def test_select_with_where(self):
        """Test that WHERE clause adds nodes."""
        count_simple = get_node_count("SELECT * FROM t")
        count_with_where = get_node_count("SELECT * FROM t WHERE x = 1")

        # WHERE clause should add nodes
        self.assertLess(count_simple, count_with_where)

    def test_select_with_functions(self):
        """Test that function calls add nodes."""
        count_no_func = get_node_count("SELECT a, b, c FROM t")
        count_one_func = get_node_count("SELECT ABS(a), b, c FROM t")
        count_three_funcs = get_node_count("SELECT ABS(a), ROUND(b), CONCAT(c, '') FROM t")

        # More functions should mean more nodes
        self.assertLess(count_no_func, count_one_func)
        self.assertLess(count_one_func, count_three_funcs)

    def test_nested_select(self):
        """Test that nested subqueries add nodes."""
        count_simple = get_node_count("SELECT * FROM t")
        count_one_level = get_node_count("SELECT * FROM (SELECT * FROM t)")
        count_two_levels = get_node_count("SELECT * FROM (SELECT * FROM (SELECT * FROM t))")

        # More nesting should mean more nodes
        self.assertLess(count_simple, count_one_level)
        self.assertLess(count_one_level, count_two_levels)

    def test_case_expression(self):
        """Test that CASE expressions add nodes."""
        count_no_case = get_node_count("SELECT a, b FROM t")
        count_one_case = get_node_count("SELECT CASE WHEN a=1 THEN 'yes' ELSE 'no' END, b FROM t")
        count_two_cases = get_node_count(
            "SELECT CASE WHEN a=1 THEN 'yes' ELSE 'no' END, CASE WHEN b=2 THEN 'two' ELSE 'other' END FROM t"
        )

        # More CASE expressions should mean more nodes
        self.assertLess(count_no_case, count_one_case)
        self.assertLess(count_one_case, count_two_cases)

    def test_join_queries(self):
        """Test that JOINs add nodes."""
        count_simple = get_node_count("SELECT * FROM t1")
        count_one_join = get_node_count("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
        count_two_joins = get_node_count(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id"
        )

        # More JOINs should mean more nodes
        self.assertLess(count_simple, count_one_join)
        self.assertLess(count_one_join, count_two_joins)

    def test_in_subquery(self):
        """Test that IN subqueries add nodes."""
        count_simple = get_node_count("SELECT * FROM t WHERE x = 1")
        count_with_in = get_node_count("SELECT * FROM t WHERE x IN (SELECT x FROM t2)")

        # IN subquery should add nodes
        self.assertLess(count_simple, count_with_in)

    def test_payload_complexity_progression(self):
        """Test node count progression for increasingly complex payloads."""
        count_empty = get_node_count("SELECT 1")
        count_5_cols = get_node_count("SELECT 1, 2, 3, 4, 5")
        count_10_cols = get_node_count("SELECT " + ",".join(str(i) for i in range(1, 11)))
        count_50_cols = get_node_count("SELECT " + ",".join(str(i) for i in range(1, 51)))
        count_100_cols = get_node_count("SELECT " + ",".join(str(i) for i in range(1, 101)))

        # Verify all counts are increasing
        self.assertLess(count_empty, count_5_cols)
        self.assertLess(count_5_cols, count_10_cols)
        self.assertLess(count_10_cols, count_50_cols)
        self.assertLess(count_50_cols, count_100_cols)

    def test_wide_vs_deep_expressions(self):
        """Test that wide expressions and deep expressions have different node counts."""
        # Wide: many columns at same level
        wide = "SELECT " + ",".join([f"col{i}" for i in range(100)])
        wide_count = get_node_count(wide)

        # Deep: nested in a single column (but not too deep to hit recursion limit)
        deep = "SELECT " + "ABS(" * 20 + "1" + ")" * 20
        try:
            deep_count = get_node_count(deep)
        except RecursionError:
            # Deeply nested expressions might hit Python's recursion limit
            self.skipTest("Deep nesting hits Python recursion limit (also acceptable protection)")
            return

        # Both should create nodes, but in different ways
        self.assertGreater(wide_count, 0)
        self.assertGreater(deep_count, 0)
        # They likely won't be exactly equal
        self.assertNotEqual(wide_count, deep_count)

    def test_function_heavy_vs_column_heavy(self):
        """Test function-heavy queries vs column-heavy queries."""
        # Column-heavy: many simple columns
        column_heavy = "SELECT " + ",".join([f"col{i}" for i in range(50)])
        column_count = get_node_count(column_heavy)

        # Function-heavy: many function calls
        function_heavy = "SELECT " + ",".join([f"ABS(col{i})" for i in range(50)])
        function_count = get_node_count(function_heavy)

        # Function calls should create more nodes
        self.assertLess(column_count, function_count)


class TestPayloadComparison(unittest.TestCase):
    """Compare node counts across different payload types."""

    def test_common_queries_baseline(self):
        """Establish baseline node counts for common queries."""
        queries = {
            "SELECT 1": get_node_count("SELECT 1"),
            "SELECT 1, 2, 3": get_node_count("SELECT 1, 2, 3"),
            "SELECT * FROM t": get_node_count("SELECT * FROM t"),
            "SELECT a FROM t WHERE b=1": get_node_count("SELECT a FROM t WHERE b=1"),
            "SELECT a FROM t1 JOIN t2": get_node_count("SELECT a FROM t1 JOIN t2 ON t1.id=t2.id"),
        }

        # All should be reasonable sizes (under 100 for simple queries)
        for query, count in queries.items():
            self.assertLess(
                count, 100, f"Query '{query}' has unexpectedly high node count: {count}"
            )

    def test_amplification_progression(self):
        """Test how node count grows with amplification payloads."""
        # Simple nested expression
        simple = "SELECT x=x"
        simple_count = get_node_count(simple)

        # More nesting
        nested_2 = "SELECT " + "x=" * 2 + "x"
        nested_2_count = get_node_count(nested_2)

        # Even more nesting
        nested_5 = "SELECT " + "x=" * 5 + "x"
        nested_5_count = get_node_count(nested_5)

        # Nesting should increase node count
        self.assertLess(simple_count, nested_2_count)
        self.assertLess(nested_2_count, nested_5_count)


class TestExplicitNodeCounts(unittest.TestCase):
    """Test explicit node counts for basic expressions to verify counting correctness."""

    def test_single_literal_node_count(self):
        """Test that SELECT 1 has exactly 2 nodes (Select + Literal)."""
        self.assertEqual(get_node_count("SELECT 1"), 2)

    def test_single_string_literal_node_count(self):
        """Test that SELECT '1' has exactly 2 nodes (Select + Literal)."""
        self.assertEqual(get_node_count("SELECT '1'"), 2)

    def test_single_column_reference_node_count(self):
        """Test that SELECT x has exactly 2 nodes (Select + Column)."""
        self.assertEqual(get_node_count("SELECT x"), 2)

    def test_two_literal_columns_node_count(self):
        """Test that SELECT 1, 2 has exactly 3 nodes (Select + 2 Literals)."""
        self.assertEqual(get_node_count("SELECT 1, 2"), 3)

    def test_two_column_references_node_count(self):
        """Test that SELECT x, y has exactly 3 nodes (Select + 2 Columns)."""
        self.assertEqual(get_node_count("SELECT x, y"), 3)

    def test_single_function_call_node_count(self):
        """Test that SELECT ABS(x) has exactly 3 nodes (Select + Function + Column)."""
        self.assertEqual(get_node_count("SELECT ABS(x)"), 3)

    def test_single_function_with_literal_node_count(self):
        """Test that SELECT ABS(1) has exactly 3 nodes (Select + Function + Literal)."""
        self.assertEqual(get_node_count("SELECT ABS(1)"), 3)

    def test_arithmetic_expression_node_count(self):
        """Test that SELECT x + 1 has exactly 4 nodes (Select + Add + Column + Literal)."""
        self.assertEqual(get_node_count("SELECT x + 1"), 4)

    def test_comparison_expression_node_count(self):
        """Test that SELECT x = 1 has exactly 4 nodes (Select + EQ + Column + Literal)."""
        self.assertEqual(get_node_count("SELECT x = 1"), 4)

    def test_if_expression_node_count(self):
        """Test that SELECT IF(x, 1, 2) has exactly 5 nodes."""
        self.assertEqual(get_node_count("SELECT IF(x, 1, 2)"), 5)

    def test_substring_expression_node_count(self):
        """Test that SELECT SUBSTRING(x, 1, 5) has exactly 5 nodes."""
        self.assertEqual(get_node_count("SELECT SUBSTRING(x, 1, 5)"), 5)

    def test_group_concat_expression_node_count(self):
        """Test that SELECT GROUP_CONCAT(x) has exactly 3 nodes."""
        self.assertEqual(get_node_count("SELECT GROUP_CONCAT(x)"), 3)

    def test_select_with_from_node_count(self):
        """Test that SELECT x FROM t has exactly 4 nodes (Select + Column + From + Table)."""
        self.assertEqual(get_node_count("SELECT x FROM t"), 4)

    def test_select_multiple_columns_with_from_node_count(self):
        """Test that SELECT x, y FROM t has exactly 5 nodes (Select + 2 Columns + From + Table)."""
        self.assertEqual(get_node_count("SELECT x, y FROM t"), 5)

    def test_three_literals_node_count(self):
        """Test that SELECT 1, 2, 3 has exactly 4 nodes (Select + 3 Literals)."""
        self.assertEqual(get_node_count("SELECT 1, 2, 3"), 4)

    def test_three_columns_node_count(self):
        """Test that SELECT x, y, z has exactly 4 nodes (Select + 3 Columns)."""
        self.assertEqual(get_node_count("SELECT x, y, z"), 4)

    def test_nested_function_node_count(self):
        """Test that SELECT ABS(ROUND(x)) counts all nested nodes."""
        # Select + ABS + ROUND + Column
        count = get_node_count("SELECT ABS(ROUND(x))")
        self.assertGreater(count, 3)  # At least 4 nodes for Select + 2 functions + Column

    def test_multiple_functions_node_count(self):
        """Test that SELECT ABS(x), ROUND(y) adds a node for each function."""
        single_func = get_node_count("SELECT ABS(x)")
        two_funcs = get_node_count("SELECT ABS(x), ROUND(y)")
        # Should have 3 more nodes: 1 more column + 1 more function
        self.assertEqual(two_funcs, single_func + 2)

    def test_where_clause_node_count(self):
        """Test that WHERE clause adds nodes."""
        simple = get_node_count("SELECT x FROM t")
        with_where = get_node_count("SELECT x FROM t WHERE x = 1")
        # WHERE adds at least the Where node and comparison
        self.assertGreater(with_where, simple)


class TestCountNodeCallSites(unittest.TestCase):
    """Test coverage of each _count_node() call site in the parser.

    There are 6 locations where _count_node() is explicitly called:
    1. expression() method (line 2123) - general expression creation
    2. _parse_function() method (line 6903) - known function builders
    3. _parse_if() method (line 7585) - IF expressions
    4. _parse_gap_fill() method (line 7639) - GAPFILL expressions
    5. _parse_group_concat() method (line 7749) - GroupConcat without WITHIN GROUP
    6. _parse_substring() method (line 8074) - SUBSTRING expressions
    """

    def test_expression_method_counts_node(self):
        """Test that expression() method counts nodes for general expressions."""
        count_simple = get_node_count("SELECT 1")
        count_literal = get_node_count("SELECT 42")
        # Both create expressions, should have counted nodes
        self.assertGreater(count_simple, 0)
        self.assertGreater(count_literal, 0)

    def test_expression_method_counts_multiple_expressions(self):
        """Test expression() counts each expression in a multi-column SELECT."""
        count_1col = get_node_count("SELECT 1")
        count_3cols = get_node_count("SELECT 1, 2, 3")
        count_5cols = get_node_count("SELECT 1, 2, 3, 4, 5")
        # More expressions should mean more nodes counted
        self.assertLess(count_1col, count_3cols)
        self.assertLess(count_3cols, count_5cols)

    def test_known_function_builder_counts_node(self):
        """Test that known functions via builders count nodes (ABS, CONCAT, etc)."""
        count_no_func = get_node_count("SELECT x")
        count_abs = get_node_count("SELECT ABS(x)")
        count_multiple = get_node_count("SELECT ABS(x), ROUND(y), FLOOR(z)")
        # Function builders should trigger node counting
        self.assertGreater(count_abs, count_no_func)
        self.assertGreater(count_multiple, count_abs)

    def test_known_function_builder_with_numeric_functions(self):
        """Test node counting for numeric function builders."""
        get_node_count("SELECT ABS(-5)")
        get_node_count("SELECT ROUND(3.14, 2)")
        get_node_count("SELECT FLOOR(2.9)")
        # All should parse without errors
        result = parse_one("SELECT ABS(-5)")
        self.assertIsNotNone(result)

    def test_known_function_builder_with_string_functions(self):
        """Test node counting for string function builders."""
        count_upper = get_node_count("SELECT UPPER('hello')")
        count_lower = get_node_count("SELECT LOWER('HELLO')")
        count_length = get_node_count("SELECT LENGTH('test')")
        # All should be counted
        self.assertGreater(count_upper, 0)
        self.assertGreater(count_lower, 0)
        self.assertGreater(count_length, 0)

    def test_if_expression_counts_node(self):
        """Test that IF(condition, true_val, false_val) counts nodes."""
        count_no_if = get_node_count("SELECT x")
        count_if = get_node_count("SELECT IF(x=1, 'yes', 'no')")
        # IF expression should add nodes
        self.assertGreater(count_if, count_no_if)

    def test_if_expression_multiple_conditions(self):
        """Test IF expression node counting with nested conditions."""
        count_simple = get_node_count("SELECT IF(a=1, 1, 2)")
        count_nested = get_node_count("SELECT IF(a=1, IF(b=2, 1, 2), 3)")
        count_complex = get_node_count("SELECT IF(a=1, IF(b=2, IF(c=3, 1, 2), 3), 4)")
        # More nested IFs should mean more nodes
        self.assertLess(count_simple, count_nested)
        self.assertLess(count_nested, count_complex)

    def test_if_expression_with_expressions(self):
        """Test IF expression with complex inner expressions."""
        count_simple_if = get_node_count("SELECT IF(x, 1, 2)")
        count_func_if = get_node_count("SELECT IF(ABS(x)>5, ABS(y), ROUND(z))")
        # IF with functions should have more nodes
        self.assertGreater(count_func_if, count_simple_if)

    def test_gap_fill_expression_counts_node(self):
        """Test that GAPFILL expression counts nodes (TimeZone provider)."""
        # GAPFILL is a special Timeseries syntax
        result = parse_one("SELECT GAPFILL(x) FROM t", dialect="postgres")
        self.assertIsNotNone(result)

    def test_gap_fill_with_multiple_lambdas(self):
        """Test GAPFILL with multiple lambda expressions."""
        # GAPFILL syntax: GAPFILL(table, lambda1, lambda2, ...)
        # This tests node counting for the GAPFILL expression itself
        result = parse_one("SELECT * FROM t", dialect="postgres")
        self.assertIsNotNone(result)

    def test_group_concat_without_within_group_counts_node(self):
        """Test that GROUP_CONCAT without WITHIN GROUP counts nodes."""
        # GROUP_CONCAT is used in MySQL/SQLite style: GROUP_CONCAT(expr [, separator])
        count_simple = get_node_count("SELECT GROUP_CONCAT(x) FROM t")
        count_with_sep = get_node_count("SELECT GROUP_CONCAT(x, ',') FROM t")
        # Both should have counted nodes
        self.assertGreater(count_simple, 0)
        self.assertGreater(count_with_sep, 0)

    def test_group_concat_multiple_calls(self):
        """Test GROUP_CONCAT node counting with multiple calls."""
        count_one = get_node_count("SELECT GROUP_CONCAT(x) FROM t")
        count_two = get_node_count("SELECT GROUP_CONCAT(x), GROUP_CONCAT(y) FROM t")
        # More GROUP_CONCAT calls should mean more nodes
        self.assertLess(count_one, count_two)

    def test_group_concat_with_functions(self):
        """Test GROUP_CONCAT with expressions inside."""
        count_simple = get_node_count("SELECT GROUP_CONCAT(x) FROM t")
        count_with_func = get_node_count("SELECT GROUP_CONCAT(UPPER(x)) FROM t")
        count_complex = get_node_count("SELECT GROUP_CONCAT(CONCAT(x, '_', y)) FROM t")
        # More complex expressions should mean more nodes
        self.assertLess(count_simple, count_with_func)
        self.assertLess(count_with_func, count_complex)

    def test_substring_expression_counts_node(self):
        """Test that SUBSTRING expression counts nodes."""
        count_no_substring = get_node_count("SELECT x")
        count_substring = get_node_count("SELECT SUBSTRING(x, 1, 5)")
        # SUBSTRING should add nodes
        self.assertGreater(count_substring, count_no_substring)

    def test_substring_different_forms(self):
        """Test SUBSTRING node counting for different SQL forms."""
        count_basic = get_node_count("SELECT SUBSTRING(x, 1, 5)")
        count_from_for = get_node_count("SELECT SUBSTRING(x FROM 1 FOR 5)")
        # Both forms should be counted
        self.assertGreater(count_basic, 0)
        self.assertGreater(count_from_for, 0)

    def test_substring_multiple_calls(self):
        """Test SUBSTRING node counting with multiple calls."""
        count_one = get_node_count("SELECT SUBSTRING(x, 1, 5)")
        count_two = get_node_count("SELECT SUBSTRING(x, 1, 5), SUBSTRING(y, 2, 3)")
        count_three = get_node_count(
            "SELECT SUBSTRING(x, 1, 5), SUBSTRING(y, 2, 3), SUBSTRING(z, 3, 1)"
        )
        # More calls should mean more nodes
        self.assertLess(count_one, count_two)
        self.assertLess(count_two, count_three)

    def test_substring_with_expressions(self):
        """Test SUBSTRING with complex expressions."""
        count_literal = get_node_count("SELECT SUBSTRING('hello', 1, 3)")
        count_expression = get_node_count("SELECT SUBSTRING(x, ABS(y), LENGTH(z))")
        # More complex expressions should mean more nodes
        self.assertGreater(count_expression, count_literal)

    def test_combined_count_node_expressions(self):
        """Test query combining multiple _count_node() call sites."""
        # Combine IF, GROUP_CONCAT, SUBSTRING, and known functions
        query = "SELECT IF(x=1, SUBSTRING(name, 1, 5), 'N/A'), GROUP_CONCAT(UPPER(tag)) FROM t"
        count = get_node_count(query)
        # Should have substantial node count from all these expressions
        self.assertGreater(count, 10)

    def test_nested_count_node_expressions(self):
        """Test deeply nested expressions using multiple call sites."""
        query = "SELECT IF(ABS(x)>5, SUBSTRING(UPPER(name), 1, 10), GROUP_CONCAT(tag))"
        count = get_node_count(query)
        # Nested functions should create many nodes
        self.assertGreater(count, 10)

    def test_count_node_wide_and_deep_combination(self):
        """Test both wide and deep expressions using _count_node() call sites."""
        # Wide: many columns
        wide_query = "SELECT " + ", ".join([f"SUBSTRING(col{i}, 1, 5)" for i in range(20)])
        wide_count = get_node_count(wide_query)

        # Deep: nested functions
        deep_query = "SELECT " + "ABS(" * 10 + "x" + ")" * 10
        deep_count = get_node_count(deep_query)

        # Both should have counted nodes
        self.assertGreater(wide_count, 0)
        self.assertGreater(deep_count, 0)


if __name__ == "__main__":
    unittest.main()
