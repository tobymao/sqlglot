import unittest

from sqlglot.errors import highlight_sql, ANSI_UNDERLINE, ANSI_RESET


class TestErrors(unittest.TestCase):
    def test_highlight_sql_single_character(self):
        sql = "SELECT a FROM t"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(7, 7)]
        )

        self.assertEqual(start_ctx, "SELECT ")
        self.assertEqual(highlight, "a")
        self.assertEqual(end_ctx, " FROM t")
        self.assertEqual(formatted, f"SELECT {ANSI_UNDERLINE}a{ANSI_RESET} FROM t")

    def test_highlight_sql_multi_character(self):
        sql = "SELECT foo FROM table"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(7, 9)]
        )

        self.assertEqual(start_ctx, "SELECT ")
        self.assertEqual(highlight, "foo")
        self.assertEqual(end_ctx, " FROM table")
        self.assertEqual(formatted, f"SELECT {ANSI_UNDERLINE}foo{ANSI_RESET} FROM table")

    def test_highlight_sql_multiple_highlights(self):
        sql = "SELECT a, b, c FROM table"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(7, 7), (10, 10)]
        )

        self.assertEqual(start_ctx, "SELECT ")
        self.assertEqual(highlight, "a, b")
        self.assertEqual(end_ctx, ", c FROM table")
        self.assertEqual(formatted, f"SELECT {ANSI_UNDERLINE}a{ANSI_RESET}, {ANSI_UNDERLINE}b{ANSI_RESET}, c FROM table")

    def test_highlight_sql_at_end(self):
        sql = "SELECT a FROM t"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(14, 14)]
        )

        self.assertEqual(start_ctx, "SELECT a FROM ")
        self.assertEqual(highlight, "t")
        self.assertEqual(end_ctx, "")
        self.assertEqual(formatted, f"SELECT a FROM {ANSI_UNDERLINE}t{ANSI_RESET}")

    def test_highlight_sql_entire_string(self):
        sql = "SELECT a"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(0, 7)]
        )

        self.assertEqual(start_ctx, "")
        self.assertEqual(highlight, "SELECT a")
        self.assertEqual(end_ctx, "")
        self.assertEqual(formatted, f"{ANSI_UNDERLINE}SELECT a{ANSI_RESET}")

    def test_highlight_sql_adjacent_highlights(self):
        sql = "SELECT ab FROM t"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(7, 7), (8, 8)]
        )

        self.assertEqual(start_ctx, "SELECT ")
        self.assertEqual(highlight, "ab")
        self.assertEqual(end_ctx, " FROM t")
        self.assertEqual(formatted, f"SELECT {ANSI_UNDERLINE}a{ANSI_RESET}{ANSI_UNDERLINE}b{ANSI_RESET} FROM t")

    def test_highlight_sql_small_context_length(self):
        sql = "SELECT a, b, c FROM table WHERE x = 1"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(7, 7), (10, 10)], context_length=5
        )

        self.assertEqual(start_ctx, "LECT ")
        self.assertEqual(highlight, "a, b")
        self.assertEqual(end_ctx, ", c F")
        self.assertEqual(formatted, f"LECT {ANSI_UNDERLINE}a{ANSI_RESET}, {ANSI_UNDERLINE}b{ANSI_RESET}, c F")

    def test_highlight_sql_empty_positions(self):
        sql = "SELECT a FROM t"
        with self.assertRaises(ValueError) as ctx:
            highlight_sql(sql, [])

    def test_highlight_sql_partial_overlap(self):
        sql = "SELECT foo FROM table"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(7, 9), (8, 10)]  # "foo" and "oo "
        )

        self.assertEqual(start_ctx, "SELECT ")
        self.assertEqual(highlight, "foo ")
        self.assertEqual(end_ctx, "FROM table")
        self.assertEqual(formatted, f"SELECT {ANSI_UNDERLINE}foo{ANSI_RESET}{ANSI_UNDERLINE} {ANSI_RESET}FROM table")

    def test_highlight_sql_full_overlap(self):
        sql = "SELECT foobar FROM table"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(7, 12), (9, 11)]  # "foobar" and "oba"
        )

        self.assertEqual(start_ctx, "SELECT ")
        self.assertEqual(highlight, "foobar")
        self.assertEqual(end_ctx, " FROM table")
        self.assertEqual(formatted, f"SELECT {ANSI_UNDERLINE}foobar{ANSI_RESET} FROM table")

    def test_highlight_sql_identical_positions(self):
        sql = "SELECT a FROM t"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(7, 7), (7, 7)]
        )

        self.assertEqual(start_ctx, "SELECT ")
        self.assertEqual(highlight, "a")
        self.assertEqual(end_ctx, " FROM t")
        self.assertEqual(formatted, f"SELECT {ANSI_UNDERLINE}a{ANSI_RESET} FROM t")

    def test_highlight_sql_reversed_positions(self):
        sql = "SELECT a, b FROM table"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(10, 10), (7, 7)]
        )

        self.assertEqual(start_ctx, "SELECT ")
        self.assertEqual(highlight, "a, b")
        self.assertEqual(end_ctx, " FROM table")
        self.assertEqual(formatted, f"SELECT {ANSI_UNDERLINE}a{ANSI_RESET}, {ANSI_UNDERLINE}b{ANSI_RESET} FROM table")

    def test_highlight_sql_zero_context_length(self):
        sql = "SELECT a, b FROM table"
        formatted, start_ctx, highlight, end_ctx = highlight_sql(
            sql, [(7, 7)], context_length=0
        )

        self.assertEqual(start_ctx, "")
        self.assertEqual(end_ctx, "")
        self.assertEqual(highlight, "a")
        self.assertEqual(formatted, f"{ANSI_UNDERLINE}a{ANSI_RESET}")


if __name__ == "__main__":
    unittest.main()
