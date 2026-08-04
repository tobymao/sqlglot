import unittest

from sqlglot.anonymize import anonymize, render
from sqlglot.tokens import Tokenizer, TokenType


class TestAnonymize(unittest.TestCase):
    def assert_anonymized(self, sql, expected, dialect=None):
        """expected: list of (TokenType, text[, comments]) for the full anonymized token stream."""
        tokens = anonymize(sql, dialect)
        self.assertEqual(
            [(t.token_type, t.text, t.comments) for t in tokens],
            [entry + ([],) if len(entry) == 2 else entry for entry in expected],
        )

    def test_identifiers_rewritten(self):
        self.assert_anonymized(
            "SELECT foo FROM bar",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.VAR, "aaa"),  # foo
                (TokenType.FROM, "FROM"),
                (TokenType.VAR, "aab"),  # bar
            ],
        )

    def test_consistent_within_call(self):
        self.assert_anonymized(
            "SELECT a, a, b FROM t",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.VAR, "a"),  # a
                (TokenType.COMMA, ","),
                (TokenType.VAR, "a"),  # a, same alias as above
                (TokenType.COMMA, ","),
                (TokenType.VAR, "b"),  # b
                (TokenType.FROM, "FROM"),
                (TokenType.VAR, "c"),  # t
            ],
        )

    def test_quoted_identifier_shares_alias(self):
        self.assert_anonymized(
            'SELECT foo, "foo"',
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.VAR, "aaa"),
                (TokenType.COMMA, ","),
                (TokenType.IDENTIFIER, "aaa"),
            ],
        )

    def test_strings_rewritten(self):
        self.assert_anonymized(
            "SELECT 'hello', 'world' FROM t",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.STRING, "aaaaa"),  # hello
                (TokenType.COMMA, ","),
                (TokenType.STRING, "aaaab"),  # world
                (TokenType.FROM, "FROM"),
                (TokenType.VAR, "c"),  # t
            ],
        )

    def test_whitespace(self):
        self.assert_anonymized(
            "SELECT 'line1\nline2', 'spam  eggs', 'a\tb', \"my table\" FROM t",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.STRING, "aaaaa\naaaaa"),  # line1 + newline + line2 (line break kept)
                (TokenType.COMMA, ","),
                (TokenType.STRING, "aaaa  aaab"),  # spam  eggs (spaces kept)
                (TokenType.COMMA, ","),
                (TokenType.STRING, "a\tc"),  # a\tb (tab kept)
                (TokenType.COMMA, ","),
                (TokenType.IDENTIFIER, "aa aaaad"),  # "my table" (space kept)
                (TokenType.FROM, "FROM"),
                (TokenType.VAR, "e"),  # t
            ],
        )

    def test_reserved_keywords_not_rewritten(self):
        # keyword tokens (e.g. window, out) aren't sensitive; they stay verbatim so
        # user SQL that trips the parser remains debuggable — while real identifiers get rewritten
        self.assert_anonymized(
            "SELECT * FROM window, OUT, apple",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.STAR, "*"),
                (TokenType.FROM, "FROM"),
                (TokenType.WINDOW, "window"),
                (TokenType.COMMA, ","),
                (TokenType.OUT, "OUT"),
                (TokenType.COMMA, ","),
                (TokenType.VAR, "aaaaa"),  # apple
            ],
        )

    def test_tokenize_error_tail_blanked(self):
        tokens = anonymize("SELECT foo, 'secret tail")
        self.assertEqual(
            [(t.token_type, t.text) for t in tokens],
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.VAR, "aaa"),  # foo
                (TokenType.COMMA, ","),
                (TokenType.UNKNOWN, "'s.........."),  # 'secret tail, delimiter kept
            ],
        )

    def test_comments_redacted(self):
        self.assert_anonymized(
            "SELECT a -- secret comment\nFROM t",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.VAR, "a", [" ...... ......."]),
                (TokenType.FROM, "FROM"),
                (TokenType.VAR, "b"),
            ],
        )

    def test_tpcds_query_reserialized(self):
        sql = """WITH inv
     AS (SELECT w_warehouse_name,
                w_warehouse_sk,
                i_item_sk,
                d_moy,
                stdev,
                mean,
                CASE mean
                  WHEN 0 THEN NULL
                  ELSE stdev / mean
                END cov
         FROM  (SELECT w_warehouse_name,
                       w_warehouse_sk,
                       i_item_sk,
                       d_moy,
                       Stddev_samp(inv_quantity_on_hand) stdev,
                       Avg(inv_quantity_on_hand)         mean
                FROM   inventory,
                       item,
                       warehouse,
                       date_dim
                WHERE  inv_item_sk = i_item_sk
                       AND inv_warehouse_sk = w_warehouse_sk
                       AND inv_date_sk = d_date_sk
                       AND d_year = 2002
                GROUP  BY w_warehouse_name,
                          w_warehouse_sk,
                          i_item_sk,
                          d_moy) foo
         WHERE  CASE mean
                  WHEN 0 THEN 0
                  ELSE stdev / mean
                END > 1)
SELECT inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
FROM   inv inv1,
       inv inv2
WHERE  inv1.i_item_sk = inv2.i_item_sk
       AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
       AND inv1.d_moy = 1
       AND inv2.d_moy = 1 + 1
ORDER  BY inv1.w_warehouse_sk,
          inv1.i_item_sk,
          inv1.d_moy,
          inv1.mean,
          inv1.cov,
          inv2.d_moy,
          inv2.mean,
          inv2.cov;"""
        expected = """WITH aaa
     AS (SELECT aaaaaaaaaaaaaaab,
                aaaaaaaaaaaaac,
                aaaaaaaad,
                aaaae,
                aaaaf,
                aaag,
                CASE aaag
                  WHEN 8 THEN NULL
                  ELSE aaaaf / aaag
                END aai
         FROM  (SELECT aaaaaaaaaaaaaaab,
                       aaaaaaaaaaaaac,
                       aaaaaaaad,
                       aaaae,
                       Stddev_samp(aaaaaaaaaaaaaaaaaaaj) aaaaf,
                       Avg(aaaaaaaaaaaaaaaaaaaj)         aaag
                FROM   aaaaaaaak,
                       aaal,
                       aaaaaaaam,
                       aaaaaaan
                WHERE  aaaaaaaaaao = aaaaaaaad
                       AND aaaaaaaaaaaaaaap = aaaaaaaaaaaaac
                       AND aaaaaaaaaaq = aaaaaaaar
                       AND aaaaas = 1019
                GROUP  BY aaaaaaaaaaaaaaab,
                          aaaaaaaaaaaaac,
                          aaaaaaaad,
                          aaaae) aau
         WHERE  CASE aaag
                  WHEN 8 THEN 8
                  ELSE aaaaf / aaag
                END > 4)
SELECT aaaw.aaaaaaaaaaaaac,
       aaaw.aaaaaaaad,
       aaaw.aaaae,
       aaaw.aaag,
       aaaw.aai,
       aaax.aaaaaaaaaaaaac,
       aaax.aaaaaaaad,
       aaax.aaaae,
       aaax.aaag,
       aaax.aai
FROM   aaa aaaw,
       aaa aaax
WHERE  aaaw.aaaaaaaad = aaax.aaaaaaaad
       AND aaaw.aaaaaaaaaaaaac = aaax.aaaaaaaaaaaaac
       AND aaaw.aaaae = 4
       AND aaax.aaaae = 4 + 4
ORDER  BY aaaw.aaaaaaaaaaaaac,
          aaaw.aaaaaaaad,
          aaaw.aaaae,
          aaaw.aaag,
          aaaw.aai,
          aaax.aaaae,
          aaax.aaag,
          aaax.aai;"""
        self.assertEqual(render(sql, anonymize(sql)), expected)
        self.assertEqual(len(expected), len(sql))

    def test_functions_anonymized(self):
        self.assert_anonymized(
            "SELECT SUM(x), my_udf(...), CURRENT_DATE, CASE WHEN 1 THEN 2 ELSE 3 END",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.VAR, "SUM"),  # known function — kept
                (TokenType.L_PAREN, "("),
                (TokenType.VAR, "a"),  # x
                (TokenType.R_PAREN, ")"),
                (TokenType.COMMA, ","),
                (TokenType.VAR, "aaaaab"),  # my_udf (unknown) — masked
                (TokenType.L_PAREN, "("),
                (TokenType.DOT, "."),
                (TokenType.DOT, "."),
                (TokenType.DOT, "."),
                (TokenType.R_PAREN, ")"),
                (TokenType.COMMA, ","),
                (TokenType.CURRENT_DATE, "CURRENT_DATE"),  # no-paren function — kept
                (TokenType.COMMA, ","),
                (TokenType.CASE, "CASE"),  # keyword — kept
                (TokenType.WHEN, "WHEN"),
                (TokenType.NUMBER, "3"),  # 1
                (TokenType.THEN, "THEN"),
                (TokenType.NUMBER, "4"),  # 2
                (TokenType.ELSE, "ELSE"),
                (TokenType.NUMBER, "5"),  # 3
                (TokenType.END, "END"),
            ],
        )

        # JSON_OBJECT is registered in FUNCTION_PARSERS rather than FUNCTIONS
        self.assert_anonymized(
            "SELECT JSON_OBJECT('k', v)",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.VAR, "JSON_OBJECT"),  # known function — kept
                (TokenType.L_PAREN, "("),
                (TokenType.STRING, "a"),  # 'k'
                (TokenType.COMMA, ","),
                (TokenType.VAR, "b"),  # v
                (TokenType.R_PAREN, ")"),
            ],
        )

    def test_string_family_forms_anonymized(self):
        self.assert_anonymized(
            "SELECT N'nat', $$her$$, x'4141'",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.NATIONAL_STRING, "aaa"),
                (TokenType.COMMA, ","),
                (TokenType.RAW_STRING, "aab"),
                (TokenType.COMMA, ","),
                (TokenType.HEX_STRING, "aaac"),
            ],
            "snowflake",
        )

    def test_empty_input(self):
        self.assertEqual(anonymize(""), [])
        self.assertEqual(anonymize("   "), [])

    def test_identifiers_with_spaces_stay_distinct(self):
        self.assert_anonymized(
            'SELECT "a b", "a c"',
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.IDENTIFIER, "a a"),
                (TokenType.COMMA, ","),
                (TokenType.IDENTIFIER, "a b"),
            ],
        )

    def test_number_and_string_same_text_stay_distinct(self):
        self.assert_anonymized(
            "SELECT 123, '123'",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.NUMBER, "100"),
                (TokenType.COMMA, ","),
                (TokenType.STRING, "aab"),
            ],
        )

    def test_comments_blanked_in_reserialization(self):
        sql = "SELECT a -- secret comment\nFROM t"
        self.assertEqual(render(sql, anonymize(sql)), "SELECT a -- ...... .......\nFROM b")

    def test_huge_numeric_literal_no_crash(self):
        tokens = anonymize("SELECT " + "1" * 4500)
        self.assertEqual(tokens[1].token_type, TokenType.NUMBER)
        self.assertEqual(len(tokens[1].text), 4500)

    def test_known_function_kept_when_passing_tokens(self):
        tokens = anonymize(
            Tokenizer(dialect="snowflake").tokenize("SELECT TO_VARIANT(x) FROM t"), "snowflake"
        )
        self.assertEqual(
            [(t.token_type, t.text) for t in tokens],
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.VAR, "TO_VARIANT"),  # dialect-specific known function — kept
                (TokenType.L_PAREN, "("),
                (TokenType.VAR, "a"),  # x
                (TokenType.R_PAREN, ")"),
                (TokenType.FROM, "FROM"),
                (TokenType.VAR, "b"),  # t
            ],
        )

    def test_known_function_comment_still_blanked(self):
        self.assert_anonymized(
            "SELECT sum/* secretpassword */(x)",
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.VAR, "sum", [" .............. "]),
                (TokenType.L_PAREN, "("),
                (TokenType.VAR, "a"),  # x
                (TokenType.R_PAREN, ")"),
            ],
        )

    def test_quotes_preserved(self):
        # the alias is fitted between the quotes of the source span, so a literal stays a
        # literal of the same kind and width instead of collapsing to a bare word
        cases = [
            ("SELECT 'hello' AS x", "SELECT 'aaaaa' AS b", None),
            ("SELECT 'a''b'", "SELECT 'aaaa'", None),  # the escape is content, so it's masked
            ("SELECT ''", "SELECT ''", None),
            ("SELECT N'nat', x'4141'", "SELECT N'aaa', x'aaab'", "snowflake"),
            ('SELECT "my table"', 'SELECT "aa aaaaa"', None),
            ("SELECT `back tick`", "SELECT `aaaa aaaa`", "mysql"),
            ("SELECT [brack et]", "SELECT [aaaaa aa]", "tsql"),
            ("SELECT $$her$$, $tag$body$tag$", "SELECT $$aaa$$, $tag$aaab$tag$", "postgres"),
        ]

        for sql, expected, dialect in cases:
            rendered = render(sql, anonymize(sql, dialect), dialect)
            self.assertEqual(rendered, expected)
            self.assertEqual(len(rendered), len(sql))

    def test_source_spelling_preserved(self):
        # tokens that aren't anonymized are rendered over their own span, so the spelling
        # the tokenizer normalized away (GROUP  BY -> GROUP BY) survives
        cases = [
            ("SELECT a FROM t GROUP  BY a", "SELECT a FROM b GROUP  BY a"),
            ("SELECT a ORDER   BY a", "SELECT a ORDER   BY a"),
            ("select * from window, OUT", "select * from window, OUT"),
        ]

        for sql, expected in cases:
            rendered = render(sql, anonymize(sql))
            self.assertEqual(rendered, expected)
            self.assertEqual(len(rendered), len(sql))

    def test_numbers_keep_their_shape(self):
        sql = "SELECT 1e, 1.e, 1e10, .5, 2002, 1e-5, 1E+2"
        rendered = render(sql, anonymize(sql))
        self.assertEqual(rendered, "SELECT 1e, 2.e, 3e10, .4, 1004, 6e-1, 7E+1")
        self.assertEqual(len(rendered), len(sql))

    def test_hints_anonymized(self):
        # a hint's text is the whole /*+ ... */ comment, whether the tokenizer emits it as a
        # HINT token (only in hint position) or leaves it in the gap as a plain comment
        cases = [
            (
                "SELECT /*+ INDEX(customers ssn_idx) */ a FROM t",
                "SELECT /*+ ............... ........ */ a FROM b",
                None,
            ),
            (
                "SELECT /*+ BROADCAST(`y`) */ x FROM y",
                "SELECT /*+ .............. */ a FROM b",
                "spark",
            ),
            (
                "INSERT /*+ APPEND */ INTO t VALUES (1)",
                "INSERT /*+ ...... */ INTO a VALUES (2)",
                "oracle",
            ),
            ("SELECT a /*+ INDEX(secret) */ b", "SELECT a /*+ ............. */ b", None),
            (
                "SELECT /*+ INDEX(customers)\n           MORE(ssn) */ a FROM t",
                "SELECT /*+ ................\n           ......... */ a FROM b",
                None,
            ),
        ]

        for sql, expected, dialect in cases:
            rendered = render(sql, anonymize(sql, dialect), dialect)
            self.assertEqual(rendered, expected)
            self.assertEqual(len(rendered), len(sql))

    def test_hint_does_not_desync_following_comments(self):
        # a hint's body is recorded in Token.comments but its marker isn't left in a gap, so
        # pairing gap markers with recorded bodies drifts and copies the source through
        sql = "SELECT /*+ x */ a -- password is hunter2\nFROM t"
        self.assertEqual(
            render(sql, anonymize(sql)), "SELECT /*+ . */ a -- ........ .. .......\nFROM b"
        )

    def test_comments_without_tokens(self):
        # nothing for the tokenizer to attach the comment to
        cases = [
            ("-- top secret", "-- ... ......"),
            ("/* COMMENT */", "/* ....... */"),
            ("/*", "/*"),
        ]

        for sql, expected in cases:
            rendered = render(sql, anonymize(sql))
            self.assertEqual(rendered, expected)
            self.assertEqual(len(rendered), len(sql))

    def test_tokenize_error_keeps_delimiter(self):
        # the two leading characters survive so the delimiter the tokenizer choked on is
        # still identifiable - an unterminated string reads differently to a comment
        cases = [
            ("SELECT a, 'unterminated string", "SELECT a, 'u..................", None),
            ("SELECT a, /* unterminated comment", "SELECT a, /*.....................", None),
            ('SELECT a, "unterminated ident', 'SELECT a, "u.................', None),
            ("SELECT a, $$unterminated heredoc", "SELECT a, $$....................", "postgres"),
            ("SELECT a, `unterminated backtick", "SELECT a, `u....................", "mysql"),
            ("'unterminated secret", "'u..................", None),  # nothing tokenized at all
            ("SELECT a, '", "SELECT a, '", None),  # remainder shorter than the delimiter
        ]

        for sql, expected, dialect in cases:
            rendered = render(sql, anonymize(sql, dialect), dialect)
            self.assertEqual(rendered, expected)
            self.assertEqual(len(rendered), len(sql))

    def test_comment_bodies_fully_blanked(self):
        # markers inside a comment body are body, not structure, so they're blanked too
        cases = [
            ("SELECT a /* x /* nested */ y */ b", "SELECT a /* . .. ...... .. . */ b"),
            ("SELECT a -- /* not a real block\nFROM t", "SELECT a -- .. ... . .... .....\nFROM b"),
        ]

        for sql, expected in cases:
            rendered = render(sql, anonymize(sql))
            self.assertEqual(rendered, expected)
            self.assertEqual(len(rendered), len(sql))

    def test_render_without_dialect_still_redacts(self):
        # an unrecognized marker costs readability, never redaction
        sql = "SELECT a # secret"
        self.assertEqual(render(sql, anonymize(sql, "mysql"), "mysql"), "SELECT a # ......")
        self.assertEqual(render(sql, anonymize(sql, "mysql")), "SELECT a . ......")

    def test_render(self):
        cases = [
            ("", "", None),
            (" \t\n", " \t\n", None),
            ("-- leading secret\nSELECT foo", "-- ....... ......\nSELECT aaa", None),
            (
                "SELECT /* hidden value */ foo -- trailing secret",
                "SELECT /* ...... ..... */ aaa -- ........ ......",
                None,
            ),
            (
                "SELECT foo /* first */ /* second */ FROM bar",
                "SELECT aaa /* ..... */ /* ...... */ FROM aab",
                None,
            ),
            ("SELECT foo, 'secret tail", "SELECT aaa, 's..........", None),
            ("SELECT foo // secret", "SELECT aaa // ......", "snowflake"),
            ("SELECT foo # secret", "SELECT aaa # ......", "mysql"),
        ]

        for sql, expected, dialect in cases:
            rendered = render(sql, anonymize(sql, dialect), dialect)
            self.assertEqual(rendered, expected)
            self.assertEqual(len(rendered), len(sql))
