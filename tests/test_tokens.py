import unittest

from sqlglot.tokens import Tokenizer, TokenType


class TestTokens(unittest.TestCase):
    def test_comment_attachment(self):
        tokenizer = Tokenizer()
        sql_comment = [
            ("/*comment*/ foo", ["comment"]),
            ("/*comment*/ foo --test", ["comment", "test"]),
            ("--comment\nfoo --test", ["comment", "test"]),
            ("foo --comment", ["comment"]),
            ("foo", []),
            ("foo /*comment 1*/ /*comment 2*/", ["comment 1", "comment 2"]),
        ]

        for sql, comment in sql_comment:
            self.assertEqual(tokenizer.tokenize(sql)[0].comments, comment)

    def test_token_line(self):
        tokens = Tokenizer().tokenize(
            """SELECT /*
            line break
            */
            'x
            y',
            x"""
        )

        self.assertEqual(tokens[-1].line, 6)

    def test_jinja(self):
        tokenizer = Tokenizer()

        tokens = tokenizer.tokenize(
            """
            SELECT
               {{ x }},
               {{- x -}},
               {# it's a comment #}
               {% for x in y -%}
               a {{+ b }}
               {% endfor %};
        """
        )

        tokens = [(token.token_type, token.text) for token in tokens]

        self.assertEqual(
            tokens,
            [
                (TokenType.SELECT, "SELECT"),
                (TokenType.L_BRACE, "{"),
                (TokenType.L_BRACE, "{"),
                (TokenType.VAR, "x"),
                (TokenType.R_BRACE, "}"),
                (TokenType.R_BRACE, "}"),
                (TokenType.COMMA, ","),
                (TokenType.BLOCK_START, "{{-"),
                (TokenType.VAR, "x"),
                (TokenType.BLOCK_END, "-}}"),
                (TokenType.COMMA, ","),
                (TokenType.BLOCK_START, "{%"),
                (TokenType.FOR, "for"),
                (TokenType.VAR, "x"),
                (TokenType.IN, "in"),
                (TokenType.VAR, "y"),
                (TokenType.BLOCK_END, "-%}"),
                (TokenType.VAR, "a"),
                (TokenType.BLOCK_START, "{{+"),
                (TokenType.VAR, "b"),
                (TokenType.R_BRACE, "}"),
                (TokenType.R_BRACE, "}"),
                (TokenType.BLOCK_START, "{%"),
                (TokenType.VAR, "endfor"),
                (TokenType.BLOCK_END, "%}"),
                (TokenType.SEMICOLON, ";"),
            ],
        )
