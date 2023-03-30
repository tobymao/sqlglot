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
            ("foo\n-- comment", [" comment"]),
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

    def test_command(self):
        tokens = Tokenizer().tokenize("SHOW;")
        self.assertEqual(tokens[0].token_type, TokenType.SHOW)
        self.assertEqual(tokens[1].token_type, TokenType.SEMICOLON)

        tokens = Tokenizer().tokenize("EXECUTE")
        self.assertEqual(tokens[0].token_type, TokenType.EXECUTE)
        self.assertEqual(len(tokens), 1)

        tokens = Tokenizer().tokenize("FETCH;SHOW;")
        self.assertEqual(tokens[0].token_type, TokenType.FETCH)
        self.assertEqual(tokens[1].token_type, TokenType.SEMICOLON)
        self.assertEqual(tokens[2].token_type, TokenType.SHOW)
        self.assertEqual(tokens[3].token_type, TokenType.SEMICOLON)

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
