import unittest

from sqlglot.tokens import Tokenizer


class TestTokens(unittest.TestCase):
    def test_comment_attachment(self):
        tokenizer = Tokenizer()
        sql_comment = [
            ("/*comment*/ foo", "comment"),
            ("/*comment*/ foo --test", "comment"),
            ("--comment\nfoo --test", "comment"),
            ("foo --comment", "comment"),
            ("foo", None),
            ("foo /*comment 1*/ /*comment 2*/", "comment 1"),
        ]

        for sql, comment in sql_comment:
            self.assertEqual(tokenizer.tokenize(sql)[0].comment, comment)
