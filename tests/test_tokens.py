import unittest

from sqlglot.tokens import Tokenizer


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
