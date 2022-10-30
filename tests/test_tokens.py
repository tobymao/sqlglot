import unittest

from sqlglot.tokens import Tokenizer


class TestTokens(unittest.TestCase):
    def test_comment_attachment(self):
        tokenizer = Tokenizer()

        sqls = ["/*comment*/ foo", "/*comment*/ foo --test", "--comment\nfoo --test", "foo --comment", "foo"]
        comments = ["comment", "comment", "comment", "comment", None]

        for sql, comment in zip(sqls, comments):
            self.assertEqual(tokenizer.tokenize(sql)[0].comment, comment)
