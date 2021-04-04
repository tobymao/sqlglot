import unittest

from sqlglot import Generator, parse
from sqlglot.rewriter import Rewriter

class TestRewriter(unittest.TestCase):
    def test_ctas(self):
        expression = parse("SELECT * FROM y")[0]

        self.assertEqual(
            Generator().generate(Rewriter(expression).ctas('x').expression),
            'CREATE TABLE x AS SELECT * FROM y'
        )

        self.assertEqual(
            Generator().generate(
                Rewriter(expression).ctas('x', db='foo', file_format='parquet').expression
            ),
            'CREATE TABLE foo.x STORED AS parquet AS SELECT * FROM y'
        )

        self.assertEqual(Generator().generate(expression), 'SELECT * FROM y')

        rewriter = Rewriter(expression).ctas('x')
        self.assertEqual(Generator().generate(rewriter.expression), 'CREATE TABLE x AS SELECT * FROM y')
        self.assertEqual(
            Generator().generate(rewriter.ctas('y').expression),
            'CREATE TABLE y AS SELECT * FROM y'
        )

        expression = parse("CREATE TABLE x AS SELECT * FROM y")[0]
        rewriter = Rewriter(expression, copy=False).ctas('x', file_format='ORC')
        self.assertEqual(
            Generator().generate(expression),
            'CREATE TABLE x STORED AS ORC AS SELECT * FROM y'
        )
