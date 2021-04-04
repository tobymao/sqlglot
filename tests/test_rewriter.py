import unittest

from sqlglot import parse
from sqlglot.dialects import Hive
from sqlglot.rewriter import Rewriter

class TestRewriter(unittest.TestCase):
    def test_ctas(self):
        expression = parse("SELECT * FROM y")[0]
        generator = Hive().generator()

        self.assertEqual(
            generator.generate(Rewriter(expression).ctas('x').expression),
            'CREATE TABLE x AS SELECT * FROM y'
        )

        self.assertEqual(
            generator.generate(
                Rewriter(expression).ctas('x', db='foo', file_format='parquet').expression
            ),
            'CREATE TABLE foo.x STORED AS parquet AS SELECT * FROM y'
        )

        self.assertEqual(generator.generate(expression), 'SELECT * FROM y')

        rewriter = Rewriter(expression).ctas('x')
        self.assertEqual(generator.generate(rewriter.expression), 'CREATE TABLE x AS SELECT * FROM y')
        self.assertEqual(
            generator.generate(rewriter.ctas('y').expression),
            'CREATE TABLE y AS SELECT * FROM y'
        )

        expression = parse("CREATE TABLE x AS SELECT * FROM y")[0]
        rewriter = Rewriter(expression, copy=False).ctas('x', file_format='ORC')
        self.assertEqual(
            generator.generate(expression),
            'CREATE TABLE x STORED AS ORC AS SELECT * FROM y'
        )
