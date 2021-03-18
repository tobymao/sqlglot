# pylint: disable=no-member
import sqlglot.expressions as exp
from sqlglot.generator import Generator
from sqlglot.helper import RegisteringMeta
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class Dialect(metaclass=RegisteringMeta):
    identifier = None
    quote = None
    functions = {}
    transforms = {}

    def parse(self, code):
        return self.parser().parse(self.tokenizer().tokenize(code), code)

    def generate(self, expression, **opts):
        return self.generator(**opts).generate(expression)

    def transpile(self, code, **opts):
        return self.generate(self.parse(code), **opts)

    def generator(self, **opts):
        return Generator(**{
            'identifier': self.identifier,
            'quote': self.quote,
            'transforms': {**self.transforms, **opts.pop('transforms', {})},
            **opts,
        })

    def parser(self, **opts):
        return Parser(functions=self.functions, **opts)

    def tokenizer(self, **opts):
        return Tokenizer(identifier=self.identifier, quote=self.quote, **opts)


class MySQL(Dialect):
    identifier = '`'


class Postgres(Dialect):
    transforms = {
        TokenType.TINYINT: 'SMALLINT',
        TokenType.FLOAT: 'REAL',
        TokenType.DOUBLE: 'DOUBLE PRECISION',
        TokenType.BINARY: 'BYTEA',
    }


class Presto(Dialect):
    def _approx_distinct_sql(gen, e):
        accuracy = ', ' + gen.sql(e, 'accuracy') if e.args.get('accuracy') else ''
        return f"APPROX_DISTINCT({gen.sql(e, 'this')}{accuracy})"

    def _parse_approx_distinct(args):
        return exp.ApproxDistinct(
            this=args[0],
            accuracy=args[1] if len(args) > 1 else None,
        )

    transforms = {
        TokenType.INT: 'INTEGER',
        TokenType.FLOAT: 'REAL',
        TokenType.BINARY: 'VARBINARY',
        exp.ApproxDistinct: _approx_distinct_sql,
    }

    functions = {
        'APPROX_DISTINCT': _parse_approx_distinct,
    }

class Spark(Dialect):
    def _approx_distinct_sql(gen, e):
        if e.args.get('accuracy'):
            gen.unsupported('APPROX_COUNT_DISTINCT does not support accuracy')
        return f"APPROX_COUNT_DISTINCT({gen.sql(e, 'this')})"

    transforms = {
        exp.ApproxDistinct: _approx_distinct_sql,
        TokenType.TINYINT: 'BYTE',
        TokenType.SMALLINT: 'SHORT',
        TokenType.BIGINT: 'BIGINT',
        TokenType.CHAR: 'CHAR',
        TokenType.VARCHAR: 'VARCHAR',
        TokenType.TEXT: 'STRING',
        TokenType.BINARY: 'ARRAY[BYTE]',
    }

    functions = {
        'APPROX_COUNT_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0]),
    }

    identifier = '`'


class SQLite(Dialect):
    transforms = {
        TokenType.BOOLEAN: 'INTEGER',
        TokenType.TINYINT: 'INTEGER',
        TokenType.SMALLINT: 'INTEGER',
        TokenType.INT: 'INTEGER',
        TokenType.BIGINT: 'INTEGER',
        TokenType.FLOAT: 'REAL',
        TokenType.DOUBLE: 'REAL',
        TokenType.DECIMAL: 'REAL',
        TokenType.CHAR: 'TEXT',
        TokenType.VARCHAR: 'TEXT',
        TokenType.BINARY: 'BLOB',
    }
