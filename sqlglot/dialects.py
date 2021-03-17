# pylint: disable=no-member
import sqlglot.expressions as exp
from sqlglot.generator import Generator
from sqlglot.helper import RegisteringMeta
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class Dialect(metaclass=RegisteringMeta):
    identifier = None
    quote = None
    types = None
    functions = {}

    def parse(self, code):
        return self.parser().parse(self.tokenizer().tokenize(code), code)

    def generate(self, expression, **opts):
        return self.generator(types=self.types, **opts).generate(expression)

    def transpile(self, code, **opts):
        return self.generate(self.parse(code), **opts)

    def generator(self, **opts):
        return Generator(
            identifier=self.identifier,
            quote=self.quote,
            functions=self.functions.get('generate'),
            **opts,
        )

    def parser(self, **opts):
        return Parser(functions=self.functions.get('parse'), **opts)

    def tokenizer(self, **opts):
        return Tokenizer(identifier=self.identifier, quote=self.quote, **opts)


class MySQL(Dialect):
    identifier = '`'


class Postgres(Dialect):
    types = {
        TokenType.TINYINT: 'SMALLINT',
        TokenType.FLOAT: 'REAL',
        TokenType.DOUBLE: 'DOUBLE PRECISION',
        TokenType.BINARY: 'BYTEA',
    }


class Presto(Dialect):
    types = {
        TokenType.INT: 'INTEGER',
        TokenType.FLOAT: 'REAL',
        TokenType.BINARY: 'VARBINARY',
    }

    def _parse_approx_distinct(args):
        return exp.ApproxDistinct(
            this=args[0],
            accuracy=args[1] if len(args) > 1 else None,
        )

    def _approx_distinct_sql(gen, e):
        accuracy = ', ' + gen.sql(e, 'accuracy') if e.args.get('accuracy') else ''
        return f"APPROX_DISTINCT({gen.sql(e, 'this')}{accuracy})"


    functions = {
        'parse': {
            'APPROX_DISTINCT': _parse_approx_distinct,
        },
        'generate': {
            exp.ApproxDistinct: _approx_distinct_sql,
        }
    }

class Spark(Dialect):
    identifier = '`'

    types = {
        TokenType.TINYINT: 'BYTE',
        TokenType.SMALLINT: 'SHORT',
        TokenType.BIGINT: 'BIGINT',
        TokenType.CHAR: 'CHAR',
        TokenType.VARCHAR: 'VARCHAR',
        TokenType.TEXT: 'STRING',
        TokenType.BINARY: 'ARRAY[BYTE]',
    }

    def _approx_distinct_sql(gen, e):
        if e.args.get('accuracy'):
            gen.unsupported('APPROX_COUNT_DISTINCT does not support accuracy')
        return f"APPROX_COUNT_DISTINCT({gen.sql(e, 'this')})"

    functions = {
        'parse': {
            'APPROX_COUNT_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0]),
        },
        'generate': {
            exp.ApproxDistinct: _approx_distinct_sql,
        }
    }


class SQLite(Dialect):
    types = {
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
