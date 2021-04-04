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
    def _approx_distinct_sql(self, expression):
        accuracy = expression.args.get('accuracy')
        accuracy = ', ' + self.sql(accuracy) if accuracy else ''
        return f"APPROX_DISTINCT({self.sql(expression, 'this')}{accuracy})"

    def _parse_approx_distinct(args):
        return exp.ApproxDistinct(
            this=args[0],
            accuracy=args[1] if len(args) > 1 else None,
        )

    def _create_sql(self, expression):
        table_sql = self.sql(expression, 'table')
        this_sql = self.sql(expression, 'this')
        exists_sql = ' IF NOT EXISTS ' if expression.args.get('exists') else ' '
        file_format = self.sql(expression.args.get('file_format')).replace(self.quote, '')
        file_format = f" WITH (FORMAT = '{file_format}') " if file_format else ' '
        return f"CREATE TABLE{exists_sql}{table_sql}{file_format}AS{self.sep()}{this_sql}"

    transforms = {
        TokenType.INT: 'INTEGER',
        TokenType.FLOAT: 'REAL',
        TokenType.BINARY: 'VARBINARY',
        exp.ApproxDistinct: _approx_distinct_sql,
        exp.Create: _create_sql,
    }

    functions = {
        'APPROX_DISTINCT': _parse_approx_distinct,
    }


class Hive(Dialect):
    identifier = '`'

    def _approx_distinct_sql(self, expression):
        if expression.args.get('accuracy'):
            self.unsupported('APPROX_COUNT_DISTINCT does not support accuracy')
        return f"APPROX_COUNT_DISTINCT({self.sql(expression, 'this')})"

    transforms = {
        exp.ApproxDistinct: _approx_distinct_sql,
    }

    functions = {
        'APPROX_COUNT_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0]),
    }


class Spark(Hive):
    transforms = {
        **Hive.transforms,
        TokenType.TINYINT: 'BYTE',
        TokenType.SMALLINT: 'SHORT',
        TokenType.BIGINT: 'BIGINT',
        TokenType.CHAR: 'CHAR',
        TokenType.VARCHAR: 'VARCHAR',
        TokenType.TEXT: 'STRING',
        TokenType.BINARY: 'ARRAY[BYTE]',
    }


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
