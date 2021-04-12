# pylint: disable=no-member
import sqlglot.expressions as exp
from sqlglot.generator import Generator
from sqlglot.helper import RegisteringMeta
from sqlglot.parser import Parser
from sqlglot.tokens import Token, Tokenizer, TokenType


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


def _approx_count_distinct_sql(self, expression):
    if expression.args.get('accuracy'):
        self.unsupported('APPROX_COUNT_DISTINCT does not support accuracy')
    return f"APPROX_COUNT_DISTINCT({self.sql(expression, 'this')})"


class DuckDB(Dialect):
    def _unix_to_str_sql(self, expression):
        unix_to_time = f"EPOCH_MS(CAST(({self.sql(expression, 'this')} AS BIGINT) * 1000))"
        return f"STRFTIME({unix_to_time}, {self.sql(expression, 'format')})"

    transforms = {
        exp.ApproxDistinct: _approx_count_distinct_sql,
        exp.Array: lambda self, e: f"LIST_VALUE({self.expressions(e, flat=True)})",
        exp.StrToTime: lambda self, e: f"STRPTIME({self.sql(e, 'this')}, {self.sql(e, 'format')})",
        exp.StrToUnix: lambda self, e: f"EPOCH(STRPTIME({self.sql(e, 'this')}, {self.sql(e, 'format')}))",
        exp.TimeToStr: lambda self, e: f"STRFTIME({self.sql(e, 'this')}, {self.sql(e, 'format')})",
        exp.TimeToUnix: lambda self, e: f"EPOCH({self.sql(e, 'this')})",
        exp.UnixToStr: _unix_to_str_sql,
        exp.UnixToTime: lambda self, e: f"EPOCH_MS(CAST(({self.sql(e, 'this')} AS BIGINT) * 1000))",
    }

    functions = {
        'APPROX_COUNT_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0]),
        'EPOCH': lambda args: exp.TimeToUnix(this=args[0]),
        'EPOCH_MS': lambda args: exp.UnixToTime(this=exp.Slash(
            this=args[0],
            expression=Token(TokenType.NUMBER, '1000', 0, 0)
        )),
        'LIST_VALUE': lambda args: exp.Array(expressions=args),
        'STRFTIME': lambda args: exp.TimeToStr(this=args[0], format=args[1]),
        'STRPTIME': lambda args: exp.StrToTime(this=args[0], format=args[1]),
    }


class Hive(Dialect):
    identifier = '`'

    def _fileformat_sql(self, expression):
        file_format = self.sql(expression, 'this').replace(self.quote, '')
        if file_format:
            return F"STORED AS {file_format}"
        return ''

    def _str_to_unix(self, expression):
        return f"UNIX_TIMESTAMP({self.sql(expression, 'this')}, {self.sql(expression, 'format')})"

    def _str_to_time(self, expression):
        return f"DATE_FORMAT({self.sql(expression, 'this')}, 'yyyy-MM-dd HH:mm:ss')"

    def _time_to_str(self, expression):
        return f"DATE_FORMAT({self.sql(expression, 'this')}, {self.sql(expression, 'format')})"

    def _time_to_unix(self, expression):
        time_to_str = f"DATE_FORMAT({self.sql(expression, 'this')}, 'yyyy-MM-dd HH:mm:ss')"
        return f"UNIX_TIMESTAMP({time_to_str}, 'yyyy-MM-dd HH:mm:ss')"

    def _unix_to_time(self, expression):
        unix_to_str = f"FROM_UNIXTIME({self.sql(expression, 'this')}, 'yyyy-MM-dd HH:mm:ss')"
        return f"TO_UTC_TIMESTAMP({unix_to_str}, 'UTC')"

    transforms = {
        exp.ApproxDistinct: _approx_count_distinct_sql,
        exp.ArrayAgg: lambda self, e: f"COLLECT_LIST({self.sql(e, 'this')})",
        exp.FileFormat: _fileformat_sql,
        exp.JSONPath: lambda self, e: f"GET_JSON_OBJECT({self.sql(e, 'this')}, {self.sql(e, 'path')})",
        exp.StrToTime: _str_to_time,
        exp.StrToUnix: _str_to_unix,
        exp.TimeStrToTime: lambda self, e: self.sql(e, 'this'),
        exp.TimeStrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')})",
        exp.TimeToStr: _time_to_str,
        exp.TimeToTimeStr: lambda self, e: self.sql(e, 'this'),
        exp.TimeToUnix: _time_to_unix,
        exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({self.sql(e, 'this')}, {self.sql(e, 'format')})",
        exp.UnixToTime: _unix_to_time,
        exp.UnixToTimeStr: _unix_to_time,
    }

    functions = {
        'APPROX_COUNT_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0]),
        'COLLECT_LIST': lambda args: exp.ArrayAgg(this=args[0]),
        'DATE_FORMAT': lambda args: exp.TimeToStr(this=args[0], format=args[1]),
        'FROM_UNIXTIME': lambda args: exp.UnixToStr(this=args[0], format=args[1]),
        'GET_JSON_OBJECT': lambda args: exp.JSONPath(this=args[0], path=args[1]),
        'UNIX_TIMESTAMP': lambda args: exp.StrToUnix(this=args[0], format=args[1]),
    }


class MySQL(Dialect):
    identifier = '`'


class Postgres(Dialect):
    transforms = {
        TokenType.TINYINT: 'SMALLINT',
        TokenType.FLOAT: 'REAL',
        TokenType.DOUBLE: 'DOUBLE PRECISION',
        TokenType.BINARY: 'BYTEA',
        exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.sql(e, 'format')})",
    }

    functions = {
        'TO_TIMESTAMP': lambda args: exp.StrToTime(this=args[0], format=args[1]),
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

    def _fileformat_sql(self, expression):
        file_format = self.sql(expression, 'this').replace(self.quote, '')
        if file_format:
            return F"WITH (FORMAT = '{file_format}')"
        return ''

    transforms = {
        TokenType.INT: 'INTEGER',
        TokenType.FLOAT: 'REAL',
        TokenType.BINARY: 'VARBINARY',
        exp.ApproxDistinct: _approx_distinct_sql,
        exp.Array: lambda self, e: f"ARRAY[{self.expressions(e, flat=True)}]",
        exp.FileFormat: _fileformat_sql,
        exp.JSONPath: lambda self, e: f"JSON_EXTRACT({self.sql(e, 'this')}, {self.sql(e, 'path')})",
        exp.StrToTime: lambda self, e: f"DATE_PARSE({self.sql(e, 'this')}, {self.sql(e, 'format')})",
        exp.StrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {self.sql(e, 'format',)}))",
        exp.TimeStrToTime: lambda self, e: f"DATE_PARSE({self.sql(e, 'this')}, '%Y-%m-%d %H:%i:%s')",
        exp.TimeStrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, '%Y-%m-%d %H:%i:%s'))",
        exp.TimeToStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.sql(e, 'format',)})",
        exp.TimeToTimeStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, '%Y-%m-%d %H:%i:%s')",
        exp.TimeToUnix: lambda self, e: f"TO_UNIXTIME({self.sql(e, 'this')})",
        exp.UnixToStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), {self.sql(e, 'format',)})",
        exp.UnixToTime: lambda self, e: f"FROM_UNIXTIME({self.sql(e, 'this')})",
        exp.UnixToTimeStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), '%Y-%m-%d %H:%i:%s')",
    }

    functions = {
        'APPROX_DISTINCT': _parse_approx_distinct,
        'DATE_FORMAT': lambda args: exp.TimeToStr(this=args[0], format=args[1]),
        'DATE_PARSE': lambda args: exp.StrToTime(this=args[0], format=args[1]),
        'FROM_UNIXTIME': lambda args: exp.UnixToTime(this=args[0]),
        'JSON_EXTRACT': lambda args: exp.JSONPath(this=args[0], path=args[1]),
        'TO_UNIXTIME': lambda args: exp.TimeToUnix(this=args[0]),
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
        exp.Hint: lambda self, e: f" /*+ {self.sql(e, 'this').strip()} */",
        exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e, 'this')}, {self.sql(e, 'format')})",
    }

    functions = {
        **Hive.functions,
        'TO_UNIX_TIMESTAMP': lambda args: exp.StrToUnix(this=args[0], format=args[1]),
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
