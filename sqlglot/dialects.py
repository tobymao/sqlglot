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
        unix_to_time = f"EPOCH_MS(CAST(({self.sql(expression.args['this'])} AS BIGINT) * 1000))"
        return f"STRFTIME({unix_to_time}, {self.sql(expression.args['format'])})"

    transforms = {
        exp.ApproxDistinct: _approx_count_distinct_sql,
        exp.StrToTime: lambda self, e: f"STRPTIME({self.sql(e.args['this'])}, {self.sql(e.args['format'])})",
        exp.StrToUnix: lambda self, e: f"EPOCH(STRPTIME({self.sql(e.args['this'])}, {self.sql(e.args['format'])}))",
        exp.TimeToStr: lambda self, e: f"STRFTIME({self.sql(e.args['this'])}, {self.sql(e.args['format'])})",
        exp.TimeToUnix: lambda self, e: f"EPOCH({self.sql(e.args['this'])})",
        exp.UnixToStr: _unix_to_str_sql,
        exp.UnixToTime: lambda self, e: f"EPOCH_MS(CAST(({self.sql(e.args['this'])} AS BIGINT) * 1000))",
    }

    functions = {
        'APPROX_COUNT_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0]),
        'EPOCH': lambda args: exp.TimeToUnix(this=args[0]),
        'EPOCH_MS': lambda args: exp.UnixToTime(this=exp.Slash(
            this=args[0],
            expression=Token(TokenType.NUMBER, '1000', 0, 0)
        )),
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
        return f"UNIX_TIMESTAMP({self.sql(expression.args['this'])}, {self.sql(expression.args['format'])})"

    def _str_to_time(self, expression):
        return f"DATE_FORMAT({self.sql(expression.args['this'])}, 'yyyy-MM-dd HH:mm:ss')"

    def _time_to_str(self, expression):
        return f"DATE_FORMAT({self.sql(expression.args['this'])}, {self.sql(expression.args['format'])})"

    def _time_to_unix(self, expression):
        time_to_str = f"DATE_FORMAT({self.sql(expression.args['this'])}, 'yyyy-MM-dd HH:mm:ss')"
        return f"UNIX_TIMESTAMP({time_to_str}, 'yyyy-MM-dd HH:mm:ss')"

    def _unix_to_time(self, expression):
        unix_to_str = f"FROM_UNIXTIME({self.sql(expression.args['this'])}, 'yyyy-MM-dd HH:mm:ss')"
        return f"TO_UTC_TIMESTAMP({unix_to_str}, 'UTC')"

    transforms = {
        exp.ApproxDistinct: _approx_count_distinct_sql,
        exp.FileFormat: _fileformat_sql,
        exp.JSONPath: lambda self, e: f"GET_JSON_OBJECT({self.sql(e.args['this'])}, {self.sql(e.args['path'])})",
        exp.StrToTime: _str_to_time,
        exp.StrToUnix: _str_to_unix,
        exp.TimeToStr: _time_to_str,
        exp.TimeToUnix: _time_to_unix,
        exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({self.sql(e.args['this'])}, {self.sql(e.args['format'])})",
        exp.UnixToTime: _unix_to_time,
    }

    functions = {
        'APPROX_COUNT_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0]),
        'FROM_UNIXTIME': lambda args: exp.UnixToStr(this=args[0], format=args[1]),
        'GET_JSON_OBJECT': lambda args: exp.JSONPath(this=args[0], path=args[1]),
        'UNIX_TIMESTAMP': lambda args: exp.StrToUnix(this=args[0], format=args[1]),
        'DATE_FORMAT': lambda args: exp.TimeToStr(this=args[0], format=args[1]),
    }


class MySQL(Dialect):
    identifier = '`'


class Postgres(Dialect):
    transforms = {
        TokenType.TINYINT: 'SMALLINT',
        TokenType.FLOAT: 'REAL',
        TokenType.DOUBLE: 'DOUBLE PRECISION',
        TokenType.BINARY: 'BYTEA',
        exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e.args['this'])}, {self.sql(e.args['format'])})",
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
        exp.FileFormat: _fileformat_sql,
        exp.JSONPath: lambda self, e: f"JSON_EXTRACT({self.sql(e.args['this'])}, {self.sql(e.args['path'])})",
        exp.StrToTime: lambda self, e: f"DATE_PARSE({self.sql(e.args['this'])}, {self.sql(e.args['format'])})",
        exp.StrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e.args['this'])}, {self.sql(e.args['format'])}))",
        exp.TimeToStr: lambda self, e: f"DATE_FORMAT({self.sql(e.args['this'])}, {self.sql(e.args['format'])})",
        exp.TimeToUnix: lambda self, e: f"TO_UNIXTIME({self.sql(e.args['this'])})",
        exp.UnixToStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e.args['this'])}), {self.sql(e.args['format'])})",
        exp.UnixToTime: lambda self, e: f"FROM_UNIXTIME({self.sql(e.args['this'])})",
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
        exp.StrToTime: lambda self, e: f"TO_TIMESTAMP({self.sql(e.args['this'])}, {self.sql(e.args['format'])})",
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
