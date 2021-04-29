# pylint: disable=no-member
import sqlglot.expressions as exp
from sqlglot.generator import Generator
from sqlglot.helper import RegisteringMeta, csv, list_get
from sqlglot.parser import Parser
from sqlglot.tokens import Token, Tokenizer, TokenType


class Dialect(metaclass=RegisteringMeta):
    identifier = None
    quote = None
    escape = None
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
            'escape': self.escape,
            'transforms': {**self.transforms, **opts.pop('transforms', {})},
            **opts,
        })

    def parser(self, **opts):
        return Parser(functions=self.functions, **opts)

    def tokenizer(self, **opts):
        return Tokenizer(
            identifier=self.identifier,
            quote=self.quote,
            escape=self.escape,
            **opts,
        )


def _approx_count_distinct_sql(self, expression):
    if expression.args.get('accuracy'):
        self.unsupported('APPROX_COUNT_DISTINCT does not support accuracy')
    return f"APPROX_COUNT_DISTINCT({self.sql(expression, 'this')})"


def _if_sql(self, expression):
    expressions = csv(
        self.sql(expression, 'condition'),
        self.sql(expression, 'true'),
        self.sql(expression, 'false')
    )
    return f"IF({expressions})"


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
            expression=Token.number(1000),
        )),
        'LIST_VALUE': lambda args: exp.Array(expressions=args),
        'STRFTIME': lambda args: exp.TimeToStr(this=args[0], format=args[1]),
        'STRPTIME': lambda args: exp.StrToTime(this=args[0], format=args[1]),
    }


class Hive(Dialect):
    identifier = '`'

    DATE_FORMAT = "'yyyy-MM-dd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    def _time_format(self, expression):
        time_format = self.sql(expression, 'format')
        if time_format == Hive.TIME_FORMAT:
            return None
        return time_format

    def _fileformat_sql(self, expression):
        file_format = self.sql(expression, 'this').replace(self.quote, '')
        if file_format:
            return F"STORED AS {file_format}"
        return ''

    def _str_to_unix(self, expression):
        return f"UNIX_TIMESTAMP({csv(self.sql(expression, 'this'), Hive._time_format(self, expression))})"

    def _str_to_time(self, expression):
        time_format = self.sql(expression, 'format')
        if time_format in (Hive.TIME_FORMAT, Hive.DATE_FORMAT):
            return f"DATE_FORMAT({self.sql(expression, 'this')}, {Hive.TIME_FORMAT})"
        return f"FROM_UNIXTIME({Hive._str_to_unix(self, expression)})"

    def _time_to_str(self, expression):
        this = self.sql(expression, 'this')
        time_format = self.sql(expression, 'format')
        if time_format == Hive.DATE_FORMAT:
            return f"TO_DATE({this})"
        return f"DATE_FORMAT({this}, {time_format})"

    def _time_to_unix(self, expression):
        return f"UNIX_TIMESTAMP({self.sql(expression, 'this')})"

    def _unix_to_time(self, expression):
        return f"FROM_UNIXTIME({self.sql(expression, 'this')})"

    transforms = {
        TokenType.TEXT: 'STRING',
        TokenType.VARCHAR: 'STRING',
        exp.ApproxDistinct: _approx_count_distinct_sql,
        exp.ArrayAgg: lambda self, e: f"COLLECT_LIST({self.sql(e, 'this')})",
        exp.DateDiff: lambda self, e: f"DATEDIFF({self.sql(e, 'this')}, {self.sql(e, 'value')})",
        exp.DateStrToDate: lambda self, e: self.sql(e, 'this'),
        exp.FileFormat: _fileformat_sql,
        exp.If: _if_sql,
        exp.JSONPath: lambda self, e: f"GET_JSON_OBJECT({self.sql(e, 'this')}, {self.sql(e, 'path')})",
        exp.StrToTime: _str_to_time,
        exp.StrToUnix: _str_to_unix,
        exp.TimeStrToDate: lambda self, e: f"TO_DATE({self.sql(e, 'this')})",
        exp.TimeStrToTime: lambda self, e: self.sql(e, 'this'),
        exp.TimeStrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')})",
        exp.TimeToStr: _time_to_str,
        exp.TimeToTimeStr: lambda self, e: self.sql(e, 'this'),
        exp.TimeToUnix: _time_to_unix,
        exp.TsOrDsToDateStr: lambda self, e: f"TO_DATE({self.sql(e, 'this')})",
        exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({csv(self.sql(e, 'this'), Hive._time_format(self, e))})",
        exp.UnixToTime: _unix_to_time,
        exp.UnixToTimeStr: _unix_to_time,
    }

    functions = {
        'APPROX_COUNT_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0]),
        'COLLECT_LIST': lambda args: exp.ArrayAgg(this=args[0]),
        'DATE_ADD': lambda args: exp.DateAdd(this=exp.DateStrToDate(this=args[0]), value=args[1]),
        'DATEDIFF': lambda args: exp.DateDiff(
            this=exp.DateStrToDate(this=args[0]),
            value=exp.DateStrToDate(this=args[1]),
        ),
        'DATE_SUB': lambda args: exp.DateAdd(
            this=exp.DateStrToDate(this=args[0]),
            value=exp.Star(this=args[1], expression=Token.number(-1)),
        ),
        'DATE_FORMAT': lambda args: exp.TimeToStr(this=args[0], format=args[1]),
        'FROM_UNIXTIME': lambda args: exp.UnixToStr(
            this=args[0],
            format=list_get(args, 1) or Hive.TIME_FORMAT,
        ),
        'GET_JSON_OBJECT': lambda args: exp.JSONPath(this=args[0], path=args[1]),
        'TO_DATE': lambda args: exp.TsOrDsToDateStr(this=args[0]),
        'UNIX_TIMESTAMP': lambda args: exp.StrToUnix(
            this=args[0],
            format=list_get(args, 1) or Hive.TIME_FORMAT,
        ),
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
    escape = "'"

    TIME_FORMAT = "'%Y-%m-%d %H:%i:%s'"

    def _approx_distinct_sql(self, expression):
        accuracy = expression.args.get('accuracy')
        accuracy = ', ' + self.sql(accuracy) if accuracy else ''
        return f"APPROX_DISTINCT({self.sql(expression, 'this')}{accuracy})"

    def _fileformat_sql(self, expression):
        file_format = self.sql(expression, 'this').replace(self.quote, '')
        if file_format:
            return F"WITH (FORMAT = '{file_format}')"
        return ''

    def _date_parse_sql(self, expression):
        return f"DATE_PARSE({self.sql(expression, 'this')}, '%Y-%m-%d %H:%i:%s')"

    def _ts_or_ds_to_date_str_sql(self, expression):
        this = self.sql(expression, 'this')
        time_str = f"DATE_FORMAT({Presto._date_parse_sql(self, expression)}, '%Y-%m%-d')"
        return f"IF(LENGTH({this}) > 10, {time_str}, {this})"

    transforms = {
        TokenType.INT: 'INTEGER',
        TokenType.FLOAT: 'REAL',
        TokenType.BINARY: 'VARBINARY',
        TokenType.TEXT: 'VARCHAR',
        exp.ApproxDistinct: _approx_distinct_sql,
        exp.Array: lambda self, e: f"ARRAY[{self.expressions(e, flat=True)}]",
        exp.DateAdd: lambda self, e: f"DATE_ADD('day', {self.sql(e, 'value')}, {self.sql(e, 'this')})",
        exp.DateDiff: lambda self, e: f"DATE_DIFF('day', {self.sql(e, 'value')}, {self.sql(e, 'this')})",
        exp.DateStrToDate: lambda self, e: f"DATE_PARSE({self.sql(e, 'this')}, '%Y-%m-%d')",
        exp.FileFormat: _fileformat_sql,
        exp.If: _if_sql,
        exp.JSONPath: lambda self, e: f"JSON_EXTRACT({self.sql(e, 'this')}, {self.sql(e, 'path')})",
        exp.StrToTime: lambda self, e: f"DATE_PARSE({self.sql(e, 'this')}, {self.sql(e, 'format')})",
        exp.StrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {self.sql(e, 'format')}))",
        exp.TimeStrToDate: _date_parse_sql,
        exp.TimeStrToTime: _date_parse_sql,
        exp.TimeStrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {Presto.TIME_FORMAT}))",
        exp.TimeToStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.sql(e, 'format')})",
        exp.TimeToTimeStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {Presto.TIME_FORMAT})",
        exp.TimeToUnix: lambda self, e: f"TO_UNIXTIME({self.sql(e, 'this')})",
        exp.TsOrDsToDateStr: _ts_or_ds_to_date_str_sql,
        exp.UnixToStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), {self.sql(e, 'format')})",
        exp.UnixToTime: lambda self, e: f"FROM_UNIXTIME({self.sql(e, 'this')})",
        exp.UnixToTimeStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), {Presto.TIME_FORMAT})",
    }

    functions = {
        'APPROX_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0], accuracy=list_get(args, 1)),
        'DATE_ADD': lambda args: exp.DateAdd(this=args[2], value=args[1]),
        'DATE_DIFF': lambda args: exp.DateDiff(this=args[2], value=args[1]),
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
