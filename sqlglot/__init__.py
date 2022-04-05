from sqlglot.dialects import Dialect
from sqlglot.errors import ErrorLevel, UnsupportedError, ParseError, TokenError
from sqlglot.expressions import Expression
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType
from sqlglot.parser import Parser


__version__ = "1.28.1"


def parse(code, read=None, **opts):
    """
    Parses the given SQL string into a collection of syntax trees, one per
    parsed SQL statement.

    Args
        code (str): the SQL code string to parse.
        read (str): the SQL dialect to apply during parsing
            (eg. "spark", "hive", "presto", "mysql").
        opts (dict): other options.

    Returns
        the list of parsed syntax trees.
    """
    dialect = Dialect.get_or_raise(read)()
    return dialect.parse(code, **opts)


def parse_one(code, read=None, **opts):
    """
    Parses the given SQL string and returns a syntax tree for the first
    parsed SQL statement.

    Args
        code (str): the SQL code string to parse.
        read (str): the SQL dialect to apply during parsing
            (eg. "spark", "hive", "presto", "mysql").
        opts (dict): other options.

    Returns
        the syntax tree for the first parsed statement.
    """
    return parse(code, read=read, **opts)[0]


def transpile(code, read=None, write=None, identity=True, error_level=None, **opts):
    """
    Parses the given SQL string using the source dialect and returns a list of SQL strings
    transformed to conform to the target dialect. Each string in the returned list represents
    a single transformed SQL statement.

    Args
        code (str): the SQL code string to transpile.
        read (str): the source dialect used to parse the input string
            (eg. "spark", "hive", "presto", "mysql").
        write (str): the target dialect into which the input should be transformed
            (eg. "spark", "hive", "presto", "mysql").
        identity (bool): if set to True and if the target dialect is not specified
            the source dialect will be used as both: the source and the target dialect.
        error_level (ErrorLevel): the desired error level of the parser.
        opts (dict): other options.

    Returns
        the list of transpiled SQL statements / expressions.
    """
    write = write or read if identity else write
    return [
        Dialect.get_or_raise(write)().generate(expression, **opts)
        for expression in parse(code, read, error_level=error_level)
    ]
