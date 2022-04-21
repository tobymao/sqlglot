from sqlglot.dialects import Dialect
from sqlglot.errors import ErrorLevel, UnsupportedError, ParseError, TokenError
from sqlglot.expressions import Expression, Select
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType
from sqlglot.parser import Parser


__version__ = "2.0.0"


def parse(sql, read=None, **opts):
    """
    Parses the given SQL string into a collection of syntax trees, one per
    parsed SQL statement.

    Args:
        sql (str): the SQL code string to parse.
        read (str): the SQL dialect to apply during parsing
            (eg. "spark", "hive", "presto", "mysql").
        **opts: other options.

    Returns:
        typing.List[Expression]: the list of parsed syntax trees.
    """
    dialect = Dialect.get_or_raise(read)()
    return dialect.parse(sql, **opts)


def parse_one(sql, read=None, **opts):
    """
    Parses the given SQL string and returns a syntax tree for the first
    parsed SQL statement.

    Args:
        sql (str): the SQL code string to parse.
        read (str): the SQL dialect to apply during parsing
            (eg. "spark", "hive", "presto", "mysql").
        **opts: other options.

    Returns:
        Expression: the syntax tree for the first parsed statement.
    """
    return parse(sql, read=read, **opts)[0]


def transpile(sql, read=None, write=None, identity=True, error_level=None, **opts):
    """
    Parses the given SQL string using the source dialect and returns a list of SQL strings
    transformed to conform to the target dialect. Each string in the returned list represents
    a single transformed SQL statement.

    Args:
        sql (str): the SQL code string to transpile.
        read (str): the source dialect used to parse the input string
            (eg. "spark", "hive", "presto", "mysql").
        write (str): the target dialect into which the input should be transformed
            (eg. "spark", "hive", "presto", "mysql").
        identity (bool): if set to True and if the target dialect is not specified
            the source dialect will be used as both: the source and the target dialect.
        error_level (ErrorLevel): the desired error level of the parser.
        **opts: other options.

    Returns:
        typing.List[str]: the list of transpiled SQL statements / expressions.
    """
    write = write or read if identity else write
    return [
        Dialect.get_or_raise(write)().generate(expression, **opts)
        for expression in parse(sql, read, error_level=error_level)
    ]


def select(*expressions, dialect=None, **opts):
    """
    Initializes a syntax tree from one or multiple SELECT expressions.

    Example:
        >>> select("col1", "col2").from_("tbl").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expressions (str or Expression): the SQL code string to parse as the expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().select(*expressions, dialect=dialect, parser_opts=opts)


def from_(*expressions, dialect=None, **opts):
    """
    Initializes a syntax tree from a FROM expression.

    Example:
        >>> from_("tbl").select("col1", "col2").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expressionss (str or Expression): the SQL code string to parse as the FROM expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect (str): the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().from_(*expressions, dialect=dialect, parser_opts=opts)
