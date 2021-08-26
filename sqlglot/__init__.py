from sqlglot.dialects import Dialect
from sqlglot.expressions import Expression
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer, TokenType
from sqlglot.parser import Parser


__version__ = '1.2.6'


def parse(code, read=None, **opts):
    dialect = Dialect.get(read, Dialect)()
    return dialect.parse(code, **opts)


def transpile(code, read=None, write=None, identity=True, error_level=None, **opts):
    write = write or read if identity else write
    return [
        Dialect.get(write, Dialect)().generate(expression, **opts)
        for expression in parse(code, read, error_level=error_level)
    ]
