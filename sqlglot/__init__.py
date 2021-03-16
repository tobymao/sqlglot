from sqlglot.dialects import Dialect
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer
from sqlglot.parser import Parser


__version__ = '0.1.0'


def parse(code, read=None):
    dialect = Dialect.get(read, Dialect)()
    return dialect.parse(code)


def transpile(code, read=None, write=None, **opts):
    return [
        Dialect.get(write, Dialect)().generate(expression, **opts)
        for expression in parse(code, read)
    ]
