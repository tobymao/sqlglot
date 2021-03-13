from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer
from sqlglot.transpiler import Transpiler


__version__ = '0.1.0'


def parse(code, parse_opts=None, tokenize_opts=None):
    tokenize_opts = tokenize_opts or {}
    parse_opts = parse_opts or {}
    return Parser(Tokenizer(code, **tokenize_opts).tokenize(), **parse_opts).parse()


def transpile(code, transpile_opts=None, parse_opts=None, tokenize_opts=None):
    tokenize_opts = tokenize_opts or {}
    parse_opts = parse_opts or {}
    transpile_opts = transpile_opts or {}

    return [
        Transpiler(**transpile_opts).transpile(exp)
        for exp in parse(code, parse_opts=parse_opts, tokenize_opts=tokenize_opts)
    ]
