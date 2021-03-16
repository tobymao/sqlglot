import sqlglot.expressions as exp
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer

class registeringMeta(type):
    classes = {}

    @classmethod
    def __getitem__(cls, key):
        return cls.classes[key]

    @classmethod
    def get(cls, key, default):
        return cls.classes.get(key, default)

    def __new__(cls, clsname, bases, attrs):
        clazz = super().__new__(cls, clsname, bases, attrs)
        cls.classes[clsname.lower()] = clazz
        return clazz


class Dialect(metaclass=registeringMeta):

    def parse(self, code):
        return self.parser().parse(self.tokenizer().tokenize(code))

    def generate(self, expression, **opts):
        return self.generator(**opts).generate(expression)

    def transpile(self, code, **opts):
        return self.generate(self.parse(code), **opts)

    def generator(self, **opts):
        return Generator(**opts)

    def parser(self, **opts):
        return Parser(**opts)

    def tokenizer(self, **opts):
        return Tokenizer(**opts)


class Presto(Dialect):
    def parser(self, **opts):
        return Parser(
            functions={
                'APPROX_DISTINCT': self._parse_approx_distinct,
            },
            **opts,
        )

    def generator(self, **opts):
        return Generator(
            functions={
                exp.ApproxDistinct: self._approx_distinct_sql,
            },
            **opts,
        )

    def tokenizer(self, **opts):
        return Tokenizer(**opts)

    def _parse_approx_distinct(self, args):
        return exp.ApproxDistinct(
            this=args[0],
            accuracy=args[1] if len(args) > 1 else None,
        )

    def _approx_distinct_sql(self, gen, e):
        accuracy = ', ' + gen.sql(e, 'accuracy') if e.args.get('accuracy') else ''
        return f"APPROX_DISTINCT({gen.sql(e, 'this')}{accuracy})"


class Spark(Dialect):
    def generator(self, **opts):
        return Generator(
            functions={
                exp.ApproxDistinct: self._approx_distinct_sql,
            },
            identifier='`',
            **opts,
        )

    def parser(self, **opts):
        return Parser(
            functions={
                'APPROX_COUNT_DISTINCT': lambda args: exp.ApproxDistinct(this=args[0]),
            },
            **opts,
        )

    def tokenizer(self, **opts):
        return Tokenizer(**{
            'quote': '"',
            'identifier': '`',
        }, **opts)

    def _approx_distinct_sql(self, gen, e):
        if e.args.get('accuracy'):
            gen.unsupported('APPROX_COUNT_DISTINCT does not support accuracy')
        return f"APPROX_COUNT_DISTINCT({gen.sql(e, 'this')})"
