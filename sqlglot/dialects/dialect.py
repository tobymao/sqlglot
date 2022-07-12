from sqlglot import exp
from sqlglot.generator import Generator
from sqlglot.helper import csv, list_get
from sqlglot.parser import Parser
from sqlglot.time import format_time
from sqlglot.tokens import Tokenizer
from sqlglot.trie import new_trie


class _Dialect(type):
    classes = {}

    @classmethod
    def __getitem__(cls, key):
        return cls.classes[key]

    @classmethod
    def get(cls, key, default=None):
        return cls.classes.get(key, default)

    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)
        cls.classes[clsname.lower()] = klass

        klass.time_trie = new_trie(klass.time_mapping)
        klass.inverse_time_mapping = {v: k for k, v in klass.time_mapping.items()}
        klass.inverse_time_trie = new_trie(klass.inverse_time_mapping)

        klass.tokenizer_class = getattr(klass, "Tokenizer", Tokenizer)
        klass.parser_class = getattr(klass, "Parser", Parser)
        klass.generator_class = getattr(klass, "Generator", Generator)

        klass.tokenizer = klass.tokenizer_class(
            identifier=klass.identifier,
            escape=klass.escape,
        )
        return klass


class Dialect(metaclass=_Dialect):
    identifier = None
    escape = "'"
    index_offset = 0

    date_format = "'%Y-%m-%d'"
    dateint_format = "'%Y%m%d'"
    time_format = "'%Y-%m-%d %H:%M:%S'"
    time_mapping = {}

    # autofilled
    time_trie = None
    inverse_time_mapping = None
    inverse_time_trie = None
    tokenizer_class = None
    parser_class = None
    generator_class = None
    tokenizer = None

    @classmethod
    def get_or_raise(cls, dialect):
        if not dialect:
            return cls
        result = cls.get(dialect)
        if not result:
            raise ValueError(f"Unknown dialect '{dialect}'")
        return result

    @classmethod
    def format_time(cls, expression):
        if isinstance(expression, str):
            return exp.Literal.string(
                format_time(
                    expression[1:-1],  # the time formats are quoted
                    cls.time_mapping,
                    cls.time_trie,
                )
            )
        if isinstance(expression, exp.Literal) and expression.is_string:
            return exp.Literal.string(
                format_time(
                    expression.this,
                    cls.time_mapping,
                    cls.time_trie,
                )
            )
        return expression

    def parse(self, sql, **opts):
        return self.parser(**opts).parse(self.tokenizer.tokenize(sql), sql)

    def parse_into(self, expression_type, sql, **opts):
        return self.parser(**opts).parse_into(
            expression_type, self.tokenizer.tokenize(sql), sql
        )

    def generate(self, expression, **opts):
        return self.generator(**opts).generate(expression)

    def transpile(self, code, **opts):
        return self.generate(self.parse(code), **opts)

    def parser(self, **opts):
        # pylint: disable=not-callable
        return self.parser_class(
            **{
                "index_offset": self.index_offset,
                **opts,
            },
        )

    def generator(self, **opts):
        # pylint: disable=not-callable
        return self.generator_class(
            **{
                "quote": self.tokenizer_class.QUOTES[0],
                "identifier": self.identifier,
                "escape": self.escape,
                "index_offset": self.index_offset,
                "time_mapping": self.inverse_time_mapping,
                "time_trie": self.inverse_time_trie,
                **opts,
            }
        )


def rename_func(name):
    return (
        lambda self, expression: f"{name}({csv(*[self.sql(e) for e in expression.args.values()])})"
    )


def approx_count_distinct_sql(self, expression):
    if expression.args.get("accuracy"):
        self.unsupported("APPROX_COUNT_DISTINCT does not support accuracy")
    return f"APPROX_COUNT_DISTINCT({self.sql(expression, 'this')})"


def if_sql(self, expression):
    expressions = csv(
        self.sql(expression, "this"),
        self.sql(expression, "true"),
        self.sql(expression, "false"),
    )
    return f"IF({expressions})"


def no_ilike_sql(self, expression):
    return self.like_sql(
        exp.Like(
            this=exp.Lower(this=expression.this),
            expression=expression.args["expression"],
        )
    )


def no_paren_current_date_sql(self, expression):
    zone = self.sql(expression, "this")
    return f"CURRENT_DATE AT TIME ZONE {zone}" if zone else "CURRENT_DATE"


def no_recursive_cte_sql(self, expression):
    if expression.args.get("recursive"):
        self.unsupported("Recursive CTEs are unsupported")
        expression.args["recursive"] = False
    return self.with_sql(expression)


def no_safe_divide_sql(self, expression):
    n = self.sql(expression, "this")
    d = self.sql(expression, "expression")
    return f"IF({d} <> 0, {n} / {d}, NULL)"


def no_tablesample_sql(self, expression):
    self.unsupported("TABLESAMPLE unsupported")
    return self.sql(expression.this)


def no_trycast_sql(self, expression):
    return self.cast_sql(expression)


def struct_extract_sql(self, expression):
    this = self.sql(expression, "this")
    struct_key = self.sql(expression, "expression").replace(self.quote, self.identifier)
    return f"{this}.{struct_key}"


def format_time_lambda(exp_class, dialect, default=False):
    def _format_time(args):
        return exp_class(
            this=list_get(args, 0),
            format=Dialect[dialect].format_time(
                list_get(args, 1) or (Dialect[dialect].time_format if default else None)
            ),
        )

    return _format_time
