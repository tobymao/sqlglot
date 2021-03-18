import logging

import sqlglot.expressions as exp
from sqlglot.errors import ErrorLevel, UnsupportedError
from sqlglot.tokens import Token, TokenType


class Generator:
    BODY_TOKENS = {
        TokenType.SELECT,
        TokenType.FROM,
        TokenType.JOIN,
        TokenType.WHERE,
        TokenType.GROUP,
        TokenType.HAVING,
        TokenType.ORDER,
        TokenType.UNION,
    }

    TRANSFORMS = {
        TokenType.BOOLEAN: 'BOOLEAN',
        TokenType.TINYINT: 'TINYINT',
        TokenType.SMALLINT: 'SMALLINT',
        TokenType.INT: 'INT',
        TokenType.BIGINT: 'BIGINT',
        TokenType.FLOAT: 'FLOAT',
        TokenType.DOUBLE: 'DOUBLE',
        TokenType.CHAR: 'CHAR',
        TokenType.VARCHAR: 'VARCHAR',
        TokenType.TEXT: 'TEXT',
        TokenType.BINARY: 'BINARY',
        TokenType.JSON: 'JSON',
    }

    def __init__(self, **opts):
        self.transforms = {**self.TRANSFORMS, **(opts.get('transforms') or {})}
        self.pretty = opts.get('pretty')
        self.identifier = opts.get('identifier') or '"'
        self.identify = opts.get('identify', False)
        self.quote = opts.get('quote') or "'"
        self.pad = opts.get('pad', 2)
        self.unsupported_level = opts.get('unsupported_level', ErrorLevel.WARN)
        self.unsupported_messages = []
        self._indent = opts.get('indent', 4)
        self._level = 0

    def generate(self, expression):
        self.unsupported_messages = []
        sql = self.sql(expression)

        if self.unsupported_level == ErrorLevel.IGNORE:
            return sql

        if self.unsupported_level == ErrorLevel.RAISE:
            raise UnsupportedError

        for msg in self.unsupported_messages:
            logging.warning(msg)

        return sql

    def unsupported(self, message):
        self.unsupported_messages.append(message)

    def indent(self, sql, level=None, pad=0):
        level = self._level if level is None else level
        if self.pretty:
            sql = f"{' ' * (level * self._indent + pad)}{sql}"
        return sql

    def sep(self, sep=' '):
        return f"{sep.strip()}\n" if self.pretty else sep

    def seg(self, sql, sep=' ', level=None, pad=0):
        return f"{self.sep(sep)}{self.indent(sql, level=level, pad=pad)}"

    def wrap(self, expression):
        self._level += 1
        this_sql = self.indent(self.sql(expression, 'this'))
        self._level -= 1
        return f"({self.sep('')}{this_sql}{self.seg(')', sep='')}"

    def sql(self, expression, key=None, identify=False):
        if not expression:
            return ''

        if isinstance(expression, str):
            return expression

        if key:
            return self.sql(expression.args.get(key), identify=identify)

        transform = self.transforms.get(expression.__class__) or self.transforms.get(expression.token_type)

        if callable(transform):
            return transform(self, expression)
        if transform:
            return transform

        if isinstance(expression, Token):
            text = expression.text
            if expression.token_type == TokenType.IDENTIFIER:
                text = f"{self.identifier}{text[1:-1]}{self.identifier}"
            elif self.identify and identify:
                text = f"{self.identifier}{text}{self.identifier}"
            return text

        return getattr(self, f"{expression.key}_sql")(expression)

    def column_sql(self, expression):
        return '.'.join(part for part in [
            self.sql(expression, 'table', identify=True),
            self.sql(expression, 'this', identify=True),
        ] if part)

    def cte_sql(self, expression):
        sql = ', '.join(
            f"{self.sql(e, 'to')} AS {self.wrap(e)}"
            for e in expression.args['expressions']
        )

        return f"WITH {sql}{self.sep()}{self.sql(expression, 'this')}"

    def table_sql(self, expression):
        return '.'.join(part for part in [
            self.sql(expression, 'db', identify=True),
            self.sql(expression, 'table', identify=True),
            self.sql(expression, 'this', identify=True),
        ] if part)

    def from_sql(self, expression):
        expression_sql = self.sql(expression, 'expression')
        this_sql = self.sql(expression, 'this')
        return f"{expression_sql}{self.seg('FROM')} {this_sql}"

    def group_sql(self, expression):
        return self.op_expressions('GROUP BY', expression)

    def having_sql(self, expression):
        return self.op_expression('HAVING', expression)

    def join_sql(self, expression):
        side = self.sql(expression, 'side')
        kind = self.sql(expression, 'kind')
        op_sql = self.seg(' '.join(op for op in [side, kind, 'JOIN'] if op))
        on_sql = self.sql(expression, 'on')

        if on_sql:
            on_sql = self.seg(on_sql, pad=self.pad)
            on_sql = f" ON{on_sql}"

        expression_sql = self.sql(expression, 'expression')
        this_sql = self.sql(expression, 'this')
        return f"{expression_sql}{op_sql} {this_sql}{on_sql}"

    def order_sql(self, expression, flat=False):
        sql = self.op_expressions('ORDER BY', expression, flat=flat)
        if expression.args['desc']:
            sql = f"{sql} DESC"
        return sql

    def select_sql(self, expression):
        return f"SELECT{self.sep()}{self.expressions(expression)}"

    def union_sql(self, expression):
        distinct = '' if expression.args['distinct'] else ' ALL'
        return self.op_expression(f"UNION{distinct}", expression, pad=0)

    def where_sql(self, expression):
        return self.op_expression('WHERE', expression)

    def window_sql(self, expression):
        this_sql = self.sql(expression, 'this')
        partition = expression.args.get('partition')
        partition = 'PARTITION BY ' +  ', '.join(self.sql(by) for by in partition) if partition else ''
        order = expression.args.get('order')
        order_sql = self.order_sql(order, flat=True) if order else ''
        partition_sql = partition + ' ' if partition and order else partition
        return f"{this_sql} OVER({partition_sql}{order_sql})"

    def between_sql(self, expression):
        this = self.sql(expression, 'this')
        low = self.sql(expression, 'low')
        high = self.sql(expression, 'high')
        return f"{this} BETWEEN {low} AND {high}"

    def bracket_sql(self, expression):
        return f"{self.sql(expression, 'this')}[{self.expressions(expression, flat=True)}]"

    def case_sql(self, expression):
        pad = self.pad + 2

        ifs = [
            self.seg(f"WHEN {self.sql(e, 'condition')} THEN {self.sql(e, 'true')}", pad=pad)
            for e in expression.args['ifs']
        ]

        if expression.args.get('default') is not None:
            ifs.append(self.seg(f"ELSE {self.sql(expression, 'default')}", pad=pad))

        return f"CASE{''.join(ifs)}{self.seg('END', pad=self.pad)}"

    def decimal_sql(self, expression):
        if isinstance(expression, Token):
            return 'DECIMAL'
        args = ', '.join(
            arg.text
            for arg in [expression.args.get('precision'), expression.args.get('scale')]
            if arg
        )
        return f"DECIMAL({args})"

    def if_sql(self, expression):
        return self.case_sql(exp.Case(ifs=[expression], default=expression.args['false']))

    def in_sql(self, expression):
        return f"{self.sql(expression, 'this')} IN ({self.expressions(expression, flat=True)})"

    def func_sql(self, expression):
        return f"{self.sql(expression, 'this').upper()}({self.expressions(expression, flat=True)})"

    def paren_sql(self, expression):
        return f"({self.sql(expression, 'this')})"

    def neg_sql(self, expression):
        return f"-{self.sql(expression, 'this')}"

    def not_sql(self, expression):
        return f"NOT {self.sql(expression, 'this')}"

    def alias_sql(self, expression):
        this_sql = self.sql(expression, 'this')
        to_sql = self.sql(expression, 'to')
        to_sql = f" AS {to_sql}" if to_sql else ''

        if expression.args['this'].token_type in self.BODY_TOKENS:
            if self.pretty:
                return f"{self.wrap(expression)}{to_sql}"
            return f"({this_sql}){to_sql}"
        return f"{this_sql}{to_sql}"

    def and_sql(self, expression):
        return self.binary(expression, 'AND')

    def cast_sql(self, expression):
        return f"CAST({self.sql(expression, 'this')} AS {self.sql(expression.args['to'])})"

    def count_sql(self, expression):
        distinct = 'DISTINCT ' if expression.args['distinct'] else ''
        return f"COUNT({distinct}{self.sql(expression, 'this')})"

    def dot_sql(self, expression):
        return self.binary(expression, '.')

    def eq_sql(self, expression):
        return self.binary(expression, '=')

    def gt_sql(self, expression):
        return self.binary(expression, '>')

    def gte_sql(self, expression):
        return self.binary(expression, '>=')

    def is_sql(self, expression):
        return self.binary(expression, 'IS')

    def lt_sql(self, expression):
        return self.binary(expression, '<')

    def lte_sql(self, expression):
        return self.binary(expression, '<=')

    def minus_sql(self, expression):
        return self.binary(expression, '-')

    def or_sql(self, expression):
        return self.binary(expression, 'OR')

    def plus_sql(self, expression):
        return self.binary(expression, '+')

    def slash_sql(self, expression):
        return self.binary(expression, '/')

    def star_sql(self, expression):
        return self.binary(expression, '*')

    def binary(self, expression, op):
        return f"{self.sql(expression, 'this')} {op} {self.sql(expression, 'expression')}"

    def expressions(self, expression, flat=False, pad=0):
        if flat:
            return ', '.join(self.sql(e) for e in expression.args['expressions'])

        return self.sep(', ').join(
            self.indent(f"{'  ' if self.pretty else ''}{self.sql(e)}", pad=pad)
            for e in expression.args['expressions']
        )

    def op_expression(self, op, expression, pad=None):
        pad = self.pad if pad is None else pad
        this_sql = self.sql(expression, 'this')
        op_sql = self.seg(op)
        expression_sql = self.indent(self.sql(expression, 'expression'), pad=pad)
        return f"{this_sql}{op_sql}{self.sep()}{expression_sql}"

    def op_expressions(self, op, expression, flat=False):
        this_sql = self.sql(expression, 'this')
        op_sql = self.seg(op)
        expressions_sql = self.expressions(expression, flat=flat)
        if flat:
            this_sql = f"{this_sql} " if this_sql else ''
            return f"{this_sql}{op} {expressions_sql}"
        return f"{this_sql}{op_sql}{self.sep()}{expressions_sql}"
