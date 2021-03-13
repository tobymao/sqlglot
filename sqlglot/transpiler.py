from sqlglot.tokens import Token, TokenType
import sqlglot.expressions as exp

class Transpiler:
    BODY_TOKENS = {
        TokenType.FROM,
        TokenType.JOIN,
        TokenType.WHERE,
        TokenType.GROUP,
        TokenType.HAVING,
        TokenType.ORDER,
    }

    FUNCTIONS = {
        exp.Avg: lambda self, e: self.simple_func(e, 'AVG'),
        exp.Coalesce: lambda self, e: f"COALESCE({self.expressions(e, flat=True)})",
        exp.Count: lambda self, e: f"COUNT({'DISTINCT ' if e.args['distinct'] else ''}{self.sql(e, 'this')})",
        exp.First: lambda self, e: self.simple_func(e, 'FIRST'),
        exp.Last: lambda self, e: self.simple_func(e, 'LAST'),
        exp.If: lambda self, e: self.case_sql(exp.Case(ifs=[e], default=e.args['false'])),
        exp.LN: lambda self, e: self.simple_func(e, 'LN'),
        exp.Max: lambda self, e: self.simple_func(e, 'MAX'),
        exp.Min: lambda self, e: self.simple_func(e, 'MIN'),
        exp.Sum: lambda self, e: self.simple_func(e, 'SUM'),
    }

    TYPES = {
        TokenType.BOOLEAN: 'BOOL',
        TokenType.TINYINT: 'INT2',
        TokenType.SMALLINT: 'INT2',
        TokenType.INT: 'INT4',
        TokenType.BIGINT: 'INT8',
        TokenType.REAL: 'FLOAT4',
        TokenType.DOUBLE: 'FLOAT8',
        TokenType.DECIMAL: lambda self, e: 'DECIMAL',
        TokenType.CHAR: 'CHAR',
        TokenType.VARCHAR: 'VARCHAR',
        TokenType.TEXT: 'TEXT',
        TokenType.BINARY: 'BYTEA',
        TokenType.JSON: 'JSON',
    }

    def __init__(self, **kwargs):
        self.functions = {**self.FUNCTIONS, **kwargs.get('functions', {})}
        self.types = {**self.TYPES, **kwargs.get('types', {})}
        self.pretty = kwargs.get('pretty')
        self.pad = kwargs.get('pad', 2)
        self._indent = kwargs.get('indent', 4)
        self._level = 0

    def transpile(self, expression):
        return self.sql(expression)

    def sep(self, sep=' '):
        return f"{sep.strip()}\n" if self.pretty else sep

    def indent(self, sql, level=None, pad=0):
        if level is None:
            level = self._level
        if self.pretty:
            sql = f"{' ' * (level * self._indent + pad)}{sql}"
        return sql

    def seg(self, sql, sep=' ', pad=0):
        return f"{self.sep(sep)}{self.indent(sql, pad=pad)}"

    def sql(self, expression, key=None):
        if not expression:
            return ''
        if isinstance(expression, Token):
            return expression.text
        if key:
            return self.sql(expression.args.get(key))

        method = 'func' if expression.token_type == TokenType.FUNC else expression.key
        return getattr(self, f"{method}_sql")(expression)

    def column_sql(self, expression):
        return '.'.join(part for part in [
            self.sql(expression, 'table'),
            self.sql(expression, 'this'),
        ] if part)

    def table_sql(self, expression):
        return '.'.join(part for part in [
            self.sql(expression, 'db'),
            self.sql(expression, 'table'),
            self.sql(expression, 'this'),
        ] if part)

    def select_sql(self, expression):
        return f"SELECT{self.sep()}{self.expressions(expression)}"

    def from_sql(self, expression):
        expression_sql = self.sql(expression, 'expression')
        self._level += 1
        this_sql = self.sql(expression, 'this')
        self._level -= 1

        return f"{expression_sql}{self.seg('FROM')} {this_sql}"

    def group_sql(self, expression):
        return self.op_expressions('GROUP BY', expression)

    def having_sql(self, expression):
        return self.op_expression('HAVING', expression)

    def join_sql(self, expression):
        joiner = self.sql(expression, 'joiner')
        op_sql = self.seg(f"{joiner}{' ' if joiner else ''}JOIN")
        on_sql = self.sql(expression, 'on')

        if on_sql:
            on_sql = self.seg(on_sql, pad=self.pad)
            on_sql = f"ON{on_sql}"

        expression_sql = self.sql(expression, 'expression')
        self._level += 1
        this_sql = self.sql(expression, 'this')
        self._level -= 1

        return f"{expression_sql}{op_sql} {this_sql} {on_sql}"

    def order_sql(self, expression):
        sql = self.op_expressions('ORDER BY', expression)
        if expression.args['desc']:
            sql = f"{sql} DESC"
        return sql

    def where_sql(self, expression):
        return self.op_expression('WHERE', expression)

    def between_sql(self, expression):
        this = self.sql(expression, 'this')
        low = self.sql(expression, 'low')
        high = self.sql(expression, 'high')
        return f"{this} BETWEEN {low} AND {high}"

    def case_sql(self, expression):
        pad = self.pad + 2

        ifs = [
            self.seg(f"WHEN {self.sql(e, 'condition')} THEN {self.sql(e, 'true')}", pad=pad)
            for e in expression.args['ifs']
        ]

        if expression.args.get('default') is not None:
            ifs.append(self.seg(f"ELSE {self.sql(expression, 'default')}", pad=pad))

        return f"CASE{''.join(ifs)}{self.seg('END', pad=self.pad)}"

    def in_sql(self, expression):
        this_sql = self.sql(expression, 'this')
        values_sql = self.seg(self.expressions(expression, pad=self.pad), sep='')
        return f"{this_sql} IN ({values_sql}{self.seg(')', sep='', pad=self.pad)}"

    def func_sql(self, expression):
        return self.functions[expression.__class__](self, expression)

    def paren_sql(self, expression):
        return f"({self.sql(expression, 'this')})"

    def neg_sql(self, expression):
        return f"-{self.sql(expression, 'this')}"

    def not_sql(self, expression):
        return f"NOT {self.sql(expression, 'this')}"

    def alias_sql(self, expression):
        this_sql = self.sql(expression, 'this')
        to_sql = self.sql(expression, 'to')

        if expression.args['this'].token_type in self.BODY_TOKENS:
            this_sql = self.indent(this_sql)
            if self.pretty:
                return f"(\n{this_sql}\n{self.indent(')', level=self._level-1)} AS {to_sql}"
            return f"({this_sql}) AS {to_sql}"
        return f"{this_sql} AS {to_sql}"

    def and_sql(self, expression):
        return self.binary(expression, 'AND')

    def cast_sql(self, expression):
        to_sql = self.types[expression.args['to'].token_type]
        if not isinstance(to_sql, str):
            to_sql = to_sql(self, expression)
        return f"CAST({self.sql(expression, 'this')} AS {to_sql})"

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

    def simple_func(self, expression, name):
        return f"{name}({self.sql(expression, 'this')})"

    def op_expression(self, op, expression):
        this_sql = self.sql(expression, 'this')
        op_sql = self.seg(op)
        expression_sql = self.indent(self.sql(expression, 'expression'), pad=self.pad)
        return f"{this_sql}{op_sql}{self.sep()}{expression_sql}"

    def op_expressions(self, op, expression):
        this_sql = self.sql(expression, 'this')
        op_sql = self.seg(op)
        expressions_sql = self.expressions(expression)
        return f"{this_sql}{op_sql}{self.sep()}{expressions_sql}"
