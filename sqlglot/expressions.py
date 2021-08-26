import weakref

from sqlglot.tokens import TokenType


class Expression:
    token_type = None
    arg_types = {'this': True}

    def __init__(self, **args):
        self.key = self.__class__.__name__.lower()
        self.args = args
        self.validate()
        self._parent = None
        self.arg_key = None

    @property
    def parent(self):
        return self._parent() if self._parent else None

    @property
    def depth(self):
        if self.parent:
            return self.parent.depth + 1
        return 0

    @parent.setter
    def parent(self, new_parent):
        self._parent = weakref.ref(new_parent)

    def find(self, expression_type):
        return next(self.find_all(expression_type), None)

    def find_all(self, expression_type):
        for expression, _, _ in self.walk():
            if isinstance(expression, expression_type):
                yield expression

    def walk(self, bfs=True):
        if bfs:
            yield from self.bfs()
        else:
            yield from self.dfs(self.parent, None)

    def dfs(self, parent, key):
        yield self, parent, key

        for k, v in self.args.items():
            nodes = v if isinstance(v, list) else [v]

            for node in nodes:
                if isinstance(node, Expression):
                    yield from node.dfs(self, k)
                else:
                    yield node, self, k

    def bfs(self):
        queue = [(self, self.parent, None)]

        while queue:
            item, parent, key = queue.pop()

            yield item, parent, key

            if isinstance(item, Expression):
                for k, v in item.args.items():
                    nodes = v if isinstance(v, list) else [v]

                    for node in nodes:
                        queue.append((node, item, k))


    def validate(self):
        for k, v in self.args.items():
            if k not in self.arg_types:
                raise ValueError(f"Unexpected keyword: {k} for {self.token_type}")

        for k, v in self.arg_types.items():
            if v and k not in self.args:
                raise ValueError(f"Required keyword: {k} missing for {self.token_type}")

    def __repr__(self):
        return self.to_s()

    def sql(self, dialect=None, **opts):
        from sqlglot.dialects import Dialect
        return Dialect.get(dialect, Dialect)().generate(self, **opts)

    def to_s(self, level=0):
        indent = '' if not level else "\n"
        indent += ''.join(['  '] * level)
        left = f"({self.key.upper()} "

        args = {
            k: ', '.join(
                v.to_s(level + 1) if hasattr(v, 'to_s') else str(v)
                for v in (vs if isinstance(vs, list) else [vs])
                if v
            )
            for k, vs in self.args.items()
        }

        right = ', '.join(f"{k}: {v}" for k, v in args.items())
        right += ')'

        return indent + left + right


class Create(Expression):
    token_type = TokenType.CREATE
    arg_types = {'this': True, 'kind': True, 'expression': False, 'exists': False, 'file_format': False, 'temporary': False}


class CTE(Expression):
    token_type = TokenType.WITH
    arg_types = {'this': True, 'expressions': True}


class Column(Expression):
    token_type = TokenType.COLUMN
    arg_types = {'this': True, 'db': False, 'table': False}


class Comment(Expression):
    token_type = TokenType.COMMENT
    arg_types = {'this': True, 'comment': True}


class Drop(Expression):
    token_type = TokenType.DROP
    arg_types = {'this': False, 'kind': False, 'exists': False}


class FileFormat(Expression):
    token_type = TokenType.FORMAT


class From(Expression):
    token_type = TokenType.FROM
    arg_types = {'expressions': True}


class Having(Expression):
    token_type = TokenType.HAVING


class Hint(Expression):
    token_type = TokenType.HINT


class Insert(Expression):
    token_type = TokenType.INSERT
    arg_types = {'this': True, 'expression': True, 'overwrite': False, 'exists': False}


class Group(Expression):
    token_type = TokenType.GROUP
    arg_types = {'expressions': True}


class Limit(Expression):
    token_type = TokenType.LIMIT


class Join(Expression):
    token_type = TokenType.JOIN
    arg_types = {'this': True, 'on': True, 'side': False, 'kind': False}


class Lateral(Expression):
    token_type = TokenType.LATERAL
    arg_types = {'this': True, 'outer': False, 'table': False, 'columns': False}


class Order(Expression):
    token_type = TokenType.ORDER
    arg_types = {'expressions': True}


class Ordered(Expression):
    token_type = TokenType.ORDERED
    arg_types = {'this': True, 'desc': False}


class Table(Expression):
    token_type = TokenType.TABLE
    arg_types = {'this': True, 'db': False}


class Tuple(Expression):
    token_type = TokenType.TUPLE
    arg_types = {'expressions': True}


class Union(Expression):
    token_type = TokenType.UNION
    arg_types = {'this': True, 'expression': True, 'distinct': True}


class Unnest(Expression):
    token_type = TokenType.UNNEST
    arg_types = {'expressions': True, 'ordinality': False, 'table': False, 'columns': False}


class Values(Expression):
    token_type = TokenType.VALUES
    arg_types = {'expressions': True}


class Select(Expression):
    token_type = TokenType.SELECT
    arg_types = {
        'expressions': True,
        'hint': False,
        'distinct': False,
        'from': False,
        'laterals': False,
        'joins': False,
        'where': False,
        'group': False,
        'having': False,
        'order': False,
        'limit': False,
    }


class Window(Expression):
    token_type = TokenType.OVER
    arg_types = {'this': True, 'partition': False, 'order': False, 'spec': False}


class WindowSpec(Expression):
    token_type = TokenType.SPEC
    arg_types = {'kind': False, 'start': False, 'start_side': False, 'end': False, 'end_side': False}


class Where(Expression):
    token_type = TokenType.WHERE


# Binary Expressions
# (PLUS a b)
# (FROM table selects)
class Binary(Expression):
    arg_types = {'this': True, 'expression': True}


class And(Binary):
    token_type = TokenType.AND


class BitwiseAnd(Binary):
    token_type = TokenType.AMP


class BitwiseLeftShift(Binary):
    token_type = TokenType.LSHIFT


class BitwiseOr(Binary):
    token_type = TokenType.PIPE


class BitwiseRightShift(Binary):
    token_type = TokenType.RSHIFT


class BitwiseXor(Binary):
    token_type = TokenType.CARET


class Minus(Binary):
    token_type = TokenType.DASH


class Div(Binary):
    token_type = TokenType.DIV


class Dot(Binary):
    token_type = TokenType.DOT


class DPipe(Binary):
    token_type = TokenType.DPIPE


class EQ(Binary):
    token_type = TokenType.EQ


class GT(Binary):
    token_type = TokenType.GT


class GTE(Binary):
    token_type = TokenType.GTE


class Is(Binary):
    token_type = TokenType.IS


class Like(Binary):
    token_type = TokenType.LIKE


class LT(Binary):
    token_type = TokenType.LT


class LTE(Binary):
    token_type = TokenType.LTE


class Mod(Binary):
    token_type = TokenType.MOD


class NEQ(Binary):
    token_type = TokenType.NEQ


class Or(Binary):
    token_type = TokenType.OR


class Plus(Binary):
    token_type = TokenType.PLUS


class Star(Binary):
    token_type = TokenType.STAR


class Slash(Binary):
    token_type = TokenType.SLASH


# Unary Expressions
# (NOT a)
class Unary(Expression):
    pass


class BitwiseNot(Unary):
    token_type = TokenType.TILDA


class Not(Unary):
    token_type = TokenType.NOT


class Paren(Unary):
    token_type = TokenType.PAREN


class Neg(Unary):
    token_type = TokenType.DASH

# Special Functions
class Alias(Expression):
    token_type = TokenType.ALIAS
    arg_types = {'this': True, 'alias': True}


class Between(Expression):
    token_type = TokenType.BETWEEN
    arg_types = {'this': True, 'low': True, 'high': True}


class Bracket(Expression):
    token_type = TokenType.BRACKET
    arg_types = {'this': True, 'expressions': True}


class Case(Expression):
    token_type = TokenType.CASE
    arg_types = {'this': False, 'ifs': True, 'default': False}


class Cast(Expression):
    token_type = TokenType.CAST
    arg_types = {'this': True, 'to': True}


class Decimal(Expression):
    token_type = TokenType.DECIMAL
    arg_types = {'precision': False, 'scale': False}


class Extract(Expression):
    token_type = TokenType.EXTRACT
    arg_types = {'this': True, 'expression': True}


class In(Expression):
    token_type = TokenType.IN
    arg_types = {'this': True, 'expressions': False, 'query': False}


class Interval(Expression):
    token_type = TokenType.IN
    arg_types = {'this': True, 'unit': True}


# Functions
class Func(Expression):
    token_type = TokenType.FUNC


class Anonymous(Func):
    arg_types = {'this': True, 'expressions': True}


class ApproxDistinct(Func):
    arg_types = {'this': True, 'accuracy': False}


class Array(Func):
    token_type = TokenType.ARRAY
    arg_types = {'expressions': True}


class ArrayAgg(Func):
    pass


class ArrayContains(Func):
    arg_types = {'this': True, 'expression': True}


class ArraySize(Func):
    pass


class Count(Func):
    arg_types = {'this': False, 'distinct': False}


class DateAdd(Func):
    arg_types = {'this': True, 'expression': True}


class DateDiff(Func):
    arg_types = {'this': True, 'expression': True}


class DateStrToDate(Func):
    pass


class If(Func):
    arg_types = {'this': True, 'true': True, 'false': False}


class Initcap(Func):
    pass


class JSONPath(Func):
    arg_types = {'this': True, 'path': True}


class Map(Func):
    token_type = TokenType.MAP
    arg_types = {'keys': True, 'values': True}


class RegexLike(Func):
    token_type = TokenType.RLIKE
    arg_types = {'this': True, 'expression': True}


class StrPosition(Func):
    arg_types = {'this': True, 'substr': True, 'position': False}


class StrToTime(Func):
    arg_types = {'this': True, 'format': True}


class StrToUnix(Func):
    arg_types = {'this': True, 'format': True}


class TimeToStr(Func):
    arg_types = {'this': True, 'format': True}


class TimeToTimeStr(Func):
    pass


class TimeToUnix(Func):
    pass


class TimeStrToDate(Func):
    pass


class TimeStrToTime(Func):
    pass


class TimeStrToUnix(Func):
    pass


class TsOrDsToDateStr(Func):
    pass


class UnixToStr(Func):
    arg_types = {'this': True, 'format': True}


class UnixToTime(Func):
    pass


class UnixToTimeStr(Func):
    pass
