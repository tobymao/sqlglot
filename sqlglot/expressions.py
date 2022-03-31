from enum import auto
import inspect
import sys

from sqlglot.helper import AutoName, camel_to_snake_case, ensure_list


class Expression:
    """
    The base class for all expressions in a syntax tree.

    Attributes:
        arg_types (dict): determines arguments supported by this expression.
            The key in a dictionary defines a unique key of an argument using
            which the argument's value can be retrieved. The value is a boolean
            flag which indiciates whether the argument's value is required (True)
            or optional (False).
    """

    arg_types = {"this": True}
    __slots__ = ("key", "args", "parent", "arg_key")

    def __init__(self, **args):
        self.key = self.__class__.__name__.lower()
        self.args = args
        self.parent = None
        self.arg_key = None

    def __eq__(self, other):
        return type(self) is type(other) and _norm_args(self) == _norm_args(other)

    def __hash__(self):
        return hash(
            (
                self.key,
                tuple(
                    (k, tuple(v) if isinstance(v, list) else v)
                    for k, v in _norm_args(self).items()
                ),
            )
        )

    @property
    def this(self):
        return self.args.get("this")

    def text(self, key):
        field = self.args.get(key)
        if isinstance(field, str):
            return field
        if isinstance(field, (Identifier, Literal)):
            return field.this
        return ""

    def copy(self):
        from copy import deepcopy

        return deepcopy(self)

    @property
    def depth(self):
        """
        Returns the depth of this tree.
        """
        if self.parent:
            return self.parent.depth + 1
        return 0

    def find(self, *expression_types):
        """
        Returns the first node in this tree which matches at least one of
        the specified types.

        Args:
            expression_types (type): the expression type to match.

        Returns:
            the node which matches the criteria or None if no node matching
            the criteria was found.
        """
        return next(self.find_all(*expression_types), None)

    def find_all(self, *expression_types):
        """
        Returns a generator object which visits all nodes in this tree and only
        yields those that match at least one of the specified expression types.

        Args:
            expression_types (type): the expression type to match.

        Returns:
            the generator object.
        """
        for expression, _, _ in self.walk():
            if isinstance(expression, expression_types):
                yield expression

    def walk(self, bfs=True):
        """
        Returns a generator object which visits all nodes in this tree.

        Args:
            bfs (bool): if set to True the BFS traversal order will be applied,
                otherwise the DFS traversal will be used instead.

        Returns:
            the generator object.
        """
        if bfs:
            yield from self.bfs()
        else:
            yield from self.dfs(self.parent, None)

    def dfs(self, parent, key):
        """
        Returns a generator object which visits all nodes in this tree in
        the DFS (Depth-first) order.

        Returns:
            the generator object.
        """
        yield self, parent, key

        for k, v in self.args.items():
            nodes = ensure_list(v)

            for node in nodes:
                if isinstance(node, Expression):
                    yield from node.dfs(self, k)
                else:
                    yield node, self, k

    def bfs(self):
        """
        Returns a generator object which visits all nodes in this tree in
        the BFS (Breadth-first) order.

        Returns:
            the generator object.
        """
        queue = [(self, self.parent, None)]

        while queue:
            item, parent, key = queue.pop()

            yield item, parent, key

            if isinstance(item, Expression):
                for k, v in item.args.items():
                    nodes = ensure_list(v)

                    for node in nodes:
                        queue.append((node, item, k))

    def __repr__(self):
        return self.to_s()

    def sql(self, dialect=None, **opts):
        """
        Returns SQL string representation of this tree.

        Args
            dialect (str): the dialect of the output SQL string
                (eg. "spark", "hive", "presto", "mysql").
            opts (dict): other :class:`~sqlglot.generator.Generator` options.

        Returns
            the SQL string.
        """
        from sqlglot.dialects import Dialect

        return Dialect.get_or_raise(dialect)().generate(self, **opts)

    def to_s(self, hide_missing=True, level=0):
        indent = "" if not level else "\n"
        indent += "".join(["  "] * level)
        left = f"({self.key.upper()} "

        args = {
            k: ", ".join(
                v.to_s(hide_missing=hide_missing, level=level + 1)
                if hasattr(v, "to_s")
                else str(v)
                for v in ensure_list(vs)
                if v is not None
            )
            for k, vs in self.args.items()
        }
        args = {k: v for k, v in args.items() if v or not hide_missing}

        right = ", ".join(f"{k}: {v}" for k, v in args.items())
        right += ")"

        return indent + left + right

    def transform(self, fun, *args, copy=True, **kwargs):
        """
        Recursively visits all tree nodes (excluding already transformed ones)
        and applies the given transformation function to each node.

        Args:
            fun (function): a function which takes a node as an argument and returns a
                new transformed node or the same node without modifications.
            copy (bool): if set to True a new tree instance is constructed, otherwise the tree is
                modified in place.

        Returns:
            the transformed tree.
        """
        node = self.copy() if copy else self
        new_node = fun(node, *args, **kwargs)

        if new_node is None:
            raise ValueError("A transformed node cannot be None")
        if not isinstance(new_node, Expression) or new_node is not node:
            return new_node

        for k, v in new_node.args.items():
            is_list_arg = isinstance(v, list)

            child_nodes = v if is_list_arg else [v]
            new_child_nodes = []

            for cn in child_nodes:
                if isinstance(cn, Expression):
                    new_child_node = cn.transform(fun, *args, copy=False, **kwargs)
                    new_child_node.parent = new_node
                else:
                    new_child_node = cn
                new_child_nodes.append(new_child_node)

            new_node.args[k] = new_child_nodes if is_list_arg else new_child_nodes[0]
        return new_node


class Annotation(Expression):
    arg_types = {
        "this": True,
        "expression": True,
    }


class Cache(Expression):
    arg_types = {
        "this": True,
        "lazy": False,
        "options": False,
        "expression": False,
    }


class Uncache(Expression):
    arg_types = {"this": True, "exists": False}


class Create(Expression):
    arg_types = {
        "this": True,
        "kind": True,
        "expression": False,
        "exists": False,
        "properties": False,
        "temporary": False,
        "replace": False,
        "engine": False,
        "auto_increment": False,
        "character_set": False,
        "collate": False,
        "comment": False,
    }


class CharacterSet(Expression):
    arg_types = {"this": True, "default": False}


class CTE(Expression):
    arg_types = {"this": True, "expressions": True, "recursive": False}


class Column(Expression):
    arg_types = {"this": False, "table": False, "db": False, "fields": False}

    @property
    def table(self):
        return self.args.get("table")

    @property
    def db(self):
        return self.args.get("db")

    @property
    def fields(self):
        return self.args.get("fields")


class ColumnDef(Expression):
    arg_types = {
        "this": True,
        "kind": True,
        "auto_increment": False,
        "comment": False,
        "collate": False,
        "default": False,
        "not_null": False,
        "primary": False,
    }


class Delete(Expression):
    arg_types = {"this": True, "where": False}


class Drop(Expression):
    arg_types = {"this": False, "kind": False, "exists": False}


class Except(Expression):
    arg_types = {"this": True, "expression": True, "distinct": False}


class Exists(Expression):
    arg_types = {"this": True, "not": False}


class From(Expression):
    arg_types = {"expressions": True}


class Having(Expression):
    pass


class Hint(Expression):
    arg_types = {"expressions": True}


class Identifier(Expression):
    arg_types = {"this": True, "quoted": False}

    def __eq__(self, other):
        return (
            isinstance(other, Identifier)
            and (self.this or "").upper() == (other.this or "").upper()
        )

    def __hash__(self):
        return hash((self.key, self.this.upper()))


class Insert(Expression):
    arg_types = {
        "this": True,
        "expression": True,
        "overwrite": False,
        "exists": False,
        "partition": False,
    }


class Partition(Expression):
    pass


class Intersect(Expression):
    arg_types = {"this": True, "expression": True, "distinct": False}


class Group(Expression):
    arg_types = {"expressions": True}


class Lambda(Expression):
    arg_types = {"this": True, "expressions": True}


class Limit(Expression):
    pass


class Literal(Expression):
    arg_types = {"this": True, "is_string": True}

    def __eq__(self, other):
        return (
            isinstance(other, Literal)
            and self.this == other.this
            and self.args.get("is_string") == other.args.get("is_string")
        )

    def __hash__(self):
        return hash((self.key, self.this, self.args.get("is_string")))

    @classmethod
    def number(cls, number):
        return cls(this=str(number), is_string=False)

    @classmethod
    def string(cls, string):
        return cls(this=string, is_string=True)

    @property
    def is_string(self):
        return self.args["is_string"]

    @property
    def is_int(self):
        return not self.is_string and self.this.isdigit()


class Join(Expression):
    arg_types = {"this": True, "on": False, "side": False, "kind": False}


class Lateral(Expression):
    arg_types = {"this": True, "outer": False, "table": False, "columns": False}


class Offset(Expression):
    pass


class Order(Expression):
    arg_types = {"expressions": True}


class Ordered(Expression):
    arg_types = {"this": True, "desc": False}


class Properties(Expression):
    arg_types = {"expressions": True}


class Property(Expression):
    arg_types = {"this": True, "value": True}


class Table(Expression):
    arg_types = {"this": True, "db": False}


class Tuple(Expression):
    arg_types = {"expressions": True}


class Union(Expression):
    arg_types = {"this": True, "expression": True, "distinct": False}


class Unnest(Expression):
    arg_types = {
        "expressions": True,
        "ordinality": False,
        "table": False,
        "columns": False,
    }


class Update(Expression):
    arg_types = {"this": True, "expressions": True, "from": False, "where": False}


class Values(Expression):
    arg_types = {"expressions": True}


class Schema(Expression):
    arg_types = {"this": False, "expressions": True}


class Select(Expression):
    arg_types = {
        "expressions": False,
        "hint": False,
        "distinct": False,
        "from": False,
        "laterals": False,
        "joins": False,
        "where": False,
        "group": False,
        "having": False,
        "order": False,
        "limit": False,
        "offset": False,
    }


class TableSample(Expression):
    arg_types = {
        "this": True,
        "bucket_numerator": False,
        "bucket_denominator": False,
        "bucket_field": False,
        "percent": False,
        "rows": False,
        "size": False,
    }


class Window(Expression):
    arg_types = {"this": True, "partition_by": False, "order": False, "spec": False}


class WindowSpec(Expression):
    arg_types = {
        "kind": False,
        "start": False,
        "start_side": False,
        "end": False,
        "end_side": False,
    }


class Where(Expression):
    pass


class Star(Expression):
    arg_types = {}


class Null(Expression):
    arg_types = {}


class Boolean(Expression):
    pass


class DataType(Expression):
    arg_types = {
        "this": True,
        "expressions": False,
        "nested": True,
    }

    class Type(AutoName):
        CHAR = auto()
        TEXT = auto()
        VARCHAR = auto()
        BINARY = auto()
        INT = auto()
        TINYINT = auto()
        SMALLINT = auto()
        BIGINT = auto()
        FLOAT = auto()
        DOUBLE = auto()
        DECIMAL = auto()
        BOOLEAN = auto()
        JSON = auto()
        TIMESTAMP = auto()
        TIMESTAMPTZ = auto()
        DATE = auto()
        ARRAY = auto()
        MAP = auto()
        UUID = auto()


# Commands to interact with the databases or engines
# These expressions don't truly parse the expression and consume
# whatever exists as a string until the end or a semicolon
class Command(Expression):
    arg_types = {"this": True, "expression": False}


# Binary Expressions
# (ADD a b)
# (FROM table selects)
class Binary(Expression):
    arg_types = {"this": True, "expression": True}

    @property
    def left(self):
        return self.this

    @property
    def right(self):
        return self.args.get("expression")


class Add(Binary):
    pass


class And(Binary):
    pass


class BitwiseAnd(Binary):
    pass


class BitwiseLeftShift(Binary):
    pass


class BitwiseOr(Binary):
    pass


class BitwiseRightShift(Binary):
    pass


class BitwiseXor(Binary):
    pass


class Div(Binary):
    pass


class Dot(Binary):
    pass


class DPipe(Binary):
    pass


class EQ(Binary):
    pass


class GT(Binary):
    pass


class GTE(Binary):
    pass


class ILike(Binary):
    pass


class IntDiv(Binary):
    pass


class Is(Binary):
    pass


class Like(Binary):
    pass


class LT(Binary):
    pass


class LTE(Binary):
    pass


class Mod(Binary):
    pass


class Mul(Binary):
    pass


class NEQ(Binary):
    pass


class Or(Binary):
    pass


class Sub(Binary):
    pass


# Unary Expressions
# (NOT a)
class Unary(Expression):
    pass


class BitwiseNot(Unary):
    pass


class Not(Unary):
    pass


class Paren(Unary):
    pass


class Neg(Unary):
    pass


# Special Functions
class Alias(Expression):
    arg_types = {"this": True, "alias": False}

    @property
    def alias(self):
        return self.args.get("alias")


class Aliases(Expression):
    arg_types = {"this": True, "expressions": True}

    @property
    def aliases(self):
        return self.args["expressions"]


class Between(Expression):
    arg_types = {"this": True, "low": True, "high": True}


class Bracket(Expression):
    arg_types = {"this": True, "expressions": True}


class Case(Expression):
    arg_types = {"this": False, "ifs": True, "default": False}


class Cast(Expression):
    arg_types = {"this": True, "to": True}


class Decimal(Expression):
    arg_types = {"precision": False, "scale": False}


class Extract(Expression):
    arg_types = {"this": True, "expression": True}


class In(Expression):
    arg_types = {"this": True, "expressions": False, "query": False}


class Interval(Expression):
    arg_types = {"this": True, "unit": True}


class TryCast(Cast):
    pass


# Functions
class Func(Expression):
    """
    The base class for all function expressions.

    Attributes
        is_var_len_args (bool): if set to True the last argument defined in
            arg_types will be treated as a variable length argument and the
            argument's value will be stored as a list.
        _sql_names (list): determines the SQL name (1st item in the list) and
            aliases (subsequent items) for this function expression. These
            values are used to map this node to a name during parsing as well
            as to provide the function's name during SQL string generation. By
            default the SQL name is set to the expression's class name transformed
            to snake case.
    """

    is_var_len_args = False

    @classmethod
    def from_arg_list(cls, args):
        args_num = len(args)

        all_arg_keys = list(cls.arg_types)
        # If this function supports variable length argument treat the last argument as such.
        non_var_len_arg_keys = (
            all_arg_keys[:-1] if cls.is_var_len_args else all_arg_keys
        )

        args_dict = {}
        arg_idx = 0
        for arg_key in non_var_len_arg_keys:
            if arg_idx >= args_num:
                break
            if args[arg_idx] is not None:
                args_dict[arg_key] = args[arg_idx]
            arg_idx += 1

        if arg_idx < args_num and cls.is_var_len_args:
            args_dict[all_arg_keys[-1]] = args[arg_idx:]
        return cls(**args_dict)

    @classmethod
    def sql_names(cls):
        if cls is Func:
            raise NotImplementedError(
                "SQL name is only supported by concrete function implementations"
            )
        if not hasattr(cls, "_sql_names"):
            cls._sql_names = [camel_to_snake_case(cls.__name__)]
        return cls._sql_names

    @classmethod
    def sql_name(cls):
        return cls.sql_names()[0]

    @classmethod
    def default_parser_mappings(cls):
        return {name: cls.from_arg_list for name in cls.sql_names()}


class AggFunc(Func):
    pass


class Abs(Func):
    pass


class Anonymous(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ApproxDistinct(AggFunc):
    arg_types = {"this": True, "accuracy": False}


class Array(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ArrayAgg(AggFunc):
    pass


class ArrayContains(Func):
    arg_types = {"this": True, "expression": True}


class ArraySize(Func):
    pass


class Avg(AggFunc):
    pass


class Ceil(Func):
    _sql_names = ["CEIL", "CEILING"]


class Coalesce(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ConcatWs(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class Count(AggFunc):
    arg_types = {"this": False, "distinct": False}


class DateAdd(Func):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateDiff(Func):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateStrToDate(Func):
    pass


class Day(Func):
    pass


class Exp(Func):
    pass


class Explode(Func):
    pass


class Floor(Func):
    pass


class Greatest(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class If(Func):
    arg_types = {"this": True, "true": True, "false": False}


class Initcap(Func):
    pass


class JSONExtract(Func):
    arg_types = {"this": True, "path": True}
    _sql_names = ["JSON_EXTRACT"]


class JSONExtractScalar(JSONExtract):
    _sql_names = ["JSON_EXTRACT_SCALAR"]


class Least(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class Length(Func):
    pass


class Ln(Func):
    pass


class Log(Func):
    arg_types = {"this": True, "expression": False}


class Log2(Func):
    pass


class Log10(Func):
    pass


class Lower(Func):
    pass


class Map(Func):
    arg_types = {"keys": True, "values": True}
    is_var_len_args = True


class Max(AggFunc):
    pass


class Min(AggFunc):
    pass


class Month(Func):
    pass


class Posexplode(Func):
    pass


class Pow(Func):
    arg_types = {"this": True, "power": True}
    _sql_names = ["POW", "POWER"]


class Quantile(AggFunc):
    arg_types = {"this": True, "quantile": True}


class RegexpLike(Func):
    arg_types = {"this": True, "expression": True}


class RegexpSplit(Func):
    arg_types = {"this": True, "expression": True}


class Round(Func):
    arg_types = {"this": True, "decimals": False}


class SetAgg(AggFunc):
    pass


class Split(Func):
    arg_types = {"this": True, "expression": True}


class Substring(Func):
    arg_types = {"this": True, "start": True, "length": False}


class StrPosition(Func):
    arg_types = {"this": True, "substr": True, "position": False}


class StrToTime(Func):
    arg_types = {"this": True, "format": True}


class StrToUnix(Func):
    arg_types = {"this": True, "format": True}


class StructExtract(Func):
    arg_types = {"this": True, "expression": True}


class Sum(AggFunc):
    pass


class Sqrt(Func):
    pass


class Stddev(AggFunc):
    pass


class StddevPop(AggFunc):
    pass


class StddevSamp(AggFunc):
    pass


class TimeToStr(Func):
    arg_types = {"this": True, "format": True}


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


class TsOrDsAdd(Func):
    arg_types = {"this": True, "expression": True, "unit": False}


class TsOrDsToDateStr(Func):
    pass


class TsOrDsToDate(Func):
    pass


class UnixToStr(Func):
    arg_types = {"this": True, "format": True}


class UnixToTime(Func):
    pass


class UnixToTimeStr(Func):
    pass


class Upper(Func):
    pass


class Variance(AggFunc):
    pass


class VariancePop(AggFunc):
    pass


class VarianceSamp(AggFunc):
    pass


class Year(Func):
    pass


def _norm_args(expression):
    return {
        k: _norm_arg(arg) if not isinstance(arg, list) else [_norm_arg(a) for a in arg]
        for k, arg in expression.args.items()
    }


def _norm_arg(arg):
    arg = arg or ""
    return arg.upper() if isinstance(arg, str) else arg


def _all_functions():
    predicate = (
        lambda obj: inspect.isclass(obj)
        and issubclass(obj, Func)
        and obj not in (AggFunc, Anonymous, Func)
    )
    return [obj for _, obj in inspect.getmembers(sys.modules[__name__], predicate)]


ALL_FUNCTIONS = _all_functions()
