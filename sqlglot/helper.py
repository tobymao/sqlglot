import logging
import re
from enum import Enum


CAMEL_CASE_PATTERN = re.compile("(?<!^)(?=[A-Z])")
logger = logging.getLogger("sqlglot")


class AutoName(Enum):
    # pylint: disable=no-self-argument
    def _generate_next_value_(name, _start, _count, _last_values):
        return name


class RegisteringMeta(type):
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


def list_get(arr, index):
    try:
        return arr[index]
    except IndexError:
        return None


def ensure_list(value):
    if value is None:
        return []
    return value if isinstance(value, (list, tuple, set)) else [value]


def csv(*args, sep=", "):
    return sep.join(arg for arg in args if arg)


def apply_index_offset(expressions, offset):
    import sqlglot.expressions as exp

    if not offset or len(expressions) != 1:
        return expressions

    expression = expressions[0]

    if isinstance(expression, exp.Literal) and expression.is_int:
        expression = expression.copy()
        logger.warning("Applying array index offset (%s)", offset)
        expression.args["this"] = str(int(expression.args["this"]) + offset)
        return [expression]
    return expressions


def camel_to_snake_case(name):
    return CAMEL_CASE_PATTERN.sub("_", name).upper()


def while_changing(expression, func):
    while True:
        start = hash(expression)
        expression = func(expression)
        if start == hash(expression):
            break
    return expression


def tsort(dag):
    result = []

    def visit(node, visited):
        if node in result:
            return
        if node in visited:
            raise ValueError("Cycle error")

        visited.add(node)

        for dep in dag.get(node, []):
            visit(dep, visited)

        visited.remove(node)
        result.append(node)

    for node in dag:
        visit(node, set())

    return result
