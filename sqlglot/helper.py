import logging
import re
from contextlib import contextmanager
from enum import Enum


CAMEL_CASE_PATTERN = re.compile("(?<!^)(?=[A-Z])")
logger = logging.getLogger("sqlglot")


class AutoName(Enum):
    # pylint: disable=no-self-argument
    def _generate_next_value_(name, _start, _count, _last_values):
        return name


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
    if not offset or len(expressions) != 1:
        return expressions

    expression = expressions[0]

    if expression.is_int:
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


def open_file(file_name):
    """
    Open a file that may be compressed as gzip and return in newline mode.
    """
    with open(file_name, "rb") as f:
        gzipped = f.read(2) == b"\x1f\x8b"

    if gzipped:
        import gzip

        return gzip.open(file_name, "rt", newline="")

    return open(file_name, "rt", encoding="utf-8", newline="")


@contextmanager
def csv_reader(table):
    """
    Returns a csv reader given the expression READ_CSV(name, ['delimiter', '|', ...])

    Args:
        expression (Expression): An anonymous function READ_CSV

    Returns:
        A python csv reader.
    """
    file, *args = table.this.expressions
    file = file.name
    file = open_file(file)

    delimiter = ","
    args = iter(arg.name for arg in args)
    for k, v in zip(args, args):
        if k == "delimiter":
            delimiter = v

    try:
        import csv as csv_

        yield csv_.reader(file, delimiter=delimiter)
    finally:
        file.close()
