import inspect
import logging
import re
import sys
import typing as t
from contextlib import contextmanager
from copy import copy
from enum import Enum

CAMEL_CASE_PATTERN = re.compile("(?<!^)(?=[A-Z])")
logger = logging.getLogger("sqlglot")


class AutoName(Enum):
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


def subclasses(module_name, classes, exclude=()):
    """
    Returns a list of all subclasses for a specified class set, posibly excluding some of them.

    Args:
        module_name (str): The name of the module to search for subclasses in.
        classes (type|tuple[type]): Class(es) we want to find the subclasses of.
        exclude (type|tuple[type]): Class(es) we want to exclude from the returned list.
    Returns:
        A list of all the target subclasses.
    """
    return [
        obj
        for _, obj in inspect.getmembers(
            sys.modules[module_name],
            lambda obj: inspect.isclass(obj) and issubclass(obj, classes) and obj not in exclude,
        )
    ]


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
        table (exp.Table): A table expression with an anonymous function READ_CSV in it

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


def find_new_name(taken, base):
    """
    Searches for a new name.

    Args:
        taken (Sequence[str]): set of taken names
        base (str): base name to alter
    """
    if base not in taken:
        return base

    i = 2
    new = f"{base}_{i}"
    while new in taken:
        i += 1
        new = f"{base}_{i}"
    return new


def object_to_dict(obj, **kwargs):
    return {**{k: copy(v) for k, v in vars(obj).copy().items()}, **kwargs}


def split_num_words(value: str, sep: str, min_num_words: int, fill_from_start: bool = True) -> t.List[t.Optional[str]]:
    """
    Perform a split on a value and return N words as a result with None used for words that don't exist.

    Args:
        value: The value to be split
        sep: The value to use to split on
        min_num_words: The minimum number of words that are going to be in the result
        fill_from_start: Indicates that if None values should be inserted at the start or end of the list

    Examples:
        >>> split_num_words("db.table", ".", 3)
        [None, 'db', 'table']
        >>> split_num_words("db.table", ".", 3, fill_from_start=False)
        ['db', 'table', None]
        >>> split_num_words("db.table", ".", 1)
        ['db', 'table']
    """
    words = value.split(sep)
    if fill_from_start:
        return [None] * (min_num_words - len(words)) + words
    return words + [None] * (min_num_words - len(words))


def is_iterable(value: t.Any) -> bool:
    """
    Checks if the value is an iterable but does not include strings and bytes

    Examples:
        >>> is_iterable([1,2])
        True
        >>> is_iterable("test")
        False

    Args:
        value: The value to check if it is an interable

    Returns: Bool indicating if it is an iterable
    """
    return hasattr(value, "__iter__") and not isinstance(value, (str, bytes))


def flatten(values: t.Iterable[t.Union[t.Iterable[t.Any], t.Any]]) -> t.Generator[t.Any, None, None]:
    """
    Flattens a list that can contain both iterables and non-iterable elements

    Examples:
        >>> list(flatten([[1, 2], 3]))
        [1, 2, 3]
        >>> list(flatten([1, 2, 3]))
        [1, 2, 3]

    Args:
        values: The value to be flattened

    Returns:
        Yields non-iterable elements (not including str or byte as iterable)
    """
    for value in values:
        if is_iterable(value):
            yield from flatten(value)
        else:
            yield value
