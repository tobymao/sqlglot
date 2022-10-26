import inspect
import logging
import re
import sys
import typing as t
from contextlib import contextmanager
from copy import copy
from enum import Enum

from sqlglot.expressions import Expression, Table

CAMEL_CASE_PATTERN = re.compile("(?<!^)(?=[A-Z])")
logger = logging.getLogger("sqlglot")


class AutoName(Enum):
    def _generate_next_value_(name: str, _start: int, _count: int, _last_values: int) -> str:
        return name


def list_get(arr: t.List[t.Any], index: int) -> t.Optional[t.Any]:
    """Returns the value in `arr` at position `index`, or `None` if `index` is out of bounds."""
    try:
        return arr[index]
    except IndexError:
        return None


def ensure_list(value: t.Any) -> t.Union[t.List[t.Any], t.Tuple[t.Any], t.Set[t.Any]]:
    """Returns `value` wrapped in a list, unless it's already a list, tuple or set."""
    if value is None:
        return []
    return value if isinstance(value, (list, tuple, set)) else [value]


def csv(*args, sep: str = ", ") -> str:
    """
    Formats any number of strings as CSV.

    Args:
        args: string arguments to convert to CSV format.
        sep: the string to be used for separating `args`.

    Returns:
        The arguments formatted as CSV.
    """
    return sep.join(arg for arg in args if arg)


def subclasses(
    module_name: str, classes: t.Union[t.Type, t.Tuple[t.Type]], exclude: t.Union[t.Type, t.Tuple[t.Type]] = ()
) -> t.List[t.Type]:
    """
    Returns a list of all subclasses for a specified class set, posibly excluding some of them.

    Args:
        module_name: the name of the module to search for subclasses in.
        classes: class(es) we want to find the subclasses of.
        exclude: class(es) we want to exclude from the returned list.

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


def apply_index_offset(expressions: t.List[Expression], offset: int) -> t.List[Expression]:
    """
    Applies an offset to a given integer literal expression.

    Args:
        expressions: the expression the offset will be applied to, wrapped in a list.
        offset: the offset that will be applied.

    Returns:
        The original expression with the offset applied to it, wrapped in a list. If the provided
        `expressions` argument contains more than one expressions, it's returned unaffected.
    """
    if not offset or len(expressions) != 1:
        return expressions

    expression = expressions[0]

    if expression.is_int:
        expression = expression.copy()
        logger.warning("Applying array index offset (%s)", offset)
        expression.args["this"] = str(int(expression.args["this"]) + offset)
        return [expression]

    return expressions


def camel_to_snake_case(name: str) -> str:
    """Converts `name` from camelCase to snake_case and returns the result."""
    return CAMEL_CASE_PATTERN.sub("_", name).upper()


def while_changing(expression: t.Optional[Expression], func: t.Callable) -> t.Optional[Expression]:
    """
    Applies a transformation to a given expression until a fix point is reached.

    Args:
        expression: the expression to be transformed.
        func: the transformation to be applied.

    Returns:
        The transformed expression.
    """
    while True:
        start = hash(expression)
        expression = func(expression)
        if start == hash(expression):
            break
    return expression


def tsort(dag: t.Dict[t.Any, t.List[t.Any]]) -> t.List[t.Any]:
    """
    Sorts a given directed acyclic graph in topological order.

    Args:
        dag: the graph to be sorted.

    Returns:
        A list that contains all of the graph's nodes in topological order.
    """
    result = []

    def visit(node: t.Any, visited: t.Set[t.Any]) -> None:
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


def open_file(file_name: str) -> t.TextIO:
    """Open a file that may be compressed as gzip and return it in universal newline mode."""
    with open(file_name, "rb") as f:
        gzipped = f.read(2) == b"\x1f\x8b"

    if gzipped:
        import gzip

        return gzip.open(file_name, "rt", newline="")

    return open(file_name, "rt", encoding="utf-8", newline="")


@contextmanager
def csv_reader(table: Table) -> t.Any:
    """
    Returns a csv reader given the expression `READ_CSV(name, ['delimiter', '|', ...])`.

    Args:
        table: a `Table` expression with an anonymous function `READ_CSV` in it.

    Yields:
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


def find_new_name(taken: t.Sequence[str], base: str) -> str:
    """
    Searches for a new name.

    Args:
        taken: a collection of taken names.
        base: base name to alter.

    Returns:
        The new, available name.
    """
    if base not in taken:
        return base

    i = 2
    new = f"{base}_{i}"
    while new in taken:
        i += 1
        new = f"{base}_{i}"

    return new


def object_to_dict(obj: t.Any, **kwargs) -> t.Dict:
    """Returns a dictionary created from an object's attributes."""
    return {**{k: copy(v) for k, v in vars(obj).copy().items()}, **kwargs}


def split_num_words(value: str, sep: str, min_num_words: int, fill_from_start: bool = True) -> t.List[t.Optional[str]]:
    """
    Perform a split on a value and return N words as a result with `None` used for words that don't exist.

    Args:
        value: the value to be split.
        sep: the value to use to split on.
        min_num_words: the minimum number of words that are going to be in the result.
        fill_from_start: indicates that if `None` values should be inserted at the start or end of the list.

    Examples:
        >>> split_num_words("db.table", ".", 3)
        [None, 'db', 'table']
        >>> split_num_words("db.table", ".", 3, fill_from_start=False)
        ['db', 'table', None]
        >>> split_num_words("db.table", ".", 1)
        ['db', 'table']

    Returns:
        The list of words returned by `split`, possibly augmented by a number of `None` values.
    """
    words = value.split(sep)
    if fill_from_start:
        return [None] * (min_num_words - len(words)) + words
    return words + [None] * (min_num_words - len(words))


def is_iterable(value: t.Any) -> bool:
    """
    Checks if the value is an iterable but does not include strings and bytes.

    Examples:
        >>> is_iterable([1,2])
        True
        >>> is_iterable("test")
        False

    Args:
        value: the value to check if it is an iterable.

    Returns:
        A `bool` indicating if it is an iterable.
    """
    return hasattr(value, "__iter__") and not isinstance(value, (str, bytes))


def flatten(values: t.Iterable[t.Union[t.Iterable[t.Any], t.Any]]) -> t.Generator[t.Any, None, None]:
    """
    Flattens an iterable that can contain both iterables and non-iterable elements.

    Examples:
        >>> list(flatten([[1, 2], 3]))
        [1, 2, 3]
        >>> list(flatten([1, 2, 3]))
        [1, 2, 3]

    Args:
        values: the value to be flattened.

    Yields:
        Non-iterable elements in `values` (not including `str` or `byte` as iterable).
    """
    for value in values:
        if is_iterable(value):
            yield from flatten(value)
        else:
            yield value
