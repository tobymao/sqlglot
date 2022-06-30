import abc

from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.helper import csv_reader


class Schema(abc.ABC):
    """Abstract base class for database schemas"""

    @abc.abstractmethod
    def column_names(self, table):
        """
        Get the column names for a table.

        Args:
            table (sqlglot.expressions.Table): Table expression instance
        Returns:
            list[str]: list of column names
        """


class MappingSchema(Schema):
    """
    Schema based on a nested mapping.

    Args:
        schema (dict): Mapping in one of the following forms:
            1. {table: {col: type}}
            2. {db: {table: {col: type}}}
            3. {catalog: {db: {table: {col: type}}}}
    """

    def __init__(self, schema):
        self.schema = schema

        depth = _dict_depth(schema)

        if not depth:  # {}
            self.supported_table_args = []
        elif depth == 2:  # {table: {col: type}}
            self.supported_table_args = ("this",)
        elif depth == 3:  # {db: {table: {col: type}}}
            self.supported_table_args = ("db", "this")
        elif depth == 4:  # {catalog: {db: {table: {col: type}}}}
            self.supported_table_args = ("catalog", "db", "this")
        else:
            raise OptimizeError(f"Invalid schema shape. Depth: {depth}")

        self.forbidden_args = {"catalog", "db", "this"} - set(self.supported_table_args)

    def column_names(self, table):
        if not isinstance(table.this, exp.Identifier):
            return fs_get(table)

        args = tuple(table.text(p) for p in self.supported_table_args)

        for forbidden in self.forbidden_args:
            if table.text(forbidden):
                raise ValueError(
                    f"Schema doesn't support {forbidden}. Received: {table.sql()}"
                )
        return list(_nested_get(self.schema, *zip(self.supported_table_args, args)))


def ensure_schema(schema):
    if isinstance(schema, Schema):
        return schema

    return MappingSchema(schema)


def fs_get(table):
    name = table.this.name.upper()

    if name.upper() == "READ_CSV":
        with csv_reader(table) as reader:
            return next(reader)

    raise ValueError(f"Cannot read schema for {table}")


def _nested_get(d, *path):
    """
    Get a value for a nested dictionary.

    Args:
        d (dict): dictionary
        *path (tuple[str, str]): tuples of (name, key)
            `key` is the key in the dictionary to get.
            `name` is a string to use in the error if `key` isn't found.
    """
    for name, key in path:
        d = d.get(key)
        if d is None:
            name = "table" if name == "this" else name
            raise ValueError(f"Unknown {name}")
    return d


def _dict_depth(d):
    """
    Get the nesting depth of a dictionary.

    For example:
        >>> _dict_depth(None)
        0
        >>> _dict_depth({})
        1
        >>> _dict_depth({"a": "b"})
        1
        >>> _dict_depth({"a": {}})
        2
        >>> _dict_depth({"a": {"b": {}}})
        3

    Args:
        d (dict): dictionary
    Returns:
        int: depth
    """
    try:
        return 1 + _dict_depth(next(iter(d.values())))
    except AttributeError:
        # d doesn't have attribute "values"
        return 0
    except StopIteration:
        # d.values() returns an empty sequence
        return 1
