import abc

from sqlglot.errors import OptimizeError


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

        if depth == 2:  # {table: {col: type}}
            self.supported_table_args = ("this",)
        elif depth == 3:  # {db: {table: {col: type}}}
            self.supported_table_args = ("db", "this")
        elif depth == 4:  # {catalog: {db: {table: {col: type}}}}
            self.supported_table_args = ("catalog", "db", "this")
        else:
            raise OptimizeError(f"Invalid schema shape. Depth: {depth}")

    def column_names(self, table):
        args = _table_args(table, *self.supported_table_args)
        return list(_nested_get(self.schema, *zip(self.supported_table_args, args)))


def ensure_schema(schema):
    if isinstance(schema, Schema):
        return schema

    return MappingSchema(schema)


def _table_args(table, *args):
    """
    Extract the args of a table expression.

    Args:
        table (sqlglot.expressions.Table): table expression
        *args (str): arg names to extract
    Returns:
        tuple[str]: args
    """
    result = tuple(table.text(p) for p in args)

    for forbidden in {"catalog", "db", "this"} - set(args):
        if table.text(forbidden):
            raise ValueError(
                f"Schema doesn't support catalogs. Received: {table.sql()}"
            )

    return result


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
