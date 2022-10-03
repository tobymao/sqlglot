import abc
from copy import copy

from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.helper import csv_reader


class Schema(abc.ABC):
    """Abstract base class for database schemas"""

    @abc.abstractmethod
    def column_names(self, table, only_visible=False):
        """
        Get the column names for a table.
        Args:
            table (sqlglot.expressions.Table): Table expression instance
            only_visible (bool): Whether to include invisible columns
        Returns:
            list[str]: list of column names
        """

    @abc.abstractmethod
    def get_column_type(self, table, column):
        """
        Get the exp.DataType type of a column in the schema.

        Args:
            table (sqlglot.expressions.Table): The source table.
            column (sqlglot.expressions.Column): The target column.
        Returns:
            sqlglot.expressions.DataType.Type: The resulting column type.
        """


class MappingSchema(Schema):
    """
    Schema based on a nested mapping.

    Args:
        schema (dict): Mapping in one of the following forms:
            1. {table: {col: type}}
            2. {db: {table: {col: type}}}
            3. {catalog: {db: {table: {col: type}}}}
        visible (dict): Optional mapping of which columns in the schema are visible. If not provided, all columns
            are assumed to be visible. The nesting should mirror that of the schema:
            1. {table: set(*cols)}}
            2. {db: {table: set(*cols)}}}
            3. {catalog: {db: {table: set(*cols)}}}}
        dialect (str): The dialect to be used for custom type mappings.
    """

    def __init__(self, schema, visible=None, dialect=None):
        self.schema = schema
        self.visible = visible
        self.dialect = dialect
        self._type_mapping_cache = {}

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
        
    def copy(self, **kwargs):
        kwargs = {**{"schema": copy(self.schema)}, **kwargs}
        return MappingSchema(**kwargs)

    def add_table(self, table, column_mapping, copy=False):
        self._validate_table(table)
        schema = self.schema.copy() if copy else self.schema
        _nested_set(schema, [table.text(p) for p in self.supported_table_args], column_mapping)
        return self.copy(schema=schema) if copy else self

    def add_tables(self, table_mapping, copy=False):
        schema = self
        for table, column_mapping in table_mapping.items():
            schema = schema.add_table(table, column_mapping, copy=copy)
        return schema

    def column_names(self, table, only_visible=False):
        if not isinstance(table.this, exp.Identifier):
            return fs_get(table)

        self._validate_table(table)

        args = tuple(table.text(p) for p in self.supported_table_args)
        columns = list(_nested_get(self.schema, *zip(self.supported_table_args, args)))
        if not only_visible or not self.visible:
            return columns

        visible = _nested_get(self.visible, *zip(self.supported_table_args, args))
        return [col for col in columns if col in visible]

    def get_column_type(self, table, column):
        try:
            schema_type = self.schema.get(table.name, {}).get(column.name).upper()
            return self._convert_type(schema_type)
        except:
            raise OptimizeError(f"Failed to get type for column {column.sql()}")

    def _convert_type(self, schema_type):
        """
        Convert a type represented as a string to the corresponding exp.DataType.Type object.

        Args:
            schema_type (str): The type we want to convert.
        Returns:
            sqlglot.expressions.DataType.Type: The resulting expression type.
        """
        if schema_type not in self._type_mapping_cache:
            try:
                self._type_mapping_cache[schema_type] = exp.maybe_parse(
                    schema_type, into=exp.DataType, dialect=self.dialect
                ).this
            except AttributeError:
                raise OptimizeError(f"Failed to convert type {schema_type}")

        return self._type_mapping_cache[schema_type]

    def _validate_table(self, table):
        for forbidden in self.forbidden_args:
            if table.text(forbidden):
                raise ValueError(f"Schema doesn't support {forbidden}. Received: {table.sql()}")
        for expected in self.supported_table_args:
            if not table.text(expected):
                raise ValueError(f"Table is expected to have {expected}. Received: {table.sql()} ")


def ensure_schema(schema):
    if isinstance(schema, Schema):
        return schema

    return MappingSchema(schema)


def fs_get(table):
    name = table.this.name

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


def _nested_set(d, keys, value):
    """
    In-place set a value for a nested dictionary

    Ex:
        >>> _nested_set({}, ["top_key", "second_key"], "value")
        {'top_key': {'second_key': 'value'}}
        >>> _nested_set({"top_key": {"third_key": "third_value"}}, ["top_key", "second_key"], "value")
        {'top_key': {'third_key': 'third_value', 'second_key': 'value'}}

    d (dict): dictionary
    keys (Iterable[str]): ordered iterable of keys that makeup path to value
    value (Any): The value to set in the dictionary for the given key path
    """
    if not keys:
        return
    if len(keys) == 1:
        d[keys[0]] = value
        return
    subd = d
    for key in keys[:-1]:
        if key not in subd:
            subd = subd.setdefault(key, {})
        else:
            subd = subd[key]
    subd[keys[-1]] = value
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
