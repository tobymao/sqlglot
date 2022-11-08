from __future__ import annotations

import abc
import typing as t

from sqlglot import expressions as exp
from sqlglot.errors import SchemaError
from sqlglot.helper import csv_reader
from sqlglot.trie import in_trie, new_trie

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql.types import StructType

    ColumnMapping = t.Union[t.Dict, str, StructType, t.List]

TABLE_ARGS = ("this", "db", "catalog")


class Schema(abc.ABC):
    """Abstract base class for database schemas"""

    @abc.abstractmethod
    def add_table(
        self, table: exp.Table | str, column_mapping: t.Optional[ColumnMapping] = None
    ) -> None:
        """
        Register or update a table. Some implementing classes may require column information to also be provided.

        Args:
            table: table expression instance or string representing the table.
            column_mapping: a column mapping that describes the structure of the table.
        """

    @abc.abstractmethod
    def column_names(self, table: exp.Table | str, only_visible: bool = False) -> t.List[str]:
        """
        Get the column names for a table.

        Args:
            table: the `Table` expression instance.
            only_visible: whether to include invisible columns.

        Returns:
            The list of column names.
        """

    @abc.abstractmethod
    def get_column_type(self, table: exp.Table | str, column: exp.Column) -> exp.DataType.Type:
        """
        Get the :class:`sqlglot.exp.DataType` type of a column in the schema.

        Args:
            table: the source table.
            column: the target column.

        Returns:
            The resulting column type.
        """


class MappingSchema(Schema):
    """
    Schema based on a nested mapping.

    Args:
        schema (dict): Mapping in one of the following forms:
            1. {table: {col: type}}
            2. {db: {table: {col: type}}}
            3. {catalog: {db: {table: {col: type}}}}
            4. None - Tables will be added later
        visible (dict): Optional mapping of which columns in the schema are visible. If not provided, all columns
            are assumed to be visible. The nesting should mirror that of the schema:
            1. {table: set(*cols)}}
            2. {db: {table: set(*cols)}}}
            3. {catalog: {db: {table: set(*cols)}}}}
        dialect (str): The dialect to be used for custom type mappings.
    """

    def __init__(
        self,
        schema: t.Optional[t.Dict] = None,
        visible: t.Optional[t.Dict] = None,
        dialect: t.Optional[str] = None,
    ) -> None:
        self.schema = schema or {}
        self.visible = visible or {}
        self.schema_trie = self._build_trie(self.schema)
        self.dialect = dialect
        self._type_mapping_cache: t.Dict[str, exp.DataType.Type] = {}
        self._supported_table_args: t.Tuple[str, ...] = tuple()

    @classmethod
    def from_mapping_schema(cls, mapping_schema: MappingSchema) -> MappingSchema:
        return MappingSchema(
            schema=mapping_schema.schema,
            visible=mapping_schema.visible,
            dialect=mapping_schema.dialect,
        )

    def copy(self, **kwargs) -> MappingSchema:
        return MappingSchema(
            **{  # type: ignore
                "schema": self.schema.copy(),
                "visible": self.visible.copy(),
                "dialect": self.dialect,
                **kwargs,
            }
        )

    @property
    def supported_table_args(self):
        if not self._supported_table_args and self.schema:
            depth = _dict_depth(self.schema)

            if not depth or depth == 1:  # {}
                self._supported_table_args = tuple()
            elif 2 <= depth <= 4:
                self._supported_table_args = TABLE_ARGS[: depth - 1]
            else:
                raise SchemaError(f"Invalid schema shape. Depth: {depth}")

        return self._supported_table_args

    def add_table(
        self, table: exp.Table | str, column_mapping: t.Optional[ColumnMapping] = None
    ) -> None:
        """
        Register or update a table. Updates are only performed if a new column mapping is provided.

        Args:
            table: the `Table` expression instance or string representing the table.
            column_mapping: a column mapping that describes the structure of the table.
        """
        table_ = self._ensure_table(table)
        column_mapping = ensure_column_mapping(column_mapping)
        schema = self.find_schema(table_, raise_on_missing=False)

        if schema and not column_mapping:
            return

        _nested_set(
            self.schema,
            list(reversed(self.table_parts(table_))),
            column_mapping,
        )
        self.schema_trie = self._build_trie(self.schema)

    def _ensure_table(self, table: exp.Table | str) -> exp.Table:
        table_ = exp.to_table(table)

        if not table_:
            raise SchemaError(f"Not a valid table '{table}'")

        return table_

    def table_parts(self, table: exp.Table) -> t.List[str]:
        return [table.text(part) for part in TABLE_ARGS if table.text(part)]

    def column_names(self, table: exp.Table | str, only_visible: bool = False) -> t.List[str]:
        table_ = self._ensure_table(table)

        if not isinstance(table_.this, exp.Identifier):
            return fs_get(table)  # type: ignore

        schema = self.find_schema(table_)

        if schema is None:
            raise SchemaError(f"Could not find table schema {table}")

        if not only_visible or not self.visible:
            return list(schema)

        visible = self._nested_get(self.table_parts(table_), self.visible)
        return [col for col in schema if col in visible]  # type: ignore

    def find_schema(
        self, table: exp.Table, trie: t.Optional[t.Dict] = None, raise_on_missing: bool = True
    ) -> t.Optional[t.Dict[str, str]]:
        parts = self.table_parts(table)[0 : len(self.supported_table_args)]
        value, trie = in_trie(self.schema_trie if trie is None else trie, parts)

        if value == 0:
            if raise_on_missing:
                raise SchemaError(f"Cannot find schema for {table}.")
            else:
                return None
        elif value == 1:
            possibilities = flatten_schema(trie)
            if len(possibilities) == 1:
                parts.extend(possibilities[0])
            else:
                message = ", ".join(".".join(parts) for parts in possibilities)
                if raise_on_missing:
                    raise SchemaError(f"Ambiguous schema for {table}: {message}.")
                return None

        return self._nested_get(parts, raise_on_missing=raise_on_missing)

    def get_column_type(
        self, table: exp.Table | str, column: exp.Column | str
    ) -> exp.DataType.Type:
        column_name = column if isinstance(column, str) else column.name
        table_ = exp.to_table(table)
        if table_:
            table_schema = self.find_schema(table_)
            schema_type = table_schema.get(column_name).upper()  # type: ignore
            return self._convert_type(schema_type)
        raise SchemaError(f"Could not convert table '{table}'")

    def _convert_type(self, schema_type: str) -> exp.DataType.Type:
        """
        Convert a type represented as a string to the corresponding :class:`sqlglot.exp.DataType` object.

        Args:
            schema_type: the type we want to convert.

        Returns:
            The resulting expression type.
        """
        if schema_type not in self._type_mapping_cache:
            try:
                expression = exp.maybe_parse(schema_type, into=exp.DataType, dialect=self.dialect)
                if expression is None:
                    raise ValueError(f"Could not parse {schema_type}")
                self._type_mapping_cache[schema_type] = expression.this
            except AttributeError:
                raise SchemaError(f"Failed to convert type {schema_type}")

        return self._type_mapping_cache[schema_type]

    def _build_trie(self, schema: t.Dict):
        return new_trie(tuple(reversed(t)) for t in flatten_schema(schema))

    def _nested_get(
        self, parts: t.Sequence[str], d: t.Optional[t.Dict] = None, raise_on_missing=True
    ) -> t.Optional[t.Any]:
        return _nested_get(
            d or self.schema,
            *zip(self.supported_table_args, reversed(parts)),
            raise_on_missing=raise_on_missing,
        )


def ensure_schema(schema: t.Any) -> Schema:
    if isinstance(schema, Schema):
        return schema

    return MappingSchema(schema)


def ensure_column_mapping(mapping: t.Optional[ColumnMapping]):
    if isinstance(mapping, dict):
        return mapping
    elif isinstance(mapping, str):
        col_name_type_strs = [x.strip() for x in mapping.split(",")]
        return {
            name_type_str.split(":")[0].strip(): name_type_str.split(":")[1].strip()
            for name_type_str in col_name_type_strs
        }
    # Check if mapping looks like a DataFrame StructType
    elif hasattr(mapping, "simpleString"):
        return {struct_field.name: struct_field.dataType.simpleString() for struct_field in mapping}  # type: ignore
    elif isinstance(mapping, list):
        return {x.strip(): None for x in mapping}
    elif mapping is None:
        return {}
    raise ValueError(f"Invalid mapping provided: {type(mapping)}")


def flatten_schema(schema: t.Dict, keys: t.Optional[t.List[str]] = None) -> t.List[t.List[str]]:
    tables = []
    keys = keys or []
    depth = _dict_depth(schema)

    for k, v in schema.items():
        if depth >= 3:
            tables.extend(flatten_schema(v, keys + [k]))
        elif depth == 2:
            tables.append(keys + [k])
    return tables


def fs_get(table: exp.Table) -> t.List[str]:
    name = table.this.name

    if name.upper() == "READ_CSV":
        with csv_reader(table) as reader:
            return next(reader)

    raise ValueError(f"Cannot read schema for {table}")


def _nested_get(
    d: t.Dict, *path: t.Tuple[str, str], raise_on_missing: bool = True
) -> t.Optional[t.Any]:
    """
    Get a value for a nested dictionary.

    Args:
        d: the dictionary to search.
        *path: tuples of (name, key), where:
            `key` is the key in the dictionary to get.
            `name` is a string to use in the error if `key` isn't found.

    Returns:
        The value or None if it doesn't exist.
    """
    for name, key in path:
        d = d.get(key)  # type: ignore
        if d is None:
            if raise_on_missing:
                name = "table" if name == "this" else name
                raise ValueError(f"Unknown {name}")
            return None
    return d


def _nested_set(d: t.Dict, keys: t.List[str], value: t.Any) -> t.Dict:
    """
    In-place set a value for a nested dictionary

    Example:
        >>> _nested_set({}, ["top_key", "second_key"], "value")
        {'top_key': {'second_key': 'value'}}

        >>> _nested_set({"top_key": {"third_key": "third_value"}}, ["top_key", "second_key"], "value")
        {'top_key': {'third_key': 'third_value', 'second_key': 'value'}}

    Args:
        d: dictionary to update.
            keys: the keys that makeup the path to `value`.
            value: the value to set in the dictionary for the given key path.

        Returns:
                The (possibly) updated dictionary.
    """
    if not keys:
        return d

    if len(keys) == 1:
        d[keys[0]] = value
        return d

    subd = d
    for key in keys[:-1]:
        if key not in subd:
            subd = subd.setdefault(key, {})
        else:
            subd = subd[key]

    subd[keys[-1]] = value
    return d


def _dict_depth(d: t.Dict) -> int:
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
