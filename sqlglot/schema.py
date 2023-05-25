from __future__ import annotations

import abc
import typing as t

import sqlglot
from sqlglot import expressions as exp
from sqlglot._typing import T
from sqlglot.errors import ParseError, SchemaError
from sqlglot.helper import dict_depth
from sqlglot.trie import in_trie, new_trie

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql.types import StructType
    from sqlglot.dialects.dialect import DialectType

    ColumnMapping = t.Union[t.Dict, str, StructType, t.List]

TABLE_ARGS = ("this", "db", "catalog")


class Schema(abc.ABC):
    """Abstract base class for database schemas"""

    @abc.abstractmethod
    def add_table(
        self,
        table: exp.Table | str,
        column_mapping: t.Optional[ColumnMapping] = None,
        dialect: DialectType = None,
    ) -> None:
        """
        Register or update a table. Some implementing classes may require column information to also be provided.

        Args:
            table: the `Table` expression instance or string representing the table.
            column_mapping: a column mapping that describes the structure of the table.
            dialect: the SQL dialect that will be used to parse `table` if it's a string.
        """

    @abc.abstractmethod
    def column_names(
        self,
        table: exp.Table | str,
        only_visible: bool = False,
        dialect: DialectType = None,
    ) -> t.List[str]:
        """
        Get the column names for a table.

        Args:
            table: the `Table` expression instance.
            only_visible: whether to include invisible columns.
            dialect: the SQL dialect that will be used to parse `table` if it's a string.

        Returns:
            The list of column names.
        """

    @abc.abstractmethod
    def get_column_type(
        self,
        table: exp.Table | str,
        column: exp.Column,
        dialect: DialectType = None,
    ) -> exp.DataType:
        """
        Get the `sqlglot.exp.DataType` type of a column in the schema.

        Args:
            table: the source table.
            column: the target column.
            dialect: the SQL dialect that will be used to parse `table` if it's a string.

        Returns:
            The resulting column type.
        """

    @property
    @abc.abstractmethod
    def supported_table_args(self) -> t.Tuple[str, ...]:
        """
        Table arguments this schema support, e.g. `("this", "db", "catalog")`
        """

    @property
    def empty(self) -> bool:
        """Returns whether or not the schema is empty."""
        return True


class AbstractMappingSchema(t.Generic[T]):
    def __init__(
        self,
        mapping: t.Optional[t.Dict] = None,
    ) -> None:
        self.mapping = mapping or {}
        self.mapping_trie = new_trie(
            tuple(reversed(t)) for t in flatten_schema(self.mapping, depth=self._depth())
        )
        self._supported_table_args: t.Tuple[str, ...] = tuple()

    @property
    def empty(self) -> bool:
        return not self.mapping

    def _depth(self) -> int:
        return dict_depth(self.mapping)

    @property
    def supported_table_args(self) -> t.Tuple[str, ...]:
        if not self._supported_table_args and self.mapping:
            depth = self._depth()

            if not depth:  # None
                self._supported_table_args = tuple()
            elif 1 <= depth <= 3:
                self._supported_table_args = TABLE_ARGS[:depth]
            else:
                raise SchemaError(f"Invalid mapping shape. Depth: {depth}")

        return self._supported_table_args

    def table_parts(self, table: exp.Table) -> t.List[str]:
        if isinstance(table.this, exp.ReadCSV):
            return [table.this.name]
        return [table.text(part) for part in TABLE_ARGS if table.text(part)]

    def find(
        self, table: exp.Table, trie: t.Optional[t.Dict] = None, raise_on_missing: bool = True
    ) -> t.Optional[T]:
        parts = self.table_parts(table)[0 : len(self.supported_table_args)]
        value, trie = in_trie(self.mapping_trie if trie is None else trie, parts)

        if value == 0:
            return None

        if value == 1:
            possibilities = flatten_schema(trie, depth=dict_depth(trie) - 1)

            if len(possibilities) == 1:
                parts.extend(possibilities[0])
            else:
                message = ", ".join(".".join(parts) for parts in possibilities)
                if raise_on_missing:
                    raise SchemaError(f"Ambiguous mapping for {table}: {message}.")
                return None

        return self.nested_get(parts, raise_on_missing=raise_on_missing)

    def nested_get(
        self, parts: t.Sequence[str], d: t.Optional[t.Dict] = None, raise_on_missing=True
    ) -> t.Optional[t.Any]:
        return nested_get(
            d or self.mapping,
            *zip(self.supported_table_args, reversed(parts)),
            raise_on_missing=raise_on_missing,
        )


class MappingSchema(AbstractMappingSchema[t.Dict[str, str]], Schema):
    """
    Schema based on a nested mapping.

    Args:
        schema: Mapping in one of the following forms:
            1. {table: {col: type}}
            2. {db: {table: {col: type}}}
            3. {catalog: {db: {table: {col: type}}}}
            4. None - Tables will be added later
        visible: Optional mapping of which columns in the schema are visible. If not provided, all columns
            are assumed to be visible. The nesting should mirror that of the schema:
            1. {table: set(*cols)}}
            2. {db: {table: set(*cols)}}}
            3. {catalog: {db: {table: set(*cols)}}}}
        dialect: The dialect to be used for custom type mappings & parsing string arguments.
    """

    def __init__(
        self,
        schema: t.Optional[t.Dict] = None,
        visible: t.Optional[t.Dict] = None,
        dialect: DialectType = None,
    ) -> None:
        self.dialect = dialect
        self.visible = visible or {}
        self._type_mapping_cache: t.Dict[str, exp.DataType] = {}

        super().__init__(self._normalize(schema or {}))

    @classmethod
    def from_mapping_schema(cls, mapping_schema: MappingSchema) -> MappingSchema:
        return MappingSchema(
            schema=mapping_schema.mapping,
            visible=mapping_schema.visible,
            dialect=mapping_schema.dialect,
        )

    def copy(self, **kwargs) -> MappingSchema:
        return MappingSchema(
            **{  # type: ignore
                "schema": self.mapping.copy(),
                "visible": self.visible.copy(),
                "dialect": self.dialect,
                **kwargs,
            }
        )

    def add_table(
        self,
        table: exp.Table | str,
        column_mapping: t.Optional[ColumnMapping] = None,
        dialect: DialectType = None,
    ) -> None:
        """
        Register or update a table. Updates are only performed if a new column mapping is provided.

        Args:
            table: the `Table` expression instance or string representing the table.
            column_mapping: a column mapping that describes the structure of the table.
            dialect: the SQL dialect that will be used to parse `table` if it's a string.
        """
        normalized_table = self._normalize_table(
            self._ensure_table(table, dialect=dialect), dialect=dialect
        )
        normalized_column_mapping = {
            self._normalize_name(key, dialect=dialect): value
            for key, value in ensure_column_mapping(column_mapping).items()
        }

        schema = self.find(normalized_table, raise_on_missing=False)
        if schema and not normalized_column_mapping:
            return

        parts = self.table_parts(normalized_table)

        nested_set(self.mapping, tuple(reversed(parts)), normalized_column_mapping)
        new_trie([parts], self.mapping_trie)

    def column_names(
        self,
        table: exp.Table | str,
        only_visible: bool = False,
        dialect: DialectType = None,
    ) -> t.List[str]:
        normalized_table = self._normalize_table(
            self._ensure_table(table, dialect=dialect), dialect=dialect
        )

        schema = self.find(normalized_table)
        if schema is None:
            return []

        if not only_visible or not self.visible:
            return list(schema)

        visible = self.nested_get(self.table_parts(normalized_table), self.visible) or []
        return [col for col in schema if col in visible]

    def get_column_type(
        self,
        table: exp.Table | str,
        column: exp.Column,
        dialect: DialectType = None,
    ) -> exp.DataType:
        normalized_table = self._normalize_table(
            self._ensure_table(table, dialect=dialect), dialect=dialect
        )
        normalized_column_name = self._normalize_name(
            column if isinstance(column, str) else column.this, dialect=dialect
        )

        table_schema = self.find(normalized_table, raise_on_missing=False)
        if table_schema:
            column_type = table_schema.get(normalized_column_name)

            if isinstance(column_type, exp.DataType):
                return column_type
            elif isinstance(column_type, str):
                return self._to_data_type(column_type.upper(), dialect=dialect)

            raise SchemaError(f"Unknown column type '{column_type}'")

        return exp.DataType.build("unknown")

    def _normalize(self, schema: t.Dict) -> t.Dict:
        """
        Converts all identifiers in the schema into lowercase, unless they're quoted.

        Args:
            schema: the schema to normalize.

        Returns:
            The normalized schema mapping.
        """
        flattened_schema = flatten_schema(schema, depth=dict_depth(schema) - 1)

        normalized_mapping: t.Dict = {}
        for keys in flattened_schema:
            columns = nested_get(schema, *zip(keys, keys))
            assert columns is not None

            normalized_keys = [self._normalize_name(key, dialect=self.dialect) for key in keys]
            for column_name, column_type in columns.items():
                nested_set(
                    normalized_mapping,
                    normalized_keys + [self._normalize_name(column_name, dialect=self.dialect)],
                    column_type,
                )

        return normalized_mapping

    def _normalize_table(self, table: exp.Table, dialect: DialectType = None) -> exp.Table:
        normalized_table = table.copy()

        for arg in TABLE_ARGS:
            value = normalized_table.args.get(arg)
            if isinstance(value, (str, exp.Identifier)):
                normalized_table.set(arg, self._normalize_name(value, dialect=dialect))

        return normalized_table

    def _normalize_name(self, name: str | exp.Identifier, dialect: DialectType = None) -> str:
        dialect = dialect or self.dialect

        try:
            identifier = sqlglot.maybe_parse(name, dialect=dialect, into=exp.Identifier)
        except ParseError:
            return name if isinstance(name, str) else name.name

        return identifier.name if identifier.quoted else identifier.name.lower()

    def _depth(self) -> int:
        # The columns themselves are a mapping, but we don't want to include those
        return super()._depth() - 1

    def _ensure_table(self, table: exp.Table | str, dialect: DialectType = None) -> exp.Table:
        if isinstance(table, exp.Table):
            return table

        dialect = dialect or self.dialect
        parsed_table = sqlglot.parse_one(table, read=dialect, into=exp.Table)

        if not parsed_table:
            in_dialect = f" in dialect {dialect}" if dialect else ""
            raise SchemaError(f"Failed to parse table '{table}'{in_dialect}.")

        return parsed_table

    def _to_data_type(self, schema_type: str, dialect: DialectType = None) -> exp.DataType:
        """
        Convert a type represented as a string to the corresponding `sqlglot.exp.DataType` object.

        Args:
            schema_type: the type we want to convert.
            dialect: the SQL dialect that will be used to parse `schema_type`, if needed.

        Returns:
            The resulting expression type.
        """
        if schema_type not in self._type_mapping_cache:
            dialect = dialect or self.dialect

            try:
                expression = exp.DataType.build(schema_type, dialect=dialect)
                self._type_mapping_cache[schema_type] = expression
            except AttributeError:
                in_dialect = f" in dialect {dialect}" if dialect else ""
                raise SchemaError(f"Failed to build type '{schema_type}'{in_dialect}.")

        return self._type_mapping_cache[schema_type]


def ensure_schema(schema: t.Any, dialect: DialectType = None) -> Schema:
    if isinstance(schema, Schema):
        return schema

    return MappingSchema(schema, dialect=dialect)


def ensure_column_mapping(mapping: t.Optional[ColumnMapping]) -> t.Dict:
    if mapping is None:
        return {}
    elif isinstance(mapping, dict):
        return mapping
    elif isinstance(mapping, str):
        col_name_type_strs = [x.strip() for x in mapping.split(",")]
        return {
            name_type_str.split(":")[0].strip(): name_type_str.split(":")[1].strip()
            for name_type_str in col_name_type_strs
        }
    # Check if mapping looks like a DataFrame StructType
    elif hasattr(mapping, "simpleString"):
        return {struct_field.name: struct_field.dataType.simpleString() for struct_field in mapping}
    elif isinstance(mapping, list):
        return {x.strip(): None for x in mapping}

    raise ValueError(f"Invalid mapping provided: {type(mapping)}")


def flatten_schema(
    schema: t.Dict, depth: int, keys: t.Optional[t.List[str]] = None
) -> t.List[t.List[str]]:
    tables = []
    keys = keys or []

    for k, v in schema.items():
        if depth >= 2:
            tables.extend(flatten_schema(v, depth - 1, keys + [k]))
        elif depth == 1:
            tables.append(keys + [k])

    return tables


def nested_get(
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
                raise ValueError(f"Unknown {name}: {key}")
            return None

    return d


def nested_set(d: t.Dict, keys: t.Sequence[str], value: t.Any) -> t.Dict:
    """
    In-place set a value for a nested dictionary

    Example:
        >>> nested_set({}, ["top_key", "second_key"], "value")
        {'top_key': {'second_key': 'value'}}

        >>> nested_set({"top_key": {"third_key": "third_value"}}, ["top_key", "second_key"], "value")
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
