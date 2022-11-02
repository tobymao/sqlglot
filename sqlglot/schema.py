from __future__ import annotations

import abc
import typing as t

from sqlglot import expressions as exp
from sqlglot.errors import OptimizeError
from sqlglot.helper import csv_reader

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql.types import StructType

    ColumnMapping = t.Union[t.Dict, str, StructType, t.List]


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
        self.visible = visible
        self.dialect = dialect
        self._type_mapping_cache: t.Dict[str, exp.DataType.Type] = {}
        self.supported_table_args: t.List | t.Tuple[str, ...] = []
        self.forbidden_table_args: t.Set[str] = set()

        if self.schema:
            self._initialize_supported_args()

    @classmethod
    def from_mapping_schema(cls, mapping_schema: MappingSchema) -> MappingSchema:
        return MappingSchema(
            schema=mapping_schema.schema,
            visible=mapping_schema.visible,
            dialect=mapping_schema.dialect,
        )

    def copy(self, **kwargs) -> MappingSchema:
        return MappingSchema(**{"schema": self.schema.copy(), **kwargs})  # type: ignore

    def add_table(
        self, table: exp.Table | str, column_mapping: t.Optional[ColumnMapping] = None
    ) -> None:
        """
        Register or update a table. Updates are only performed if a new column mapping is provided.

        Args:
            table: the `Table` expression instance or string representing the table.
            column_mapping: a column mapping that describes the structure of the table.
        """
        table = exp.to_table(table)  # type: ignore
        self._validate_table(table)  # type: ignore
        column_mapping = ensure_column_mapping(column_mapping)
        table_args = [table.text(p) for p in self.supported_table_args or self._get_table_args_from_table(table)]  # type: ignore
        existing_column_mapping = _nested_get(
            self.schema, *zip(self.supported_table_args, table_args), raise_on_missing=False
        )

        if existing_column_mapping and not column_mapping:
            return

        _nested_set(
            self.schema,
            [table.text(p) for p in self.supported_table_args or self._get_table_args_from_table(table)],  # type: ignore
            column_mapping,
        )
        self._initialize_supported_args()

    def _get_table_args_from_table(self, table: exp.Table) -> t.Tuple[str, ...]:
        if table.args.get("catalog") is not None:
            return "catalog", "db", "this"
        if table.args.get("db") is not None:
            return "db", "this"
        return ("this",)

    def _validate_table(self, table: exp.Table) -> None:
        if not self.supported_table_args and isinstance(table, exp.Table):
            return
        for forbidden in self.forbidden_table_args:
            if table.text(forbidden):
                raise ValueError(f"Schema doesn't support {forbidden}. Received: {table.sql()}")
        for expected in self.supported_table_args:
            if not table.text(expected):
                raise ValueError(f"Table is expected to have {expected}. Received: {table.sql()} ")

    def column_names(self, table: exp.Table | str, only_visible: bool = False) -> t.List[str]:
        table = exp.to_table(table)  # type: ignore
        if not isinstance(table.this, exp.Identifier):  # type: ignore
            return fs_get(table)  # type: ignore

        args = tuple(table.text(p) for p in self.supported_table_args)  # type: ignore

        for forbidden in self.forbidden_table_args:
            if table.text(forbidden):  # type: ignore
                raise ValueError(f"Schema doesn't support {forbidden}. Received: {table.sql()}")  # type: ignore

        columns = list(_nested_get(self.schema, *zip(self.supported_table_args, args)))  # type: ignore
        if not only_visible or not self.visible:
            return columns

        visible = _nested_get(self.visible, *zip(self.supported_table_args, args))  # type: ignore
        return [col for col in columns if col in visible]  # type: ignore

    def get_column_type(self, table: exp.Table | str, column: exp.Column) -> exp.DataType.Type:
        try:
            if isinstance(table, exp.Table):
                table_args = [
                    table.text(p)
                    for p in self.supported_table_args or self._get_table_args_from_table(table)
                ]
                table_schema = _nested_get(
                    self.schema,
                    *zip(self.supported_table_args, table_args),
                    raise_on_missing=False,
                )
                schema_type = table_schema.get(column.name).upper()  # type: ignore
                return self._convert_type(schema_type)

            schema_type = self.schema.get(table, {}).table_schema.get(column).upper()
            return self._convert_type(schema_type)
        except:
            raise OptimizeError(
                f"Failed to get type for column {column if isinstance(column, str) else column.sql()}"
            )

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
                self._type_mapping_cache[schema_type] = exp.maybe_parse(
                    schema_type, into=exp.DataType, dialect=self.dialect
                ).this
            except AttributeError:
                raise OptimizeError(f"Failed to convert type {schema_type}")

        return self._type_mapping_cache[schema_type]

    def _initialize_supported_args(self) -> None:
        if not self.supported_table_args:
            depth = _dict_depth(self.schema)

            all_args = ["this", "db", "catalog"]
            if not depth or depth == 1:  # {}
                self.supported_table_args = []
            elif 2 <= depth <= 4:
                self.supported_table_args = tuple(reversed(all_args[: depth - 1]))
            else:
                raise OptimizeError(f"Invalid schema shape. Depth: {depth}")

            self.forbidden_table_args = {"catalog", "db", "this"} - set(self.supported_table_args)


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
