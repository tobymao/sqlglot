import abc


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


class SingleDatabaseSchema(Schema):
    """
    Schema that only supports a single database.

    Args:
        tables (dict): Mapping of table names to mappings of column names to data types.
            For example: {"table1": {"col1": "int64"}}
    """

    def __init__(self, tables):
        self._tables = tables

    def column_names(self, table):
        catalog_name = table.text("catalog")
        db_name = table.text("db")
        table_name = table.text("this")
        table_str = table.sql()

        if catalog_name:
            raise ValueError(f"Schema doesn't support catalogs. Received: {table_str}")

        if db_name:
            raise ValueError(f"Schema doesn't support databases. Received: {table_str}")

        columns = self._tables.get(table_name)
        if columns is None:
            raise ValueError(f"Table not found: Received: {table_str}")

        return list(columns)


def ensure_schema(schema):
    if isinstance(schema, Schema):
        raise schema

    return SingleDatabaseSchema(schema)
