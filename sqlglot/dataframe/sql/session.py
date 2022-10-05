import typing as t
import uuid
from collections import defaultdict

import sqlglot
from sqlglot import expressions as exp
from sqlglot.dataframe.sql.readwriter import DataFrameReader
from sqlglot.dataframe.sql import functions as F
from sqlglot.dataframe.sql.dataframe import DataFrame
from sqlglot.dataframe.sql.operations import Operation
from sqlglot.dataframe.sql.types import StructType
from sqlglot.dataframe.sql.util import get_column_mapping_from_schema_input
from sqlglot.optimizer.schema import MappingSchema

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql._typing import SchemaInput


class SparkSession:
    known_ids: t.ClassVar[t.Set[str]] = set()
    known_branch_ids: t.ClassVar[t.Set[str]] = set()
    known_sequence_ids: t.ClassVar[t.Set[str]] = set()

    def __init__(self):
        self.name_to_sequence_id_mapping = defaultdict(list)
        self.schema = MappingSchema()
        self.incrementing_id = 1

    @property
    def read(self) -> "DataFrameReader":
        return DataFrameReader(self)

    def add_table(self, table: t.Union[exp.Table, str], schema: "SchemaInput") -> None:
        table = exp.Table.from_str(table) if isinstance(table, str) else table
        column_mapping = get_column_mapping_from_schema_input(schema)
        self.schema.add_table(table, column_mapping)

    def add_tables(self, table_column_mapping: t.Dict[t.Union[exp.Table, str], "SchemaInput"]) -> None:
        for table, schema in table_column_mapping.items():
            self.add_table(table, schema)

    def table(self, tableName: str, schema: "SchemaInput") -> "DataFrame":
        return self.read.table(tableName, schema)

    def createDataFrame(
            self,
            data: t.Iterable[t.Union[t.Dict[str, t.Any], t.Iterable[t.Any]]],
            schema: t.Optional["SchemaInput"] = None,
            samplingRatio: t.Optional[float] = None,
            verifySchema: bool = False,
    ) -> "DataFrame":
        from sqlglot.dataframe.sql.dataframe import DataFrame

        if samplingRatio is not None or verifySchema:
            raise NotImplementedError("Sampling Ration and Verify Schema are not supported")
        if schema is not None and (not isinstance(schema, (StructType, str, list)) or (isinstance(schema, list) and not isinstance(schema[0], str))):
            raise NotImplementedError("Only schema of either list or string of list supported")

        if schema is not None:
            column_mapping = get_column_mapping_from_schema_input(schema)
        elif isinstance(data[0], dict):
            column_mapping = {col_name.strip(): None for col_name in data[0].keys()}
        else:
            column_mapping = {f"_{i}": None for i in range(1, len(data[0]) + 1)}

        data_expressions = [
            exp.Tuple(
                expressions=list(map(lambda x: F.lit(x).expression, row if not isinstance(row, dict) else row.values()))
            )
            for row in data
        ]

        sel_columns = [
            F.col(name).cast(data_type).alias(name).expression if data_type is not None else F.col(name).expression
            for name, data_type in column_mapping.items()
        ]

        select_kwargs = {
            'expressions': sel_columns,
            'from': exp.From(
                expressions=[exp.Subquery(
                    this=exp.Values(
                        expressions=data_expressions
                    ),
                    alias=exp.TableAlias(
                        this=exp.Identifier(this=self._auto_incrementing_name),
                        columns=[exp.Identifier(this=col_name) for col_name in column_mapping.keys()]
                    )
                )]
            )
        }

        sel_expression = exp.Select(**select_kwargs)
        return DataFrame(
            spark=self,
            expression=sel_expression,
            branch_id=self._random_branch_id,
            sequence_id=self._random_sequence_id,
            last_op=Operation.FROM,
        )

    def sql(self, sqlQuery: str) -> "DataFrame":
        expression = sqlglot.parse_one(sqlQuery, read="spark")
        df = DataFrame(self, expression, branch_id=self._random_branch_id, sequence_id=self._random_sequence_id)
        if isinstance(expression, exp.Select):
            df = df._convert_leaf_to_cte()
        return df

    @property
    def _auto_incrementing_name(self) -> str:
        name = f"a{self.incrementing_id}"
        self.incrementing_id += 1
        return name

    @property
    def _random_name(self) -> str:
        return f"a{str(uuid.uuid4())[:8]}"

    @property
    def _random_branch_id(self):
        id = self._random_id
        self.known_branch_ids.add(id)
        return id

    @property
    def _random_sequence_id(self):
        id = self._random_id
        self.known_sequence_ids.add(id)
        return id

    @property
    def _random_id(self) -> str:
        id = f"a{str(uuid.uuid4())[:8]}"
        self.known_ids.add(id)
        return id

    @property
    def _join_hint_names(self) -> t.Set[str]:
        return {
            "BROADCAST",
            "MERGE",
            "SHUFFLE_HASH",
            "SHUFFLE_REPLICATE_NL"
        }

    def _add_alias_to_mapping(self, name: str, sequence_id: str):
        self.name_to_sequence_id_mapping[name].append(sequence_id)
