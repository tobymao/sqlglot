from __future__ import annotations

import typing as t
import uuid
from collections import defaultdict

import sqlglot
from sqlglot import expressions as exp
from sqlglot.dataframe.sql import functions as F
from sqlglot.dataframe.sql.dataframe import DataFrame
from sqlglot.dataframe.sql.readwriter import DataFrameReader
from sqlglot.dataframe.sql.types import StructType
from sqlglot.dataframe.sql.util import get_column_mapping_from_schema_input

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql._typing import ColumnLiterals, SchemaInput


class SparkSession:
    known_ids: t.ClassVar[t.Set[str]] = set()
    known_branch_ids: t.ClassVar[t.Set[str]] = set()
    known_sequence_ids: t.ClassVar[t.Set[str]] = set()
    name_to_sequence_id_mapping: t.ClassVar[t.Dict[str, t.List[str]]] = defaultdict(list)

    def __init__(self):
        self.incrementing_id = 1

    def __getattr__(self, name: str) -> SparkSession:
        return self

    def __call__(self, *args, **kwargs) -> SparkSession:
        return self

    @property
    def read(self) -> DataFrameReader:
        return DataFrameReader(self)

    def table(self, tableName: str) -> DataFrame:
        return self.read.table(tableName)

    def createDataFrame(
        self,
        data: t.Sequence[t.Union[t.Dict[str, ColumnLiterals], t.List[ColumnLiterals], t.Tuple]],
        schema: t.Optional[SchemaInput] = None,
        samplingRatio: t.Optional[float] = None,
        verifySchema: bool = False,
    ) -> DataFrame:
        from sqlglot.dataframe.sql.dataframe import DataFrame

        if samplingRatio is not None or verifySchema:
            raise NotImplementedError("Sampling Ratio and Verify Schema are not supported")
        if schema is not None and (
            not isinstance(schema, (StructType, str, list))
            or (isinstance(schema, list) and not isinstance(schema[0], str))
        ):
            raise NotImplementedError("Only schema of either list or string of list supported")
        if not data:
            raise ValueError("Must provide data to create into a DataFrame")

        column_mapping: t.Dict[str, t.Optional[str]]
        if schema is not None:
            column_mapping = get_column_mapping_from_schema_input(schema)
        elif isinstance(data[0], dict):
            column_mapping = {col_name.strip(): None for col_name in data[0]}
        else:
            column_mapping = {f"_{i}": None for i in range(1, len(data[0]) + 1)}

        data_expressions = [
            exp.Tuple(
                expressions=list(
                    map(
                        lambda x: F.lit(x).expression,
                        row if not isinstance(row, dict) else row.values(),
                    )
                )
            )
            for row in data
        ]

        sel_columns = [
            F.col(name).cast(data_type).alias(name).expression
            if data_type is not None
            else F.col(name).expression
            for name, data_type in column_mapping.items()
        ]

        select_kwargs = {
            "expressions": sel_columns,
            "from": exp.From(
                expressions=[
                    exp.Values(
                        expressions=data_expressions,
                        alias=exp.TableAlias(
                            this=exp.to_identifier(self._auto_incrementing_name),
                            columns=[exp.to_identifier(col_name) for col_name in column_mapping],
                        ),
                    ),
                ],
            ),
        }

        sel_expression = exp.Select(**select_kwargs)
        return DataFrame(self, sel_expression)

    def sql(self, sqlQuery: str) -> DataFrame:
        expression = sqlglot.parse_one(sqlQuery, read="spark")
        if isinstance(expression, exp.Select):
            df = DataFrame(self, expression)
            df = df._convert_leaf_to_cte()
        elif isinstance(expression, (exp.Create, exp.Insert)):
            select_expression = expression.expression.copy()
            if isinstance(expression, exp.Insert):
                select_expression.set("with", expression.args.get("with"))
                expression.set("with", None)
            del expression.args["expression"]
            df = DataFrame(self, select_expression, output_expression_container=expression)  # type: ignore
            df = df._convert_leaf_to_cte()
        else:
            raise ValueError(
                "Unknown expression type provided in the SQL. Please create an issue with the SQL."
            )
        return df

    @property
    def _auto_incrementing_name(self) -> str:
        name = f"a{self.incrementing_id}"
        self.incrementing_id += 1
        return name

    @property
    def _random_name(self) -> str:
        return "r" + uuid.uuid4().hex

    @property
    def _random_branch_id(self) -> str:
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
        id = self._random_name
        self.known_ids.add(id)
        return id

    @property
    def _join_hint_names(self) -> t.Set[str]:
        return {"BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"}

    def _add_alias_to_mapping(self, name: str, sequence_id: str):
        self.name_to_sequence_id_mapping[name].append(sequence_id)
