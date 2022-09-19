import typing as t
import uuid
from collections import defaultdict

from sqlglot import expressions as exp
from sqlglot.dataframe.dataframe_reader import DataFrameReader
from sqlglot.dataframe import functions as F
from sqlglot.dataframe.operations import Operation

if t.TYPE_CHECKING:
    from sqlglot.dataframe.dataframe import DataFrame


class SparkSession:
    def __init__(self):
        self.name_to_sequence_id_mapping = defaultdict(list)
        self.known_ids = set()
        self.known_branch_ids = set()
        self.known_sequence_ids = set()

    @property
    def read(self) -> "DataFrameReader":
        return DataFrameReader(self)

    @classmethod
    @property
    def random_name(cls) -> str:
        return f"a{str(uuid.uuid4())[:8]}"

    @property
    def random_branch_id(self):
        id = self._random_id
        self.known_branch_ids.add(id)
        return id

    @property
    def random_sequence_id(self):
        id = self._random_id
        self.known_sequence_ids.add(id)
        return id

    @property
    def _random_id(self) -> str:
        id = f"a{str(uuid.uuid4())[:8]}"
        self.known_ids.add(id)
        return id

    @property
    def join_hint_names(self) -> t.Set[str]:
        return {
            "BROADCAST",
            "MERGE",
            "SHUFFLE_HASH",
            "SHUFFLE_REPLICATE_NL"
        }

    def add_alias_to_mapping(self, name: str, sequence_id: str):
        self.name_to_sequence_id_mapping[name].append(sequence_id)

    def createDataFrame(
            self,
            data: t.Iterable[t.Union[t.Dict[str, t.Any], t.Iterable[t.Any]]],
            schema: t.Optional[t.Union[str, t.List[str]]] = None,
            samplingRatio: t.Optional[float] = None,
            verifySchema: bool = False,
    ) -> "DataFrame":
        from sqlglot.dataframe.dataframe import DataFrame

        if samplingRatio is not None or verifySchema:
            raise NotImplementedError("Sampling Ration and Verify Schema are not supported")
        if schema is not None and (not isinstance(schema, (str, list)) or (isinstance(schema, list) and not isinstance(schema[0], str))):
            raise NotImplementedError("Only schema of either list or string of list supported")

        data_expressions = [
            exp.Tuple(
                expressions=list(map(lambda x: F.lit(x).expression, row if not isinstance(row, dict) else row.values()))
            )
            for row in data
        ]

        if schema is not None:
            if isinstance(schema, str):
                col_name_type_strs = [x.strip() for x in schema.split(",")]
                col_name_type_pairs = [(name_type_str.split(':')[0].strip(), name_type_str.split(':')[1].strip()) for name_type_str in col_name_type_strs]
            else:
                col_name_type_pairs = [(x.strip(), None) for x in schema]
        elif isinstance(data[0], dict):
            col_name_type_pairs = [(col_name.strip(), None) for col_name in data[0].keys()]
        else:
            col_name_type_pairs = [(f"_{i}", None) for i in range(1, len(data_expressions[0].expressions) + 1)]

        sel_columns = [
            F.col(name).cast(data_type).alias(name).expression if data_type is not None else F.col(name).expression
            for name, data_type in col_name_type_pairs
        ]

        select_kwargs = {
            'expressions': sel_columns,
            'from': exp.From(
                expressions=[exp.Subquery(
                    this=exp.Values(
                        expressions=data_expressions
                    ),
                    alias=exp.TableAlias(
                        this=exp.Identifier(this='tab'),
                        columns=[exp.Identifier(this=col_name) for col_name, _ in col_name_type_pairs]
                    )
                )]
            )
        }

        sel_expression = exp.Select(**select_kwargs)
        return DataFrame(
            spark=self,
            expression=sel_expression,
            branch_id=self.random_branch_id,
            sequence_id=self.random_sequence_id,
            last_op=Operation.FROM,
        )
