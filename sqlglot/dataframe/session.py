import typing as t
import uuid
from collections import defaultdict

from sqlglot.dataframe.dataframe_reader import DataFrameReader


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
