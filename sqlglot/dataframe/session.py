import uuid
from collections import defaultdict

from sqlglot.dataframe.dataframe_reader import DataFrameReader


class SparkSession:
    def __init__(self):
        self.name_to_sequence_id_mapping = defaultdict(list)
        self.known_ids = set()

    @property
    def read(self) -> "DataFrameReader":
        return DataFrameReader(self)

    @classmethod
    @property
    def random_name(cls) -> str:
        return f"a{str(uuid.uuid4())[:8]}"

    @property
    def random_id(self) -> str:
        id = f"a{str(uuid.uuid4())[:8]}"
        self.known_ids.add(id)
        return id

    def add_alias_to_mapping(self, name: str, sequence_id: str):
        self.name_to_sequence_id_mapping[name].append(sequence_id)
