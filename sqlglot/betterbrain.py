from dataclasses import dataclass
import typing as t
from sqlglot.tokens import TokenType

@dataclass
class TableIdentifier:
    name: str
    alias: t.Optional[str] = None

@dataclass
class Suggestion:
    suggestions: t.List[t.Union[TokenType, str]]
    table_ids: t.Optional[t.List[TableIdentifier]] = None

    def __eq__(self, __value: object) -> bool:
        return set(self.suggestions) == set(__value.suggestions) and sorted(self.table_ids if self.table_ids else [], key=lambda x: x.name) == sorted(__value.table_ids if __value.table_ids else [], key=lambda x: x.name)