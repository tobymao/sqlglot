import typing as t

from sqlglot.dataframe.column import Column


def ensure_strings(args: t.List[t.Union[str, Column]]) -> t.List[str]:
    return [x.expression if isinstance(x, Column) else x for x in args]


def ensure_columns(args: t.List[t.Union[str, Column]]) -> t.List[Column]:
    return [Column(x) if not isinstance(x, Column) else x for x in args]


def convert_join_type(join_type: str) -> str:
    if join_type == "left_outer":
        return "left outer"
    elif join_type == "inner":
        return "inner"
    raise RuntimeError(f"Unimplemented join type: {join_type}")
