import typing as t

if t.TYPE_CHECKING:
    from sqlglot.dataframe.dataframe import DataFrame


class DataFrameNaFunctions:
    def __init__(self, df: "DataFrame"):
        self.df = df

    def drop(self, how: str = "any", thresh: t.Optional[int] = None,
             subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None) -> "DataFrame":
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    def fill(self,
             value: t.Union[int, bool, float, str, t.Dict[str, t.Any]],
             subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None) -> "DataFrame":
        return self.df.fillna(value=value, subset=subset)
