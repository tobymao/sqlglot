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

    def replace(self, to_replace: t.Union[bool, int, float, str, t.List, t.Dict],
                value: t.Optional[t.Union[bool, int, float, str, t.List]] = None,
                subset: t.Optional[t.Union[str, t.List[str]]] = None) -> "DataFrame":
        return self.df.replace(to_replace=to_replace, value=value, subset=subset)
