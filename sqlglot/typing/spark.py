from __future__ import annotations

from sqlglot import exp
from sqlglot.typing.spark2 import EXPRESSION_METADATA


EXPRESSION_METADATA = {
    **EXPRESSION_METADATA,
    **{
        exp_type: {"returns": exp.DType.BINARY}
        for exp_type in {
            exp.BitmapConstructAgg,
            exp.ToBinary,
        }
    },
    **{
        exp_type: {"returns": exp.DType.DATE}
        for exp_type in {
            exp.DateFromUnixDate,
            # 2-arg `date_add(startDate, numDays)` / `date_sub` are routed to
            # TsOrDsAdd by Hive/Spark parsers; both return DATE per the Spark
            # and Databricks contracts.
            exp.TsOrDsAdd,
        }
    },
    **{
        exp_type: {"returns": exp.DType.DOUBLE}
        for exp_type in {
            exp.Sec,
        }
    },
    **{
        exp_type: {"returns": exp.DType.VARCHAR}
        for exp_type in {
            exp.Collation,
            exp.CurrentTimezone,
            exp.Randstr,
            exp.ToChar,
        }
    },
    **{
        exp_type: {"annotator": lambda self, e: self._annotate_by_args(e, "this")}
        for exp_type in {
            exp.ArrayCompact,
            exp.ArrayInsert,
            exp.BitwiseAndAgg,
            exp.BitwiseOrAgg,
            exp.BitwiseXorAgg,
            exp.Left,
            exp.Overlay,
        }
    },
    exp.BitmapCount: {"returns": exp.DType.BIGINT},
    exp.Grouping: {"returns": exp.DType.TINYINT},
    exp.Localtimestamp: {"returns": exp.DType.TIMESTAMPNTZ},
}
