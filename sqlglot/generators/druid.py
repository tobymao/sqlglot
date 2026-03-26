from __future__ import annotations

import typing as t

from sqlglot import exp, generator
from sqlglot.dialects.dialect import rename_func


class DruidGenerator(generator.Generator):
    SELECT_KINDS: t.Tuple[str, ...] = ()
    TRY_SUPPORTED = False
    SUPPORTS_UESCAPE = False
    SUPPORTS_DECODE_CASE = False

    AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS

    # https://druid.apache.org/docs/latest/querying/sql-data-types/
    TYPE_MAPPING = {
        **generator.Generator.TYPE_MAPPING,
        exp.DType.NCHAR: "STRING",
        exp.DType.NVARCHAR: "STRING",
        exp.DType.TEXT: "STRING",
        exp.DType.UUID: "STRING",
    }

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
        exp.Mod: rename_func("MOD"),
        exp.Array: lambda self, e: f"ARRAY[{self.expressions(e)}]",
    }
