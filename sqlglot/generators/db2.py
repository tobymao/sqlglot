from __future__ import annotations

from sqlglot import exp, generator


class Db2(generator.Generator):
    AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS

    TYPE_MAPPING = {
        **generator.Generator.TYPE_MAPPING,
        exp.DType.NCHAR: "NCHAR",
        exp.DType.NVARCHAR: "NVARCHAR",
    }
