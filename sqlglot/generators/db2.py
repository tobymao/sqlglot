from __future__ import annotations

from sqlglot import generator


class Db2(generator.Generator):
    AFTER_HAVING_MODIFIER_TRANSFORMS = {
        "cluster": lambda self, e: "",
        "distribute": lambda self, e: "",
        "sort": lambda self, e: "",
    }
