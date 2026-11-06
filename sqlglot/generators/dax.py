from __future__ import annotations

from sqlglot import generator


class DAXGenerator(generator.Generator):
    SUPPORTS_ARRAY_SLICING = False
    AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS
