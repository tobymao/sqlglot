from __future__ import annotations

from sqlglot import generator


class SolrGenerator(generator.Generator):
    AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS
