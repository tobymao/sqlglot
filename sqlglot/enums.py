from enum import Enum


class NullOrdering(Enum):
    NULLS_ARE_SMALL = 1
    NULLS_ARE_LARGE = 2
    NULLS_ARE_LAST = 3
