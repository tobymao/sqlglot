from sqlglot.dialects.mysql import MySQL


class SingleStore(MySQL):
    FORCE_EARLY_ALIAS_REF_EXPANSION = True
    SUPPORTS_ORDER_BY_ALL = True
