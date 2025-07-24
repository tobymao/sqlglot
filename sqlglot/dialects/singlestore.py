from sqlglot.dialects.mysql import MySQL


class SingleStore(MySQL):
    SUPPORTS_ORDER_BY_ALL = True
