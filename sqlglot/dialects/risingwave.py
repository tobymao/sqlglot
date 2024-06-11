from sqlglot.dialects.postgres import Postgres


class RisingWave(Postgres):
    class Generator(Postgres.Generator):
        LOCKING_READS_SUPPORTED = False
