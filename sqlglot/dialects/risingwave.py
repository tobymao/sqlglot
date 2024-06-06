from sqlglot.dialects.postgres import Postgres


class RisingWave(Postgres):
    class Tokenizer(Postgres.Tokenizer):
        pass

    class Parser(Postgres.Parser):
        pass

    class Generator(Postgres.Generator):
        LOCKING_READS_SUPPORTED = False
