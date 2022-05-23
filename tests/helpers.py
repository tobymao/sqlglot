import os

FILE_DIR = os.path.dirname(__file__)
FIXTURES_DIR = os.path.join(FILE_DIR, "fixtures")


def load_sql_fixtures(filename):
    with open(os.path.join(FIXTURES_DIR, filename), encoding="utf-8") as f:
        for sql in f:
            yield sql


def load_sql_fixture_pairs(filename):
    with open(os.path.join(FIXTURES_DIR, filename), encoding="utf-8") as f:
        lines = [line for line in f.readlines() if line and not line.startswith("--")]
        statements = "".join(lines).split(";")

        size = len(statements)

        for i in range(0, size, 2):
            if i + 1 < size:
                sql = statements[i].strip()
                expected = statements[i + 1].strip()
                yield sql, expected
