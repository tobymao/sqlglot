import os

FILE_DIR = os.path.dirname(__file__)
FIXTURES_DIR = os.path.join(FILE_DIR, "fixtures")


def _filter_comments(s):
    return "\n".join(
        [line for line in s.splitlines() if line and not line.startswith("--")]
    )


def load_sql_fixtures(filename):
    with open(os.path.join(FIXTURES_DIR, filename), encoding="utf-8") as f:
        for sql in _filter_comments(f.read()).splitlines():
            yield sql


def load_sql_fixture_pairs(filename):
    with open(os.path.join(FIXTURES_DIR, filename), encoding="utf-8") as f:
        statements = _filter_comments(f.read()).split(";")

        size = len(statements)

        for i in range(0, size, 2):
            if i + 1 < size:
                sql = statements[i].strip()
                expected = statements[i + 1].strip()
                yield sql, expected
