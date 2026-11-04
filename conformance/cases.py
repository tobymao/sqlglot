"""
Loads conformance cases from .sql files.

A case file holds statements separated by a `;` at the end of a line. Each statement may be
preceded by `# key: value` metadata lines; `--` comment lines between statements are ignored.
The source dialect defaults to the name of the file's directory (cases/<dialect>/<topic>.sql).

Metadata keys:
    read     source dialect (defaults to the directory name)
    write    comma-separated transpilation targets, or "none" (defaults to every other engine)
    ordered  "true" / "false": compare rows in order (defaults to whether the query has a
             top-level ORDER BY)
    name     stable, readable case name (defaults to a hash of the SQL)
    rel_tol  relative tolerance for floating point comparisons
    skip     reason for skipping the case
"""

from __future__ import annotations

import hashlib
import typing as t
from dataclasses import dataclass
from pathlib import Path

from conformance.dataset.tpch import TPCH_DIR

CASES_DIR = Path(__file__).resolve().parent / "cases"
META_KEYS = {"read", "write", "ordered", "name", "rel_tol", "skip"}


class CaseError(Exception):
    pass


@dataclass
class Case:
    id: str
    sql: str
    read: str
    # None means every engine other than `read`
    write: tuple[str, ...] | None = None
    # None means: inferred from the presence of a top-level ORDER BY
    ordered: bool | None = None
    rel_tol: float | None = None
    skip: str | None = None
    file: str = ""
    line: int = 0

    @property
    def location(self) -> str:
        return f"{self.file}:{self.line}" if self.line else self.file


def load_cases(paths: t.Iterable[str | Path] | None = None) -> list[Case]:
    """Loads cases from the given files or directories, or from every bundled source."""
    cases: list[Case] = []
    if paths is None:
        files = sorted(CASES_DIR.rglob("*.sql"))
        cases.extend(tpch_cases())
    else:
        files = []
        for path in map(Path, paths):
            files.extend(sorted(path.rglob("*.sql")) if path.is_dir() else [path])

    for file in files:
        cases.extend(parse_case_file(file))

    seen: dict[str, Case] = {}
    for case in cases:
        if case.id in seen:
            raise CaseError(
                f"duplicate case {case.id} at {case.location} and {seen[case.id].location}"
            )
        seen[case.id] = case

    return sorted(cases, key=lambda case: case.id)


def parse_case_file(path: Path) -> list[Case]:
    path = path.resolve()
    try:
        prefix = path.relative_to(CASES_DIR).with_suffix("").as_posix()
        display = path.relative_to(CASES_DIR.parent.parent).as_posix()
    except ValueError:
        prefix = path.stem
        display = str(path)

    cases = []
    meta: dict[str, str] = {}
    lines: list[str] = []
    start = 0

    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines() + [";"], 1):
        stripped = line.strip()
        if not lines:
            if not stripped or stripped.startswith("--") or stripped == ";":
                continue
            if stripped.startswith("#"):
                key, sep, value = stripped[1:].partition(":")
                key = key.strip().lower()
                if not sep or key not in META_KEYS:
                    raise CaseError(
                        f"{display}:{number}: expected '# key: value' with a key in {sorted(META_KEYS)}"
                    )
                meta[key] = value.strip()
                continue
            start = number

        lines.append(line)
        if stripped.endswith(";"):
            sql = "\n".join(lines).strip()[:-1].strip()
            if sql:
                cases.append(_make_case(sql, meta, prefix, display, start, path))
            lines, meta = [], {}

    return cases


def tpch_cases() -> list[Case]:
    """
    The TPC-H queries from the optimizer fixtures, run from both Postgres and Spark. The fixture is
    written in sqlglot's own dialect, so each query is first transpiled into the source dialect.
    That's sound because the source engine computes the ground truth of whatever SQL it is given.
    """
    import sqlglot

    path = TPCH_DIR / "tpc-h.sql"
    text = "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines() if not line.startswith("--")
    )
    # The fixture alternates between each query and its optimized form
    statements = [s.strip() for s in text.split(";") if s.strip()]
    queries = statements[0::2]

    return [
        Case(
            id=f"tpch/{read}:q{i:02d}",
            sql=sqlglot.transpile(query, write=read, pretty=True)[0],
            read=read,
            file=str(path.relative_to(CASES_DIR.parent.parent)),
        )
        for read in ("postgres", "spark")
        for i, query in enumerate(queries, 1)
    ]


def _make_case(
    sql: str, meta: dict[str, str], prefix: str, display: str, line: int, path: Path
) -> Case:
    read = meta.get("read") or (path.parent.name if path.parent.parent == CASES_DIR else "")
    if not read:
        raise CaseError(f"{display}:{line}: missing '# read: <dialect>'")

    write: tuple[str, ...] | None = None
    if "write" in meta:
        value = meta["write"].lower()
        write = () if value == "none" else tuple(w.strip() for w in value.split(",") if w.strip())

    ordered = None
    if "ordered" in meta:
        ordered = meta["ordered"].lower() in ("1", "true", "yes")

    name = meta.get("name")
    suffix = f":{name}" if name else "#" + hashlib.sha1(sql.encode()).hexdigest()[:8]

    return Case(
        id=prefix + suffix,
        sql=sql,
        read=read,
        write=write,
        ordered=ordered,
        rel_tol=float(meta["rel_tol"]) if "rel_tol" in meta else None,
        skip=meta.get("skip"),
        file=display,
        line=line,
    )
