"""
Tokenizer-only profiling script.

Usage:
    python -m benchmarks.profile_tokenizer
    python -m benchmarks.profile_tokenizer --callgrind   # for kcachegrind
"""

import cProfile
import io
import pstats
import sys
import time

import sqlglot.tokenizer_core as _tc

IS_COMPILED = _tc.__file__.endswith(".so")
print(f"tokenizer_core: {'compiled (.so)' if IS_COMPILED else 'pure Python'}")
print(f"Python: {sys.version.split()[0]}")
print()

from sqlglot.tokens import Tokenizer

# ── SQL fixtures ──────────────────────────────────────────────────────────────

SHORT = "SELECT 1 AS a, CASE WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 3 END AS b, c FROM x"

LONG = """\
SELECT
  "e"."employee_id" AS "Employee #",
  "e"."first_name" || ' ' || "e"."last_name" AS "Name",
  "e"."email" AS "Email",
  "e"."phone_number" AS "Phone",
  TO_CHAR("e"."hire_date", 'MM/DD/YYYY') AS "Hire Date",
  TO_CHAR("e"."salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS "Salary",
  "e"."commission_pct" AS "Commission %",
  'works as ' || "j"."job_title" || ' in ' || "d"."department_name" ||
  ' department (manager: ' || "dm"."first_name" || ' ' || "dm"."last_name" ||
  ') and immediate supervisor: ' || "m"."first_name" || ' ' || "m"."last_name" AS "Current Job",
  TO_CHAR("j"."min_salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') ||
  ' - ' || TO_CHAR("j"."max_salary", 'L99G999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS "Current Salary",
  "l"."street_address" || ', ' || "l"."postal_code" || ', ' || "l"."city" || ', ' ||
  "l"."state_province" || ', ' || "c"."country_name" || ' (' || "r"."region_name" || ')' AS "Location",
  "jh"."job_id" AS "History Job ID",
  'worked from ' || TO_CHAR("jh"."start_date", 'MM/DD/YYYY') || ' to ' ||
  TO_CHAR("jh"."end_date", 'MM/DD/YYYY') || ' as ' || "jj"."job_title" ||
  ' in ' || "dd"."department_name" || ' department' AS "History Job Title",
  CASE WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 WHEN 4 THEN 4 WHEN 5 THEN 5
       ELSE a(b(c + 1 * 3 % 4)) END
FROM "employees" AS e
JOIN "jobs" AS j ON "e"."job_id" = "j"."job_id"
LEFT JOIN "employees" AS m ON "e"."manager_id" = "m"."employee_id"
LEFT JOIN "departments" AS d ON "d"."department_id" = "e"."department_id"
LEFT JOIN "employees" AS dm ON "d"."manager_id" = "dm"."employee_id"
LEFT JOIN "locations" AS l ON "e"."location_id" = "l"."location_id"
LEFT JOIN "countries" AS c ON "l"."country_id" = "c"."country_id"
LEFT JOIN "regions" AS r ON "c"."region_id" = "r"."region_id"
LEFT JOIN "job_history" AS jh ON "e"."employee_id" = "jh"."employee_id"
LEFT JOIN "jobs" AS jj ON "jh"."job_id" = "jj"."job_id"
LEFT JOIN "departments" AS dd ON "jh"."department_id" = "dd"."department_id"
LEFT JOIN "employees" AS djm ON "dd"."manager_id" = "djm"."employee_id"
WHERE "e"."employee_id" < 200
ORDER BY "e"."employee_id\""""

CRAZY = (
    "SELECT 1+"
    + "+".join(str(i) for i in range(500))
    + " AS a, 2*"
    + "*".join(str(i) for i in range(500))
    + " AS b FROM x"
)

TPCH = None
try:
    with open("tests/fixtures/optimizer/tpc-h/tpc-h.sql") as f:
        TPCH = f.read()
except FileNotFoundError:
    pass

# ── Timing benchmark ──────────────────────────────────────────────────────────

tok = Tokenizer()

CASES: list[tuple[str, str, int]] = [
    ("short", SHORT, 50_000),
    ("long", LONG, 10_000),
    ("crazy", CRAZY, 2_000),
]
if TPCH:
    CASES.append(("tpch", TPCH, 500))

print("=== Wall-clock timing (tokenize only) ===")
print(f"{'name':8}  {'chars':>7}  {'tokens':>7}  {'µs/call':>9}  {'MB/s':>7}")
print("-" * 50)

for name, sql, n in CASES:
    # warmup
    for _ in range(max(n // 10, 50)):
        tok.tokenize(sql)

    t0 = time.perf_counter()
    for _ in range(n):
        tok.tokenize(sql)
    elapsed = time.perf_counter() - t0

    ntokens = len(tok.tokenize(sql))
    us = elapsed / n * 1e6
    mbps = len(sql.encode()) / (elapsed / n) / 1e6
    print(f"{name:8}  {len(sql):>7}  {ntokens:>7}  {us:>9.1f}  {mbps:>7.1f}")

# ── cProfile: tottime (self time) ─────────────────────────────────────────────

print()
print("=== cProfile: top self-time, long SQL (50k iters) ===")

pr = cProfile.Profile()
pr.enable()
for _ in range(50_000):
    tok.tokenize(LONG)
pr.disable()

s = io.StringIO()
ps = pstats.Stats(pr, stream=s).sort_stats("tottime")
ps.print_stats(25)
print(s.getvalue())

# ── cProfile: call counts (cumtime) ──────────────────────────────────────────

print("=== cProfile: top cumtime, crazy SQL (5k iters) ===")

pr2 = cProfile.Profile()
pr2.enable()
for _ in range(5_000):
    tok.tokenize(CRAZY)
pr2.disable()

s2 = io.StringIO()
ps2 = pstats.Stats(pr2, stream=s2).sort_stats("cumtime")
ps2.print_stats(20)
print(s2.getvalue())

# ── Per-token cost breakdown ──────────────────────────────────────────────────

print("=== Per-token cost breakdown ===")
for name, sql, n in CASES:
    ntokens = len(tok.tokenize(sql))
    t0 = time.perf_counter()
    for _ in range(n):
        tok.tokenize(sql)
    elapsed = time.perf_counter() - t0
    ns_per_tok = elapsed / n / ntokens * 1e9
    print(f"  {name:8}: {ns_per_tok:6.1f} ns/token  ({ntokens} tokens)")
