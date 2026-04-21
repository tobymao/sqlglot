"""Refleak detector for sqlglot[c].

Runs the unit-test suite N times against the compiled build, snapshotting
``gc.get_objects()`` type counts between runs and flagging types whose count
grows monotonically with positive slope. Also reports ``sys.gettotalrefcount``
drift (debug Python only) and RSS drift as coarse backstops.

Why three signals, in order of strength:

1. **Per-type object-count growth**. Works on every Python build. Catches
   leaks of gc-tracked objects — the class of leak mypyc refcount bugs
   typically produce, because mypyc-generated classes are proper PyObject
   types registered with the gc.
2. **``sys.gettotalrefcount`` drift**. Debug Python only (``--with-pydebug``).
   Catches leaks of non-gc-tracked objects (small ints, short strings,
   interned tuples) that signal #1 misses.
3. **RSS drift**. OS-level backstop for raw-malloc leaks that never show up
   in CPython bookkeeping at all (e.g. unreleased arenas, third-party C
   libraries).

The workload is the real unit test suite — same as ``make unitc``. Tests are
loaded once and re-run in-process each iteration; running them out-of-process
would reset the heap and hide the growth we're trying to see.

Usage:
    python -m tests.leakcheck                  # discover tests/ pattern test_*.py
    python -m tests.leakcheck --top tests/dialects
    python -m tests.leakcheck --pattern test_snowflake.py
    python -m tests.leakcheck --warmup 2 --samples 5 --verbose
"""

from __future__ import annotations

import argparse
import gc
import os
import resource
import sys
import unittest
from collections import Counter


# Built-in bookkeeping types whose counts fluctuate for reasons unrelated to
# the workload (stack frames come and go, tracebacks get created lazily,
# iterators are ephemeral). Ignoring them keeps the "Growing types" report
# focused on domain-meaningful types like Expression subclasses.
#
# unittest-machinery entries here: each TextTestRunner.run() call allocates a
# fresh result/runner/stream wrapper, and the suite's subtest tracking tracks
# new _SubTest/_OrderedChainMap per test. These grow linearly with iteration
# count purely because we re-run the suite, not because sqlglot leaks.
_NOISY = frozenset(
    {
        "frame",
        "traceback",
        "cell",
        "function",
        "method",
        "builtin_function_or_method",
        "method_descriptor",
        "wrapper_descriptor",
        "member_descriptor",
        "getset_descriptor",
        "classmethod_descriptor",
        "list_iterator",
        "tuple_iterator",
        "dict_keyiterator",
        "dict_valueiterator",
        "dict_itemiterator",
        "set_iterator",
        # unittest internals re-allocated per run
        "TestResult",
        "TextTestResult",
        "TextTestRunner",
        "_WritelnDecorator",
        "_SubTest",
        "_OrderedChainMap",
        "_GeneratorContextManager",
        "_Outcome",
        "generator",
        "ReferenceType",
    }
)

# Structural containers are load-bearing but legitimately grow from lazy
# caches (dialect registries, normalized-name caches, etc. keep filling).
# Treat them with a higher delta threshold so first-run cache warmup doesn't
# flip the verdict; a real leak still wins because its slope is linear and
# unbounded.
_STRUCTURAL = frozenset({"dict", "list", "tuple", "set", "frozenset"})


# Opened once at import rather than per-iteration to avoid FD churn and to
# keep it out of the per-sample object count.
_NULL_STREAM = open(os.devnull, "w")


def _maxrss_kb() -> int:
    """Return current resident-set size in KB, normalized across OSes.

    ``resource.ru_maxrss`` is specified in KB on Linux but in bytes on macOS
    (and BSD). Normalizing here lets the RSS-drift threshold be a single
    value regardless of platform.
    """
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return raw // 1024 if sys.platform == "darwin" else raw


def _snapshot():
    """Snapshot the live Python heap: per-type counts, totalrefcount, RSS.

    Returns ``(counts, totalrefcount, maxrss_kb)`` where ``counts`` is
    ``{type_name: count}`` over every gc-tracked object alive right now.

    **Why two ``gc.collect`` calls.** A single collection can leave work for
    a second pass in two situations:

    * Weakref callbacks fire during collection and can drop the last strong
      reference to an object; that object is then only freed on the next
      pass.
    * Objects with finalizers (``__del__``) are moved to ``gc.garbage``-like
      handling per PEP 442 and may only be reclaimed in a subsequent cycle.

    Without the second pass the snapshot would include objects that are
    logically dead but not yet freed — iteration-to-iteration that "live"
    set fluctuates, producing non-monotonic noise that masks the linear
    leak signal we're trying to isolate. Two collects makes the heap state
    stable enough that the growth we measure reflects real retention.

    **Why ``sys.gettotalrefcount`` is probed via ``getattr``.** It only
    exists on debug Python builds; on stock CPython the symbol is absent
    and ``getattr(..., None)()`` returns ``None`` so the caller can treat
    the signal as "unavailable" rather than raise.
    """
    gc.collect()
    gc.collect()
    counts: Counter[str] = Counter()
    for obj in gc.get_objects():
        counts[type(obj).__name__] += 1
    rc = getattr(sys, "gettotalrefcount", lambda: None)()
    return dict(counts), rc, _maxrss_kb()


def _slope(values):
    """Least-squares slope of ``values`` against their index.

    Used as the primary leak-signal metric rather than ``values[-1] -
    values[0]`` because:

    * A per-iteration-constant leak produces a straight line (``+k, +k,
      +k, ...``); least-squares slope recovers ``k`` exactly.
    * Averaging across all samples is robust to a single noisy outlier
      that a first-vs-last delta would overweight.
    * A plateauing cache (``+k, +k, 0, 0, 0, ...``) produces a lower
      slope than a sustained leak of the same magnitude, so the
      threshold-based check naturally prefers the latter.
    """
    n = len(values)
    mean_x = (n - 1) / 2
    mean_y = sum(values) / n
    num = sum((i - mean_x) * (v - mean_y) for i, v in enumerate(values))
    den = sum((i - mean_x) ** 2 for i in range(n))
    return num / den if den else 0.0


def check_leaks(
    workload,
    *,
    warmup=1,
    samples=3,
    slope_min=0.5,
    # Full-suite workloads have a small baseline growth from test fixtures
    # that register module-level state (e.g. tests that declare new Dialect
    # subclasses). We observe ~3 such allocations per iteration in a clean
    # build, so delta_min=10 puts real leaks (always far louder) above the
    # floor while absorbing fixture noise.
    delta_min=10,
    # Structural containers (dict/list/tuple/set/frozenset) get a higher bar
    # because the full unit-test suite legitimately allocates thousands of
    # them per run (TestResult fields, collected-tests lists, etc.). A real
    # leak still wins — the post1 property-getter bug produced dict delta
    # ~750k per 3 samples, two orders of magnitude above this threshold.
    delta_min_structural=10000,
    # ``ru_maxrss`` is a peak high-water mark, not current RSS — once the
    # process touches a page, the counter never goes back down. Running the
    # full unit suite repeatedly causes significant variance from arena
    # fragmentation and test-specific working sets (we've observed 20 MB on
    # one run and 470 MB on the next). Treat RSS as a coarse backstop for
    # truly pathological growth rather than a precise gate.
    rss_threshold_kb=1_000_000,
    refcount_threshold=2000,
):
    """Run ``workload`` repeatedly and return growth findings.

    Execution model:

    * **Warmup phase** — ``warmup`` runs without sampling, letting lazy
      module-level state converge (dialect class registries, tokenizer
      trie construction, normalized-name caches, etc.). Without warmup
      the first few "samples" would conflate first-use cache population
      with real leak growth.
    * **Sampling phase** — ``samples`` more runs, snapshotting the heap
      both before the first run and after each subsequent one. The
      resulting per-type series is what the detection logic operates on.

    A type is flagged if **all three** hold:

    1. Its count is strictly non-decreasing across samples. Any
       oscillation rules it out as noise or churn — caches sometimes
       shrink briefly under GC, real leaks never do.
    2. Its least-squares slope is at least ``slope_min``. A constant
       slope is the fingerprint of a per-call leak of a fixed number
       of objects.
    3. Its total delta is at least ``delta_min`` (or
       ``delta_min_structural`` for dict/list/tuple/set/frozenset, which
       grow legitimately during cache warmup and need a stricter bar).

    Also computed:

    * ``rc_delta`` — ``sys.gettotalrefcount`` drift from first to last
      snapshot. ``None`` on release Python builds.
    * ``rss_delta`` — process RSS growth in KB.

    The overall verdict is ``leaked = bool(reasons)`` where ``reasons``
    aggregates any of: growing types found, refcount drift above
    ``refcount_threshold``, RSS drift above ``rss_threshold_kb``.

    Returns ``(growing, rc_delta, rss_delta_kb, reasons)``. ``growing``
    is ``[(type_name, slope, delta, values), ...]`` sorted by slope
    descending; ``reasons`` is a list of short strings (empty ⇒ clean).
    """
    for _ in range(warmup):
        workload()

    snaps = [_snapshot()]
    for _ in range(samples):
        workload()
        snaps.append(_snapshot())

    # Union of every type that appeared in any snapshot — a type can first
    # appear mid-run (lazy import), so we can't just take the first snapshot.
    names: set[str] = set()
    for counts, _, _ in snaps:
        names.update(counts)

    growing = []
    for tname in sorted(names - _NOISY):
        vals = [c.get(tname, 0) for c, _, _ in snaps]
        if not all(vals[i + 1] >= vals[i] for i in range(len(vals) - 1)):
            continue
        slope = _slope(vals)
        delta = vals[-1] - vals[0]
        thresh = delta_min_structural if tname in _STRUCTURAL else delta_min
        if slope >= slope_min and delta >= thresh:
            growing.append((tname, slope, delta, vals))
    # Steepest slopes first so truncated reports keep the most-interesting rows.
    growing.sort(key=lambda g: -g[1])

    rc0, rc1 = snaps[0][1], snaps[-1][1]
    rc_delta = rc1 - rc0 if rc0 is not None else None
    rss_delta = snaps[-1][2] - snaps[0][2]

    reasons = []
    if growing:
        reasons.append(f"{len(growing)} type(s) growing")
    if rc_delta is not None and rc_delta >= refcount_threshold:
        reasons.append(f"totalrefcount +{rc_delta}")
    if rss_delta >= rss_threshold_kb:
        reasons.append(f"RSS +{rss_delta} KB")

    return growing, rc_delta, rss_delta, reasons


def _print_report(name, growing, rc_delta, rss_delta, reasons, *, verbose=False):
    """Format the tuple returned by ``check_leaks`` to stdout.

    Default truncates at 15 growing types; ``verbose=True`` prints all of
    them. Each row shows the full per-iteration count trail so the reader
    can eyeball monotonicity without re-running.
    """
    status = "LEAK" if reasons else " OK "
    header = f"[{status}] {name}"
    if reasons:
        header += "  — " + "; ".join(reasons)
    print(header)
    if growing:
        print("  Growing object types:")
        shown = growing if verbose else growing[:15]
        for tname, slope, delta, vals in shown:
            trail = " ".join(str(v) for v in vals)
            print(f"    {tname:<36} slope={slope:+6.2f}/iter  delta={delta:+d}   [{trail}]")
        if not verbose and len(growing) > len(shown):
            print(f"    ... and {len(growing) - len(shown)} more")
    if rc_delta is not None:
        print(f"  sys.gettotalrefcount delta: {rc_delta:+d}")
    print(f"  maxrss delta: {rss_delta:+d} KB")


def _make_workload(top, pattern):
    """Return a zero-arg callable that re-runs the unit test suite.

    The suite is discovered **once** so the ``type`` objects for each
    TestCase subclass are created exactly one time; a fresh discover per
    iteration would inflate the ``type`` count and produce false-positive
    growth on that row.

    ``unittest.BaseTestSuite._cleanup`` defaults to ``True``: after each
    test runs, the suite sets its slot to ``None`` to drop the reference
    (a memory optimisation for normal single-shot runs). That would leave
    the second iteration running zero tests. We disable it class-wide so
    the same suite object can be replayed N times.
    """
    unittest.BaseTestSuite._cleanup = False
    suite = unittest.TestLoader().discover(top, pattern=pattern)

    def run():
        unittest.TextTestRunner(stream=_NULL_STREAM, verbosity=0).run(suite)

    return run


def main():
    """CLI entry point. Exits 1 if a leak was flagged, else 0."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--top", default="tests", help="Test discovery root (default: tests)")
    p.add_argument("--pattern", default="test_*.py", help="Test file glob (default: test_*.py)")
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--samples", type=int, default=3)
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    # Integration tests hit external DBs and are flaky/slow — unfit for a
    # harness that runs the suite multiple times. ``setdefault`` lets the
    # caller opt in explicitly by pre-exporting ``SKIP_INTEGRATION=0``.
    os.environ.setdefault("SKIP_INTEGRATION", "1")

    # Detect whether we're running against the compiled extension. The .so
    # is resolved by the normal import machinery (it wins over the co-located
    # .py), so probing any mypyc-compiled module's __file__ is sufficient.
    import sqlglot.expressions.core as _ec

    compiled = _ec.__file__.endswith(".so")
    print(f"build: {'sqlglotc (compiled)' if compiled else 'pure python'}")
    print(f"debug Python: {'yes' if hasattr(sys, 'gettotalrefcount') else 'no'}")
    print(f"workload: unittest discover {args.top} pattern {args.pattern}")
    print(f"warmup={args.warmup} samples={args.samples}")
    print()

    workload = _make_workload(args.top, args.pattern)
    growing, rc_delta, rss_delta, reasons = check_leaks(
        workload, warmup=args.warmup, samples=args.samples
    )
    _print_report(
        f"{args.top} ({args.pattern})",
        growing,
        rc_delta,
        rss_delta,
        reasons,
        verbose=args.verbose,
    )
    return 1 if reasons else 0


if __name__ == "__main__":
    raise SystemExit(main())
