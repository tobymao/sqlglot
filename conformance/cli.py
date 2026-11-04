"""Command line interface: python -m conformance --help"""

from __future__ import annotations

import argparse
import sys
import time
import typing as t
from pathlib import Path

from conformance import docker
from conformance.cases import Case, CaseError, load_cases
from conformance.dataset.loader import DatasetError, ensure_dataset
from conformance.engines import ENGINES, Engine, EngineUnavailable

if t.TYPE_CHECKING:
    from conformance.runner import BaselineDiff, CaseResult, Leg

PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_BASELINE = PACKAGE_DIR / "baseline.json"
DEFAULT_REPORT = PACKAGE_DIR / ".reports" / "latest.json"
STARTUP_TIMEOUT_SECONDS = 180


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except (docker.DockerError, EngineUnavailable, DatasetError, CaseError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m conformance",
        description="Checks sqlglot dialects by running queries and their transpilations on real engines.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    def add_engines(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--engines",
            type=_names,
            default=list(ENGINES),
            help=f"comma-separated engines to use (default: {','.join(ENGINES)})",
        )

    up = commands.add_parser("up", help="start the engines and load the dataset")
    add_engines(up)
    up.set_defaults(handler=_up)

    down = commands.add_parser("down", help="stop and remove the bundled engines")
    down.set_defaults(handler=_down)

    status = commands.add_parser("status", help="show whether the engines are up")
    add_engines(status)
    status.set_defaults(handler=_status)

    load = commands.add_parser(
        "load", help="load the dataset (only changed tables, unless --force)"
    )
    add_engines(load)
    load.add_argument("--force", action="store_true", help="reload every table")
    load.set_defaults(handler=_load)

    list_ = commands.add_parser("list", help="list cases")
    _add_selection(list_)
    list_.set_defaults(handler=_list)

    run = commands.add_parser("run", help="run cases")
    add_engines(run)
    _add_selection(run)
    run.add_argument(
        "--sql", help="run a single ad hoc query instead of the case files (needs --read)"
    )
    run.add_argument("--timezone", default="UTC", help="session time zone for every engine")
    run.add_argument(
        "--baseline",
        type=Path,
        default=DEFAULT_BASELINE,
        help="known failures (default: %(default)s)",
    )
    run.add_argument("--no-baseline", action="store_true", help="report every failure as new")
    run.add_argument(
        "--update-baseline", action="store_true", help="record the current failures in the baseline"
    )
    run.add_argument("--report", type=Path, default=DEFAULT_REPORT, help="JSON report path")
    run.add_argument("--show-rows", action="store_true", help="print result rows of failing legs")
    run.add_argument(
        "--all", action="store_true", help="print known failures too, not just new ones"
    )
    run.add_argument("-v", "--verbose", action="store_true", help="print passing legs and warnings")
    run.add_argument("--no-start", action="store_true", help="don't start engines that are down")
    run.set_defaults(handler=_run)

    return parser


def _add_selection(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "-k",
        dest="patterns",
        action="append",
        default=[],
        help="only cases whose id or file contains this text (repeatable)",
    )
    command.add_argument("--read", help="only cases whose source is this dialect")
    command.add_argument("--write", type=_names, help="only transpile to these dialects")
    command.add_argument(
        "--cases",
        dest="case_paths",
        action="append",
        type=Path,
        help="case file or directory to use instead of the bundled cases (repeatable)",
    )


def _names(value: str) -> list[str]:
    names = [name.strip() for name in value.split(",") if name.strip()]
    unknown = sorted(set(names) - set(ENGINES))
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown engines {unknown}, expected some of {list(ENGINES)}"
        )
    return names


def _up(args: argparse.Namespace) -> int:
    _start_engines(args.engines, "UTC", start=True)
    return 0


def _down(args: argparse.Namespace) -> int:
    docker.down()
    return 0


def _status(args: argparse.Namespace) -> int:
    if not all(ENGINES[name]().external for name in args.engines):
        docker.ps()
    for name in args.engines:
        engine = ENGINES[name]()
        state = f"up, version {engine.version()}" if engine.ping() else "down"
        print(f"{name}: {state}{' (external)' if engine.external else ''}")
        engine.close()
    return 0


def _load(args: argparse.Namespace) -> int:
    for engine in _start_engines(args.engines, "UTC", start=False, load=False).values():
        reloaded = ensure_dataset(engine, force=args.force)
        print(f"{engine.name}: {len(reloaded)} tables loaded")
    return 0


def _list(args: argparse.Namespace) -> int:
    for case in _select_cases(args):
        first_line = case.sql.splitlines()[0]
        print(f"{case.id:<45} {case.location:<45} {first_line[:80]}")
    return 0


def _run(args: argparse.Namespace) -> int:
    from conformance.runner import (
        Runner,
        diff_baseline,
        load_baseline,
        write_baseline,
        write_report,
    )

    if args.sql:
        if not args.read:
            raise CaseError("--sql needs --read <dialect>")
        cases = [Case("adhoc", args.sql.strip().rstrip(";"), args.read, file="--sql")]
    else:
        cases = _select_cases(args)

    needed = {case.read for case in cases} | set(args.write or args.engines)
    engines = _start_engines(
        [name for name in args.engines if name in needed], args.timezone, start=not args.no_start
    )
    _warn_about_compiled_sqlglot()

    adhoc = bool(args.sql)
    runner = Runner(engines, targets=args.write, show_rows=args.show_rows or adhoc)
    use_baseline = not (adhoc or args.no_baseline)
    baseline = load_baseline(args.baseline) if use_baseline else {}

    start = time.perf_counter()
    results = runner.run(cases, progress=_progress)
    seconds = time.perf_counter() - start
    print(file=sys.stderr)

    diff = diff_baseline(results, baseline)
    new_failures = set(diff.new_failures)
    for result in results:
        for leg in result.legs:
            new = result.key(leg) in new_failures
            if adhoc or args.verbose or (leg.failed and (new or args.all)):
                _print_leg(result, leg, new=new and use_baseline, show_rows=runner.show_rows)

    _print_summary(results, engines, seconds)
    if use_baseline:
        _print_baseline_diff(diff, args.baseline)

    write_report(args.report, results, engines, diff if use_baseline else None, seconds)
    print(f"Report: {args.report}")

    if args.update_baseline:
        complete = not (args.patterns or args.read or args.write or args.case_paths or adhoc)
        failures = write_baseline(args.baseline, results, baseline, complete)
        print(f"Baseline updated: {len(failures)} known failures in {args.baseline}")
        return 0

    return 1 if diff.new_failures else 0


def _select_cases(args: argparse.Namespace) -> list[Case]:
    cases = load_cases(args.case_paths)
    if args.read:
        cases = [case for case in cases if case.read == args.read]
    if args.patterns:
        cases = [
            case
            for case in cases
            if any(pattern in case.id or pattern in case.file for pattern in args.patterns)
        ]
    return cases


def _start_engines(
    names: list[str], timezone: str, start: bool, load: bool = True
) -> dict[str, Engine]:
    engines = {name: ENGINES[name](timezone) for name in names}
    down = [engine for engine in engines.values() if not engine.ping()]

    if down:
        external = [engine.name for engine in down if engine.external]
        if external or not start:
            raise EngineUnavailable(
                f"{', '.join(e.name for e in down)} not reachable"
                + ("" if external else "; start them with `python -m conformance up`")
            )

        print(f"Starting {', '.join(e.name for e in down)}...", file=sys.stderr)
        docker.up([engine.service for engine in down])
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        for engine in down:
            while not engine.ping():
                if time.monotonic() > deadline:
                    raise EngineUnavailable(f"{engine.name} didn't become reachable")
                time.sleep(1)

    if load:
        for engine in engines.values():
            ensure_dataset(engine, log=lambda message: print(message, file=sys.stderr))

    return engines


def _warn_about_compiled_sqlglot() -> None:
    import sqlglot

    compiled = sorted(
        name
        for name, module in list(sys.modules.items())
        if name.startswith("sqlglot")
        and (getattr(module, "__file__", None) or ".py").endswith((".so", ".pyd"))
    )
    print(f"sqlglot {sqlglot.__version__} from {Path(sqlglot.__file__).parent}", file=sys.stderr)
    if compiled:
        print(
            f"warning: {len(compiled)} compiled sqlglotc modules are loaded (e.g. {compiled[0]}), so "
            "edits to their .py files have no effect. Run `make hidec` or `make clean` first.",
            file=sys.stderr,
        )


def _progress(result: CaseResult) -> None:
    worst = min((leg for leg in result.legs), key=lambda leg: _rank(leg.status))
    sys.stderr.write("." if not worst.failed else worst.status[0].upper())
    sys.stderr.flush()


def _rank(status: str) -> int:
    from conformance.runner import STATUSES

    return STATUSES.index(status)


def _print_leg(result: CaseResult, leg: Leg, new: bool, show_rows: bool) -> None:
    case = result.case
    marker = " [new]" if new else ""
    print(f"{leg.status.upper():<14} {case.id}  {leg.name}{marker}")
    print(f"  at {case.location}")

    if leg.name.startswith(("source:", "parse:")) or leg.sql is None:
        print(_indent(f"{case.read}: {case.sql}"))
    else:
        print(_indent(f"{case.read} (source): {case.sql}"))
        print(_indent(f"{leg.name.split(':')[1].split('->')[-1]} (generated): {leg.sql}"))

    if leg.reason:
        print(_indent(leg.reason))
    for key, value in leg.details.items():
        if key == "traceback":
            continue
        if isinstance(value, list):
            for item in value:
                print(_indent(f"{key}: {item}", 4))
        else:
            print(_indent(f"{key}: {value}", 4))
    for warning in leg.warnings:
        print(_indent(f"warning: {warning}"))

    if show_rows:
        source = result.legs[0]
        if leg is not source and source.rows is not None:
            print(_indent("expected rows:"))
            for row in source.rows:
                print(_indent(row, 4))
        if leg.rows is not None:
            print(_indent("rows:"))
            for row in leg.rows:
                print(_indent(row, 4))
    print()


def _print_summary(results: list[CaseResult], engines: dict[str, Engine], seconds: float) -> None:
    from conformance.runner import STATUSES, summarize

    summary = summarize(results)
    statuses = [s for s in STATUSES if any(s in counts for counts in summary.values())]
    width = max([len(name) for name in summary] + [4])

    described = []
    for engine in engines.values():
        settings = ", ".join(f"{k}={v}" for k, v in engine.settings().items())
        described.append(f"{engine.name} {engine.version()} ({settings})")
    print(f"Engines: {'; '.join(described)}")
    print(f"Ran {len(results)} cases in {seconds:.1f}s")

    print("  " + "leg".ljust(width) + "".join(s.rjust(max(len(s), 4) + 2) for s in statuses))
    for name, counts in summary.items():
        cells = "".join(str(counts.get(s, "")).rjust(max(len(s), 4) + 2) for s in statuses)
        print("  " + name.ljust(width) + cells)


def _print_baseline_diff(diff: BaselineDiff, path: Path) -> None:
    print(
        f"Baseline: {len(diff.new_failures)} new failures, {len(diff.fixed)} fixed, "
        f"{len(diff.changed)} changed status"
    )
    for key in diff.fixed:
        print(f"  fixed: {key}")
    for key, old, new in diff.changed:
        print(f"  changed: {key} ({old} -> {new})")
    if diff.fixed or diff.changed:
        print(f"  Record these with --update-baseline ({path})")


def _indent(text: str, spaces: int = 2) -> str:
    return "\n".join(" " * spaces + line for line in str(text).splitlines())
