#!/usr/bin/env python3
"""
This script is intended to be used as part of a GitHub Actions workflow in order to decide if the integration tests should:

a) be triggered at all
b) if they should be triggered, should they be triggered for a subset of dialects or all dialects?

The tests can be triggered manually by using the following directive in the PR description:

 /integration-tests

To limit them to a certain dialect or dialects, you can specify:

 /integration-tests dialects=bigquery,duckdb

If you specify nothing, a `git diff` will be performed between your PR branch and the base branch.
If any files modified contain one of the SUPPORTED_DIALECTS in the filename, that dialect will be added to the
list of dialects to test. If no files match, the integration tests will be skipped.

Note that integration tests in the remote workflow are only implemented for a subset of dialects.
If new ones are added, update the SUPPORTED_DIALECTS constant below.

Each dialect is tested against itself (roundtrip) and duckdb (transpilation).
Supplying a dialect not in this list will cause the tests to get skipped.
"""

import typing as t
import os
import sys
import json
import subprocess
from pathlib import Path

TRIGGER = "/integration-test"
SUPPORTED_DIALECTS = ["duckdb", "bigquery", "snowflake"]


def get_dialects_from_manual_trigger(trigger: str) -> t.Set[str]:
    """
    Takes a trigger string and parses out the supported dialects

    /integration_test -> []
    /integration_test dialects=bigquery -> ["bigquery"]
    /integration_test dialects=bigquery,duckdb -> ["bigquery","duckdb"]
    /integration_test dialects=exasol,duckdb -> ["duckdb"]
    """

    if not trigger.startswith(TRIGGER):
        raise ValueError(f"Invalid trigger: {trigger}")

    # trim off start at first space (to cover both /integration-test and /integration-tests)
    trigger_parts = trigger.split(" ")[1:]

    print(f"Parsing trigger args: {trigger_parts}")

    dialects: t.List[str] = []
    for part in trigger_parts:
        # try to parse key=value pairs
        maybe_kv = part.split("=", maxsplit=1)
        if len(maybe_kv) >= 2:
            k, v = maybe_kv[0], maybe_kv[1]
            if k.lower().startswith("dialect"):
                dialects.extend([d.lower().strip() for d in v.split(",")])

    return {d for d in dialects if d in SUPPORTED_DIALECTS}


def get_dialects_from_git(base_ref: str, current_ref: str) -> t.Set[str]:
    """
    Takes two git refs and runs `git diff --name-only <base_ref> <current_ref>`

    If any of the returned file names contain a dialect from SUPPORTED_DIALECTS as
    a substring, that dialect is included in the returned set
    """
    print(f"Checking for files changed between '{base_ref}' and '{current_ref}'")

    result = subprocess.run(
        ["git", "diff", "--name-only", base_ref, current_ref],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output = result.stdout.decode("utf8")

    if result.returncode != 0:
        raise ValueError(f"Git process failed with exit code {result.returncode}:\n{output}")

    print(f"Git output:\n{output}")

    matching_dialects = []

    for l in output.splitlines():
        l = l.strip().lower()

        matching_dialects.extend([d for d in SUPPORTED_DIALECTS if d in l])

    return set(matching_dialects)


if __name__ == "__main__":
    github_event_path = os.environ.get("GITHUB_EVENT_PATH")
    github_output = os.environ.get("GITHUB_OUTPUT")

    if not os.environ.get("GITHUB_ACTIONS") or not github_event_path or not github_output:
        print(f"This script needs to run within GitHub Actions")
        sys.exit(1)

    github_event_path = Path(github_event_path)
    github_output = Path(github_output)

    with github_event_path.open("r") as f:
        event: t.Dict[str, t.Any] = json.load(f)

    print(f"Handling event: \n" + json.dumps(event, indent=2))

    # for pull_request events, the body is located at github.event.pull_request.body
    pr_description: str = event.get("pull_request", {}).get("body") or ""

    dialects = []
    should_run = False

    pr_description_lines = [l.strip().lower() for l in pr_description.splitlines()]
    if trigger_line := [l for l in pr_description_lines if l.startswith(TRIGGER)]:
        # if the user has explicitly requested /integration-tests then use that
        print(f"Handling trigger line: {trigger_line[0]}")
        dialects = get_dialects_from_manual_trigger(trigger_line[0])
        should_run = True
    else:
        # otherwise, do a git diff and inspect the changed files
        print(f"Explicit trigger line not detected; performing git diff")
        pull_request_base_ref = event.get("pull_request", {}).get("base", {}).get("sha")
        if not pull_request_base_ref:
            raise ValueError("Unable to determine base ref")

        current_ref = event.get("pull_request", {}).get("head", {}).get("sha")

        if not current_ref:
            raise ValueError("Unable to determine current/head ref")

        print(f"Comparing '{current_ref}' against '{pull_request_base_ref}'")
        # otherwise, look at git files changed and only trigger if a file relating
        # to a supported dialect has changed
        dialects = get_dialects_from_git(base_ref=pull_request_base_ref, current_ref=current_ref)
        if dialects:
            should_run = True

    if should_run:
        dialects_str = (
            f"the following dialects: {', '.join(dialects)}"
            if dialects
            else f"all supported dialects"
        )
        print(f"Conclusion: should run tests for {dialects_str}")
    else:
        print(f"Conclusion: No tests to run")

    # write output variables
    lines = []
    if should_run:
        lines.append("skip=false")
        if dialects:
            lines.append(f"dialects={','.join(dialects)}")
    else:
        lines.append("skip=true")

    with github_output.open("a") as f:
        f.writelines(f"{l}\n" for l in lines)
