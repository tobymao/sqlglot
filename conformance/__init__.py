"""
Conformance tests for sqlglot dialects.

Each test case is a query written in a source dialect. The harness runs it on the source engine to
get the ground truth, then checks that sqlglot's output produces the same result:

- roundtrip: the query is parsed and regenerated in the source dialect, then run on the source engine
- transpile: the query is transpiled to every other dialect and run on that dialect's engine

Engines run locally in Docker. See conformance/README.md for usage.
"""
