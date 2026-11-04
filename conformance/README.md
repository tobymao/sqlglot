# Conformance tests

The conformance tests check sqlglot dialects against real database engines. Each case is a query
written in a source dialect. The harness runs it on the source engine to get the ground truth, then
checks that the SQL sqlglot generates returns the same result:

| Leg | What runs | What a failure means |
|---|---|---|
| `source:<read>` | The case SQL on its own engine | The case itself is invalid |
| `parse:<read>` | sqlglot parses the case SQL | The parser can't handle valid SQL |
| `roundtrip:<read>` | Parse, regenerate in the same dialect, run on the same engine | The dialect's parser or generator changes semantics |
| `transpile:<read>-><write>` | Transpile to another dialect, run on that engine | The transpilation changes semantics |

Expected results are never written by hand: they always come from the source engine. A case can't
be made to pass by editing it, only by fixing sqlglot.

The suite runs locally only; it isn't part of CI. It currently covers Postgres 18 and Spark 4.2.

## Setup

You need Docker with Compose v2 and Python 3.10+. No local JVM is needed, because Spark is
reached through Spark Connect.

```bash
pip install -e ".[dev,conformance]"   # adds psycopg and pyspark-client
python -m conformance up              # starts the engines and loads the dataset (about a minute)
python -m conformance run             # runs every case (about a minute)
python -m conformance down            # stops the engines and deletes their data
```

`run` starts the engines itself if they're down and reloads any dataset table that changed, so
`up` is optional. Make targets wrap the same commands: `make conformance-up`,
`make conformance ARGS="-k numeric"`, `make conformance-down`.

## Workflow for changing a dialect

1. Find something to fix. `baseline.json` lists every leg that's known to fail. Run a subset with
   all its failures printed:

   ```bash
   python -m conformance run --all -k postgres/numeric
   ```

2. Reproduce it with an ad hoc query. This prints the SQL and the rows of every leg:

   ```bash
   python -m conformance run --read postgres --write spark --sql "SELECT 5 / 2"
   ```

3. Fix the parser or generator in `sqlglot/`, and add unit tests in `tests/dialects/` as usual.

4. Rerun the subset, then the whole suite. The run lists the legs you fixed and fails with exit code
   1 on any new failure. When the change is complete, record the new state:

   ```bash
   python -m conformance run --update-baseline
   ```

   Commit `baseline.json` together with the fix. Only update the baseline to record fixes or genuinely
   unfixable differences, never to hide a regression.

5. To test a new behavior that no case covers yet, add a case (see below). A new case that fails is
   reported as a new failure until it's fixed or recorded in the baseline.

`python -m conformance run --help` lists every option. Useful ones:

| Option | Effect |
|---|---|
| `-k TEXT` | Only cases whose id or file contains TEXT (repeatable) |
| `--read DIALECT` | Only cases written in DIALECT |
| `--write DIALECTS` | Only transpile to these dialects |
| `--sql SQL --read DIALECT` | Run one ad hoc query, printing every leg and its rows |
| `--show-rows` | Print result rows of failing legs |
| `--all` | Print known failures too, not just new ones |
| `-v` | Print passing legs and type warnings |
| `--timezone ZONE` | Run every engine in another session time zone |
| `--no-baseline` | Treat every failure as new |

Every run writes a JSON report to `conformance/.reports/latest.json`. It holds every leg's
generated SQL, status, reason, row differences and type warnings.

If compiled `sqlglotc` modules are present, edits to the matching `.py` files have no effect, and
`run` prints a warning. Run `make hidec` or `make clean` first.

## Statuses

| Status | Meaning | Counts as failure |
|---|---|---|
| `pass` | Results match | No |
| `weak` | Results match, but have no rows or only NULLs, so the check proves little | No |
| `unsupported` | sqlglot raised `UnsupportedError` for the target | Yes |
| `mismatch` | The generated SQL ran but returned different rows | Yes |
| `error` | The engine rejected the generated SQL | Yes |
| `sqlglot_error` | sqlglot crashed while parsing or generating; the report has the traceback | Yes |
| `invalid_case` | The case SQL failed on its own engine, so the case needs fixing | Yes |
| `skipped` | The case has a `# skip:` reason | No |

## Writing cases

Cases live in `cases/<dialect>/<topic>.sql`. The directory name is the source dialect. Statements end
with a `;` at the end of a line, and can be preceded by metadata lines:

```sql
-- Comments between statements are ignored.

# name: integer_division
SELECT 5 / 2, -5 / 2, 5 % 3;

# name: spark_only_check
# write: none
SELECT id, v_map['a'] FROM spark_extras ORDER BY id;
```

| Key | Meaning | Default |
|---|---|---|
| `name` | Stable, readable id: `<dialect>/<topic>:<name>` | A hash of the SQL |
| `read` | Source dialect | The directory name |
| `write` | Comma-separated targets, or `none` for roundtrip only | Every other engine |
| `ordered` | Compare rows in order | `true` if the query has a top-level `ORDER BY` |
| `rel_tol` | Relative tolerance for float comparisons | `1e-9` |
| `skip` | Reason for skipping the case | Not skipped |

Guidelines:

- Results must be deterministic. Make any top-level `ORDER BY` a total order, for example by adding
  `id`. Avoid `CURRENT_DATE`, `RANDOM()` and nondeterministic aggregates.
- Query the dataset rather than literals, so every edge case row is exercised.
- Keep each case focused on one feature. A single unsupported function makes the whole leg
  `unsupported` and hides everything else in that query.
- Filter out rows that make the source engine raise an error, such as overflows, instead of
  dropping the column.
- Always give cases a `name`, so that ids and `baseline.json` stay readable.

TPC-H cases are generated from `tests/fixtures/optimizer/tpc-h/tpc-h.sql`. Each query is transpiled
into Postgres and Spark, which then serve as the source dialects.

## Dataset

The dataset is defined in Python in `dataset/tables.py` and `dataset/tpch.py`. Every engine that a
table supports gets the table loaded. The harness then reads it back and compares it against the
definition, so data that can't be stored identically fails loudly instead of skewing results. Tables
are reloaded automatically whenever their definition or data changes.

| Table | Rows | Purpose |
|---|---|---|
| `scalars` | 32 | One row per edge case, described in `label`, with one column per type |
| `facts` | 10,000 | Generated rows with skew, ties and NULLs, for aggregates, windows and joins |
| `join_left`, `join_right` | 6, 5 | Join keys with duplicates, NULLs and unmatched keys |
| `pg_extras` | 4 | Postgres-only types: UUID, TIME, INTERVAL, unconstrained NUMERIC, JSON, INET, 2D arrays |
| `spark_extras` | 4 | Spark-only types: TINYINT, MAP, STRUCT, nested arrays |
| TPC-H tables | 5 to 8,233 | `lineitem`, `orders`, `customer`, `part`, `supplier`, `partsupp`, `nation`, `region` |

The `scalars` columns and their physical types:

| Column | Postgres | Spark |
|---|---|---|
| `v_bool` | BOOLEAN | BOOLEAN |
| `v_int2`, `v_int4`, `v_int8` | SMALLINT, INTEGER, BIGINT | SMALLINT, INT, BIGINT |
| `v_float4`, `v_float8` | REAL, DOUBLE PRECISION | FLOAT, DOUBLE |
| `v_dec`, `v_dec38` | NUMERIC(10, 2), NUMERIC(38, 10) | DECIMAL(10, 2), DECIMAL(38, 10) |
| `v_text`, `v_varchar`, `v_char` | TEXT, VARCHAR(20), CHAR(5) | STRING, VARCHAR(20), CHAR(5) |
| `v_bytes` | BYTEA | BINARY |
| `v_date` | DATE | DATE |
| `v_ts` | TIMESTAMP | TIMESTAMP_NTZ |
| `v_tstz` | TIMESTAMPTZ | TIMESTAMP |
| `v_int_arr`, `v_text_arr` | INTEGER[], TEXT[] | `ARRAY<INT>`, `ARRAY<STRING>` |
| `v_json` | JSONB | STRING |

Physical types are written by hand, never generated with sqlglot. Otherwise a wrong type mapping
would make both engines store the same wrong thing. Tables that only some engines support are only
used for roundtrip checks. Cases that query them skip transpilation to the other engines automatically.

## How results are compared

Columns are matched by position, because engines name unaliased expressions differently. Rows are
compared as a multiset unless the query has a top-level `ORDER BY`. Before comparing, values are
normalized:

- **Floats** match within a relative tolerance of `1e-9`. NaN equals NaN, `-0.0` equals `0.0`, and
  REAL values are compared at single precision.
- **Integers** match only exactly, including against floats, so `2` never matches `2.5`.
- **Decimals** with different scales match when they agree at the coarser scale, provided that scale is at
  least 6. This covers engines that round AVG or division differently, but `3` still doesn't match `2.5`.
- **Timestamps with a time zone** are compared as wall-clock times in the session time zone.
  A DATE matches a TIMESTAMP at midnight.
- **JSON** is compared structurally when either side is a JSON type, because Spark returns JSON as
  strings with different formatting.
- **Maps** are compared regardless of key order, and **structs** by position.

Differences in result types, such as `integer` versus `decimal`, appear as warnings in the report and
with `-v`. They don't make a leg fail.

## Engines

| Engine | Image | Settings |
|---|---|---|
| Postgres | `postgres:18.4` | UTC, `PG_UNICODE_FAST` builtin locale, so ordering and UPPER/LOWER match Spark |
| Spark | `apache/spark:4.2.0` | Spark Connect, UTC, ANSI mode on, in-memory catalog |

Both run on non-default host ports. Environment variables configure the connections:

| Variable | Default |
|---|---|
| `CONFORMANCE_POSTGRES_PORT` | `55432` |
| `CONFORMANCE_SPARK_PORT` | `55002` |
| `CONFORMANCE_POSTGRES_DSN` | Set this to use an existing Postgres server instead of Docker |
| `CONFORMANCE_SPARK_REMOTE` | Set this to an `sc://host:port` URL to use an existing Spark Connect server |

To debug an engine, use `python -m conformance status` or
`docker compose -p sqlglot-conformance logs spark`. To reload the dataset from scratch, run
`python -m conformance load --force`.

## Adding an engine

1. Subclass `Engine` in `engines/<name>.py`. Implement `connect`, `close`, `version`, `query` and
   `load_table`, map driver types to the families used in `compare.py`, and register the class in
   `engines/__init__.py`.
2. Add a service to `docker-compose.yml`. Pin its version, use UTC, and choose settings that match
   the other engines' string ordering.
3. Add the engine's physical types to the shared columns in `dataset/tables.py` and `dataset/tpch.py`.
   Run `python -m conformance load` to check that every table reads back exactly.
4. Add `cases/<dialect>/` with cases written in the new dialect, then run with `--update-baseline`.
