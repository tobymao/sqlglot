# Spark FROM_JSON to Trino

Trino generation recognizes the existing `FromJson` expression and parses its literal
schema with SQLGlot's Spark data-type parser. The original schema and options remain in
the AST, so generating Spark SQL preserves them. Other target dialects are unchanged.
Spark type annotation also derives the result type from a literal schema and propagates
map key types through `MAP_KEYS`. This lets `SORT_ARRAY(MAP_KEYS(FROM_JSON(...)))`
retain its array type when used by functions such as `CONCAT_WS`.

With the default `unsupported_level=ErrorLevel.WARN`:

```python
import sqlglot

sqlglot.transpile(
    """SELECT FROM_JSON('{"a":1.0}', 'MAP<STRING,DOUBLE>')""",
    read="spark",
    write="trino",
)
# Warns that JSON parsing and type coercion differ from Spark, and produces:
# SELECT TRY(CAST(JSON_PARSE('{"a":1.0}') AS MAP(VARCHAR, DOUBLE)))
```

This is a **best-effort translation**, not a complete implementation of Spark's
PERMISSIVE mode. Every translated call reports this limitation through SQLGlot's
unsupported-feature mechanism. `ErrorLevel.RAISE` or `IMMEDIATE` rejects the conversion;
`IGNORE` explicitly suppresses the warning.

Supported schema syntax is a literal `MAP<STRING, T>` or `ARRAY<T>`, including nested
maps and arrays. Leaves may be `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`,
`FLOAT`, `DOUBLE`, or `STRING` (including their Spark aliases). Inputs that use ordinary
JSON and values matching these types can be converted, including SQL NULL, JSON null,
empty containers, and null elements/values. `TRY` wraps both operations because
`TRY_CAST(JSON_PARSE(...))` does not catch errors raised by `JSON_PARSE`.

The following differences were executed with Spark 4.0.1 and Trino 480 using synthetic
inputs. These are examples of limitations, not an exhaustive equivalence guarantee:

| Input / schema | Spark FROM_JSON | Trino TRY + parse + cast |
| --- | --- | --- |
| `{"a":1.0}` / `MAP<STRING,DOUBLE>` | `{a: 1.0}` | `{a: 1.0}` |
| `bad` / `MAP<STRING,DOUBLE>` | NULL | NULL |
| `{"a":1,"b":"bad"}` / `MAP<STRING,DOUBLE>` | NULL | NULL |
| `{"a":"1.5"}` / `MAP<STRING,DOUBLE>` | NULL | `{a: 1.5}` |
| `{"a":true}` / `MAP<STRING,DOUBLE>` | NULL | `{a: 1.0}` |
| `{'a':1}` / `MAP<STRING,DOUBLE>` | `{a: 1.0}` | NULL |

Spark accepts some nonstandard JSON by default, such as single-quoted strings and
non-finite numbers. Trino has different coercions for numeric, boolean, and string
values; Spark can also stringify JSON containers for a STRING leaf. Do not use this
translation when those behaviors are required.

All explicit options (including modes), nonliteral schemas, JSON-encoded schemas,
DDL field lists, and unsupported types are reported as unsupported and left as
`FROM_JSON` in best-effort output. That fallback cannot execute in Trino. Use strict
unsupported handling to reject it. This change does not evaluate `SCHEMA_OF_JSON`.

STRUCT/ROW, including structures nested inside maps or arrays, is deliberately excluded.
Spark can return a non-null struct with null or partially recovered fields where Trino's
whole-value TRY cast returns NULL; Trino also accepts positional JSON arrays as rows.
Corrupt-record fields and field-name matching need additional handling. Decimal, binary,
date, timestamp, interval, variant, and user-defined types are also excluded.

References:

- [Spark FROM_JSON](https://spark.apache.org/docs/latest/api/sql/json-functions/#from_json)
- [Spark JSON options](https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option)
- [Trino JSON_PARSE](https://trino.io/docs/current/functions/json.html#json_parse)
- [Trino casts from JSON](https://trino.io/docs/current/functions/json.html#cast-from-json)
