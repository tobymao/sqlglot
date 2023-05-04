Changelog
=========

## [v12.0.0] - 2023-05-04
### :sparkles: New Features
- [`862cbeb`](https://github.com/tobymao/sqlglot/commit/862cbeb2f48ccc004fbc06eacd55b331faeeac9b) - Use alternative transform for dialects that do not support distinct on *(PR [#1524](https://github.com/tobymao/sqlglot/pull/1524) by [@crericha](https://github.com/crericha))*
- [`52c80e0`](https://github.com/tobymao/sqlglot/commit/52c80e00f6498c438ef519058b4494a67f83c270) - Support regex function in Starrocks *(PR [#1528](https://github.com/tobymao/sqlglot/pull/1528) by [@acreux](https://github.com/acreux))*
- [`00b4779`](https://github.com/tobymao/sqlglot/commit/00b47798bce06f43c70ee48a907261a3d666f342) - **spark**: new Spark2 dialect, improve DATEDIFF sql generation BREAKING *(PR [#1529](https://github.com/tobymao/sqlglot/pull/1529) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`e2593ba`](https://github.com/tobymao/sqlglot/commit/e2593baf806a7c51cc089e280e3aa83983f1e82d) - Add BYTES type to BigQuery dialect *(PR [#1536](https://github.com/tobymao/sqlglot/pull/1536) by [@relud](https://github.com/relud))*
  - :arrow_lower_right: *addresses issue [#1533](undefined) opened by [@relud](https://github.com/relud)*
- [`911e4e9`](https://github.com/tobymao/sqlglot/commit/911e4e9cfaf30d35eee06b23a2b1a4fe4f1c1ff2) - distinct on builder *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`5c26f56`](https://github.com/tobymao/sqlglot/commit/5c26f56ad6f1814b543c81d9e8263ebfd11a5634) - windowspec generator *(commit by [@tobymao](https://github.com/tobymao))*
- [`e7111ba`](https://github.com/tobymao/sqlglot/commit/e7111ba6afdb67ae7be52cf39384d7bcb86a8dac) - **trino**: wrap SEQUENCE in an UNNEST call if used as a source *(PR [#1527](https://github.com/tobymao/sqlglot/pull/1527) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`3d964c6`](https://github.com/tobymao/sqlglot/commit/3d964c623e3ec3972161a9e3e69294bc12fdb8b8) - use preprocess instead of expanding transform dicts BREAKING *(PR [#1525](https://github.com/tobymao/sqlglot/pull/1525) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5fc27d3`](https://github.com/tobymao/sqlglot/commit/5fc27d351412063493f66576d7b0c1a0c350c76b) - trim with double pipes *(commit by [@tobymao](https://github.com/tobymao))*
- [`9778c16`](https://github.com/tobymao/sqlglot/commit/9778c168d57fcd886d1653df10e75df59722ce1b) - create table options for bigquery closes [#1531](https://github.com/tobymao/sqlglot/pull/1531) *(commit by [@tobymao](https://github.com/tobymao))*
- [`55dc509`](https://github.com/tobymao/sqlglot/commit/55dc5098e9ccac3ae5757948e3e1575a1ae2b732) - **bigquery**: allow 2nd argument for PERCENTILE_[CONT|DISC] *(PR [#1537](https://github.com/tobymao/sqlglot/pull/1537) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1534](undefined) opened by [@relud](https://github.com/relud)*
- [`5c59747`](https://github.com/tobymao/sqlglot/commit/5c59747db3beed334b4a34e04856780d2f842ea3) - qualifying correlated subqueries *(commit by [@tobymao](https://github.com/tobymao))*
- [`19c0490`](https://github.com/tobymao/sqlglot/commit/19c04903a5143971f31261ba01bdc7ee2acf37aa) - parse unnest array type closes [#1532](https://github.com/tobymao/sqlglot/pull/1532) *(commit by [@tobymao](https://github.com/tobymao))*
- [`51ca411`](https://github.com/tobymao/sqlglot/commit/51ca411b0708563bca815b9709cb927e0c768403) - expand laterals first if no schema is present *(commit by [@tobymao](https://github.com/tobymao))*
- [`abfbce2`](https://github.com/tobymao/sqlglot/commit/abfbce2c79e1af956033e759bd6a5a87038f2c4c) - bigquery udf existing func clash closes [#1535](https://github.com/tobymao/sqlglot/pull/1535) *(commit by [@tobymao](https://github.com/tobymao))*
- [`444dd94`](https://github.com/tobymao/sqlglot/commit/444dd948af23a88db83491be432573ac852dd324) - **Spark**: Add DOUBLE, FLOAT cast functions *(PR [#1530](https://github.com/tobymao/sqlglot/pull/1530) by [@vegarsti](https://github.com/vegarsti))*
- [`0578d6d`](https://github.com/tobymao/sqlglot/commit/0578d6d3b168a4cfac67e297f29f44ad67c51cda) - **oracle**: allow parsing of @dblink in table names *(PR [#1540](https://github.com/tobymao/sqlglot/pull/1540) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1539](undefined) opened by [@Pizzi95](https://github.com/Pizzi95)*
- [`b7e08cc`](https://github.com/tobymao/sqlglot/commit/b7e08cc88a6d91057aeb9b81a4aa3efa6c81ee8f) - **duckdb**: parse DATEDIFF correctly *(PR [#1546](https://github.com/tobymao/sqlglot/pull/1546) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`f21abb7`](https://github.com/tobymao/sqlglot/commit/f21abb79efb09658502bff397e453593f4fd3471) - **oracle**: set post_tablesample_alias=True to fix alias parsing *(PR [#1548](https://github.com/tobymao/sqlglot/pull/1548) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1541](undefined) opened by [@Pizzi95](https://github.com/Pizzi95)*
- [`8ebba48`](https://github.com/tobymao/sqlglot/commit/8ebba48eca51122ad647f9d83fb99ae7a58bc8b3) - binary_double/float types closes [#1543](https://github.com/tobymao/sqlglot/pull/1543) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2a6a3e7`](https://github.com/tobymao/sqlglot/commit/2a6a3e7f2aafff29937d8ff1a1a296ffcb5d9dec) - make some SQL builders pure *(PR [#1526](https://github.com/tobymao/sqlglot/pull/1526) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`99310c4`](https://github.com/tobymao/sqlglot/commit/99310c434494e1dd302bac907e51daadfa7fa01e) - **duckdb**: remove parentheses from CurrentTimestamp, CurrentDate *(PR [#1551](https://github.com/tobymao/sqlglot/pull/1551) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1550](undefined) opened by [@BTheunissen](https://github.com/BTheunissen)*
- [`a113685`](https://github.com/tobymao/sqlglot/commit/a11368522ec527bce9ecfb63796689b52ba814d5) - **spark**: cast UnixToTime to TIMESTAMP BREAKING *(PR [#1549](https://github.com/tobymao/sqlglot/pull/1549) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1545](undefined) opened by [@joshmarcus](https://github.com/joshmarcus)*

### :wrench: Chores
- [`d2f2e80`](https://github.com/tobymao/sqlglot/commit/d2f2e8093091a132dbaea8d6dd5b14b91e3c3f12) - test 3.11 *(commit by [@tobymao](https://github.com/tobymao))*


## [v11.7.0] - 2023-05-02
### :sparkles: New Features
- [`20cacba`](https://github.com/tobymao/sqlglot/commit/20cacba59b697c70986baac3e3176875893e16e1) - **tsql, oracle**: add support for NEXT VALUE FOR clause *(PR [#1521](https://github.com/tobymao/sqlglot/pull/1521) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`96bb150`](https://github.com/tobymao/sqlglot/commit/96bb150fa3ef3d58cbae8003b3f29f9690cbe481) - builder methods for basic ops *(PR [#1516](https://github.com/tobymao/sqlglot/pull/1516) by [@tobymao](https://github.com/tobymao))*
- [`e11a5ce`](https://github.com/tobymao/sqlglot/commit/e11a5cef9dc2c8c59e1290013c29a4137e07c8aa) - add case when change func to parse BREAKING *(commit by [@tobymao](https://github.com/tobymao))*
- [`455b9e9`](https://github.com/tobymao/sqlglot/commit/455b9e9db1a1b8518a5a772c18f8a4bcec3b96fd) - **oracle**: support KEEP (.. [FIRST|LAST] ..) window function syntax *(PR [#1522](https://github.com/tobymao/sqlglot/pull/1522) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1520](undefined) opened by [@Pizzi95](https://github.com/Pizzi95)*

### :bug: Bug Fixes
- [`94fed8c`](https://github.com/tobymao/sqlglot/commit/94fed8c8ca171ecece53ea2fd72db6a37dfc73ec) - call _parse_bitwise as a fallback for nested type args *(PR [#1515](https://github.com/tobymao/sqlglot/pull/1515) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1514](undefined) opened by [@ethack](https://github.com/ethack)*
- [`52ab03d`](https://github.com/tobymao/sqlglot/commit/52ab03d41aba8a48e5b3b3688e9530a645cac1c7) - array with method calls *(commit by [@tobymao](https://github.com/tobymao))*
- [`c3db2b8`](https://github.com/tobymao/sqlglot/commit/c3db2b8a8f1c9f694f31631fc0f185c735dab768) - allow parsing 'if' as an identifier *(PR [#1517](https://github.com/tobymao/sqlglot/pull/1517) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`d5d360a`](https://github.com/tobymao/sqlglot/commit/d5d360a18a481804bbcee388a07c32c59ea46d4e) - Remove older CHANGELOG entries and let CI handle it from now on *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v11.6.3] - 2023-05-01
### :sparkles: New Features
- [`80287dd`](https://github.com/tobymao/sqlglot/commit/80287dd290e9076ce5981ea440dc193df27e0d46) - **presto**: transpile explode/posexplode into (cross join) unnest *(PR [#1501](https://github.com/tobymao/sqlglot/pull/1501) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1495](undefined) opened by [@vegarsti](https://github.com/vegarsti)*

### :bug: Bug Fixes
- [`efd8c05`](https://github.com/tobymao/sqlglot/commit/efd8c0587e9e441f3ea30bd6fb263135e069b9f8) - redshift doesn't support locks *(commit by [@tobymao](https://github.com/tobymao))*
- [`f4ece7c`](https://github.com/tobymao/sqlglot/commit/f4ece7cf7e459fa63c71d5f9f2bcbf2e0aecbf0f) - comment after paren closes [#1504](https://github.com/tobymao/sqlglot/pull/1504) *(commit by [@tobymao](https://github.com/tobymao))*
- [`85b2c00`](https://github.com/tobymao/sqlglot/commit/85b2c0060c579bd5f9f9731b21395b80bf0bc408) - postgres doesn't support plural interval closes [#1503](https://github.com/tobymao/sqlglot/pull/1503) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2e0eee6`](https://github.com/tobymao/sqlglot/commit/2e0eee6037c8afb31e5ed1ea7023f4f867f9a29e) - postgres date_part type closes [#1506](https://github.com/tobymao/sqlglot/pull/1506) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2dcbc7f`](https://github.com/tobymao/sqlglot/commit/2dcbc7f307edf11d8147331b711afacbbb09f725) - tsql hashbytes closes [#1508](https://github.com/tobymao/sqlglot/pull/1508) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5347c7a`](https://github.com/tobymao/sqlglot/commit/5347c7ace49eb9cc23f8fdc19d74fe94e3259742) - change str to Expression in alias_ isinstance check *(PR [#1510](https://github.com/tobymao/sqlglot/pull/1510) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`f137815`](https://github.com/tobymao/sqlglot/commit/f137815db8264739d16339a8b8543a6fb6fc3c12) - BigQuery `TIMESTAMP` and `TIMESTAMPTZ` types *(PR [#1511](https://github.com/tobymao/sqlglot/pull/1511) by [@plaflamme](https://github.com/plaflamme))*
- [`6143491`](https://github.com/tobymao/sqlglot/commit/614349162d26429bc79ba5f091bee7dcfcb08011) - allow $ to appear in postgres/redshift identifiers *(PR [#1512](https://github.com/tobymao/sqlglot/pull/1512) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`adc526c`](https://github.com/tobymao/sqlglot/commit/adc526c3954c4a046e226af8410e3b5731691e2d) - add note about conventional commit naming in CONTRIBUTING.md *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`9421650`](https://github.com/tobymao/sqlglot/commit/942165055bfe655112e38a247481f7429823e608) - set action for automatic conventional changelog generation *(PR [#1513](https://github.com/tobymao/sqlglot/pull/1513) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


[v11.6.3]: https://github.com/tobymao/sqlglot/compare/v11.6.2...v11.6.3

[v11.7.0]: https://github.com/tobymao/sqlglot/compare/v11.6.3...v11.7.0
[v12.0.0]: https://github.com/tobymao/sqlglot/compare/v11.7.1...v12.0.0