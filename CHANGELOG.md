Changelog
=========

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