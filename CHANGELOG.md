Changelog
=========

## [v13.3.0] - 2023-05-19
### :boom: BREAKING CHANGES
- due to [`8c9e5ec`](https://github.com/tobymao/sqlglot/commit/8c9e5ec0601140112cb282ab19108ec5ed4ccf06) - multi threading issues with simplify *(commit by [@tobymao](https://github.com/tobymao))*:

  multi threading issues with simplify


### :bug: Bug Fixes
- [`8c9e5ec`](https://github.com/tobymao/sqlglot/commit/8c9e5ec0601140112cb282ab19108ec5ed4ccf06) - multi threading issues with simplify *(commit by [@tobymao](https://github.com/tobymao))*


## [v13.2.2] - 2023-05-19
### :bug: Bug Fixes
- [`bb20335`](https://github.com/tobymao/sqlglot/commit/bb20335f77652e8c78f6a3fed776c5ea8ecbeb3b) - make schema type handle UNKNOWN *(commit by [@tobymao](https://github.com/tobymao))*


## [v13.2.1] - 2023-05-19
### :sparkles: New Features
- [`e7f6455`](https://github.com/tobymao/sqlglot/commit/e7f64555469d54b28e7d898c3e2817608387acc7) - **executor**: add support for qualified table references *(PR [#1659](https://github.com/tobymao/sqlglot/pull/1659) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1657](undefined) opened by [@wmaiouiru](https://github.com/wmaiouiru)*

### :bug: Bug Fixes
- [`3f2f4df`](https://github.com/tobymao/sqlglot/commit/3f2f4df38ec607c6964e0cb9d0b2eb8cb4c90cbe) - **redshift**: handle VALUES clause more robustly *(PR [#1654](https://github.com/tobymao/sqlglot/pull/1654) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`da51f1e`](https://github.com/tobymao/sqlglot/commit/da51f1e93d9fa7670b0441378d56a27c6808ed87) - cast to boolean closes [#1658](https://github.com/tobymao/sqlglot/pull/1658) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2cefcaa`](https://github.com/tobymao/sqlglot/commit/2cefcaaa192a110ac9c613bc1a211700a8a8399b) - add exp.Neg to UNWRAPPED_INTERVAL_VALUES *(PR [#1662](https://github.com/tobymao/sqlglot/pull/1662) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1660](undefined) opened by [@joshmarcus](https://github.com/joshmarcus)*
- [`7130db0`](https://github.com/tobymao/sqlglot/commit/7130db040fa0ebe8852bf47fb9c5990bf949b784) - **lineage**: remove unnecessary optimization *(PR [#1663](https://github.com/tobymao/sqlglot/pull/1663) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1647](undefined) opened by [@b-per](https://github.com/b-per)*
- [`cfbadfa`](https://github.com/tobymao/sqlglot/commit/cfbadfa9fddf408b8c05ef99d4a17d94c3c36a73) - **optimizer**: expand refs in QUALIFY into corresponding projections *(PR [#1665](https://github.com/tobymao/sqlglot/pull/1665) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1661](undefined) opened by [@homebase3](https://github.com/homebase3)*

### :wrench: Chores
- [`19a56d9`](https://github.com/tobymao/sqlglot/commit/19a56d9d2caa3bdef10644f242394fba68f36d5f) - explain versioning system in README *(PR [#1652](https://github.com/tobymao/sqlglot/pull/1652) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`862deab`](https://github.com/tobymao/sqlglot/commit/862deabcb51777f8508cc29d90d2f23f532cc9c8) - **snowflake**: remove select_sql and values_sql as the bug was resolved *(PR [#1653](https://github.com/tobymao/sqlglot/pull/1653) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v13.2.0] - 2023-05-18
### :wrench: Chores
- [`95b29a1`](https://github.com/tobymao/sqlglot/commit/95b29a1e9389e88263d7cce015cca62f06d915d5) - remove unused tableformat property *(commit by [@tobymao](https://github.com/tobymao))*


## [v13.1.0] - 2023-05-18
### :sparkles: New Features
- [`51e8b1f`](https://github.com/tobymao/sqlglot/commit/51e8b1f0dfbec6a502d4a4f8c4efe75fc9c04dd8) - add as_ builder *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`f88ea48`](https://github.com/tobymao/sqlglot/commit/f88ea485dea217560344441647ed4e584986d536) - **snowflake**: add support for GEOGRAPHY, GEOMETRY types *(PR [#1640](https://github.com/tobymao/sqlglot/pull/1640) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1639](undefined) opened by [@dlouseiro](https://github.com/dlouseiro)*
- [`6d04523`](https://github.com/tobymao/sqlglot/commit/6d045232c7bb1d00ef8f9cfb049ff417fa0fcb3e) - Spark table format vs file format *(PR [#1644](https://github.com/tobymao/sqlglot/pull/1644) by [@barakalon](https://github.com/barakalon))*
- [`6f9d531`](https://github.com/tobymao/sqlglot/commit/6f9d531ea7ea31e7e5b0f0ac879f43a2bdb8a3ad) - **postgres, redshift**: use single string interval logic *(PR [#1651](https://github.com/tobymao/sqlglot/pull/1651) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1649](undefined) opened by [@ibestvina](https://github.com/ibestvina)*
- [`72c5995`](https://github.com/tobymao/sqlglot/commit/72c59953408446c7d9418a7357cd3c94a4b52d5e) - interval execution closes [#1650](https://github.com/tobymao/sqlglot/pull/1650) *(commit by [@tobymao](https://github.com/tobymao))*
- [`c4ef23c`](https://github.com/tobymao/sqlglot/commit/c4ef23ca369c6f92419f259ec068b029acfc17fc) - **clickhouse**: absorb _parse_ternary logic in _parse_conjunction *(PR [#1646](https://github.com/tobymao/sqlglot/pull/1646) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1645](undefined) opened by [@ArtjomKotkov](https://github.com/ArtjomKotkov)*

### :wrench: Chores
- [`1d1c0d7`](https://github.com/tobymao/sqlglot/commit/1d1c0d773ba8976d101befe80f7e85dcea2c0eed) - issue a warning if __version__ can't be imported *(PR [#1648](https://github.com/tobymao/sqlglot/pull/1648) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v13.0.2] - 2023-05-16
### :sparkles: New Features
- [`40820b1`](https://github.com/tobymao/sqlglot/commit/40820b1fa2da41b83e432746ce41e27822b1265b) - add is_ builder. *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`cc7a5de`](https://github.com/tobymao/sqlglot/commit/cc7a5de55ffc5fbb121c15900ba29d807e094f57) - ensure unit is a var in parse_date_delta *(PR [#1637](https://github.com/tobymao/sqlglot/pull/1637) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :recycle: Refactors
- [`b9140b6`](https://github.com/tobymao/sqlglot/commit/b9140b6cb92b1a8a417896318cb0029fbedb2529) - **CI/CD**: make it so that deployment only requires tag push *(PR [#1638](https://github.com/tobymao/sqlglot/pull/1638) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v13.0.1] - 2023-05-16
### :sparkles: New Features
- [`8610298`](https://github.com/tobymao/sqlglot/commit/86102989139a90f81316d794b967b283aaa763f6) - **snowflake**: add support for the CLONE clause in DDL statements *(PR [#1627](https://github.com/tobymao/sqlglot/pull/1627) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1626](undefined) opened by [@ianhook](https://github.com/ianhook)*
- [`50025ea`](https://github.com/tobymao/sqlglot/commit/50025eac213b2c4406fecaa2cb46b6108d9f75c8) - **clickhouse**: add support for clickhouse's placeholders *(PR [#1628](https://github.com/tobymao/sqlglot/pull/1628) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`4b1aa02`](https://github.com/tobymao/sqlglot/commit/4b1aa02194dcf836918f6b36204e4e34bc70a00b) - **optimizer**: optimize pivots *(PR [#1617](https://github.com/tobymao/sqlglot/pull/1617) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`13a731a`](https://github.com/tobymao/sqlglot/commit/13a731ac6d9618a55edc160d729163ed8c080225) - **snowflake**: translate [CHAR|NCHAR] VARYING into VARCHAR *(PR [#1634](https://github.com/tobymao/sqlglot/pull/1634) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1632](undefined) opened by [@b-per](https://github.com/b-per)*
- [`06d6990`](https://github.com/tobymao/sqlglot/commit/06d699000cde1beb794ee99070158cdd7a83f501) - **snowflake**: translate CHARACTER VARYING into VARCHAR too *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`c01edb0`](https://github.com/tobymao/sqlglot/commit/c01edb014b5101eac9913b04404753b324581214) - create builders for the INSERT statement *(PR [#1630](https://github.com/tobymao/sqlglot/pull/1630) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`99532d9`](https://github.com/tobymao/sqlglot/commit/99532d9febafd4b5f952769214dbf340cd6cb908) - tablesample losing db closes [#1629](https://github.com/tobymao/sqlglot/pull/1629) *(commit by [@tobymao](https://github.com/tobymao))*
- [`409f13d`](https://github.com/tobymao/sqlglot/commit/409f13de8e4588904a4323677364bb3a8daca4cd) - missing from comment and spacing closes [#1631](https://github.com/tobymao/sqlglot/pull/1631) *(commit by [@tobymao](https://github.com/tobymao))*
- [`bba360c`](https://github.com/tobymao/sqlglot/commit/bba360c0f9779c34f8392d83173e1addc6e433d4) - **clickhouse**: map ApproxDistinct to uniq, AnyValue to any *(PR [#1635](https://github.com/tobymao/sqlglot/pull/1635) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1633](undefined) opened by [@ewjoachim](https://github.com/ewjoachim)*


## [v13.0.0] - 2023-05-15
### :sparkles: New Features
- [`31a82cc`](https://github.com/tobymao/sqlglot/commit/31a82ccd0b0162018d9220bed5fdc0bb35b9638b) - add math associativity simplification *(commit by [@tobymao](https://github.com/tobymao))*
- [`a973113`](https://github.com/tobymao/sqlglot/commit/a9731133b835be938c9cd559688465912da95a99) - reparse bigquery nested identifiers *(commit by [@tobymao](https://github.com/tobymao))*
- [`a33112f`](https://github.com/tobymao/sqlglot/commit/a33112fd18ae0f185315dc770448358b6cec9aba) - **bigquery**: allow first part of table names to contain dashes *(PR [#1624](https://github.com/tobymao/sqlglot/pull/1624) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`0acfaf7`](https://github.com/tobymao/sqlglot/commit/0acfaf721330ef1596a8e9634144ee5859832e16) - clickhouse settings and format closes [#1605](https://github.com/tobymao/sqlglot/pull/1605) closes [#1604](https://github.com/tobymao/sqlglot/pull/1604) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b5f0abc`](https://github.com/tobymao/sqlglot/commit/b5f0abc4f29b690f4bf901bffd0fd8c6aca49006) - global join clickhouse closes [#1606](https://github.com/tobymao/sqlglot/pull/1606) *(commit by [@tobymao](https://github.com/tobymao))*
- [`d92964b`](https://github.com/tobymao/sqlglot/commit/d92964b5c2b0200b2014d4ad1c54184b3f913a3c) - clickhouse attach command closes [#1608](https://github.com/tobymao/sqlglot/pull/1608) *(commit by [@tobymao](https://github.com/tobymao))*
- [`72f1984`](https://github.com/tobymao/sqlglot/commit/72f1984379a1be7c7917de914b1685c35fb1415b) - clickhouse cast to string closes [#1607](https://github.com/tobymao/sqlglot/pull/1607) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2f34d6f`](https://github.com/tobymao/sqlglot/commit/2f34d6f4ba29efcac8060f848ee135c3ce122775) - clickhouse group by with totals closes [#1609](https://github.com/tobymao/sqlglot/pull/1609) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b75b006`](https://github.com/tobymao/sqlglot/commit/b75b006d3e8419fd6e11b8f53352f6b5531596f2) - clickhouse paramaterized func closes [#1610](https://github.com/tobymao/sqlglot/pull/1610) *(commit by [@tobymao](https://github.com/tobymao))*
- [`4601831`](https://github.com/tobymao/sqlglot/commit/4601831629fdb742502c829a6f737b351d027c2b) - join/pivot/lateral order and simplify *(commit by [@tobymao](https://github.com/tobymao))*
- [`29e5af2`](https://github.com/tobymao/sqlglot/commit/29e5af21ba117d4824c19f60ea96859777a06094) - remove unconditional expression copy *(PR [#1611](https://github.com/tobymao/sqlglot/pull/1611) by [@tobymao](https://github.com/tobymao))*
- [`a67b2de`](https://github.com/tobymao/sqlglot/commit/a67b2dea4812efe7dd50e9a26639c76c3701baa3) - **clickhouse**: join type/kind ordering issues *(PR [#1614](https://github.com/tobymao/sqlglot/pull/1614) by [@pkit](https://github.com/pkit))*
- [`6875d07`](https://github.com/tobymao/sqlglot/commit/6875d077ae82ba9c5eac03c17e65305256d42fd7) - **clickhouse**: `USING` allows unwrapped col list *(PR [#1615](https://github.com/tobymao/sqlglot/pull/1615) by [@pkit](https://github.com/pkit))*
- [`2f7473b`](https://github.com/tobymao/sqlglot/commit/2f7473b65edaa75f05400129ffb1e9dedffecbbc) - presto sequence to unnest closes [#1600](https://github.com/tobymao/sqlglot/pull/1600) *(commit by [@tobymao](https://github.com/tobymao))*
- [`966dfbb`](https://github.com/tobymao/sqlglot/commit/966dfbb990a12e14bab83b18f554c0f36ed8bae3) - **tokenizer**: avoid edge case bug in the trie lookup loop *(PR [#1619](https://github.com/tobymao/sqlglot/pull/1619) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`4833953`](https://github.com/tobymao/sqlglot/commit/48339539f8be579800e509e76a473dbee7d95948) - simplify from to a single expression closes [#1620](https://github.com/tobymao/sqlglot/pull/1620) *(PR [#1620](https://github.com/tobymao/sqlglot/pull/1620) by [@tobymao](https://github.com/tobymao))*
  - :arrow_lower_right: *fixes issue [#1618](undefined) opened by [@pglass](https://github.com/pglass)*
- [`bc0b021`](https://github.com/tobymao/sqlglot/commit/bc0b021d490b8878843f8d5fdf418f5ded671890) - allow identifier params. *(commit by [@tobymao](https://github.com/tobymao))*


## [v12.4.0] - 2023-05-12
### :sparkles: New Features
- [`f585eef`](https://github.com/tobymao/sqlglot/commit/f585eefe312531306ffadc223aac6ff3fd8c2d66) - **clickhouse**: parse ternary operator *(PR [#1603](https://github.com/tobymao/sqlglot/pull/1603) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1602](undefined) opened by [@ArtjomKotkov](https://github.com/ArtjomKotkov)*

### :bug: Bug Fixes
- [`41b90be`](https://github.com/tobymao/sqlglot/commit/41b90bed8ed91a1ef7b8e737b508508d3abba9cf) - **snowflake**: Handle form of CONVERT_TIMEZONE with a source TZ *(PR [#1598](https://github.com/tobymao/sqlglot/pull/1598) by [@pmsanford](https://github.com/pmsanford))*
- [`4dd413b`](https://github.com/tobymao/sqlglot/commit/4dd413bdcae63d21e72b26d6a448745ef3850fa9) - expand alias refs was buggy and did the samething expand lateral… *(PR [#1599](https://github.com/tobymao/sqlglot/pull/1599) by [@tobymao](https://github.com/tobymao))*


## [v12.2.0] - 2023-05-09
### :sparkles: New Features
- [`1fa8ae9`](https://github.com/tobymao/sqlglot/commit/1fa8ae97491b9a2c69a8b6f9a7bfe804014e1666) - sqlite primary key transforms closes [#1557](https://github.com/tobymao/sqlglot/pull/1557) *(commit by [@tobymao](https://github.com/tobymao))*
- [`fb819f0`](https://github.com/tobymao/sqlglot/commit/fb819f0d970417e86820f266c8aa10da05fcaa19) - **optimizer**: expand join constructs into SELECT * from subqueries *(PR [#1560](https://github.com/tobymao/sqlglot/pull/1560) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1554](undefined) opened by [@SudarshanVS](https://github.com/SudarshanVS)*
- [`e173dd5`](https://github.com/tobymao/sqlglot/commit/e173dd59275e6ae5cc598da5d35bab70d8a09a6e) - improve tokenizer perf significantly on sql with many strings *(commit by [@tobymao](https://github.com/tobymao))*
- [`c9103fe`](https://github.com/tobymao/sqlglot/commit/c9103feb818a867aad7bfe8a0bde211cc5198bb1) - **Clickhouse**: Support large data types *(PR [#1568](https://github.com/tobymao/sqlglot/pull/1568) by [@matthax](https://github.com/matthax))*

### :bug: Bug Fixes
- [`34b6038`](https://github.com/tobymao/sqlglot/commit/34b603859913124b18f7819dd58cb4f6a1461e32) - bigquery conversion without table alias *(commit by [@tobymao](https://github.com/tobymao))*
- [`6124d0c`](https://github.com/tobymao/sqlglot/commit/6124d0c2e413357f5fe947ae6d24ef341c0f8d55) - bigquery select distinct as struct *(commit by [@tobymao](https://github.com/tobymao))*
- [`54a7637`](https://github.com/tobymao/sqlglot/commit/54a7637aa5b0cd761e53e6c4b2dde5837c8b3508) - postgres exponent precedence closes [#1555](https://github.com/tobymao/sqlglot/pull/1555) *(commit by [@tobymao](https://github.com/tobymao))*
- [`ac60698`](https://github.com/tobymao/sqlglot/commit/ac60698d7343880cbe0fdc5935afb2c1ce0873a8) - array_join -> concat_ws closes [#1558](https://github.com/tobymao/sqlglot/pull/1558) *(commit by [@tobymao](https://github.com/tobymao))*
- [`79a478e`](https://github.com/tobymao/sqlglot/commit/79a478ee6e0109d7ab404b0bfaad085abd5eb08f) - comments refactor closes [#1561](https://github.com/tobymao/sqlglot/pull/1561) *(commit by [@tobymao](https://github.com/tobymao))*
- [`7b09bff`](https://github.com/tobymao/sqlglot/commit/7b09bffd7820748df5e9f1ce21f74883a7041ca8) - options inside of bigquery struct closes [#1562](https://github.com/tobymao/sqlglot/pull/1562) *(commit by [@tobymao](https://github.com/tobymao))*
- [`4f0b3ed`](https://github.com/tobymao/sqlglot/commit/4f0b3edbccd4fefae80e3a1fc280a152e535698c) - bigquery date_part WEEK(WEEKDAY) closes [#1563](https://github.com/tobymao/sqlglot/pull/1563) *(commit by [@tobymao](https://github.com/tobymao))*
- [`4744742`](https://github.com/tobymao/sqlglot/commit/474474213bc9de3397a633996362c44d469d4037) - **presto, spark**: remove WITHIN GROUP when transpiling percentile_[cont|disc] *(PR [#1565](https://github.com/tobymao/sqlglot/pull/1565) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`23cf246`](https://github.com/tobymao/sqlglot/commit/23cf24655925290b2a38f272f565ca7137379105) - need to differentiate between peek and curr tokenizers *(commit by [@tobymao](https://github.com/tobymao))*
- [`9f13b6c`](https://github.com/tobymao/sqlglot/commit/9f13b6c1c49d7f209e0f7aa38a3e64f961b5f878) - base64 closes [#1567](https://github.com/tobymao/sqlglot/pull/1567) *(commit by [@tobymao](https://github.com/tobymao))*
- [`bcfae2c`](https://github.com/tobymao/sqlglot/commit/bcfae2c9bd979fc0ac434d04037a4c4e21113a78) - subquery selects *(PR [#1569](https://github.com/tobymao/sqlglot/pull/1569) by [@barakalon](https://github.com/barakalon))*
- [`34d99ab`](https://github.com/tobymao/sqlglot/commit/34d99abfff9f34b9e973a8895b77f78c0d254de4) - **spark**: unqualify columns in PIVOT expressions *(PR [#1572](https://github.com/tobymao/sqlglot/pull/1572) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`e1713f3`](https://github.com/tobymao/sqlglot/commit/e1713f36c753719591aa65c311e6915fc63a0b91) - preserve quotes in aliases *(commit by [@tobymao](https://github.com/tobymao))*
- [`fa0f3a1`](https://github.com/tobymao/sqlglot/commit/fa0f3a1a56452d7ea748171603521c9b6d4d498f) - allow any identifier as name when parsing a struct field *(PR [#1573](https://github.com/tobymao/sqlglot/pull/1573) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :recycle: Refactors
- [`951d407`](https://github.com/tobymao/sqlglot/commit/951d407f4b13a7d743c7e21870009ae18b540313) - preserve the full text of hex/bin literals *(PR [#1552](https://github.com/tobymao/sqlglot/pull/1552) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5bbb7e8`](https://github.com/tobymao/sqlglot/commit/5bbb7e80552ae44a227d271514a4f6d1e908f80b) - simplify tokenizer alnum logic *(PR [#1570](https://github.com/tobymao/sqlglot/pull/1570) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`c580cb3`](https://github.com/tobymao/sqlglot/commit/c580cb3567c9935c0a7c54717d9c2c1de78ea3b6) - update README optimizer example *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v12.1.0] - 2023-05-05
### :bug: Bug Fixes
- [`0b46fa6`](https://github.com/tobymao/sqlglot/commit/0b46fa69f440e3756266f22c25136efc503209c2) - remove bigquery workaround for values type inference *(commit by [@tobymao](https://github.com/tobymao))*
- [`dfae784`](https://github.com/tobymao/sqlglot/commit/dfae784159499159602b6d518d48f0a501e32e25) - sqlite no table options closes [#1553](https://github.com/tobymao/sqlglot/pull/1553) *(commit by [@tobymao](https://github.com/tobymao))*
- [`aef9cfa`](https://github.com/tobymao/sqlglot/commit/aef9cfa9c4dd29ac637d23737b74b0239f253a84) - double json spark closes [#1547](https://github.com/tobymao/sqlglot/pull/1547) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`7e5cede`](https://github.com/tobymao/sqlglot/commit/7e5cede6afc906dcb00eaaf5e9061a042f18a912) - cleanup bigquery transform *(commit by [@tobymao](https://github.com/tobymao))*


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
[v12.1.0]: https://github.com/tobymao/sqlglot/compare/v12.0.0...v12.1.0
[v12.2.0]: https://github.com/tobymao/sqlglot/compare/v12.1.0...v12.2.0
[v12.4.0]: https://github.com/tobymao/sqlglot/compare/v12.3.0...v12.4.0
[v13.0.0]: https://github.com/tobymao/sqlglot/compare/v12.4.0...v13.0.0
[v13.0.1]: https://github.com/tobymao/sqlglot/compare/v13.0.0...v13.0.1
[v13.0.2]: https://github.com/tobymao/sqlglot/compare/v13.0.1...v13.0.2
[v13.1.0]: https://github.com/tobymao/sqlglot/compare/v13.0.2...v13.1.0
[v13.2.0]: https://github.com/tobymao/sqlglot/compare/v13.1.0...v13.2.0
[v13.2.1]: https://github.com/tobymao/sqlglot/compare/v13.2.0...v13.2.1
[v13.2.2]: https://github.com/tobymao/sqlglot/compare/v13.2.1...v13.2.2
[v13.3.0]: https://github.com/tobymao/sqlglot/compare/v13.2.2...v13.3.0