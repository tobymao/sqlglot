Changelog
=========

## [v17.8.1] - 2023-07-27
### :sparkles: New Features
- [`59847f5`](https://github.com/tobymao/sqlglot/commit/59847f52e30f61734568c4e02dff5df2c0ea3352) - **parser**: improved comment parsing *(PR [#1956](https://github.com/tobymao/sqlglot/pull/1956) by [@mpf82](https://github.com/mpf82))*
- [`8448141`](https://github.com/tobymao/sqlglot/commit/8448141e100f5408b888fd36cca848b8333ffc48) - **tsql**: improve transpilation of temp table DDLs *(PR [#1958](https://github.com/tobymao/sqlglot/pull/1958) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`92849bd`](https://github.com/tobymao/sqlglot/commit/92849bdda4edb056db9a7598f1761fbe93635caf) - **optimizer**: traverse UNNEST scope *(PR [#1960](https://github.com/tobymao/sqlglot/pull/1960) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`7f79592`](https://github.com/tobymao/sqlglot/commit/7f79592ed87fe2241ae7924a8f421e66b5feffcc) - using type *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`4ed04d5`](https://github.com/tobymao/sqlglot/commit/4ed04d5ab710cd88f26a0eb26593eaa4be5b8a66) - minor README addition *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.8.0] - 2023-07-24
### :sparkles: New Features
- [`75d49b7`](https://github.com/tobymao/sqlglot/commit/75d49b736b767496479a1138581960c5322cb7a3) - **schema**: improve overridability of normalization setting *(PR [#1954](https://github.com/tobymao/sqlglot/pull/1954) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`da5a4d1`](https://github.com/tobymao/sqlglot/commit/da5a4d1d06835d97ec0a01fc193422984a0b9707) - **oracle**: improve handling of KEEP (...) OVER (...) window syntax *(PR [#1953](https://github.com/tobymao/sqlglot/pull/1953) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1952](undefined) opened by [@push2prod](https://github.com/push2prod)*


## [v17.7.0] - 2023-07-23
### :boom: BREAKING CHANGES
- due to [`2aa62d1`](https://github.com/tobymao/sqlglot/commit/2aa62d19252c2ed6eb26d962ff8253d988ff32e5) - mysql 5 does not support select * from values *(commit by [@tobymao](https://github.com/tobymao))*:

  mysql 5 does not support select * from values


### :sparkles: New Features
- [`b82573b`](https://github.com/tobymao/sqlglot/commit/b82573b1d720c69da221e3dda9dcc00a6aebc222) - **redshift**: improve transpilation of ADD_MONTHS function *(PR [#1945](https://github.com/tobymao/sqlglot/pull/1945) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`46b5dfa`](https://github.com/tobymao/sqlglot/commit/46b5dfa09bba7c339dd8b0bd077455946dec8d8d) - **duckdb**: ensure 'day' will be generated for exp.DateDiff by default *(PR [#1944](https://github.com/tobymao/sqlglot/pull/1944) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1943](undefined) opened by [@richard-a-lott](https://github.com/richard-a-lott)*
- [`327451f`](https://github.com/tobymao/sqlglot/commit/327451f78f049787a5afc68e142f33583150a115) - limit with select subquery closes [#1948](https://github.com/tobymao/sqlglot/pull/1948) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f2f4084`](https://github.com/tobymao/sqlglot/commit/f2f4084f1c78e7b9322222ae6ba30b80ad027b30) - offset subquery *(commit by [@tobymao](https://github.com/tobymao))*
- [`2aa62d1`](https://github.com/tobymao/sqlglot/commit/2aa62d19252c2ed6eb26d962ff8253d988ff32e5) - mysql 5 does not support select * from values *(commit by [@tobymao](https://github.com/tobymao))*
- [`3b5d0a6`](https://github.com/tobymao/sqlglot/commit/3b5d0a6f529346d27c86982aa92cce60fd336d14) - mysql cast only supports a few data types *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.6.1] - 2023-07-21
### :sparkles: New Features
- [`4b7e9f1`](https://github.com/tobymao/sqlglot/commit/4b7e9f1d2e3de9eb9b96acfd9e6d8566663666c4) - **clickhouse**: add support for the logical xor function *(PR [#1937](https://github.com/tobymao/sqlglot/pull/1937) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1936](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`1d2b5e0`](https://github.com/tobymao/sqlglot/commit/1d2b5e0a8519692cb7ffdb461d2360304349b415) - **hive**: add support for the query TRANSFORM clause *(PR [#1935](https://github.com/tobymao/sqlglot/pull/1935) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1933](undefined) opened by [@sangongs](https://github.com/sangongs)*

### :bug: Bug Fixes
- [`b8de650`](https://github.com/tobymao/sqlglot/commit/b8de650ca6b9e21e65c29f037dd3c1167e3bf29a) - union lineage with > 2 sources closes [#1934](https://github.com/tobymao/sqlglot/pull/1934) *(commit by [@tobymao](https://github.com/tobymao))*
- [`79efb42`](https://github.com/tobymao/sqlglot/commit/79efb42fde0b8c6e0f878b55ca292cac5a535535) - **duckdb, presto**: improve bitwise support *(PR [#1938](https://github.com/tobymao/sqlglot/pull/1938) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.6.0] - 2023-07-19
### :sparkles: New Features
- [`2510999`](https://github.com/tobymao/sqlglot/commit/2510999192aa98e74d4ea713eb3e7f7d5b4b846f) - isin unnest builder *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.5.0] - 2023-07-18
### :sparkles: New Features
- [`e6b3a01`](https://github.com/tobymao/sqlglot/commit/e6b3a017da747fb38d298617ca310bb7657711da) - **tsql**: improve support for transaction statements *(PR [#1907](https://github.com/tobymao/sqlglot/pull/1907) by [@dmoore247](https://github.com/dmoore247))*
- [`048b9bc`](https://github.com/tobymao/sqlglot/commit/048b9bcd023e6dd6a47fff1536d66e1ca94991f4) - add refresh command *(commit by [@tobymao](https://github.com/tobymao))*
- [`3456bbf`](https://github.com/tobymao/sqlglot/commit/3456bbf09ce8adf28fa29ac4c2c7a196ecb0099f) - add RegexpReplace expression *(PR [#1925](https://github.com/tobymao/sqlglot/pull/1925) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`d6c8722`](https://github.com/tobymao/sqlglot/commit/d6c8722a1a1d10f0599c7216a668d083edac0fab) - clickhouse array join *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`d928ee0`](https://github.com/tobymao/sqlglot/commit/d928ee0e4ced53ec9e4868388f334b03c339e5e0) - duckdb filter where optional *(commit by [@tobymao](https://github.com/tobymao))*
- [`cbcb113`](https://github.com/tobymao/sqlglot/commit/cbcb113f9b2a5cf2d46871b0c8d15af383452576) - **spark**: add support for RLIKE function *(PR [#1911](https://github.com/tobymao/sqlglot/pull/1911) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`cc33749`](https://github.com/tobymao/sqlglot/commit/cc337493378be5815c04f6c40ee471657f19a485) - preserve comments in exp.Drop *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`35e05f7`](https://github.com/tobymao/sqlglot/commit/35e05f758547f9d249ef77f383ddef698947179b) - make end transactions postgres specific closes [#1921](https://github.com/tobymao/sqlglot/pull/1921) *(commit by [@tobymao](https://github.com/tobymao))*
- [`aaee594`](https://github.com/tobymao/sqlglot/commit/aaee59457eca7255540c27a4aa13a7a4f87e6acc) - **teradata**: separate POST_EXPRESSION props from POST_INDEX *(PR [#1924](https://github.com/tobymao/sqlglot/pull/1924) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1923](undefined) opened by [@MarkBell920](https://github.com/MarkBell920)*
- [`a523c8b`](https://github.com/tobymao/sqlglot/commit/a523c8b507c7af393aa178dce4a6e337b9548e8e) - comments above limit *(commit by [@tobymao](https://github.com/tobymao))*
- [`e9f1cb5`](https://github.com/tobymao/sqlglot/commit/e9f1cb53b1b73f5698d5cce5728cfc92d2020375) - **spark,duckdb**: transpile TO_TIMESTAMP, MONTHS_BETWEEN correctly *(PR [#1929](https://github.com/tobymao/sqlglot/pull/1929) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1928](undefined) opened by [@richard-a-lott](https://github.com/richard-a-lott)*

### :recycle: Refactors
- [`726306e`](https://github.com/tobymao/sqlglot/commit/726306e9a77a700555b31fbe136e6438f711178f) - **scope**: rename _is_subquery_scope to _is_derived_table for clarity *(PR [#1912](https://github.com/tobymao/sqlglot/pull/1912) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.4.1] - 2023-07-11
### :bug: Bug Fixes
- [`461aeaf`](https://github.com/tobymao/sqlglot/commit/461aeaf794cf541cebc1a5cce727907f93d44119) - handle dots in table name *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.4.0] - 2023-07-11
### :boom: BREAKING CHANGES
- due to [`c511278`](https://github.com/tobymao/sqlglot/commit/c51127842ffbb5dbe02c33de3ba8e04b69e6ebc3) - table_name force quotes if unsafe identifiers *(commit by [@tobymao](https://github.com/tobymao))*:

  table_name force quotes if unsafe identifiers


### :sparkles: New Features
- [`21246fb`](https://github.com/tobymao/sqlglot/commit/21246fbd8e308d8113ee56ad58f542eaad1d823f) - Add FLOAT(n) mappings to Hive *(PR [#1896](https://github.com/tobymao/sqlglot/pull/1896) by [@dmoore247](https://github.com/dmoore247))*
- [`d68f844`](https://github.com/tobymao/sqlglot/commit/d68f844bf6f65c933dbf5d155faa3a88e93ec6e7) - **tsql**: insert output closes [#1901](https://github.com/tobymao/sqlglot/pull/1901) *(commit by [@tobymao](https://github.com/tobymao))*
- [`273daf9`](https://github.com/tobymao/sqlglot/commit/273daf96b19c2166a3d960e018153ef0748f97c0) - add returning as alias for postgres *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`900ad7e`](https://github.com/tobymao/sqlglot/commit/900ad7eddde7bc065e6a6ab46389077cfd43e353) - **mysql**: add support some logical operators *(PR [#1899](https://github.com/tobymao/sqlglot/pull/1899) by [@brosoul](https://github.com/brosoul))*
- [`c3b4b66`](https://github.com/tobymao/sqlglot/commit/c3b4b66b765af262b36774ff7205944b34b6e20c) - expansions of literals in select and where *(commit by [@tobymao](https://github.com/tobymao))*
- [`6ccb595`](https://github.com/tobymao/sqlglot/commit/6ccb5956568f60d154c66a63050e0edbfc2394ea) - postgres nested jsonb closes [#1903](https://github.com/tobymao/sqlglot/pull/1903) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a27521b`](https://github.com/tobymao/sqlglot/commit/a27521bb8a3491de0b85c86af336233c70710f72) - improve parsing / generation of REGEXP_EXTRACT *(PR [#1905](https://github.com/tobymao/sqlglot/pull/1905) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`3e8c6cb`](https://github.com/tobymao/sqlglot/commit/3e8c6cb1b27e7f4c3ecad0f9f3a9d6fb6237ba3f) - **oracle**: keep NCHAR as-is *(PR [#1908](https://github.com/tobymao/sqlglot/pull/1908) by [@mpf82](https://github.com/mpf82))*
- [`5eed17e`](https://github.com/tobymao/sqlglot/commit/5eed17e394f4698362de32c7e202e5db3b3db486) - use sql method to generate index name *(PR [#1909](https://github.com/tobymao/sqlglot/pull/1909) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`c511278`](https://github.com/tobymao/sqlglot/commit/c51127842ffbb5dbe02c33de3ba8e04b69e6ebc3) - table_name force quotes if unsafe identifiers *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`3b215ad`](https://github.com/tobymao/sqlglot/commit/3b215adc413772bc1af46a67e2410b38ad8872a2) - **hive**: cleanup handling of FLOAT(n) in the generator *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`69a69f6`](https://github.com/tobymao/sqlglot/commit/69a69f67d06ebf8d328eb30fd0ef7d717758a4c9) - show an 'unsupported' error when transpiling (+) *(PR [#1906](https://github.com/tobymao/sqlglot/pull/1906) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.3.0] - 2023-07-07
### :sparkles: New Features
- [`fafccf5`](https://github.com/tobymao/sqlglot/commit/fafccf5a8e21028ef3a678cf989332b4e7db6825) - **postgres**: add MONEY type, revert in Spark to use DECIMAL(15, 4) *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`24dda47`](https://github.com/tobymao/sqlglot/commit/24dda47dd85abab79cd5ea126262a8e00639ca6f) - improve transpilation of BigQuery's TO_HEX(MD5(..)) *(PR [#1897](https://github.com/tobymao/sqlglot/pull/1897) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`2ab8887`](https://github.com/tobymao/sqlglot/commit/2ab888786eea5b2c67446c8eb7779b85288bc07b) - **spark**: map MONEY type to a broader DECIMAL type *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`dd7e494`](https://github.com/tobymao/sqlglot/commit/dd7e494c022a724d76fb592bc67c41c18f3c94ec) - enforce function arg order *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`5cf9304`](https://github.com/tobymao/sqlglot/commit/5cf930420e0dd774ff871ecc36115caae452389d) - **optimizer**: add more tests, improve titles in qualify_tables.sql *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.2.0] - 2023-07-06
### :bug: Bug Fixes
- [`1f575db`](https://github.com/tobymao/sqlglot/commit/1f575db44f9c75c92fd5abf6c514c9933b1175b6) - **duckdb**: improve transpilation of BigQuery DATE function *(PR [#1895](https://github.com/tobymao/sqlglot/pull/1895) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.1.0] - 2023-07-04
### :sparkles: New Features
- [`b178e1c`](https://github.com/tobymao/sqlglot/commit/b178e1c3f48c2460c59d525a40363cbda488cad8) - dialect argument for parse_one *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`83db4c0`](https://github.com/tobymao/sqlglot/commit/83db4c0bc45cc34af7b7aa41153a9e44847c1f10) - **tsql**: rename EXTRACT to DATEPART closes [#1885](https://github.com/tobymao/sqlglot/pull/1885) *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5a84605`](https://github.com/tobymao/sqlglot/commit/5a84605324ad2aab5eb0e76016845d262b458cd3) - order by agg closes [#1887](https://github.com/tobymao/sqlglot/pull/1887) *(PR [#1889](https://github.com/tobymao/sqlglot/pull/1889) by [@tobymao](https://github.com/tobymao))*
- [`d1ad7da`](https://github.com/tobymao/sqlglot/commit/d1ad7da4446d58ef78f5ebfa4765818d06fa2c6b) - performance regression due to 6f80cc80 *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.0.0] - 2023-07-04
### :boom: BREAKING CHANGES
- due to [`df4448d`](https://github.com/tobymao/sqlglot/commit/df4448dcb8d82470a2312d845506e9a429411451) - use a dictionary for query modifier search *(commit by [@tobymao](https://github.com/tobymao))*:

  use a dictionary for query modifier search

- due to [`f747260`](https://github.com/tobymao/sqlglot/commit/f747260a66d1f59f26b697f3580b2ddc8c876bd3) - hashable args is now more efficient and identifiers no longer accomodate case insensitivity because that is dialect specific *(commit by [@tobymao](https://github.com/tobymao))*:

  hashable args is now more efficient and identifiers no longer accomodate case insensitivity because that is dialect specific


### :sparkles: New Features
- [`47d999c`](https://github.com/tobymao/sqlglot/commit/47d999ce7db944b33239df6c5021d776c7d49062) - **mysql**: add support for the MEMBER OF operator *(PR [#1872](https://github.com/tobymao/sqlglot/pull/1872) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1866](undefined) opened by [@brosoul](https://github.com/brosoul)*
- [`156afcd`](https://github.com/tobymao/sqlglot/commit/156afcd843c1be01b8d592ad5e1fe302184d3be8) - add the ability to parse nested joins implements [#1878](https://github.com/tobymao/sqlglot/pull/1878) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`8a19d7a`](https://github.com/tobymao/sqlglot/commit/8a19d7ae6e88801706899497089e41484ed5c2d2) - **mysql**: improve parsing of INSERT .. SELECT statement *(PR [#1871](https://github.com/tobymao/sqlglot/pull/1871) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1862](undefined) opened by [@brosoul](https://github.com/brosoul)*
- [`58e1683`](https://github.com/tobymao/sqlglot/commit/58e1683f2d7ba4962d80a3ef3a09b4f5f43ab30d) - **bigquery**: improve support for cast to timestamp with format, time zone *(PR [#1873](https://github.com/tobymao/sqlglot/pull/1873) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1870](undefined) opened by [@dskarbrevik](https://github.com/dskarbrevik)*
- [`0197119`](https://github.com/tobymao/sqlglot/commit/019711981ed47dc7a66539769526bba78a115467) - convert JSONArrayContains to a Func expression *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`f4fb1f4`](https://github.com/tobymao/sqlglot/commit/f4fb1f4a3ecd3ad1d91e55d7e58732b44759522c) - tsql datepart format casing closes [#1869](https://github.com/tobymao/sqlglot/pull/1869) *(commit by [@tobymao](https://github.com/tobymao))*
- [`fe69102`](https://github.com/tobymao/sqlglot/commit/fe69102b82591d13bc9d252234f243c57921b9a7) - duckdb date_trunc to time closes [#1875](https://github.com/tobymao/sqlglot/pull/1875) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a957388`](https://github.com/tobymao/sqlglot/commit/a9573884953671d1bb38a7a2c534753372cb2e96) - **mysql**: add support more kind for MEMBER OF content *(PR [#1880](https://github.com/tobymao/sqlglot/pull/1880) by [@brosoul](https://github.com/brosoul))*
- [`6f80cc8`](https://github.com/tobymao/sqlglot/commit/6f80cc8096b7ba5765cab4d6eb5bf99f12709ca8) - **parser**: handle chained table join with consecutive USING clauses *(PR [#1883](https://github.com/tobymao/sqlglot/pull/1883) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`82f8fff`](https://github.com/tobymao/sqlglot/commit/82f8fff767034b57659f3eedc0d8b28c13cef78b) - bigquery don't strip nested types *(commit by [@tobymao](https://github.com/tobymao))*
- [`cf12c8a`](https://github.com/tobymao/sqlglot/commit/cf12c8ae22d2885225bbccd5e69c45b2daaf6299) - python is literal warning *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`df4448d`](https://github.com/tobymao/sqlglot/commit/df4448dcb8d82470a2312d845506e9a429411451) - use a dictionary for query modifier search *(commit by [@tobymao](https://github.com/tobymao))*
- [`f747260`](https://github.com/tobymao/sqlglot/commit/f747260a66d1f59f26b697f3580b2ddc8c876bd3) - hashable args is now more efficient and identifiers no longer accomodate case insensitivity because that is dialect specific *(commit by [@tobymao](https://github.com/tobymao))*
- [`f621e85`](https://github.com/tobymao/sqlglot/commit/f621e85ea252daf651bda091eb38c7d83e3067e5) - remove unused line, no tests fail *(commit by [@tobymao](https://github.com/tobymao))*
- [`0114b6d`](https://github.com/tobymao/sqlglot/commit/0114b6d77f4adccc32098072ad8079729dcb42ee) - change to lambda that returns tuple *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.8.1] - 2023-06-30
### :bug: Bug Fixes
- [`154ece5`](https://github.com/tobymao/sqlglot/commit/154ece50d089bc1961cd20d9e424ccb32490379c) - don't normalize udf defs *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.8.0] - 2023-06-30
### :boom: BREAKING CHANGES
- due to [`fcf7dd0`](https://github.com/tobymao/sqlglot/commit/fcf7dd0504a123c3b550a5f7c070da0a90e90c25) - big query single quotes does not support line breaks *(commit by [@tobymao](https://github.com/tobymao))*:

  big query single quotes does not support line breaks


### :sparkles: New Features
- [`3800158`](https://github.com/tobymao/sqlglot/commit/38001582953423b6ea34b4694cf5715446e131f2) - datediff python executor *(commit by [@tobymao](https://github.com/tobymao))*
- [`2e1a2b8`](https://github.com/tobymao/sqlglot/commit/2e1a2b807250b4bdded90642cbbbaa3b6f9ee814) - **snowflake**: add support for GROUP BY ALL *(PR [#1864](https://github.com/tobymao/sqlglot/pull/1864) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`6e81ac6`](https://github.com/tobymao/sqlglot/commit/6e81ac6358fa5f842fc829a3108353ea7b50859e) - **makefile**: add rule to skip integration tests *(PR [#1865](https://github.com/tobymao/sqlglot/pull/1865) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`2911bbb`](https://github.com/tobymao/sqlglot/commit/2911bbba122f505cde2c31591510ed00af1af2db) - typo *(commit by [@tobymao](https://github.com/tobymao))*
- [`d6c1569`](https://github.com/tobymao/sqlglot/commit/d6c1569c7489087eca624d335e8b78ddbea00af6) - **executor**: allow non-projected aggregates in ORDER BY *(PR [#1863](https://github.com/tobymao/sqlglot/pull/1863) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1855](undefined) opened by [@stonyw](https://github.com/stonyw)*
- [`fcf7dd0`](https://github.com/tobymao/sqlglot/commit/fcf7dd0504a123c3b550a5f7c070da0a90e90c25) - big query single quotes does not support line breaks *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`d8eeda2`](https://github.com/tobymao/sqlglot/commit/d8eeda2f3c4b69b0d17962d7c500a23962d3cbd9) - move group by finalizer to simplify because that is who cares *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.7.7] - 2023-06-30
### :bug: Bug Fixes
- [`3005306`](https://github.com/tobymao/sqlglot/commit/30053066bf9909ff521e8ac28fd3063a91159a81) - qualify column in func with equality *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.7.6] - 2023-06-30
### :bug: Bug Fixes
- [`9919e62`](https://github.com/tobymao/sqlglot/commit/9919e62046967f1fcbc40a638ab6234bc6082c65) - make bigquery unnest unalias more comprehensive *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`15ac3c1`](https://github.com/tobymao/sqlglot/commit/15ac3c13f3af7a8bdc15a9631a932bd2d2a95332) - **executor**: get rid of 'running' set *(PR [#1861](https://github.com/tobymao/sqlglot/pull/1861) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v16.7.4] - 2023-06-29
### :sparkles: New Features
- [`08c3074`](https://github.com/tobymao/sqlglot/commit/08c3074eea09a2c1ba0d1114bfd05910e6c61b5c) - **bigquery**: support the full syntax of ANY_VALUE *(PR [#1860](https://github.com/tobymao/sqlglot/pull/1860) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1858](undefined) opened by [@lucia-vargas-a](https://github.com/lucia-vargas-a)*

### :bug: Bug Fixes
- [`0357d63`](https://github.com/tobymao/sqlglot/commit/0357d63c518d2efdcec585f344059ff082cf9429) - bigquery quoted udf project id *(commit by [@tobymao](https://github.com/tobymao))*
- [`28e1024`](https://github.com/tobymao/sqlglot/commit/28e10244d260e8fbc1b4358051158a5728efbd03) - group and order cannot replace with literals *(commit by [@tobymao](https://github.com/tobymao))*
- [`5dabb96`](https://github.com/tobymao/sqlglot/commit/5dabb96c1e98e345ecc2c93b6f75c8b2db639721) - alias snowflake timediff/timestampdiff to datediff closes [#1851](https://github.com/tobymao/sqlglot/pull/1851) *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`f837b17`](https://github.com/tobymao/sqlglot/commit/f837b178eb5a95682819b07f026b88b9ba615836) - cleanup schema normalize_table, avoiding double copy *(PR [#1853](https://github.com/tobymao/sqlglot/pull/1853) by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`1e76107`](https://github.com/tobymao/sqlglot/commit/1e7610706dc80a4c8d4f06fb13f1076c780371b7) - **snowflake**: fix tests *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v16.7.2] - 2023-06-28
### :bug: Bug Fixes
- [`eaed790`](https://github.com/tobymao/sqlglot/commit/eaed790b331d45cfec049bafd5251e4250c663a0) - workaround bigquery grouped alias with order *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.7.1] - 2023-06-28
### :sparkles: New Features
- [`95a4b70`](https://github.com/tobymao/sqlglot/commit/95a4b70146bfb82ef2beca263a6dd6612994d224) - **bigquery**: pushdown CTE column names *(PR [#1847](https://github.com/tobymao/sqlglot/pull/1847) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`f81dd26`](https://github.com/tobymao/sqlglot/commit/f81dd26df3dc00a511f6d4d461d83c2e844d9476) - **bigquery**: add support for casting to string w/ format *(PR [#1848](https://github.com/tobymao/sqlglot/pull/1848) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`898f1a2`](https://github.com/tobymao/sqlglot/commit/898f1a2ed4f575d11f7d8680c2d642c3d5b8320c) - add test case for bigquery table normalization *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.7.0] - 2023-06-28
### :boom: BREAKING CHANGES
- due to [`d72caf4`](https://github.com/tobymao/sqlglot/commit/d72caf49d1d2f516142d84ca12b4e3fc616a71a4) - bigquery udfs are case sensitive *(commit by [@tobymao](https://github.com/tobymao))*:

  bigquery udfs are case sensitive


### :bug: Bug Fixes
- [`ebe04bb`](https://github.com/tobymao/sqlglot/commit/ebe04bbbe1c039c9067cd461a5c57d6e66f4469e) - table name with dots *(commit by [@tobymao](https://github.com/tobymao))*
- [`e3c43f2`](https://github.com/tobymao/sqlglot/commit/e3c43f2b88deba0b1a0c41b91fb0ad6f3e0be0da) - redshift to ast incorrect *(commit by [@tobymao](https://github.com/tobymao))*
- [`d72caf4`](https://github.com/tobymao/sqlglot/commit/d72caf49d1d2f516142d84ca12b4e3fc616a71a4) - bigquery udfs are case sensitive *(commit by [@tobymao](https://github.com/tobymao))*
- [`7cb01a0`](https://github.com/tobymao/sqlglot/commit/7cb01a09c1897357905428b46f095f80cdfe4804) - **bigquery**: transpile explode projection to cross join unnest, clean up tests *(PR [#1844](https://github.com/tobymao/sqlglot/pull/1844) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`abdf34b`](https://github.com/tobymao/sqlglot/commit/abdf34b273acd0f1a33648912059dfd42104cc2f) - **bigquery**: STRING_AGG parsing bug *(PR [#1846](https://github.com/tobymao/sqlglot/pull/1846) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v16.6.0] - 2023-06-27
### :boom: BREAKING CHANGES
- due to [`71818f9`](https://github.com/tobymao/sqlglot/commit/71818f948b083f87c691a9b3b7cf38ffd6f34007) - bigquery normalize dot tables and aliases *(commit by [@tobymao](https://github.com/tobymao))*:

  bigquery normalize dot tables and aliases


### :bug: Bug Fixes
- [`b60e19b`](https://github.com/tobymao/sqlglot/commit/b60e19b7f1464f727cadfd8f51fc27c6bfd2e0a9) - spark clustered by dml *(commit by [@tobymao](https://github.com/tobymao))*
- [`40928b7`](https://github.com/tobymao/sqlglot/commit/40928b720dcc61cec29731e7530104fbfe696d3a) - full support for spark clustered by *(commit by [@tobymao](https://github.com/tobymao))*
- [`71818f9`](https://github.com/tobymao/sqlglot/commit/71818f948b083f87c691a9b3b7cf38ffd6f34007) - bigquery normalize dot tables and aliases *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.5.0] - 2023-06-27
### :boom: BREAKING CHANGES
- due to [`451dad2`](https://github.com/tobymao/sqlglot/commit/451dad22e7c7b6ca8f6ed5cd5cd17d350c22f8e4) - use alias for order by after group by closes [#1822](https://github.com/tobymao/sqlglot/pull/1822) *(commit by [@tobymao](https://github.com/tobymao))*:

  use alias for order by after group by closes #1822

- due to [`9de9667`](https://github.com/tobymao/sqlglot/commit/9de9667126a32826b7be4e905c60c7c5b038e0f3) - group by having closes [#1831](https://github.com/tobymao/sqlglot/pull/1831) *(commit by [@tobymao](https://github.com/tobymao))*:

  group by having closes #1831


### :sparkles: New Features
- [`5d5795d`](https://github.com/tobymao/sqlglot/commit/5d5795d5ac7812790e95befa54b5c2bc10757934) - **postgres**: improve transpilation of ELEMENT_AT *(PR [#1830](https://github.com/tobymao/sqlglot/pull/1830) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1829](undefined) opened by [@SudarshanVS](https://github.com/SudarshanVS)*
- [`763d25b`](https://github.com/tobymao/sqlglot/commit/763d25bca6d823c5f80e91eb53e86a0b6d52c9a9) - **mysql**: add support for SIGNED [INTEGER] and UNSIGNED [INTEGER] types *(PR [#1828](https://github.com/tobymao/sqlglot/pull/1828) by [@brosoul](https://github.com/brosoul))*

### :bug: Bug Fixes
- [`451dad2`](https://github.com/tobymao/sqlglot/commit/451dad22e7c7b6ca8f6ed5cd5cd17d350c22f8e4) - use alias for order by after group by closes [#1822](https://github.com/tobymao/sqlglot/pull/1822) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8aef4c3`](https://github.com/tobymao/sqlglot/commit/8aef4c3687637149a13c50bde5eeee36a518796c) - dont expand bq pseudocolumns in optimizer star expansion *(PR [#1826](https://github.com/tobymao/sqlglot/pull/1826) by [@z3z1ma](https://github.com/z3z1ma))*
- [`f7abc28`](https://github.com/tobymao/sqlglot/commit/f7abc2887d48808edd96a972f537a0232f2c635e) - **mysql**: convert (U)BIGINT to (UN)SIGNED in CAST expressions *(PR [#1832](https://github.com/tobymao/sqlglot/pull/1832) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`9de9667`](https://github.com/tobymao/sqlglot/commit/9de9667126a32826b7be4e905c60c7c5b038e0f3) - group by having closes [#1831](https://github.com/tobymao/sqlglot/pull/1831) *(commit by [@tobymao](https://github.com/tobymao))*
- [`cb0ac10`](https://github.com/tobymao/sqlglot/commit/cb0ac10bdb3fc5b071f7b46259b60de9d9553525) - unnest subqueries in executor closes [#1835](https://github.com/tobymao/sqlglot/pull/1835) *(commit by [@tobymao](https://github.com/tobymao))*
- [`32a86aa`](https://github.com/tobymao/sqlglot/commit/32a86aab329a7c62e914e0039354c6973cecc919) - **bigquery**: handle reserved keywords *(PR [#1839](https://github.com/tobymao/sqlglot/pull/1839) by [@serkef](https://github.com/serkef))*
- [`4de255c`](https://github.com/tobymao/sqlglot/commit/4de255c7f3ffd6218d234b32165cc2f40d0967e4) - interval precedence parsing bug *(PR [#1837](https://github.com/tobymao/sqlglot/pull/1837) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1834](undefined) opened by [@WSKINGS](https://github.com/WSKINGS)*
- [`e4d6ba5`](https://github.com/tobymao/sqlglot/commit/e4d6ba555d4b7de23a8ca0f4b8acd90d70b7342e) - remove group alias for spark 3 *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`19295cc`](https://github.com/tobymao/sqlglot/commit/19295cc5d0080883af183771512f8e8a4050eecd) - fix test *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.4.2] - 2023-06-23
### :sparkles: New Features
- [`91ebaf5`](https://github.com/tobymao/sqlglot/commit/91ebaf5c36780e68bed26dec5f57b63d831634ea) - **snowflake**: add support for BYTEINT type *(PR [#1819](https://github.com/tobymao/sqlglot/pull/1819) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1818](undefined) opened by [@criccomini](https://github.com/criccomini)*
- [`2367bfc`](https://github.com/tobymao/sqlglot/commit/2367bfc2d9e0a43ebf37e37cfc4711c9e243cb89) - make table_name more robust by quoting unsafe parts *(PR [#1820](https://github.com/tobymao/sqlglot/pull/1820) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5572e76`](https://github.com/tobymao/sqlglot/commit/5572e768ac1af31f743fb4a0fae5a1e73569f167) - execute LEFT and RIGHT *(PR [#1821](https://github.com/tobymao/sqlglot/pull/1821) by [@barakalon](https://github.com/barakalon))*


## [v16.4.1] - 2023-06-23
### :sparkles: New Features
- [`088e745`](https://github.com/tobymao/sqlglot/commit/088e745b83358080dacda2ade79ede5cbb09c99d) - **databricks**: add support for REPLACE WHERE in INSERT statement *(PR [#1817](https://github.com/tobymao/sqlglot/pull/1817) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`8079b50`](https://github.com/tobymao/sqlglot/commit/8079b50818d12cf1cceaea24bd81163cd834a27a) - **executor**: ensure IN clause can work with a single value *(PR [#1815](https://github.com/tobymao/sqlglot/pull/1815) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#892](undefined) opened by [@treysp](https://github.com/treysp)*


## [v16.4.0] - 2023-06-21
### :boom: BREAKING CHANGES
- due to [`1db023f`](https://github.com/tobymao/sqlglot/commit/1db023fb2135f28e09ddd757b3b16dfcf3454916) - simplify mypy type hints for parse_one *(PR [#1797](https://github.com/tobymao/sqlglot/pull/1797) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  simplify mypy type hints for parse_one (#1797)


### :sparkles: New Features
- [`15f6f26`](https://github.com/tobymao/sqlglot/commit/15f6f2648641a4cc8541ff12787af8176f2970b1) - support BigQuery GENERATE_ARRAY *(PR [#1800](https://github.com/tobymao/sqlglot/pull/1800) by [@r1b](https://github.com/r1b))*
- [`2f43629`](https://github.com/tobymao/sqlglot/commit/2f436299fbcf5f691ffc1eb8e7315fc1dbe2d3fc) - **bigquery**: support TO_JSON_STRING *(PR [#1802](https://github.com/tobymao/sqlglot/pull/1802) by [@r1b](https://github.com/r1b))*
- [`e62c50c`](https://github.com/tobymao/sqlglot/commit/e62c50c449e557f84fa3970c783fa72c44e10080) - add support for LIMIT clause in DELETE statement *(PR [#1804](https://github.com/tobymao/sqlglot/pull/1804) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`a2bf084`](https://github.com/tobymao/sqlglot/commit/a2bf0841117e1cad95568498e34dcbcb4f3fe24b) - **duckdb**: support TO_JSON *(PR [#1803](https://github.com/tobymao/sqlglot/pull/1803) by [@r1b](https://github.com/r1b))*
- [`b8d9a19`](https://github.com/tobymao/sqlglot/commit/b8d9a19007a3c6b055d027ed8fc94da60bcf626b) - add support for LIMIT clause in UPDATE statement *(PR [#1808](https://github.com/tobymao/sqlglot/pull/1808) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`2e67bf9`](https://github.com/tobymao/sqlglot/commit/2e67bf9c77b64708337fad28c4844183a3c203cb) - **teradata**: add support for the SELECT TOP N syntax *(PR [#1799](https://github.com/tobymao/sqlglot/pull/1799) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1798](undefined) opened by [@Srivatzn](https://github.com/Srivatzn)*
- [`6945b28`](https://github.com/tobymao/sqlglot/commit/6945b283566b33405a0640020da6754485415f51) - remove side on condition simplification *(commit by [@tobymao](https://github.com/tobymao))*
- [`d7c1e7d`](https://github.com/tobymao/sqlglot/commit/d7c1e7d53c952f9f08109e4bd82630f9fed2bc7b) - **snowflake**: add support for TOP <n> keyword *(commit by [@ftom](https://github.com/ftom))*

### :recycle: Refactors
- [`1db023f`](https://github.com/tobymao/sqlglot/commit/1db023fb2135f28e09ddd757b3b16dfcf3454916) - simplify mypy type hints for parse_one *(PR [#1797](https://github.com/tobymao/sqlglot/pull/1797) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1796](undefined) opened by [@pkit](https://github.com/pkit)*
- [`458f12d`](https://github.com/tobymao/sqlglot/commit/458f12d2a00936b0d240a458af83aa0cd0147bdd) - **hive**: improve transpilation of TO_JSON *(PR [#1809](https://github.com/tobymao/sqlglot/pull/1809) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`4da37aa`](https://github.com/tobymao/sqlglot/commit/4da37aa5caec407f66405b39f37d5dad057e66c2) - clean up some comments in helper.py *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`73cddc4`](https://github.com/tobymao/sqlglot/commit/73cddc4865271e4bacf40c92e38a8f211ff39ca7) - fix ANNOTATORS mypy type hint *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v16.3.1] - 2023-06-16
### :bug: Bug Fixes
- [`18db68c`](https://github.com/tobymao/sqlglot/commit/18db68c15e607884572adaae3dd6bd0c6c4bc582) - cluster/distribute/sort by for hive *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.3.0] - 2023-06-16
### :boom: BREAKING CHANGES
- due to [`038afc9`](https://github.com/tobymao/sqlglot/commit/038afc90b1f3fe261ea6ffb4d3654006bb4317fd) - switch presto tsords to cast timestamp -> date *(commit by [@tobymao](https://github.com/tobymao))*:

  switch presto tsords to cast timestamp -> date

- due to [`4084ba3`](https://github.com/tobymao/sqlglot/commit/4084ba322d8ae07620c69bf7586571d209e68917) - move normalization logic in Dialect, update case-sensitivity info *(PR [#1784](https://github.com/tobymao/sqlglot/pull/1784) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  move normalization logic in Dialect, update case-sensitivity info (#1784)


### :sparkles: New Features
- [`fc9afb3`](https://github.com/tobymao/sqlglot/commit/fc9afb3e3033bdf61d58592625f23c6f915370e0) - **snowflake**: add support for COPY GRANTS property *(PR [#1793](https://github.com/tobymao/sqlglot/pull/1793) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1791](undefined) opened by [@vedas2](https://github.com/vedas2)*

### :bug: Bug Fixes
- [`311380c`](https://github.com/tobymao/sqlglot/commit/311380c14b5f9627e0b5fea5a04e60d23c062dd9) - select as struct transpilation closes [#1788](https://github.com/tobymao/sqlglot/pull/1788) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1b62c0a`](https://github.com/tobymao/sqlglot/commit/1b62c0a3a4cbf3d5ac442e7c8fa9e462e3fff982) - **parser**: cast coalesce arg to text in the context of a CONCAT call *(PR [#1792](https://github.com/tobymao/sqlglot/pull/1792) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`d27e8f8`](https://github.com/tobymao/sqlglot/commit/d27e8f8ead98373fc5b699abaeb0a235efcf9f6e) - **schema**: ensure tables aren't normalized for BigQuery *(PR [#1794](https://github.com/tobymao/sqlglot/pull/1794) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`58fe190`](https://github.com/tobymao/sqlglot/commit/58fe1903166b88eddb82ceddd296c3363cd8a06d) - rawstring backslashes for bigquery *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`038afc9`](https://github.com/tobymao/sqlglot/commit/038afc90b1f3fe261ea6ffb4d3654006bb4317fd) - switch presto tsords to cast timestamp -> date *(commit by [@tobymao](https://github.com/tobymao))*
- [`4084ba3`](https://github.com/tobymao/sqlglot/commit/4084ba322d8ae07620c69bf7586571d209e68917) - move normalization logic in Dialect, update case-sensitivity info *(PR [#1784](https://github.com/tobymao/sqlglot/pull/1784) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v16.2.1] - 2023-06-16
### :boom: BREAKING CHANGES
- due to [`88249b8`](https://github.com/tobymao/sqlglot/commit/88249b825fbe446e3450a364ef8e108e8495767e) - cyclic joins in the optimizer *(PR [#1786](https://github.com/tobymao/sqlglot/pull/1786) by [@tobymao](https://github.com/tobymao))*:

  cyclic joins in the optimizer (#1786)


### :bug: Bug Fixes
- [`88249b8`](https://github.com/tobymao/sqlglot/commit/88249b825fbe446e3450a364ef8e108e8495767e) - cyclic joins in the optimizer *(PR [#1786](https://github.com/tobymao/sqlglot/pull/1786) by [@tobymao](https://github.com/tobymao))*
- [`f957a07`](https://github.com/tobymao/sqlglot/commit/f957a07468df3e9a30393cbcb9baafcb41ad0bc6) - overly aggressive cross join removal *(commit by [@tobymao](https://github.com/tobymao))*
- [`cacf8bf`](https://github.com/tobymao/sqlglot/commit/cacf8bf65dcb0dfc9c0b6339d84f2cfec9bcb46b) - build null types *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`d696d7f`](https://github.com/tobymao/sqlglot/commit/d696d7fe195a841d9418487627496054b0d4492f) - cleanup merge_subqueries *(commit by [@tobymao](https://github.com/tobymao))*
- [`8e1b6a7`](https://github.com/tobymao/sqlglot/commit/8e1b6a723fa48181314dcb867a04ff1346bab27b) - speed up executor tests *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.2.0] - 2023-06-15
### :bug: Bug Fixes
- [`b29a421`](https://github.com/tobymao/sqlglot/commit/b29a421843bc94d88e5f67dd787ee07a675d16ab) - parsing unknown into data type build *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`3233c73`](https://github.com/tobymao/sqlglot/commit/3233c73a4acb803e31143b3afe8aece7ef80313c) - mute logger *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.1.4] - 2023-06-15
### :bug: Bug Fixes
- [`4a1068b`](https://github.com/tobymao/sqlglot/commit/4a1068b51fcf6c9e49ec32c29345eac189d24ef2) - **Postgres**: Set INDEX_OFFSET to 1 *(PR [#1782](https://github.com/tobymao/sqlglot/pull/1782) by [@vegarsti](https://github.com/vegarsti))*
  - :arrow_lower_right: *fixes issue [#1781](undefined) opened by [@vegarsti](https://github.com/vegarsti)*
- [`f523dd6`](https://github.com/tobymao/sqlglot/commit/f523dd62f516a94cd69ecb51d864ee6aea45820a) - build uppercasing everything *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.1.3] - 2023-06-15
### :sparkles: New Features
- [`9660c33`](https://github.com/tobymao/sqlglot/commit/9660c331f7a7c4dac267b38ceffe16d33da69015) - add enum/set types to mysql closes [#1778](https://github.com/tobymao/sqlglot/pull/1778) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`fd0fc97`](https://github.com/tobymao/sqlglot/commit/fd0fc971393eaa9a61f48aead04eeea11d74c897) - bigquery timestamp -> timestamptz *(commit by [@tobymao](https://github.com/tobymao))*
- [`b86f7e8`](https://github.com/tobymao/sqlglot/commit/b86f7e8aced2fbb71aef8532073ede16810babe6) - dialect build *(commit by [@tobymao](https://github.com/tobymao))*


## [v16.1.1] - 2023-06-15
### :bug: Bug Fixes
- [`697c8b1`](https://github.com/tobymao/sqlglot/commit/697c8b13f4983a9e00110ae88ab7d58e5ba22e06) - **bigquery**: allow SPLIT call with 1 argument *(PR [#1770](https://github.com/tobymao/sqlglot/pull/1770) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`0796cdc`](https://github.com/tobymao/sqlglot/commit/0796cdc924fc525e819122c323713ec1570d0357) - join using struct *(commit by [@tobymao](https://github.com/tobymao))*
- [`b13d0b9`](https://github.com/tobymao/sqlglot/commit/b13d0b9faf36808f01354ba4d161fcec827bba92) - map "RETURNING" to its token in the base Tokenizer *(PR [#1773](https://github.com/tobymao/sqlglot/pull/1773) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1771](undefined) opened by [@LilyFoote](https://github.com/LilyFoote)*
- [`a2deee3`](https://github.com/tobymao/sqlglot/commit/a2deee38e7667f9b555edf18fd102472409a07d9) - **parser**: don't parse an alias for non-source UNNESTs *(PR [#1774](https://github.com/tobymao/sqlglot/pull/1774) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`0a1362b`](https://github.com/tobymao/sqlglot/commit/0a1362b8ca5e18c2bee2cc8a6ab8554ed9142a78) - bigquery regexp_extract closes [#1776](https://github.com/tobymao/sqlglot/pull/1776) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f84732e`](https://github.com/tobymao/sqlglot/commit/f84732eee1b09b481bdd67fa8d20de933463f06a) - bigquery timestamp mapping *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`1dbed85`](https://github.com/tobymao/sqlglot/commit/1dbed8595c43f4c7eef5ed835ba06e7430cdafef) - **optimizer**: make the type annotator more dry *(PR [#1777](https://github.com/tobymao/sqlglot/pull/1777) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v16.1.0] - 2023-06-13
### :sparkles: New Features
- [`a4934cb`](https://github.com/tobymao/sqlglot/commit/a4934cb6ea2bed3cc96d4207f20496a33881b83b) - add hint builder *(PR [#1758](https://github.com/tobymao/sqlglot/pull/1758) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`b87fa35`](https://github.com/tobymao/sqlglot/commit/b87fa35d00c578b672842e049b7e438bf233746e) - add copy flag to replace_tables *(commit by [@tobymao](https://github.com/tobymao))*
- [`6cfc873`](https://github.com/tobymao/sqlglot/commit/6cfc8732f02f3b34883fed98558a7f34b872d57d) - **snowflake**: add support for // comments *(PR [#1765](https://github.com/tobymao/sqlglot/pull/1765) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1763](undefined) opened by [@florian-ernst-alan](https://github.com/florian-ernst-alan)*

### :bug: Bug Fixes
- [`146e66a`](https://github.com/tobymao/sqlglot/commit/146e66acb91878ec6751b96935bd94ef643bd77a) - select x.update *(commit by [@tobymao](https://github.com/tobymao))*
- [`2b46782`](https://github.com/tobymao/sqlglot/commit/2b46782a2b302a616f48c42b2ea043c215321c5f) - json_object(*) closes [#1757](https://github.com/tobymao/sqlglot/pull/1757) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0264b43`](https://github.com/tobymao/sqlglot/commit/0264b4383e2f45a51d7c758758e918c8cf9dd4ed) - limit offset multi arg order *(commit by [@tobymao](https://github.com/tobymao))*
- [`4fcdb0f`](https://github.com/tobymao/sqlglot/commit/4fcdb0f003a543751a1b11cd63b0f36e719a7f3a) - **tokenizer**: improve tokenization of decimals ending in . *(PR [#1766](https://github.com/tobymao/sqlglot/pull/1766) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1764](undefined) opened by [@florian-ernst-alan](https://github.com/florian-ernst-alan)*
- [`35d960a`](https://github.com/tobymao/sqlglot/commit/35d960adebdd2fc2d96c2e6b00b4660870409a57) - **parser**: disallow no paren functions when parsing table parts *(PR [#1767](https://github.com/tobymao/sqlglot/pull/1767) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1762](undefined) opened by [@florian-ernst-alan](https://github.com/florian-ernst-alan)*
- [`5955b9e`](https://github.com/tobymao/sqlglot/commit/5955b9ece1c60c5d2bbfb247c990c29cdd093f17) - values inner alias snowflake closes [#1768](https://github.com/tobymao/sqlglot/pull/1768) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0a9cecb`](https://github.com/tobymao/sqlglot/commit/0a9cecbe6391949f1a86fce28fc05aeef940fd0d) - **Postgres**: Support UNNEST *(PR [#1761](https://github.com/tobymao/sqlglot/pull/1761) by [@vegarsti](https://github.com/vegarsti))*
  - :arrow_lower_right: *fixes issue [#1760](undefined) opened by [@vegarsti](https://github.com/vegarsti)*

### :recycle: Refactors
- [`46abf16`](https://github.com/tobymao/sqlglot/commit/46abf16af88bb1f1704f959cfb30dfa86fdfe636) - simplify list comprehension in hint parser *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v16.0.0] - 2023-06-12
### :boom: BREAKING CHANGES
- due to [`e00647a`](https://github.com/tobymao/sqlglot/commit/e00647af4b5998ee2c6799dd44be268a56dfde7c) - output name for parens *(commit by [@tobymao](https://github.com/tobymao))*:

  output name for parens

- due to [`2dd8cba`](https://github.com/tobymao/sqlglot/commit/2dd8cba03fea94b811ec6bf2c6ce0a60bc48744f) - misc. improvements in formatting, type hints, dialect class variables *(PR [#1750](https://github.com/tobymao/sqlglot/pull/1750) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  misc. improvements in formatting, type hints, dialect class variables (#1750)

- due to [`a233afa`](https://github.com/tobymao/sqlglot/commit/a233afa79a3f6ece1436f4950b04e2343346e4e8) - bigquery cast date format closes [#1753](https://github.com/tobymao/sqlglot/pull/1753) *(commit by [@tobymao](https://github.com/tobymao))*:

  bigquery cast date format closes #1753


### :sparkles: New Features
- [`99c41d9`](https://github.com/tobymao/sqlglot/commit/99c41d96b2afd41432ffb919caf918f3a36f612f) - **clickhouse**: support CREATE VIEW TO syntax *(PR [#1752](https://github.com/tobymao/sqlglot/pull/1752) by [@pkit](https://github.com/pkit))*
- [`e00647a`](https://github.com/tobymao/sqlglot/commit/e00647af4b5998ee2c6799dd44be268a56dfde7c) - output name for parens *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`48ad1f1`](https://github.com/tobymao/sqlglot/commit/48ad1f15a18ec1e1396e1e7c50abb746b58eaebf) - bigquery table with hyphen number *(commit by [@tobymao](https://github.com/tobymao))*
- [`68b9128`](https://github.com/tobymao/sqlglot/commit/68b9128993999cefc929ca1e7734464232bb5bf0) - index using closes [#1751](https://github.com/tobymao/sqlglot/pull/1751) *(commit by [@tobymao](https://github.com/tobymao))*
- [`55a14a3`](https://github.com/tobymao/sqlglot/commit/55a14a3df96699f32ed1dee8b12a6409aee02ddb) - selecting from table with same name as cte *(commit by [@tobymao](https://github.com/tobymao))*
- [`7000a6f`](https://github.com/tobymao/sqlglot/commit/7000a6f137aabb5d2d2417179501905f37810768) - presto offset limit order closes [#1754](https://github.com/tobymao/sqlglot/pull/1754) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1553bfa`](https://github.com/tobymao/sqlglot/commit/1553bfaf0e5f5859d557b241ce792ba66729c9fe) - count with multiple args closes [#1755](https://github.com/tobymao/sqlglot/pull/1755) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a233afa`](https://github.com/tobymao/sqlglot/commit/a233afa79a3f6ece1436f4950b04e2343346e4e8) - bigquery cast date format closes [#1753](https://github.com/tobymao/sqlglot/pull/1753) *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`2dd8cba`](https://github.com/tobymao/sqlglot/commit/2dd8cba03fea94b811ec6bf2c6ce0a60bc48744f) - misc. improvements in formatting, type hints, dialect class variables *(PR [#1750](https://github.com/tobymao/sqlglot/pull/1750) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`b3f9078`](https://github.com/tobymao/sqlglot/commit/b3f90784b0d85ff78d718d2d8231f75b0166fec7) - make schema get_type more lenient *(commit by [@tobymao](https://github.com/tobymao))*


## [v15.2.0] - 2023-06-09
### :boom: BREAKING CHANGES
- due to [`c6a540c`](https://github.com/tobymao/sqlglot/commit/c6a540c8d8b72f49472c0b1e6891c66e42ddaeb0) - store type dump so it is not reparsed *(commit by [@tobymao](https://github.com/tobymao))*:

  store type dump so it is not reparsed


### :sparkles: New Features
- [`e028d98`](https://github.com/tobymao/sqlglot/commit/e028d984cc5631c66aac5f42c29200410caca47e) - **redshift,presto**: transpile FROM_BASE to STRTOL and vice versa *(PR [#1744](https://github.com/tobymao/sqlglot/pull/1744) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1742](undefined) opened by [@pangyifish](https://github.com/pangyifish)*
- [`bb1f1a0`](https://github.com/tobymao/sqlglot/commit/bb1f1a035c8701c881f61c65742331cf7e667260) - **redshift,presto**: transpile DATEADD, DATEDIFF to presto *(PR [#1746](https://github.com/tobymao/sqlglot/pull/1746) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1745](undefined) opened by [@pangyifish](https://github.com/pangyifish)*

### :bug: Bug Fixes
- [`9b56fc9`](https://github.com/tobymao/sqlglot/commit/9b56fc9ca3229478ed3a7cc1f51857cee7f1ca2b) - add ts_or_ds to postgres *(commit by [@tobymao](https://github.com/tobymao))*
- [`0cc09cf`](https://github.com/tobymao/sqlglot/commit/0cc09cf9b39ae9c706c486444599e66123be3a05) - is true for presto closes [#1740](https://github.com/tobymao/sqlglot/pull/1740) *(commit by [@tobymao](https://github.com/tobymao))*
- [`6168fbf`](https://github.com/tobymao/sqlglot/commit/6168fbf450d47b06b730680c5f8383bd7460008e) - redshift len->length closes [#1741](https://github.com/tobymao/sqlglot/pull/1741) *(commit by [@tobymao](https://github.com/tobymao))*
- [`824fcb2`](https://github.com/tobymao/sqlglot/commit/824fcb2f0b481306bf0d3371facd3523bce3090d) - bigquery table with hyphen number *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`d2e46c3`](https://github.com/tobymao/sqlglot/commit/d2e46c3f7b68373a64bc909567ebdcbbd2ad4c76) - fix README example *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`c6a540c`](https://github.com/tobymao/sqlglot/commit/c6a540c8d8b72f49472c0b1e6891c66e42ddaeb0) - store type dump so it is not reparsed *(commit by [@tobymao](https://github.com/tobymao))*


## [v15.1.0] - 2023-06-07
### :boom: BREAKING CHANGES
- due to [`6ad00ca`](https://github.com/tobymao/sqlglot/commit/6ad00caed965be3d69ebed8c57fea0b1b05406d4) - convert left and right closes [#1733](https://github.com/tobymao/sqlglot/pull/1733) *(commit by [@tobymao](https://github.com/tobymao))*:

  convert left and right closes #1733


### :sparkles: New Features
- [`5867fc4`](https://github.com/tobymao/sqlglot/commit/5867fc4b3d0fda96c22826c2e094219c930c9c0c) - **postgres**: add support for all range/multirange types *(PR [#1718](https://github.com/tobymao/sqlglot/pull/1718) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1717](undefined) opened by [@orf](https://github.com/orf)*
- [`dd29f3f`](https://github.com/tobymao/sqlglot/commit/dd29f3fad46d8939aa9a263d73f6a9f1e5e3cdc7) - **presto**: transpile 'epoch' to '1970-01-01 00:00:00' in time-like casts *(PR [#1726](https://github.com/tobymao/sqlglot/pull/1726) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1725](undefined) opened by [@pangyifish](https://github.com/pangyifish)*

### :bug: Bug Fixes
- [`4f31a50`](https://github.com/tobymao/sqlglot/commit/4f31a50448ea4d84c602667b5c5ee9a788c33586) - clickhouse backslash str escape closes [#1719](https://github.com/tobymao/sqlglot/pull/1719) *(commit by [@tobymao](https://github.com/tobymao))*
- [`95f7ac7`](https://github.com/tobymao/sqlglot/commit/95f7ac7d7f046c123653f157923a284a7db18cb0) - **bigquery**: treat HASH as a reserved keyword *(PR [#1721](https://github.com/tobymao/sqlglot/pull/1721) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`cad14bd`](https://github.com/tobymao/sqlglot/commit/cad14bd6730f02fbd416783c66bdebed00ef63c2) - conditionally quote identifiers that start with a digit *(PR [#1729](https://github.com/tobymao/sqlglot/pull/1729) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1727](undefined) opened by [@vegarsti](https://github.com/vegarsti)*
- [`e058513`](https://github.com/tobymao/sqlglot/commit/e05851304f4c26d1eadc41603db63cbda53b1b88) - ensure pivot can be used as a table name *(PR [#1734](https://github.com/tobymao/sqlglot/pull/1734) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`be0de6e`](https://github.com/tobymao/sqlglot/commit/be0de6ecdce6c5e4772e138dec9f9e452822c92b) - window sql gen closes [#1739](https://github.com/tobymao/sqlglot/pull/1739) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`1eb338a`](https://github.com/tobymao/sqlglot/commit/1eb338a96d38e1fa186d396f714ee95900d79f31) - **optimizer**: fix pushdown_predicates comment example *(PR [#1732](https://github.com/tobymao/sqlglot/pull/1732) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`a30a828`](https://github.com/tobymao/sqlglot/commit/a30a828bab139a577b1180346ac717334cb09703) - cleanup identifer *(commit by [@tobymao](https://github.com/tobymao))*


## [v15.0.0] - 2023-06-02
### :sparkles: New Features
- [`24d44ad`](https://github.com/tobymao/sqlglot/commit/24d44ad400c1a7c3bb24a7d5b55368302a870d33) - **schema**: allow passing kwargs in ensure_schema *(PR [#1706](https://github.com/tobymao/sqlglot/pull/1706) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`1b1d9f2`](https://github.com/tobymao/sqlglot/commit/1b1d9f260c95d2e8815d8a7c039fb125e24b4134) - **mysql**: add support for the UNIQUE KEY constraint *(PR [#1708](https://github.com/tobymao/sqlglot/pull/1708) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1707](undefined) opened by [@RothNRK](https://github.com/RothNRK)*
- [`17dc0e1`](https://github.com/tobymao/sqlglot/commit/17dc0e140a9256e0e9ae544727f18beb379defdb) - **duckdb**: add support for simplified pivot syntax *(PR [#1714](https://github.com/tobymao/sqlglot/pull/1714) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1712](undefined) opened by [@csubhodeep](https://github.com/csubhodeep)*

### :bug: Bug Fixes
- [`ec7c863`](https://github.com/tobymao/sqlglot/commit/ec7c863064293487df5ba2b76ddfaf018aaa2556) - ensure maybe parse doesn't get none *(commit by [@tobymao](https://github.com/tobymao))*
- [`5f45e18`](https://github.com/tobymao/sqlglot/commit/5f45e1826f88a2eb8b25c9d09ee1b9fcc5499df4) - teradata partition order *(PR [#1696](https://github.com/tobymao/sqlglot/pull/1696) by [@tobymao](https://github.com/tobymao))*
- [`764ce6f`](https://github.com/tobymao/sqlglot/commit/764ce6fdbfcfeadc9486e92035262d186bce36ff) - clear errors on schema parse closes [#1698](https://github.com/tobymao/sqlglot/pull/1698) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a9e1483`](https://github.com/tobymao/sqlglot/commit/a9e1483851e960f2e0289f93feebebdb00b2199c) - Minor Dataframe cleanup *(PR [#1700](https://github.com/tobymao/sqlglot/pull/1700) by [@eakmanrq](https://github.com/eakmanrq))*
- [`264e9d7`](https://github.com/tobymao/sqlglot/commit/264e9d7d8bf25db60e3dec1f6a5bee528c5300d6) - make error message more robust *(commit by [@tobymao](https://github.com/tobymao))*
- [`da17c4d`](https://github.com/tobymao/sqlglot/commit/da17c4d54290f5ba4bfed5ccb716e5d6f4e3d2f9) - snowflake object_construct to struct closes [#1699](https://github.com/tobymao/sqlglot/pull/1699) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2792eaa`](https://github.com/tobymao/sqlglot/commit/2792eaa1c06be3f6ff53201a4111f6e4a145d572) - bigquery record -> struct *(commit by [@tobymao](https://github.com/tobymao))*
- [`6045b74`](https://github.com/tobymao/sqlglot/commit/6045b74cfa75f11ce1ac74319b1e9f8fef523f00) - allow type column ops for bigquery *(commit by [@tobymao](https://github.com/tobymao))*
- [`910166c`](https://github.com/tobymao/sqlglot/commit/910166c1d1d33e2110c26140e1916745dc2f1212) - set quote_identifiers in qualify, add normalize flag in schema *(PR [#1701](https://github.com/tobymao/sqlglot/pull/1701) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`611c234`](https://github.com/tobymao/sqlglot/commit/611c234b0bd0e100079011780f3f70eaf95aad01) - parse query modifiers for ddl selects *(PR [#1703](https://github.com/tobymao/sqlglot/pull/1703) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1702](undefined) opened by [@hsheth2](https://github.com/hsheth2)*
- [`6833823`](https://github.com/tobymao/sqlglot/commit/683382364fedb920497f3ea8d34dbb7f902d9803) - **duckdb**: transpile DATE_SUB into a subtraction *(PR [#1705](https://github.com/tobymao/sqlglot/pull/1705) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1704](undefined) opened by [@muscovitebob](https://github.com/muscovitebob)*
- [`dd5457c`](https://github.com/tobymao/sqlglot/commit/dd5457c9df533c3d26dbaabd828453609a224fae) - **schema**: ensure the correct dialect is used in schema methods *(PR [#1710](https://github.com/tobymao/sqlglot/pull/1710) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`92dbace`](https://github.com/tobymao/sqlglot/commit/92dbace2687308398d756d01637ede395a2ae35c) - interval preceding closes [#1715](https://github.com/tobymao/sqlglot/pull/1715) *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`12d3cca`](https://github.com/tobymao/sqlglot/commit/12d3cca8bab004dc378f6f272be7b07a7e16eaae) - **schema**: replace _ensure_table with exp.maybe_parse *(PR [#1709](https://github.com/tobymao/sqlglot/pull/1709) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5d6fbfe`](https://github.com/tobymao/sqlglot/commit/5d6fbfe00cafa0c73ab83beaf10d24aa5640e646) - factor out the the name sequence generation logic *(PR [#1716](https://github.com/tobymao/sqlglot/pull/1716) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`810522b`](https://github.com/tobymao/sqlglot/commit/810522bf53f3c78409a167fe37d72e6186a62e23) - fix return type of and_, or_ methods *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`c2c955c`](https://github.com/tobymao/sqlglot/commit/c2c955cfef86abfeee56ebb200abcff30a3da325) - make exception less broad *(commit by [@tobymao](https://github.com/tobymao))*
- [`e7abaef`](https://github.com/tobymao/sqlglot/commit/e7abaefec875a58288c366fe9a3e899c2bc6b4c1) - cleanup *(commit by [@tobymao](https://github.com/tobymao))*
- [`223c58d`](https://github.com/tobymao/sqlglot/commit/223c58d5c9845098983e94fcb2f525c61b634c4c) - fix tests *(commit by [@tobymao](https://github.com/tobymao))*


## [v14.1.1] - 2023-05-26
### :bug: Bug Fixes
- [`8f0fbad`](https://github.com/tobymao/sqlglot/commit/8f0fbad89c87741b022406256b68bc81e5887a42) - make map gen more robust *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`425af88`](https://github.com/tobymao/sqlglot/commit/425af8899143b19fb942789b81606f522c1e08c9) - cleanup key call *(commit by [@tobymao](https://github.com/tobymao))*
- [`57feaee`](https://github.com/tobymao/sqlglot/commit/57feaeee8159e119d7719ec554e42a8323015c2d) - types *(commit by [@tobymao](https://github.com/tobymao))*


## [v14.1.0] - 2023-05-26
### :boom: BREAKING CHANGES
- due to [`a6fdd59`](https://github.com/tobymao/sqlglot/commit/a6fdd59986a293be5b272a6e8f50f53482ddfd46) - improve python type hints *(PR [#1689](https://github.com/tobymao/sqlglot/pull/1689) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  improve python type hints (#1689)


### :bug: Bug Fixes
- [`964b04c`](https://github.com/tobymao/sqlglot/commit/964b04cf16cf5695008b697e2bde5e191a654877) - allow optimizer to handle non unionables *(commit by [@tobymao](https://github.com/tobymao))*
- [`7771609`](https://github.com/tobymao/sqlglot/commit/7771609202d6a53186ba02dd7f657bad6577c5e8) - redshift type *(commit by [@tobymao](https://github.com/tobymao))*
- [`543b565`](https://github.com/tobymao/sqlglot/commit/543b565a80915a3dd47b991779ed7d3e9c2bc81c) - raw strings bigquery escape closes [#1691](https://github.com/tobymao/sqlglot/pull/1691) *(PR [#1694](https://github.com/tobymao/sqlglot/pull/1694) by [@tobymao](https://github.com/tobymao))*
- [`fbf5f47`](https://github.com/tobymao/sqlglot/commit/fbf5f472fffa79538d1a58bfd1c1562796bafc4d) - create index with order closes [#1692](https://github.com/tobymao/sqlglot/pull/1692) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8465a77`](https://github.com/tobymao/sqlglot/commit/8465a777aa81c44625fdc2540797ae6615cb6edc) - **clickhouse**: allow aliases in tuple function arguments *(PR [#1695](https://github.com/tobymao/sqlglot/pull/1695) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1690](undefined) opened by [@cpcloud](https://github.com/cpcloud)*

### :wrench: Chores
- [`a6fdd59`](https://github.com/tobymao/sqlglot/commit/a6fdd59986a293be5b272a6e8f50f53482ddfd46) - improve python type hints *(PR [#1689](https://github.com/tobymao/sqlglot/pull/1689) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`6cce5fc`](https://github.com/tobymao/sqlglot/commit/6cce5fc7d216d2bf428324998aac171a8acd8564) - make bigquery hex less lenient *(commit by [@tobymao](https://github.com/tobymao))*


## [v14.0.0] - 2023-05-24
### :boom: BREAKING CHANGES
- due to [`267ea8f`](https://github.com/tobymao/sqlglot/commit/267ea8f494101db85def27672b2c5cdb7024c7d9) - cleanup unnecessary tokens *(PR [#1688](https://github.com/tobymao/sqlglot/pull/1688) by [@tobymao](https://github.com/tobymao))*:

  cleanup unnecessary tokens (#1688)

- due to [`e995ab0`](https://github.com/tobymao/sqlglot/commit/e995ab021735336fc085fe9061ba67a1d5c15f85) - use maybe_parse in exp.to_table, fix exp.Table expression parser *(PR [#1684](https://github.com/tobymao/sqlglot/pull/1684) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  use maybe_parse in exp.to_table, fix exp.Table expression parser (#1684)


### :sparkles: New Features
- [`a392114`](https://github.com/tobymao/sqlglot/commit/a392114ade6ac80059db4bef003647c64d1e2f50) - **databricks**: add support for GENERATED ALWAYS AS (expr) clause *(PR [#1686](https://github.com/tobymao/sqlglot/pull/1686) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1681](undefined) opened by [@bholzer](https://github.com/bholzer)*
- [`1cb9614`](https://github.com/tobymao/sqlglot/commit/1cb9614050d99267d86a2e08a0d5a509d33c617a) - implement transform to add column names to recursive CTEs *(PR [#1687](https://github.com/tobymao/sqlglot/pull/1687) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1683](undefined) opened by [@vegarsti](https://github.com/vegarsti)*

### :bug: Bug Fixes
- [`c567a0b`](https://github.com/tobymao/sqlglot/commit/c567a0b027d95712a90fc86762313499d2487a85) - **snowflake**: preserve TIME type instead of converting it to TIMESTAMP *(PR [#1685](https://github.com/tobymao/sqlglot/pull/1685) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1682](undefined) opened by [@wedotech-ashley](https://github.com/wedotech-ashley)*
- [`e995ab0`](https://github.com/tobymao/sqlglot/commit/e995ab021735336fc085fe9061ba67a1d5c15f85) - use maybe_parse in exp.to_table, fix exp.Table expression parser *(PR [#1684](https://github.com/tobymao/sqlglot/pull/1684) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`267ea8f`](https://github.com/tobymao/sqlglot/commit/267ea8f494101db85def27672b2c5cdb7024c7d9) - cleanup unnecessary tokens *(PR [#1688](https://github.com/tobymao/sqlglot/pull/1688) by [@tobymao](https://github.com/tobymao))*


## [v13.3.1] - 2023-05-23
### :sparkles: New Features
- [`ea130b4`](https://github.com/tobymao/sqlglot/commit/ea130b4f1479c986928ff6ea9c6f4762d07007d8) - add dot to executor closes [#1676](https://github.com/tobymao/sqlglot/pull/1676) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b875aa4`](https://github.com/tobymao/sqlglot/commit/b875aa462fc4a65df7498992553e359bde85e1d3) - **executor**: add strftime *(PR [#1679](https://github.com/tobymao/sqlglot/pull/1679) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`129931b`](https://github.com/tobymao/sqlglot/commit/129931bfc792fe3eb7f93935a56622e478beccd2) - **snowflake**: allow 2nd argument in initcap *(PR [#1670](https://github.com/tobymao/sqlglot/pull/1670) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1668](undefined) opened by [@b-per](https://github.com/b-per)*
- [`1a88b17`](https://github.com/tobymao/sqlglot/commit/1a88b17ce0b9d5c71d48f53eacab63e011dc170b) - **parser**: add asc, desc to id var tokens *(PR [#1671](https://github.com/tobymao/sqlglot/pull/1671) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1669](undefined) opened by [@b-per](https://github.com/b-per)*
- [`be5217d`](https://github.com/tobymao/sqlglot/commit/be5217d0f0f5cdeb2efb9035e533be96d78789d7) - **teradata**: improve post index property parsing *(PR [#1675](https://github.com/tobymao/sqlglot/pull/1675) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1674](undefined) opened by [@MarkBell920](https://github.com/MarkBell920)*
- [`903dde0`](https://github.com/tobymao/sqlglot/commit/903dde0edaec7085dffd872731030eb966079474) - replace between before normalization *(commit by [@tobymao](https://github.com/tobymao))*
- [`1de5684`](https://github.com/tobymao/sqlglot/commit/1de568485824ce8de715d76ac9932694461d3349) - **tokenizer**: initialize self._col properly to avoid edge case *(PR [#1678](https://github.com/tobymao/sqlglot/pull/1678) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`17be003`](https://github.com/tobymao/sqlglot/commit/17be003fef644d44f2b912517e0d2f42c3af818f) - start and end of tokens now respect quotes closes [#1677](https://github.com/tobymao/sqlglot/pull/1677) *(commit by [@tobymao](https://github.com/tobymao))*


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
[v13.3.1]: https://github.com/tobymao/sqlglot/compare/v13.3.0...v13.3.1
[v14.0.0]: https://github.com/tobymao/sqlglot/compare/v13.3.1...v14.0.0
[v14.1.0]: https://github.com/tobymao/sqlglot/compare/show...v14.1.0
[v14.1.1]: https://github.com/tobymao/sqlglot/compare/v14.1.0...v14.1.1
[v15.0.0]: https://github.com/tobymao/sqlglot/compare/v14.1.1...v15.0.0
[v15.1.0]: https://github.com/tobymao/sqlglot/compare/v15.0.0...v15.1.0
[v15.2.0]: https://github.com/tobymao/sqlglot/compare/v15.1.0...v15.2.0
[v15.3.0]: https://github.com/tobymao/sqlglot/compare/v15.2.0...v15.3.0
[v16.0.0]: https://github.com/tobymao/sqlglot/compare/v15.2.0...v16.0.0
[v16.1.0]: https://github.com/tobymao/sqlglot/compare/v16.0.0...v16.1.0
[v16.1.1]: https://github.com/tobymao/sqlglot/compare/v16.1.0...v16.1.1
[v16.1.3]: https://github.com/tobymao/sqlglot/compare/v16.1.2...v16.1.3
[v16.1.4]: https://github.com/tobymao/sqlglot/compare/v16.1.3...v16.1.4
[v16.2.0]: https://github.com/tobymao/sqlglot/compare/v16.1.4...v16.2.0
[v16.2.1]: https://github.com/tobymao/sqlglot/compare/v16.2.0...v16.2.1
[v16.3.0]: https://github.com/tobymao/sqlglot/compare/v16.2.1...v16.3.0
[v16.3.1]: https://github.com/tobymao/sqlglot/compare/v16.3.0...v16.3.1
[v16.4.0]: https://github.com/tobymao/sqlglot/compare/v16.3.1...v16.4.0
[v16.4.1]: https://github.com/tobymao/sqlglot/compare/v16.4.0...v16.4.1
[v16.4.2]: https://github.com/tobymao/sqlglot/compare/v16.4.1...v16.4.2
[v16.5.0]: https://github.com/tobymao/sqlglot/compare/v16.4.2...v16.5.0
[v16.6.0]: https://github.com/tobymao/sqlglot/compare/v16.5.0...v16.6.0
[v16.7.0]: https://github.com/tobymao/sqlglot/compare/v16.6.0...v16.7.0
[v16.7.1]: https://github.com/tobymao/sqlglot/compare/v16.7.0...v16.7.1
[v16.7.2]: https://github.com/tobymao/sqlglot/compare/v16.7.1...v16.7.2
[v16.7.4]: https://github.com/tobymao/sqlglot/compare/v16.7.3...v16.7.4
[v16.7.6]: https://github.com/tobymao/sqlglot/compare/v16.7.5...v16.7.6
[v16.7.7]: https://github.com/tobymao/sqlglot/compare/v16.7.6...v16.7.7
[v16.8.0]: https://github.com/tobymao/sqlglot/compare/v16.7.7...v16.8.0
[v16.8.1]: https://github.com/tobymao/sqlglot/compare/v16.8.0...v16.8.1
[v17.0.0]: https://github.com/tobymao/sqlglot/compare/v16.8.1...v17.0.0
[v17.1.0]: https://github.com/tobymao/sqlglot/compare/v17.0.0...v17.1.0
[v17.2.0]: https://github.com/tobymao/sqlglot/compare/v17.1.0...v17.2.0
[v17.3.0]: https://github.com/tobymao/sqlglot/compare/v17.2.0...v17.3.0
[v17.4.0]: https://github.com/tobymao/sqlglot/compare/v17.3.0...v17.4.0
[v17.4.1]: https://github.com/tobymao/sqlglot/compare/v17.4.0...v17.4.1
[v17.5.0]: https://github.com/tobymao/sqlglot/compare/v17.4.1...v17.5.0
[v17.6.0]: https://github.com/tobymao/sqlglot/compare/v17.5.0...v17.6.0
[v17.6.1]: https://github.com/tobymao/sqlglot/compare/v17.6.0...v17.6.1
[v17.7.0]: https://github.com/tobymao/sqlglot/compare/v17.6.1...v17.7.0
[v17.8.0]: https://github.com/tobymao/sqlglot/compare/v17.7.0...v17.8.0
[v17.8.1]: https://github.com/tobymao/sqlglot/compare/v17.8.0...v17.8.1