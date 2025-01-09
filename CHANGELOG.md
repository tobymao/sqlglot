Changelog
=========

## [v26.1.3] - 2025-01-09
### :bug: Bug Fixes
- [`d250846`](https://github.com/tobymao/sqlglot/commit/d250846d05711ac62a45efd4930f0ca712841b11) - **snowflake**: generate LIMIT when OFFSET exists [#4575](https://github.com/tobymao/sqlglot/pull/4575) *(PR [#4581](https://github.com/tobymao/sqlglot/pull/4581) by [@geooo109](https://github.com/geooo109))*

### :wrench: Chores
- [`ffbb935`](https://github.com/tobymao/sqlglot/commit/ffbb9350f8d0decab4555471ec2e468fa2741f5f) - install python 3.7 when building windows wheel for sqlglotrs *(PR [#4585](https://github.com/tobymao/sqlglot/pull/4585) by [@georgesittas](https://github.com/georgesittas))*
- [`1ea05c0`](https://github.com/tobymao/sqlglot/commit/1ea05c0b4e3cf53482058b22ecac7ec7c1de525d) - bump sqlglotrs to 0.3.4 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.1.2] - 2025-01-08
### :wrench: Chores
- [`e33af0b`](https://github.com/tobymao/sqlglot/commit/e33af0bcd859571dab68aef3a1fc9ecbf5c49e71) - try setup-python@v5 in the publish job *(PR [#4582](https://github.com/tobymao/sqlglot/pull/4582) by [@georgesittas](https://github.com/georgesittas))*
- [`3259f84`](https://github.com/tobymao/sqlglot/commit/3259f84f1faa6f1135ecca7d0f5fcd4b187b4da7) - bump sqlglotrs to 0.3.3 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.1.1] - 2025-01-08
### :wrench: Chores
- [`e51d1cf`](https://github.com/tobymao/sqlglot/commit/e51d1cfb0aa1028bb116851b03b759282305217b) - release sqlglotrs for Python 3.13 on windows *(PR [#4580](https://github.com/tobymao/sqlglot/pull/4580) by [@VaggelisD](https://github.com/VaggelisD))*
- [`975ffa0`](https://github.com/tobymao/sqlglot/commit/975ffa0e10f08243375e5e83384fd0e134730d14) - bump sqlglotrs to 0.3.2 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.1.0] - 2025-01-08
### :boom: BREAKING CHANGES
- due to [`07d05da`](https://github.com/tobymao/sqlglot/commit/07d05da95c7d3882a7032dade3cbeefbd96628b7) - normalize before qualifying tables *(PR [#4539](https://github.com/tobymao/sqlglot/pull/4539) by [@tobymao](https://github.com/tobymao))*:

  normalize before qualifying tables (#4539)

- due to [`cead7c3`](https://github.com/tobymao/sqlglot/commit/cead7c32bef44c0efaf48c2038976c7c7f2b709c) - require AS token in CTEs for all dialects except spark, databricks *(PR [#4546](https://github.com/tobymao/sqlglot/pull/4546) by [@georgesittas](https://github.com/georgesittas))*:

  require AS token in CTEs for all dialects except spark, databricks (#4546)

- due to [`231d032`](https://github.com/tobymao/sqlglot/commit/231d03202e4338ee097662d59770dae1a9958617) - support Unicode in sqlite, mysql, tsql, postgres, oracle *(PR [#4554](https://github.com/tobymao/sqlglot/pull/4554) by [@pruzko](https://github.com/pruzko))*:

  support Unicode in sqlite, mysql, tsql, postgres, oracle (#4554)

- due to [`83595b6`](https://github.com/tobymao/sqlglot/commit/83595b67f0aa4cafdfcf4bace7b92c17f9e9f5f3) - parse ASCII into Unicode to facilitate transpilation *(commit by [@georgesittas](https://github.com/georgesittas))*:

  parse ASCII into Unicode to facilitate transpilation

- due to [`e141960`](https://github.com/tobymao/sqlglot/commit/e1419607981cd8fe597781faeae429069b13d5fb) - improve transpilation of CHAR[ACTER]_LENGTH *(PR [#4555](https://github.com/tobymao/sqlglot/pull/4555) by [@pruzko](https://github.com/pruzko))*:

  improve transpilation of CHAR[ACTER]_LENGTH (#4555)


### :sparkles: New Features
- [`7a517d7`](https://github.com/tobymao/sqlglot/commit/7a517d71dbcab4b46538263cac604ac38e714e6b) - Introduce meta comment to parse known functions as exp.Anonymous *(PR [#4532](https://github.com/tobymao/sqlglot/pull/4532) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4522](https://github.com/tobymao/sqlglot/issues/4522) opened by [@github-christophe-oudar](https://github.com/github-christophe-oudar)*
- [`6992c18`](https://github.com/tobymao/sqlglot/commit/6992c1855f343a5d0120a3b4c993d8c406dd29ba) - **tokenizer**: Allow underscore separated number literals *(PR [#4536](https://github.com/tobymao/sqlglot/pull/4536) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4530](https://github.com/tobymao/sqlglot/issues/4530) opened by [@martijnthe](https://github.com/martijnthe)*
- [`7fe9f7f`](https://github.com/tobymao/sqlglot/commit/7fe9f7f6dbc701580ca17318400203245331704e) - **tsql**: add support for DATETRUNC [#4531](https://github.com/tobymao/sqlglot/pull/4531) *(PR [#4537](https://github.com/tobymao/sqlglot/pull/4537) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *addresses issue [#4531](https://github.com/tobymao/sqlglot/issues/4531) opened by [@rajat-wisdom](https://github.com/rajat-wisdom)*
- [`931eef6`](https://github.com/tobymao/sqlglot/commit/931eef6958be87ef88f4ff5311441e7a7004b8c5) - **duckdb**: Support simplified UNPIVOT statement *(PR [#4545](https://github.com/tobymao/sqlglot/pull/4545) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4542](https://github.com/tobymao/sqlglot/issues/4542) opened by [@Bilbottom](https://github.com/Bilbottom)*
- [`923a1f7`](https://github.com/tobymao/sqlglot/commit/923a1f7fda66f3dc61ee12755fe8960f8aeb3cd8) - treat NAMESPACE as a db creatable *(PR [#4556](https://github.com/tobymao/sqlglot/pull/4556) by [@TanviPardeshi](https://github.com/TanviPardeshi))*
- [`b0cc7d0`](https://github.com/tobymao/sqlglot/commit/b0cc7d029a78c7929daff9b30dc072608d9c80b0) - add support for IS_ASCII *(PR [#4557](https://github.com/tobymao/sqlglot/pull/4557) by [@pruzko](https://github.com/pruzko))*
- [`231d032`](https://github.com/tobymao/sqlglot/commit/231d03202e4338ee097662d59770dae1a9958617) - support Unicode in sqlite, mysql, tsql, postgres, oracle *(PR [#4554](https://github.com/tobymao/sqlglot/pull/4554) by [@pruzko](https://github.com/pruzko))*
- [`83595b6`](https://github.com/tobymao/sqlglot/commit/83595b67f0aa4cafdfcf4bace7b92c17f9e9f5f3) - **hive**: parse ASCII into Unicode to facilitate transpilation *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`095fb1e`](https://github.com/tobymao/sqlglot/commit/095fb1e834153eeeea33885dc20e1ba05f8bf814) - generate POSITION instead of STRPOS for Postgres *(PR [#4577](https://github.com/tobymao/sqlglot/pull/4577) by [@pruzko](https://github.com/pruzko))*
- [`9def0b7`](https://github.com/tobymao/sqlglot/commit/9def0b79ee623a07d8c367e0ec575ed8e63c83c6) - add support for Chr in tsql and sqlite *(PR [#4566](https://github.com/tobymao/sqlglot/pull/4566) by [@pruzko](https://github.com/pruzko))*
- [`d32d26a`](https://github.com/tobymao/sqlglot/commit/d32d26affaa7b0639abc107505db234aeb7386d4) - **postgres**: add support for XMLTABLE *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`e141960`](https://github.com/tobymao/sqlglot/commit/e1419607981cd8fe597781faeae429069b13d5fb) - improve transpilation of CHAR[ACTER]_LENGTH *(PR [#4555](https://github.com/tobymao/sqlglot/pull/4555) by [@pruzko](https://github.com/pruzko))*

### :bug: Bug Fixes
- [`07d05da`](https://github.com/tobymao/sqlglot/commit/07d05da95c7d3882a7032dade3cbeefbd96628b7) - normalize before qualifying tables *(PR [#4539](https://github.com/tobymao/sqlglot/pull/4539) by [@tobymao](https://github.com/tobymao))*
  - :arrow_lower_right: *fixes issue [#4538](https://github.com/tobymao/sqlglot/issues/4538) opened by [@karakanb](https://github.com/karakanb)*
- [`1ed3235`](https://github.com/tobymao/sqlglot/commit/1ed32358adc6b578e8b8af265ac8afe37aae9ad8) - allow When in exp.merge fixes [#4543](https://github.com/tobymao/sqlglot/pull/4543) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`cead7c3`](https://github.com/tobymao/sqlglot/commit/cead7c32bef44c0efaf48c2038976c7c7f2b709c) - **parser**: require AS token in CTEs for all dialects except spark, databricks *(PR [#4546](https://github.com/tobymao/sqlglot/pull/4546) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4544](https://github.com/tobymao/sqlglot/issues/4544) opened by [@xtess16](https://github.com/xtess16)*
- [`006b384`](https://github.com/tobymao/sqlglot/commit/006b3842f90186f8932f0dbf02453f138129608b) - **postgres**: add support for WHERE clause in INSERT DML *(PR [#4550](https://github.com/tobymao/sqlglot/pull/4550) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4549](https://github.com/tobymao/sqlglot/issues/4549) opened by [@VigneshChennai](https://github.com/VigneshChennai)*
- [`795e7e0`](https://github.com/tobymao/sqlglot/commit/795e7e0e857486417ce98246389849fc09ccb60a) - Pin ubuntu to 22.04 for Python 3.7 *(PR [#4571](https://github.com/tobymao/sqlglot/pull/4571) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2495508`](https://github.com/tobymao/sqlglot/commit/2495508fa7b3931d466c36b5ed225b2e1510b01c) - **tsql**: generate correct DateFromParts naming *(PR [#4563](https://github.com/tobymao/sqlglot/pull/4563) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4558](https://github.com/tobymao/sqlglot/issues/4558) opened by [@rajat-wisdom](https://github.com/rajat-wisdom)*
- [`2aff4ae`](https://github.com/tobymao/sqlglot/commit/2aff4ae861dc5225a616f5e3980cf04805e5b339) - **duckdb**: support parentheses with FROM-First syntax *(PR [#4569](https://github.com/tobymao/sqlglot/pull/4569) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#4561](https://github.com/tobymao/sqlglot/issues/4561) opened by [@LennartH](https://github.com/LennartH)*
- [`51ac9a7`](https://github.com/tobymao/sqlglot/commit/51ac9a7b8a91d1bb5b3b6b396e1083c03573a708) - **rust-tokenizer**: increase integer width when converting hex literals *(PR [#4573](https://github.com/tobymao/sqlglot/pull/4573) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4560](https://github.com/tobymao/sqlglot/issues/4560) opened by [@whummer](https://github.com/whummer)*
- [`94ffdb7`](https://github.com/tobymao/sqlglot/commit/94ffdb7b790c6c2235a0586c6df23c3155c184b1) - addressing mismatch in STR_POSITION argument order in executor. *(PR [#4574](https://github.com/tobymao/sqlglot/pull/4574) by [@cecilycarver](https://github.com/cecilycarver))*
- [`139b699`](https://github.com/tobymao/sqlglot/commit/139b699f61326bdf9700f0ba9bea9a44e594cf6d) - **tsql**: transpile snowflake TIMESTAMP_NTZ to DATETIME2 *(PR [#4576](https://github.com/tobymao/sqlglot/pull/4576) by [@geooo109](https://github.com/geooo109))*
- [`ceb42fa`](https://github.com/tobymao/sqlglot/commit/ceb42fabad60312699e4b15936aeebac00e22e4d) - parse & generate Length properly in clickhouse *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`9921528`](https://github.com/tobymao/sqlglot/commit/992152840ffad5fb85315e0bead2c498d4310cc5) - introduce benchmarking for rust *(PR [#4552](https://github.com/tobymao/sqlglot/pull/4552) by [@benfdking](https://github.com/benfdking))*
- [`0ffe8f9`](https://github.com/tobymao/sqlglot/commit/0ffe8f91eb8295ab8171e029aa4ccbf071028a4a) - temporarily disable sqlglotrs benchmarking *(PR [#4578](https://github.com/tobymao/sqlglot/pull/4578) by [@georgesittas](https://github.com/georgesittas))*
- [`7a0dbcf`](https://github.com/tobymao/sqlglot/commit/7a0dbcfda26ff7cf20371c84b31f454e63260959) - bump sqlglotrs to 0.3.1 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v26.0.1] - 2024-12-18
### :sparkles: New Features
- [`5d3ee4c`](https://github.com/tobymao/sqlglot/commit/5d3ee4cac1c5c9e45cbf6263c32c87fda78f9854) - **snowflake**: transpile date subtraction *(PR [#4506](https://github.com/tobymao/sqlglot/pull/4506) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4485](https://github.com/tobymao/sqlglot/issues/4485) opened by [@cisenbe](https://github.com/cisenbe)*
- [`efeb4bd`](https://github.com/tobymao/sqlglot/commit/efeb4bd870dd5c017b31d6b95c9bd6311c75b9ae) - **postgres**: add support for XMLELEMENT *(PR [#4513](https://github.com/tobymao/sqlglot/pull/4513) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4512](https://github.com/tobymao/sqlglot/issues/4512) opened by [@fresioAS](https://github.com/fresioAS)*
- [`e495777`](https://github.com/tobymao/sqlglot/commit/e495777b8612866041050c96d3df700cd829dc9c) - **clickhouse**: add support for bracket map syntax *(PR [#4528](https://github.com/tobymao/sqlglot/pull/4528) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4527](https://github.com/tobymao/sqlglot/issues/4527) opened by [@mrcljx](https://github.com/mrcljx)*
- [`cc44ed7`](https://github.com/tobymao/sqlglot/commit/cc44ed73fa4489e0bcb457b7eae8a9772415db65) - **mysql**: Support SERIAL data type *(PR [#4533](https://github.com/tobymao/sqlglot/pull/4533) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4529](https://github.com/tobymao/sqlglot/issues/4529) opened by [@AhlamHani](https://github.com/AhlamHani)*
- [`ee7dc96`](https://github.com/tobymao/sqlglot/commit/ee7dc966d533228756c3294c66422c27eceae503) - **starrocks**: add partition by range and unique key *(PR [#4509](https://github.com/tobymao/sqlglot/pull/4509) by [@pickfire](https://github.com/pickfire))*
- [`84ec478`](https://github.com/tobymao/sqlglot/commit/84ec47810e0a5c9e71a2b48e686656f9c2eafb39) - **lineage**: Extend lineage function to work with pivot operation *(PR [#4471](https://github.com/tobymao/sqlglot/pull/4471) by [@step4](https://github.com/step4))*
- [`52c8374`](https://github.com/tobymao/sqlglot/commit/52c8374876bc4037dcb81a50301fdd62cb14bb2a) - include comments in gen *(PR [#4535](https://github.com/tobymao/sqlglot/pull/4535) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`8f8e84a`](https://github.com/tobymao/sqlglot/commit/8f8e84ae81d60bea224e35b9ca88b0bb4a59512b) - **snowflake**: bitxor third parameter(padside) issue *(PR [#4501](https://github.com/tobymao/sqlglot/pull/4501) by [@ankur334](https://github.com/ankur334))*
- [`4760246`](https://github.com/tobymao/sqlglot/commit/476024653e5b942faaaaa2b3bce30a3ea1873190) - **snowflake**: generate only one INPUT => clause in unnest_sql *(PR [#4505](https://github.com/tobymao/sqlglot/pull/4505) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4503](https://github.com/tobymao/sqlglot/issues/4503) opened by [@harounp](https://github.com/harounp)*
- [`7649d50`](https://github.com/tobymao/sqlglot/commit/7649d5053e3305dadb83769bb5cec52ed8235a19) - **optimizer**: only expand stars for select scopes *(PR [#4515](https://github.com/tobymao/sqlglot/pull/4515) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4514](https://github.com/tobymao/sqlglot/issues/4514) opened by [@florian-ernst-alan](https://github.com/florian-ernst-alan)*
- [`2b68b9b`](https://github.com/tobymao/sqlglot/commit/2b68b9b7967b68465042a1b8c2ee21bb30007712) - **snowflake**: Allow alias expansion inside JOIN statements *(PR [#4504](https://github.com/tobymao/sqlglot/pull/4504) by [@florian-ernst-alan](https://github.com/florian-ernst-alan))*
  - :arrow_lower_right: *fixes issue [#4502](https://github.com/tobymao/sqlglot/issues/4502) opened by [@florian-ernst-alan](https://github.com/florian-ernst-alan)*
- [`e15cd0b`](https://github.com/tobymao/sqlglot/commit/e15cd0be1c66e0e72d9815575fa9b210e66cf7c9) - **postgres**: generate float if the type has precision *(PR [#4516](https://github.com/tobymao/sqlglot/pull/4516) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4508](https://github.com/tobymao/sqlglot/issues/4508) opened by [@RedTailedHawk](https://github.com/RedTailedHawk)*
- [`98906d4`](https://github.com/tobymao/sqlglot/commit/98906d4520a0c582a0534384ee3d0c1449846ee6) - another interval parsing edge case *(PR [#4519](https://github.com/tobymao/sqlglot/pull/4519) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4490](https://github.com/tobymao/sqlglot/issues/4490) opened by [@fuglaeff](https://github.com/fuglaeff)*
- [`992f6e9`](https://github.com/tobymao/sqlglot/commit/992f6e9fc867aa5ad60a255be593b8982a0fbcba) - **tsql**: Convert exp.Neg literal to number through to_py() *(PR [#4523](https://github.com/tobymao/sqlglot/pull/4523) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4520](https://github.com/tobymao/sqlglot/issues/4520) opened by [@DzianisKryvasheya](https://github.com/DzianisKryvasheya)*
- [`946cd42`](https://github.com/tobymao/sqlglot/commit/946cd4234a2ca403785b7c6a026a39ef604e8754) - **optimizer**: qualify snowflake queries with `level` pseudocolumn *(PR [#4524](https://github.com/tobymao/sqlglot/pull/4524) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4518](https://github.com/tobymao/sqlglot/issues/4518) opened by [@florian-ernst-alan](https://github.com/florian-ernst-alan)*
- [`bc68289`](https://github.com/tobymao/sqlglot/commit/bc68289d4d368b29241e56b8f0aefc36db65ad47) - **planner**: ensure aggregate variable is bound *(PR [#4526](https://github.com/tobymao/sqlglot/pull/4526) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4525](https://github.com/tobymao/sqlglot/issues/4525) opened by [@EyalDlph](https://github.com/EyalDlph)*

### :recycle: Refactors
- [`cd6e00f`](https://github.com/tobymao/sqlglot/commit/cd6e00f55195e26c3d02e255e66b45ab781addad) - clean up pivot lineage *(PR [#4534](https://github.com/tobymao/sqlglot/pull/4534) by [@georgesittas](https://github.com/georgesittas))*


## [v26.0.0] - 2024-12-10
### :boom: BREAKING CHANGES
- due to [`1d3c9aa`](https://github.com/tobymao/sqlglot/commit/1d3c9aa604c7bf60166a0e5587f1a8d88b89bea6) - Transpile support for bitor/bit_or snowflake function *(PR [#4486](https://github.com/tobymao/sqlglot/pull/4486) by [@ankur334](https://github.com/ankur334))*:

  Transpile support for bitor/bit_or snowflake function (#4486)

- due to [`ab10851`](https://github.com/tobymao/sqlglot/commit/ab108518c53173ddf71ac1dfd9e45df6ac621b81) - Preserve roundtrips of DATETIME/DATETIME2 *(PR [#4491](https://github.com/tobymao/sqlglot/pull/4491) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve roundtrips of DATETIME/DATETIME2 (#4491)


### :sparkles: New Features
- [`1d3c9aa`](https://github.com/tobymao/sqlglot/commit/1d3c9aa604c7bf60166a0e5587f1a8d88b89bea6) - **snowflake**: Transpile support for bitor/bit_or snowflake function *(PR [#4486](https://github.com/tobymao/sqlglot/pull/4486) by [@ankur334](https://github.com/ankur334))*
- [`822aea0`](https://github.com/tobymao/sqlglot/commit/822aea0826f09fa773193004acb2af99e495fddd) - **snowflake**: Support for inline FOREIGN KEY *(PR [#4493](https://github.com/tobymao/sqlglot/pull/4493) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4489](https://github.com/tobymao/sqlglot/issues/4489) opened by [@kylekarpack](https://github.com/kylekarpack)*

### :bug: Bug Fixes
- [`ab10851`](https://github.com/tobymao/sqlglot/commit/ab108518c53173ddf71ac1dfd9e45df6ac621b81) - **tsql**: Preserve roundtrips of DATETIME/DATETIME2 *(PR [#4491](https://github.com/tobymao/sqlglot/pull/4491) by [@VaggelisD](https://github.com/VaggelisD))*
- [`43975e4`](https://github.com/tobymao/sqlglot/commit/43975e4b7abcd640cd5a0f91aea1fbda8dd893cb) - **duckdb**: Allow escape strings similar to Postgres *(PR [#4497](https://github.com/tobymao/sqlglot/pull/4497) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4496](https://github.com/tobymao/sqlglot/issues/4496) opened by [@LennartH](https://github.com/LennartH)*


## [v25.34.1] - 2024-12-10
### :boom: BREAKING CHANGES
- due to [`f70f124`](https://github.com/tobymao/sqlglot/commit/f70f12408fbaf021dd105f2eac957b9e6fac045d) - transpile MySQL FORMAT to DuckDB *(PR [#4488](https://github.com/tobymao/sqlglot/pull/4488) by [@georgesittas](https://github.com/georgesittas))*:

  transpile MySQL FORMAT to DuckDB (#4488)


### :sparkles: New Features
- [`f70f124`](https://github.com/tobymao/sqlglot/commit/f70f12408fbaf021dd105f2eac957b9e6fac045d) - transpile MySQL FORMAT to DuckDB *(PR [#4488](https://github.com/tobymao/sqlglot/pull/4488) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4445](https://github.com/tobymao/sqlglot/issues/4445) opened by [@fanyang01](https://github.com/fanyang01)*
- [`5a276f3`](https://github.com/tobymao/sqlglot/commit/5a276f33df48dab96e77c560c4b193f9634974f7) - add parse into tuple *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`ddf7483`](https://github.com/tobymao/sqlglot/commit/ddf74833c3e033067e731eab387db658a9a803be) - enable python 3.13 in CI *(PR [#4483](https://github.com/tobymao/sqlglot/pull/4483) by [@simon-pactum](https://github.com/simon-pactum))*


## [v25.34.0] - 2024-12-06
### :boom: BREAKING CHANGES
- due to [`41c6d24`](https://github.com/tobymao/sqlglot/commit/41c6d24c99e130b3c8e35e348a25a59e9e3d5553) - Alias expanded USING STRUCT fields *(PR [#4474](https://github.com/tobymao/sqlglot/pull/4474) by [@VaggelisD](https://github.com/VaggelisD))*:

  Alias expanded USING STRUCT fields (#4474)


### :sparkles: New Features
- [`41c6d24`](https://github.com/tobymao/sqlglot/commit/41c6d24c99e130b3c8e35e348a25a59e9e3d5553) - **optimizer**: Alias expanded USING STRUCT fields *(PR [#4474](https://github.com/tobymao/sqlglot/pull/4474) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3465](https://github.com/TobikoData/sqlmesh/issues/3465) opened by [@esciara](https://github.com/esciara)*

### :bug: Bug Fixes
- [`a34bcde`](https://github.com/tobymao/sqlglot/commit/a34bcde1f7b4b2974a0132555477fa5a788126b4) - **bigquery**: properly consume dashed table parts *(PR [#4477](https://github.com/tobymao/sqlglot/pull/4477) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4476](https://github.com/tobymao/sqlglot/issues/4476) opened by [@matthewcyy](https://github.com/matthewcyy)*
- [`438ae4c`](https://github.com/tobymao/sqlglot/commit/438ae4c0691fb3ad43ef95e613118a116cb7924c) - **bigquery**: Do not generate NULL ordering on Windows *(PR [#4480](https://github.com/tobymao/sqlglot/pull/4480) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4478](https://github.com/tobymao/sqlglot/issues/4478) opened by [@goldmedal](https://github.com/goldmedal)*


## [v25.33.0] - 2024-12-04
### :boom: BREAKING CHANGES
- due to [`07fa69d`](https://github.com/tobymao/sqlglot/commit/07fa69dcb8970167ba0c55fff39175ab856ea9f3) - Make TIMESTAMP map to Type.TIMESTAMPTZ *(PR [#4451](https://github.com/tobymao/sqlglot/pull/4451) by [@VaggelisD](https://github.com/VaggelisD))*:

  Make TIMESTAMP map to Type.TIMESTAMPTZ (#4451)

- due to [`63d8f41`](https://github.com/tobymao/sqlglot/commit/63d8f41794b2e9d22f87d0a8fbfbd83125889ca2) - treat NEXT as a func keyword, parse NEXT VALUE FOR in tsql, oracle *(PR [#4467](https://github.com/tobymao/sqlglot/pull/4467) by [@georgesittas](https://github.com/georgesittas))*:

  treat NEXT as a func keyword, parse NEXT VALUE FOR in tsql, oracle (#4467)


### :sparkles: New Features
- [`3945acc`](https://github.com/tobymao/sqlglot/commit/3945acc4a0dfd58147de929c9a2c71734d8f1ade) - allow tables to be preserved in replace_table *(PR [#4468](https://github.com/tobymao/sqlglot/pull/4468) by [@georgesittas](https://github.com/georgesittas))*
- [`a9dca8d`](https://github.com/tobymao/sqlglot/commit/a9dca8dd1b523efd703003694d4389f9af9d1a12) - **postgres**: Support generated columns *(PR [#4472](https://github.com/tobymao/sqlglot/pull/4472) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4463](https://github.com/tobymao/sqlglot/issues/4463) opened by [@AKST](https://github.com/AKST)*

### :bug: Bug Fixes
- [`380dad2`](https://github.com/tobymao/sqlglot/commit/380dad2f5826caa820a69442c42805c7b3c23ada) - **bigquery**: Rename CONTAINS_SUBSTRING to CONTAINS_SUBSTR *(PR [#4457](https://github.com/tobymao/sqlglot/pull/4457) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4456](https://github.com/tobymao/sqlglot/issues/4456) opened by [@romanhaa](https://github.com/romanhaa)*
- [`ca5023d`](https://github.com/tobymao/sqlglot/commit/ca5023db5ea2a2ece804f6e389640e0bd4987598) - **presto**: Remove parentheses from CURRENT_USER *(PR [#4459](https://github.com/tobymao/sqlglot/pull/4459) by [@MikeWallis42](https://github.com/MikeWallis42))*
  - :arrow_lower_right: *fixes issue [#4458](https://github.com/tobymao/sqlglot/issues/4458) opened by [@MikeWallis42](https://github.com/MikeWallis42)*
- [`07fa69d`](https://github.com/tobymao/sqlglot/commit/07fa69dcb8970167ba0c55fff39175ab856ea9f3) - **spark**: Make TIMESTAMP map to Type.TIMESTAMPTZ *(PR [#4451](https://github.com/tobymao/sqlglot/pull/4451) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4442](https://github.com/tobymao/sqlglot/issues/4442) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`63d8f41`](https://github.com/tobymao/sqlglot/commit/63d8f41794b2e9d22f87d0a8fbfbd83125889ca2) - **parser**: treat NEXT as a func keyword, parse NEXT VALUE FOR in tsql, oracle *(PR [#4467](https://github.com/tobymao/sqlglot/pull/4467) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4466](https://github.com/tobymao/sqlglot/issues/4466) opened by [@Harmuth94](https://github.com/Harmuth94)*


## [v25.32.1] - 2024-11-27
### :bug: Bug Fixes
- [`954d8fd`](https://github.com/tobymao/sqlglot/commit/954d8fd12740071e0951d1df3a405a4b9634868d) - parse DEFAULT in VALUES clause into a Var *(PR [#4448](https://github.com/tobymao/sqlglot/pull/4448) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4446](https://github.com/tobymao/sqlglot/issues/4446) opened by [@ddh-5230](https://github.com/ddh-5230)*
- [`73afd0f`](https://github.com/tobymao/sqlglot/commit/73afd0f435b7e7ccde831ee311c9a76c14797fdc) - **bigquery**: Make JSONPathTokenizer more lenient for new standards *(PR [#4447](https://github.com/tobymao/sqlglot/pull/4447) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4441](https://github.com/tobymao/sqlglot/issues/4441) opened by [@patricksurry](https://github.com/patricksurry)*


## [v25.32.0] - 2024-11-22
### :boom: BREAKING CHANGES
- due to [`0eed45c`](https://github.com/tobymao/sqlglot/commit/0eed45cce82681bfbafc8bfb78eb2a1bce86ae53) - Add support for ATTACH/DETACH statements *(PR [#4419](https://github.com/tobymao/sqlglot/pull/4419) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for ATTACH/DETACH statements (#4419)

- due to [`da48b68`](https://github.com/tobymao/sqlglot/commit/da48b68a4f1fa6a754fa2a0a789564675d59546f) - Tokenize hints as comments *(PR [#4426](https://github.com/tobymao/sqlglot/pull/4426) by [@VaggelisD](https://github.com/VaggelisD))*:

  Tokenize hints as comments (#4426)

- due to [`fe35394`](https://github.com/tobymao/sqlglot/commit/fe3539464a153b1c0bf46975d6221dee48a48f02) - fix datetime coercion in the canonicalize rule *(PR [#4431](https://github.com/tobymao/sqlglot/pull/4431) by [@georgesittas](https://github.com/georgesittas))*:

  fix datetime coercion in the canonicalize rule (#4431)

- due to [`fddcd3d`](https://github.com/tobymao/sqlglot/commit/fddcd3dfc264a645909686c201d2288c0adf9047) - bump sqlglotrs to 0.3.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.3.0


### :sparkles: New Features
- [`0eed45c`](https://github.com/tobymao/sqlglot/commit/0eed45cce82681bfbafc8bfb78eb2a1bce86ae53) - **duckdb**: Add support for ATTACH/DETACH statements *(PR [#4419](https://github.com/tobymao/sqlglot/pull/4419) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2db757d`](https://github.com/tobymao/sqlglot/commit/2db757dfec9ded26572b8e9a71dcc8ea8a2382fe) - **bigquery**: Support FEATURES_AT_TIME *(PR [#4430](https://github.com/tobymao/sqlglot/pull/4430) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4428](https://github.com/tobymao/sqlglot/issues/4428) opened by [@YuvrajSoni-Ksolves](https://github.com/YuvrajSoni-Ksolves)*
- [`fc591ae`](https://github.com/tobymao/sqlglot/commit/fc591ae2fa80be5821cb53d78906afe8e5505654) - **risingwave**: add support for SINK, SOURCE & other DDL properties *(PR [#4387](https://github.com/tobymao/sqlglot/pull/4387) by [@lin0303-siyuan](https://github.com/lin0303-siyuan))*
- [`a2bde2e`](https://github.com/tobymao/sqlglot/commit/a2bde2e03e9ef8650756bf304db35b4876746d1f) - **mysql**: improve transpilability of CHAR[ACTER]_LENGTH *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`0acc248`](https://github.com/tobymao/sqlglot/commit/0acc248361f49f68f17d799cbaf6b3de06c57f7e) - **snowflake**: Support CREATE ... WITH TAG *(PR [#4434](https://github.com/tobymao/sqlglot/pull/4434) by [@asikowitz](https://github.com/asikowitz))*
  - :arrow_lower_right: *addresses issue [#4427](https://github.com/tobymao/sqlglot/issues/4427) opened by [@asikowitz](https://github.com/asikowitz)*
- [`37863ff`](https://github.com/tobymao/sqlglot/commit/37863ffd747cad9c2b9bed60119cc1551faeffda) - **snowflake**: Transpile non-UNNEST exp.GenerateDateArray refs *(PR [#4433](https://github.com/tobymao/sqlglot/pull/4433) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`83ee97b`](https://github.com/tobymao/sqlglot/commit/83ee97b34cd0fe269b4820f15147d1ed7523612e) - **parser**: Do not parse window function arg as exp.Column *(PR [#4415](https://github.com/tobymao/sqlglot/pull/4415) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4410](https://github.com/tobymao/sqlglot/issues/4410) opened by [@merlindso](https://github.com/merlindso)*
- [`b22e0c8`](https://github.com/tobymao/sqlglot/commit/b22e0c8680b0ee5a382e57904b698bf21a94f782) - **parser**: Extend DESCRIBE parser for MySQL FORMAT & statements *(PR [#4417](https://github.com/tobymao/sqlglot/pull/4417) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4414](https://github.com/tobymao/sqlglot/issues/4414) opened by [@AhlamHani](https://github.com/AhlamHani)*
- [`d1d2ae7`](https://github.com/tobymao/sqlglot/commit/d1d2ae7d1514abc9477d275352e5e126509157c6) - **duckdb**: Allow count arg on exp.ArgMax & exp.ArgMin *(PR [#4413](https://github.com/tobymao/sqlglot/pull/4413) by [@aersam](https://github.com/aersam))*
  - :arrow_lower_right: *fixes issue [#4412](https://github.com/tobymao/sqlglot/issues/4412) opened by [@aersam](https://github.com/aersam)*
- [`e3c45d5`](https://github.com/tobymao/sqlglot/commit/e3c45d5ec0ae6827e4b0bcfb047aeac131379732) - presto reset session closes [#4421](https://github.com/tobymao/sqlglot/pull/4421) *(commit by [@tobymao](https://github.com/tobymao))*
- [`fd81f1b`](https://github.com/tobymao/sqlglot/commit/fd81f1bfee9a566b8df8bb501828c20bd72ac481) - more presto commands *(commit by [@tobymao](https://github.com/tobymao))*
- [`da48b68`](https://github.com/tobymao/sqlglot/commit/da48b68a4f1fa6a754fa2a0a789564675d59546f) - Tokenize hints as comments *(PR [#4426](https://github.com/tobymao/sqlglot/pull/4426) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4425](https://github.com/tobymao/sqlglot/issues/4425) opened by [@mkmoisen](https://github.com/mkmoisen)*
- [`69d4a8c`](https://github.com/tobymao/sqlglot/commit/69d4a8ccdf5954f293acbdf61c420b72dde5b8af) - **tsql**: Map weekday to %w *(PR [#4438](https://github.com/tobymao/sqlglot/pull/4438) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4435](https://github.com/tobymao/sqlglot/issues/4435) opened by [@travispaice](https://github.com/travispaice)*
- [`41d6a13`](https://github.com/tobymao/sqlglot/commit/41d6a13ccfb28fbcf772fd43ea17da3b36567e67) - add return type *(PR [#4440](https://github.com/tobymao/sqlglot/pull/4440) by [@etonlels](https://github.com/etonlels))*
- [`fe35394`](https://github.com/tobymao/sqlglot/commit/fe3539464a153b1c0bf46975d6221dee48a48f02) - **optimizer**: fix datetime coercion in the canonicalize rule *(PR [#4431](https://github.com/tobymao/sqlglot/pull/4431) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4429](https://github.com/tobymao/sqlglot/issues/4429) opened by [@Ca1ypso](https://github.com/Ca1ypso)*
- [`6aea9f3`](https://github.com/tobymao/sqlglot/commit/6aea9f346ef8f91467e1d5da5a3f94cf862b44fe) - Refactor NORMALIZE_FUNCTIONS flag usage *(PR [#4437](https://github.com/tobymao/sqlglot/pull/4437) by [@VaggelisD](https://github.com/VaggelisD))*

### :recycle: Refactors
- [`f32a435`](https://github.com/tobymao/sqlglot/commit/f32a435205ec288f310ad57748ac66805e27f7f5) - **risingwave**: clean up SINK/SOURCE logic *(PR [#4432](https://github.com/tobymao/sqlglot/pull/4432) by [@georgesittas](https://github.com/georgesittas))*
- [`b24aced`](https://github.com/tobymao/sqlglot/commit/b24aced2dbb7e471d2dd0eb830ea4f2e24f9d267) - **snowflake**: clean up [WITH] TAG property / constraint *(PR [#4439](https://github.com/tobymao/sqlglot/pull/4439) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`fddcd3d`](https://github.com/tobymao/sqlglot/commit/fddcd3dfc264a645909686c201d2288c0adf9047) - bump sqlglotrs to 0.3.0 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.4] - 2024-11-17
### :bug: Bug Fixes
- [`59b8b6d`](https://github.com/tobymao/sqlglot/commit/59b8b6d1409b4112d425cc31db45519d5936b6fa) - preserve column quoting in DISTINCT ON elimination *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.3] - 2024-11-17
### :sparkles: New Features
- [`835e717`](https://github.com/tobymao/sqlglot/commit/835e71795f994599dbc19f1a5969b464154926e1) - **clickhouse**: transform function support *(PR [#4408](https://github.com/tobymao/sqlglot/pull/4408) by [@GaliFFun](https://github.com/GaliFFun))*

### :bug: Bug Fixes
- [`0479743`](https://github.com/tobymao/sqlglot/commit/047974393cebbddbbfb878071d159a3e538b0e4d) - **snowflake**: cast to TimeToStr arg to TIMESTAMP more conservatively *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.2] - 2024-11-17
### :bug: Bug Fixes
- [`d851269`](https://github.com/tobymao/sqlglot/commit/d851269780c7f0a0c756289c3dea9b1aa58d2a69) - use existing aliases in DISTINCT ON elimination, if any *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.1] - 2024-11-17
### :sparkles: New Features
- [`b00d857`](https://github.com/tobymao/sqlglot/commit/b00d857cd8a6d2452c2170077cbfa82352f708dd) - add support for specifying column in row_number function *(PR [#4406](https://github.com/tobymao/sqlglot/pull/4406) by [@GaliFFun](https://github.com/GaliFFun))*

### :bug: Bug Fixes
- [`0e46cc7`](https://github.com/tobymao/sqlglot/commit/0e46cc7fa2d80ba4e92182b3fa5f1075a63f4754) - refactor DISTINCT ON elimination transformation *(PR [#4407](https://github.com/tobymao/sqlglot/pull/4407) by [@georgesittas](https://github.com/georgesittas))*


## [v25.31.0] - 2024-11-16
### :boom: BREAKING CHANGES
- due to [`f4abfd5`](https://github.com/tobymao/sqlglot/commit/f4abfd59b8255cf8c39bf51028ee5f6ed704927f) - Support FORMAT_TIMESTAMP *(PR [#4383](https://github.com/tobymao/sqlglot/pull/4383) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support FORMAT_TIMESTAMP (#4383)

- due to [`45eef60`](https://github.com/tobymao/sqlglot/commit/45eef600064ad024b34e32e7acc3aca409fbd9c4) - use select star when eliminating distinct on *(PR [#4401](https://github.com/tobymao/sqlglot/pull/4401) by [@agrigoroi-palantir](https://github.com/agrigoroi-palantir))*:

  use select star when eliminating distinct on (#4401)


### :sparkles: New Features
- [`72ffdcb`](https://github.com/tobymao/sqlglot/commit/72ffdcb631bf7afdeda2ce96911442a94b7f11eb) - **bigquery**: Add parsing support for STRPOS(...) *(PR [#4378](https://github.com/tobymao/sqlglot/pull/4378) by [@VaggelisD](https://github.com/VaggelisD))*
- [`e7b67e0`](https://github.com/tobymao/sqlglot/commit/e7b67e0c280179188ce1bca650735978b758dca1) - **bigquery**: Support MAKE_INTERVAL *(PR [#4384](https://github.com/tobymao/sqlglot/pull/4384) by [@VaggelisD](https://github.com/VaggelisD))*
- [`37c4809`](https://github.com/tobymao/sqlglot/commit/37c4809dfda48224fd982ea8a48d3dbc5c17f9ae) - **bigquery**: Support INT64(...) *(PR [#4391](https://github.com/tobymao/sqlglot/pull/4391) by [@VaggelisD](https://github.com/VaggelisD))*
- [`9694999`](https://github.com/tobymao/sqlglot/commit/96949999d394e27df8b0287a14e9ac82d52bc0f9) - Add support for CONTAINS(...) *(PR [#4399](https://github.com/tobymao/sqlglot/pull/4399) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`f4abfd5`](https://github.com/tobymao/sqlglot/commit/f4abfd59b8255cf8c39bf51028ee5f6ed704927f) - **bigquery**: Support FORMAT_TIMESTAMP *(PR [#4383](https://github.com/tobymao/sqlglot/pull/4383) by [@VaggelisD](https://github.com/VaggelisD))*
- [`bb46ee3`](https://github.com/tobymao/sqlglot/commit/bb46ee33d481a888882cbbb26a9240dd2dbb10ee) - **parser**: Parse exp.Column for DROP COLUMN *(PR [#4390](https://github.com/tobymao/sqlglot/pull/4390) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4388](https://github.com/tobymao/sqlglot/issues/4388) opened by [@AhlamHani](https://github.com/AhlamHani)*
- [`79f6783`](https://github.com/tobymao/sqlglot/commit/79f67830d7d3ba92bff91eeb95b4dc8bdfa6c44e) - **snowflake**: Wrap DIV0 operands if they're binary expressions *(PR [#4393](https://github.com/tobymao/sqlglot/pull/4393) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4392](https://github.com/tobymao/sqlglot/issues/4392) opened by [@diogo-fernan](https://github.com/diogo-fernan)*
- [`647b98d`](https://github.com/tobymao/sqlglot/commit/647b98d84643b88a41218fb67f6a2bd83ca4c702) - **starrocks**: Add RESERVED_KEYWORDS specific to starrocks *(PR [#4402](https://github.com/tobymao/sqlglot/pull/4402) by [@notexistence](https://github.com/notexistence))*
- [`45eef60`](https://github.com/tobymao/sqlglot/commit/45eef600064ad024b34e32e7acc3aca409fbd9c4) - use select star when eliminating distinct on *(PR [#4401](https://github.com/tobymao/sqlglot/pull/4401) by [@agrigoroi-palantir](https://github.com/agrigoroi-palantir))*

### :recycle: Refactors
- [`a3af2af`](https://github.com/tobymao/sqlglot/commit/a3af2af3a893dfd6c6946b732aa086d1f1d91570) - attach stamement comments consistently *(PR [#4377](https://github.com/tobymao/sqlglot/pull/4377) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4376](https://github.com/tobymao/sqlglot/issues/4376) opened by [@YieldRay](https://github.com/YieldRay)*

### :wrench: Chores
- [`858c5b1`](https://github.com/tobymao/sqlglot/commit/858c5b1a43f74e11b8c357986c78b5068792b3af) - improve contribution guide *(PR [#4379](https://github.com/tobymao/sqlglot/pull/4379) by [@georgesittas](https://github.com/georgesittas))*
- [`160e688`](https://github.com/tobymao/sqlglot/commit/160e6883225cd6ad41a218213f73aa9f91b5fc5e) - fix relative benchmark import, comment out sqltree *(PR [#4403](https://github.com/tobymao/sqlglot/pull/4403) by [@georgesittas](https://github.com/georgesittas))*
- [`8d78add`](https://github.com/tobymao/sqlglot/commit/8d78addccaaffa4ea2dcfe1de002f8a653f137b7) - bump PYO3 to v"0.22.6" *(PR [#4400](https://github.com/tobymao/sqlglot/pull/4400) by [@MartinSahlen](https://github.com/MartinSahlen))*
- [`f78e755`](https://github.com/tobymao/sqlglot/commit/f78e755adaf52823642d2b0e1cae54da835ec653) - bump sqlglotrs to v0.2.14 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.30.0] - 2024-11-11
### :boom: BREAKING CHANGES
- due to [`60625ea`](https://github.com/tobymao/sqlglot/commit/60625eae34deb6a6fc36c0f3996f1281eae0ef6f) - Fix STRUCT cast generation *(PR [#4366](https://github.com/tobymao/sqlglot/pull/4366) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix STRUCT cast generation (#4366)


### :sparkles: New Features
- [`87ab8fe`](https://github.com/tobymao/sqlglot/commit/87ab8fe9cc4d6d060d8fe8a9c3faf8c47c2c9ed6) - **spark, bigquery**: Add support for UNIX_SECONDS(...) *(PR [#4350](https://github.com/tobymao/sqlglot/pull/4350) by [@VaggelisD](https://github.com/VaggelisD))*
- [`42da638`](https://github.com/tobymao/sqlglot/commit/42da63812ed489d1d8bbef0fc14c7dfa5ce57b7a) - **bigquery**: Support JSON_VALUE_ARRAY(...) *(PR [#4356](https://github.com/tobymao/sqlglot/pull/4356) by [@VaggelisD](https://github.com/VaggelisD))*
- [`e337a42`](https://github.com/tobymao/sqlglot/commit/e337a42dd56f5358e617750e7a70a0d4b7eab3f9) - **bigquery**: Parse REGEXP_SUBSTR as exp.RegexpExtract *(PR [#4358](https://github.com/tobymao/sqlglot/pull/4358) by [@VaggelisD](https://github.com/VaggelisD))*
- [`602dbf8`](https://github.com/tobymao/sqlglot/commit/602dbf84ce23f41fba6a87db70ecec6113044bac) - Support REGEXP_EXTRACT_ALL *(PR [#4359](https://github.com/tobymao/sqlglot/pull/4359) by [@VaggelisD](https://github.com/VaggelisD))*
- [`27a44a2`](https://github.com/tobymao/sqlglot/commit/27a44a22ff78cc35e8ab7c91b94311ef93d86c5a) - improve Levenshtein expression transpilation *(PR [#4360](https://github.com/tobymao/sqlglot/pull/4360) by [@krzysztof-kwitt](https://github.com/krzysztof-kwitt))*
- [`79c675a`](https://github.com/tobymao/sqlglot/commit/79c675a49fb44a6a7a97ea0de79822d8571724be) - **bigquery**: Support JSON_QUERY_ARRAY & JSON_EXTRACT_ARRAY *(PR [#4361](https://github.com/tobymao/sqlglot/pull/4361) by [@VaggelisD](https://github.com/VaggelisD))*
- [`57722db`](https://github.com/tobymao/sqlglot/commit/57722db90394d9a102c0e76a3e4d32a9f72f9ff9) - optionally wrap connectors when using builders *(PR [#4369](https://github.com/tobymao/sqlglot/pull/4369) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4362](https://github.com/tobymao/sqlglot/issues/4362) opened by [@gabrielteotonio](https://github.com/gabrielteotonio)*
  - :arrow_lower_right: *addresses issue [#4367](https://github.com/tobymao/sqlglot/issues/4367) opened by [@gabrielteotonio](https://github.com/gabrielteotonio)*

### :bug: Bug Fixes
- [`eb8e2fe`](https://github.com/tobymao/sqlglot/commit/eb8e2fe3ab3fb4b88f72843a5bd21f4a3c1d895c) - bubble up comments in qualified column refs fixes [#4353](https://github.com/tobymao/sqlglot/pull/4353) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`efcbfdb`](https://github.com/tobymao/sqlglot/commit/efcbfdb67b12853581fbfc0d4c4a450c0281849b) - **clickhouse**: Generate exp.Median as lowercase *(PR [#4355](https://github.com/tobymao/sqlglot/pull/4355) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4354](https://github.com/tobymao/sqlglot/issues/4354) opened by [@cpcloud](https://github.com/cpcloud)*
- [`60625ea`](https://github.com/tobymao/sqlglot/commit/60625eae34deb6a6fc36c0f3996f1281eae0ef6f) - **duckdb**: Fix STRUCT cast generation *(PR [#4366](https://github.com/tobymao/sqlglot/pull/4366) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4365](https://github.com/tobymao/sqlglot/issues/4365) opened by [@NickCrews](https://github.com/NickCrews)*
- [`a665030`](https://github.com/tobymao/sqlglot/commit/a665030323b200f3bed241bb928993b9807c4100) - safe removal while iterating expression list for multiple UNNEST expressions *(PR [#4364](https://github.com/tobymao/sqlglot/pull/4364) by [@gauravsagar483](https://github.com/gauravsagar483))*
- [`a71cee4`](https://github.com/tobymao/sqlglot/commit/a71cee4b4eafad9988b945c69dc75583ae105ec7) - Transpilation of exp.ArraySize from Postgres (read) *(PR [#4370](https://github.com/tobymao/sqlglot/pull/4370) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4368](https://github.com/tobymao/sqlglot/issues/4368) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`702fe31`](https://github.com/tobymao/sqlglot/commit/702fe318dadbe6cb83676e2a23ee830774697bb0) - Remove flaky timing test *(PR [#4371](https://github.com/tobymao/sqlglot/pull/4371) by [@VaggelisD](https://github.com/VaggelisD))*
- [`4d3904e`](https://github.com/tobymao/sqlglot/commit/4d3904e8906f0573f3352ad82282ea09c571daa8) - **spark**: Support DB's TIMESTAMP_DIFF *(PR [#4373](https://github.com/tobymao/sqlglot/pull/4373) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4372](https://github.com/tobymao/sqlglot/issues/4372) opened by [@nikmalviya](https://github.com/nikmalviya)*
- [`060ecfc`](https://github.com/tobymao/sqlglot/commit/060ecfc75fd8a07ffbc19f34959155a0fce317b6) - don't generate comments in table_name *(PR [#4375](https://github.com/tobymao/sqlglot/pull/4375) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`e19fb62`](https://github.com/tobymao/sqlglot/commit/e19fb620dbe6e405518aee381183e4640b638aa4) - improve error handling for unnest_to_explode *(PR [#4339](https://github.com/tobymao/sqlglot/pull/4339) by [@gauravsagar483](https://github.com/gauravsagar483))*


## [v25.29.0] - 2024-11-05
### :boom: BREAKING CHANGES
- due to [`e92904e`](https://github.com/tobymao/sqlglot/commit/e92904e61ab3b14fe18d472df19311f9b014f6cc) - Transpile ANY to EXISTS *(PR [#4305](https://github.com/tobymao/sqlglot/pull/4305) by [@VaggelisD](https://github.com/VaggelisD))*:

  Transpile ANY to EXISTS (#4305)

- due to [`23e620f`](https://github.com/tobymao/sqlglot/commit/23e620f7cd2860fbce45a5377a75ae0c8f031ce0) - Support MEDIAN() function *(PR [#4317](https://github.com/tobymao/sqlglot/pull/4317) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support MEDIAN() function (#4317)

- due to [`a093ae7`](https://github.com/tobymao/sqlglot/commit/a093ae750af8a351e54f1431deba1f2ce6843666) - always wrap value in NOT value IS ... *(PR [#4331](https://github.com/tobymao/sqlglot/pull/4331) by [@georgesittas](https://github.com/georgesittas))*:

  always wrap value in NOT value IS ... (#4331)

- due to [`84f78aa`](https://github.com/tobymao/sqlglot/commit/84f78aafd5d7e74da407167cd394d2bff0718cfb) - parse information schema views into a single identifier *(PR [#4336](https://github.com/tobymao/sqlglot/pull/4336) by [@georgesittas](https://github.com/georgesittas))*:

  parse information schema views into a single identifier (#4336)


### :sparkles: New Features
- [`efd9b4e`](https://github.com/tobymao/sqlglot/commit/efd9b4ed5a761a2ebfc47a1582e9d1b2eb7cb277) - **postgres**: Support JSONB_EXISTS *(PR [#4302](https://github.com/tobymao/sqlglot/pull/4302) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4299](https://github.com/tobymao/sqlglot/issues/4299) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`e92904e`](https://github.com/tobymao/sqlglot/commit/e92904e61ab3b14fe18d472df19311f9b014f6cc) - **spark**: Transpile ANY to EXISTS *(PR [#4305](https://github.com/tobymao/sqlglot/pull/4305) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4298](https://github.com/tobymao/sqlglot/issues/4298) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`2af4936`](https://github.com/tobymao/sqlglot/commit/2af4936bd9b318c695aae249324ff67bcd1292f6) - **snowflake**: Transpile BQ's TIMESTAMP() function *(PR [#4309](https://github.com/tobymao/sqlglot/pull/4309) by [@VaggelisD](https://github.com/VaggelisD))*
- [`50a1c91`](https://github.com/tobymao/sqlglot/commit/50a1c919d0d46384e3bd9ba1d45c24dd07efe6d2) - **snowflake**: Transpile exp.TimestampAdd *(PR [#4320](https://github.com/tobymao/sqlglot/pull/4320) by [@VaggelisD](https://github.com/VaggelisD))*
- [`01671ce`](https://github.com/tobymao/sqlglot/commit/01671ce137c9cf8d0f12dadc66e0db141f797d16) - **teradata**: add support for hexadecimal literals *(PR [#4323](https://github.com/tobymao/sqlglot/pull/4323) by [@thomascjohnson](https://github.com/thomascjohnson))*
- [`23e620f`](https://github.com/tobymao/sqlglot/commit/23e620f7cd2860fbce45a5377a75ae0c8f031ce0) - Support MEDIAN() function *(PR [#4317](https://github.com/tobymao/sqlglot/pull/4317) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4315](https://github.com/tobymao/sqlglot/issues/4315) opened by [@cpcloud](https://github.com/cpcloud)*
- [`9faef8d`](https://github.com/tobymao/sqlglot/commit/9faef8d1ceff91dd88db46b2c187d64f15490bf4) - **snowflake**: Transpile exp.TimestampSub *(PR [#4329](https://github.com/tobymao/sqlglot/pull/4329) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2d98cac`](https://github.com/tobymao/sqlglot/commit/2d98cacc723bc0c0df8ce11895983fb7cb9f5237) - **BigQuery**: Support JSON_VALUE() *(PR [#4332](https://github.com/tobymao/sqlglot/pull/4332) by [@VaggelisD](https://github.com/VaggelisD))*
- [`f8fec0a`](https://github.com/tobymao/sqlglot/commit/f8fec0ab098df37c3b54d91c24e5d8ec84f7cdbe) - **snowflake**: Transpile exp.DatetimeDiff *(PR [#4334](https://github.com/tobymao/sqlglot/pull/4334) by [@VaggelisD](https://github.com/VaggelisD))*
- [`16fd1ea`](https://github.com/tobymao/sqlglot/commit/16fd1ea2653a602bdc0d8b81e971fb1acadee585) - **BigQuery**: Support JSON_QUERY *(PR [#4333](https://github.com/tobymao/sqlglot/pull/4333) by [@VaggelisD](https://github.com/VaggelisD))*
- [`c09b6a2`](https://github.com/tobymao/sqlglot/commit/c09b6a2a37807795ead251f4fb81a9ba144cce27) - **duckdb**: support flags for RegexpExtract *(PR [#4326](https://github.com/tobymao/sqlglot/pull/4326) by [@NickCrews](https://github.com/NickCrews))*
- [`536973c`](https://github.com/tobymao/sqlglot/commit/536973cfc9d00110e388e8af1ed91d73607e07c2) - **trino**: add support for the ON OVERFLOW clause in LISTAGG *(PR [#4340](https://github.com/tobymao/sqlglot/pull/4340) by [@georgesittas](https://github.com/georgesittas))*
- [`4584935`](https://github.com/tobymao/sqlglot/commit/4584935cab328eced61c62a998cc013cab5cc3e3) - **snowflake**: Transpile exp.StrToDate *(PR [#4348](https://github.com/tobymao/sqlglot/pull/4348) by [@VaggelisD](https://github.com/VaggelisD))*
- [`71f4a47`](https://github.com/tobymao/sqlglot/commit/71f4a47910d5db97fa1a286891d72b5c4694d294) - **snowflake**: Transpile exp.DatetimeAdd *(PR [#4349](https://github.com/tobymao/sqlglot/pull/4349) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`551afff`](https://github.com/tobymao/sqlglot/commit/551afff58ea7bc1047775bfcd5d80b812fb3f682) - handle a Move edge case in the semantic differ *(PR [#4295](https://github.com/tobymao/sqlglot/pull/4295) by [@georgesittas](https://github.com/georgesittas))*
- [`a66e721`](https://github.com/tobymao/sqlglot/commit/a66e721dcd63488f7f3b427569a2115ae044c71b) - **generator**: Add NULL FILTER on ARRAY_AGG only for columns *(PR [#4301](https://github.com/tobymao/sqlglot/pull/4301) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4300](https://github.com/tobymao/sqlglot/issues/4300) opened by [@elad-sachs](https://github.com/elad-sachs)*
- [`b4ea602`](https://github.com/tobymao/sqlglot/commit/b4ea602ab17b0e8e85ddb090156c7bd2c6354de4) - **clickhouse**: improve parsing of WITH FILL ... INTERPOLATE *(PR [#4311](https://github.com/tobymao/sqlglot/pull/4311) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4310](https://github.com/tobymao/sqlglot/issues/4310) opened by [@brunorpinho](https://github.com/brunorpinho)*
- [`749886b`](https://github.com/tobymao/sqlglot/commit/749886b574a5dfa03aeb78b76d9cc097aa0f3e65) - **tsql**: Generate LOG(...) for exp.Ln *(PR [#4318](https://github.com/tobymao/sqlglot/pull/4318) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4316](https://github.com/tobymao/sqlglot/issues/4316) opened by [@cpcloud](https://github.com/cpcloud)*
- [`5c1b1f4`](https://github.com/tobymao/sqlglot/commit/5c1b1f43014967f6853752ba8d0899757a3efcd5) - **parser**: optionally parse a Stream expression *(PR [#4325](https://github.com/tobymao/sqlglot/pull/4325) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4324](https://github.com/tobymao/sqlglot/issues/4324) opened by [@lancewl](https://github.com/lancewl)*
- [`bb49a00`](https://github.com/tobymao/sqlglot/commit/bb49a00b16487356369bbb77aff9c2ff3f9cda52) - **oracle**: Do not normalize time units for exp.DateTrunc *(PR [#4328](https://github.com/tobymao/sqlglot/pull/4328) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4321](https://github.com/tobymao/sqlglot/issues/4321) opened by [@cpcloud](https://github.com/cpcloud)*
- [`a093ae7`](https://github.com/tobymao/sqlglot/commit/a093ae750af8a351e54f1431deba1f2ce6843666) - **clickhouse**: always wrap value in NOT value IS ... *(PR [#4331](https://github.com/tobymao/sqlglot/pull/4331) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4330](https://github.com/tobymao/sqlglot/issues/4330) opened by [@elchyn-cheliabiyeu](https://github.com/elchyn-cheliabiyeu)*
- [`def4f1e`](https://github.com/tobymao/sqlglot/commit/def4f1e3a9eac7545dfad223a5d49cee4fb7eeb8) - Refactor exp.RegexpExtract (follow up 4326) *(PR [#4341](https://github.com/tobymao/sqlglot/pull/4341) by [@VaggelisD](https://github.com/VaggelisD))*
- [`c1456d0`](https://github.com/tobymao/sqlglot/commit/c1456d07097c42a2ba2078ad30a8afe4cc89597d) - presto/trino current_time closes [#4344](https://github.com/tobymao/sqlglot/pull/4344) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8e16abe`](https://github.com/tobymao/sqlglot/commit/8e16abe2fed324b7ed6c718753cc623a8eb37814) - **duckdb**: we ALWAYS need to render group if params is present for RegexpExtract *(PR [#4343](https://github.com/tobymao/sqlglot/pull/4343) by [@NickCrews](https://github.com/NickCrews))*
- [`1689dc7`](https://github.com/tobymao/sqlglot/commit/1689dc7adbb913fe603b5e37eba29cc10d344cd2) - **bigquery**: Parse timezone for DATE_TRUNC *(PR [#4347](https://github.com/tobymao/sqlglot/pull/4347) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4346](https://github.com/tobymao/sqlglot/issues/4346) opened by [@CYFish](https://github.com/CYFish)*
- [`84f78aa`](https://github.com/tobymao/sqlglot/commit/84f78aafd5d7e74da407167cd394d2bff0718cfb) - **bigquery**: parse information schema views into a single identifier *(PR [#4336](https://github.com/tobymao/sqlglot/pull/4336) by [@georgesittas](https://github.com/georgesittas))*


## [v25.28.0] - 2024-10-25
### :boom: BREAKING CHANGES
- due to [`1691388`](https://github.com/tobymao/sqlglot/commit/16913887f5573f01eb8cd2b9336d4b37b84a449a) - Fix chained exp.SetOperation type annotation *(PR [#4274](https://github.com/tobymao/sqlglot/pull/4274) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix chained exp.SetOperation type annotation (#4274)

- due to [`c3c1997`](https://github.com/tobymao/sqlglot/commit/c3c199714df04edfe3698594680bac06575ca285) - Add support for STRING function *(PR [#4284](https://github.com/tobymao/sqlglot/pull/4284) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for STRING function (#4284)


### :sparkles: New Features
- [`379f487`](https://github.com/tobymao/sqlglot/commit/379f487080d95ef6e87cbbae8003541cde381ac0) - **bigquery**: transpile EDIT_DISTANCE, closes [#4283](https://github.com/tobymao/sqlglot/pull/4283) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`c3c1997`](https://github.com/tobymao/sqlglot/commit/c3c199714df04edfe3698594680bac06575ca285) - **bigquery**: Add support for STRING function *(PR [#4284](https://github.com/tobymao/sqlglot/pull/4284) by [@VaggelisD](https://github.com/VaggelisD))*
- [`1a26bff`](https://github.com/tobymao/sqlglot/commit/1a26bff619315a6e9dc3eab4dec07746b4820796) - **snowflake**: Transpile exp.SafeDivide *(PR [#4294](https://github.com/tobymao/sqlglot/pull/4294) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`ac66d2f`](https://github.com/tobymao/sqlglot/commit/ac66d2f4b94e6a984adbf3df01139b6378248158) - **clickhouse**: properly parse CREATE FUNCTION DDLs *(PR [#4282](https://github.com/tobymao/sqlglot/pull/4282) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3276](https://github.com/TobikoData/sqlmesh/issues/3276) opened by [@jwhitaker-gridcog](https://github.com/jwhitaker-gridcog)*
- [`1691388`](https://github.com/tobymao/sqlglot/commit/16913887f5573f01eb8cd2b9336d4b37b84a449a) - **optimizer**: Fix chained exp.SetOperation type annotation *(PR [#4274](https://github.com/tobymao/sqlglot/pull/4274) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4261](https://github.com/tobymao/sqlglot/issues/4261) opened by [@gabrielteotonio](https://github.com/gabrielteotonio)*
- [`559e7bc`](https://github.com/tobymao/sqlglot/commit/559e7bc5bbc77e94dea6de0470659b3c3fa6851f) - **clickhouse**: Wrap subquery if it's LHS of IS NOT NULL *(PR [#4287](https://github.com/tobymao/sqlglot/pull/4287) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4285](https://github.com/tobymao/sqlglot/issues/4285) opened by [@EugeneTorap](https://github.com/EugeneTorap)*
- [`47bc09a`](https://github.com/tobymao/sqlglot/commit/47bc09a85a3781682f5e58bfde5f453fb1a7c50b) - **sqlite**: Fix UNIQUE parsing *(PR [#4293](https://github.com/tobymao/sqlglot/pull/4293) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4291](https://github.com/tobymao/sqlglot/issues/4291) opened by [@tshu-w](https://github.com/tshu-w)*
- [`ee266ef`](https://github.com/tobymao/sqlglot/commit/ee266ef8f92fe72252eea36b56e8825715644a4f) - improve support for identifier delimiter escaping *(PR [#4288](https://github.com/tobymao/sqlglot/pull/4288) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`c6ff7f1`](https://github.com/tobymao/sqlglot/commit/c6ff7f1a0b6e443d80bc0f0ad1086d5c7b13b9f4) - bump sqlglotrs to v0.2.13 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.27.0] - 2024-10-22
### :boom: BREAKING CHANGES
- due to [`4d86499`](https://github.com/tobymao/sqlglot/commit/4d8649940d02ac319f2fec372a52674488f01de5) - include the target node for Move edits *(PR [#4277](https://github.com/tobymao/sqlglot/pull/4277) by [@georgesittas](https://github.com/georgesittas))*:

  include the target node for Move edits (#4277)

- due to [`9771965`](https://github.com/tobymao/sqlglot/commit/97719657d1b2074dabfbe54af0e1ea3acd6d4744) - Add support for TIMESTAMP_NTZ_FROM_PARTS *(PR [#4280](https://github.com/tobymao/sqlglot/pull/4280) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add support for TIMESTAMP_NTZ_FROM_PARTS (#4280)

- due to [`768adb3`](https://github.com/tobymao/sqlglot/commit/768adb3d85ed88931d761e5ecc8fb4a3a40d0dc5) - time string literals containing fractional seconds *(PR [#4269](https://github.com/tobymao/sqlglot/pull/4269) by [@treysp](https://github.com/treysp))*:

  time string literals containing fractional seconds (#4269)


### :sparkles: New Features
- [`9771965`](https://github.com/tobymao/sqlglot/commit/97719657d1b2074dabfbe54af0e1ea3acd6d4744) - **snowflake**: Add support for TIMESTAMP_NTZ_FROM_PARTS *(PR [#4280](https://github.com/tobymao/sqlglot/pull/4280) by [@VaggelisD](https://github.com/VaggelisD))*
- [`9e11654`](https://github.com/tobymao/sqlglot/commit/9e11654c6ebf7451f14d46c006070effe452519a) - **clickhouse**: add geometry types *(PR [#4278](https://github.com/tobymao/sqlglot/pull/4278) by [@jwhitaker-gridcog](https://github.com/jwhitaker-gridcog))*

### :bug: Bug Fixes
- [`c25a9ab`](https://github.com/tobymao/sqlglot/commit/c25a9ab577d7f0a1056e8afab680ca7801c47fff) - **tsql**: Keep CTE's attached to the query when emulating IF NOT EXISTS *(PR [#4279](https://github.com/tobymao/sqlglot/pull/4279) by [@erindru](https://github.com/erindru))*
- [`768adb3`](https://github.com/tobymao/sqlglot/commit/768adb3d85ed88931d761e5ecc8fb4a3a40d0dc5) - **clickhouse**: time string literals containing fractional seconds *(PR [#4269](https://github.com/tobymao/sqlglot/pull/4269) by [@treysp](https://github.com/treysp))*

### :recycle: Refactors
- [`4d86499`](https://github.com/tobymao/sqlglot/commit/4d8649940d02ac319f2fec372a52674488f01de5) - **diff**: include the target node for Move edits *(PR [#4277](https://github.com/tobymao/sqlglot/pull/4277) by [@georgesittas](https://github.com/georgesittas))*


## [v25.26.0] - 2024-10-21
### :boom: BREAKING CHANGES
- due to [`142c3e7`](https://github.com/tobymao/sqlglot/commit/142c3e75b25374ba9259f21b51cd728bbeb280ef) - Support TO_DOUBLE function *(PR [#4255](https://github.com/tobymao/sqlglot/pull/4255) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support TO_DOUBLE function (#4255)

- due to [`13d0696`](https://github.com/tobymao/sqlglot/commit/13d06966a2ca9264f35d5a58e1eaff1baa7dc66e) - Support TRY_TO_TIMESTAMP function *(PR [#4257](https://github.com/tobymao/sqlglot/pull/4257) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support TRY_TO_TIMESTAMP function (#4257)

- due to [`7fc0055`](https://github.com/tobymao/sqlglot/commit/7fc0055fb04713ba047baa5eda1ce0baf1cc79e2) - dont parse right-hand side operands of ARRAY JOIN as Tables *(PR [#4258](https://github.com/tobymao/sqlglot/pull/4258) by [@georgesittas](https://github.com/georgesittas))*:

  dont parse right-hand side operands of ARRAY JOIN as Tables (#4258)

- due to [`222152e`](https://github.com/tobymao/sqlglot/commit/222152e32521dbc6de3384b18ab4c677239c6088) - Add type hints for optimizer rules eliminate & merge subqueries *(PR [#4267](https://github.com/tobymao/sqlglot/pull/4267) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add type hints for optimizer rules eliminate & merge subqueries (#4267)


### :sparkles: New Features
- [`6f32e53`](https://github.com/tobymao/sqlglot/commit/6f32e5348d9aeba9c5d51a892023b2e14e072119) - support non-strict qualify_columns *(PR [#4243](https://github.com/tobymao/sqlglot/pull/4243) by [@hsheth2](https://github.com/hsheth2))*
- [`ed97954`](https://github.com/tobymao/sqlglot/commit/ed97954ecd7c2d7d4fe1bbf2ec0ecc000dd02b32) - **duckdb**: Transpile Spark's LATERAL VIEW EXPLODE *(PR [#4252](https://github.com/tobymao/sqlglot/pull/4252) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4247](https://github.com/tobymao/sqlglot/issues/4247) opened by [@idanyadgar-clutch](https://github.com/idanyadgar-clutch)*
- [`8f5efc7`](https://github.com/tobymao/sqlglot/commit/8f5efc7bc01ba5923584cd6ef38a4d81e763ccae) - **oracle**: parse hints  *(PR [#4249](https://github.com/tobymao/sqlglot/pull/4249) by [@mkmoisen](https://github.com/mkmoisen))*
- [`8b7ff5e`](https://github.com/tobymao/sqlglot/commit/8b7ff5ee8713a3ba50c48addd3700927a0240cf5) - **starrocks**: support for ALTER TABLE SWAP WITH *(PR [#4256](https://github.com/tobymao/sqlglot/pull/4256) by [@mrhamburg](https://github.com/mrhamburg))*
- [`1c43348`](https://github.com/tobymao/sqlglot/commit/1c433487a45379298ef27b3688723df2bd740fd1) - **trino**: Support for LISTAGG function *(PR [#4253](https://github.com/tobymao/sqlglot/pull/4253) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4250](https://github.com/tobymao/sqlglot/issues/4250) opened by [@npochhi](https://github.com/npochhi)*
- [`142c3e7`](https://github.com/tobymao/sqlglot/commit/142c3e75b25374ba9259f21b51cd728bbeb280ef) - **snowflake**: Support TO_DOUBLE function *(PR [#4255](https://github.com/tobymao/sqlglot/pull/4255) by [@VaggelisD](https://github.com/VaggelisD))*
- [`13d0696`](https://github.com/tobymao/sqlglot/commit/13d06966a2ca9264f35d5a58e1eaff1baa7dc66e) - **snowflake**: Support TRY_TO_TIMESTAMP function *(PR [#4257](https://github.com/tobymao/sqlglot/pull/4257) by [@VaggelisD](https://github.com/VaggelisD))*
- [`04dccf3`](https://github.com/tobymao/sqlglot/commit/04dccf3cdaf1c3a0466dda113aba5439f1639ae0) - **tsql**: Support for stored procedure options *(PR [#4260](https://github.com/tobymao/sqlglot/pull/4260) by [@rsanchez-xtillion](https://github.com/rsanchez-xtillion))*
- [`36f6841`](https://github.com/tobymao/sqlglot/commit/36f68416b3dd0d9ac703dd926d1f74bc43566e0d) - **bigquery**: support EDIT_DISTANCE (Levinshtein) function *(PR [#4276](https://github.com/tobymao/sqlglot/pull/4276) by [@esciara](https://github.com/esciara))*
  - :arrow_lower_right: *addresses issue [#4275](https://github.com/tobymao/sqlglot/issues/4275) opened by [@esciara](https://github.com/esciara)*

### :bug: Bug Fixes
- [`fcc05c9`](https://github.com/tobymao/sqlglot/commit/fcc05c9daa31c7a51474ec9c72ceafd682359f90) - **bigquery**: Early expand only aliased names in GROUP BY *(PR [#4246](https://github.com/tobymao/sqlglot/pull/4246) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5655cfb`](https://github.com/tobymao/sqlglot/commit/5655cfba7afdf8f95dea53d5ededfde209b77c30) - add support for negative intervals in to_interval *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`51f4d26`](https://github.com/tobymao/sqlglot/commit/51f4d26ed8694365c61fdefd810a420fcfefdeca) - generate single argument ArrayConcat without trailing comma fixes [#4259](https://github.com/tobymao/sqlglot/pull/4259) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`7fc0055`](https://github.com/tobymao/sqlglot/commit/7fc0055fb04713ba047baa5eda1ce0baf1cc79e2) - **clickhouse**: dont parse right-hand side operands of ARRAY JOIN as Tables *(PR [#4258](https://github.com/tobymao/sqlglot/pull/4258) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4254](https://github.com/tobymao/sqlglot/issues/4254) opened by [@xtess16](https://github.com/xtess16)*
- [`8f49ad8`](https://github.com/tobymao/sqlglot/commit/8f49ad87fa795349183d13129110ad59387bfe11) - **clickhouse**: traverse_scope with FINAL modifier *(PR [#4263](https://github.com/tobymao/sqlglot/pull/4263) by [@pkit](https://github.com/pkit))*
  - :arrow_lower_right: *fixes issue [#4262](https://github.com/tobymao/sqlglot/issues/4262) opened by [@obazna](https://github.com/obazna)*
- [`83167ea`](https://github.com/tobymao/sqlglot/commit/83167eaa3039195f756c7b1ad95fc9162f19b1b1) - hive dialect hierarchy has no CURRENT_TIME func *(PR [#4264](https://github.com/tobymao/sqlglot/pull/4264) by [@georgesittas](https://github.com/georgesittas))*
- [`7a5c7e0`](https://github.com/tobymao/sqlglot/commit/7a5c7e036fa84eb30bcae75829f3cb94503fa99e) - **presto**: transpile BIT to BOOLEAN *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`48be3d8`](https://github.com/tobymao/sqlglot/commit/48be3d89b1df96c7b8d81536862f53a98e414f11) - make the semantic diffing aware of changes to non-expression leaves *(PR [#4268](https://github.com/tobymao/sqlglot/pull/4268) by [@georgesittas](https://github.com/georgesittas))*
- [`4543fb3`](https://github.com/tobymao/sqlglot/commit/4543fb3cd052dfb20428f5a6254b38def9e756ee) - **optimizer**: Fix merge_subqueries.py::rename_inner_sources() *(PR [#4266](https://github.com/tobymao/sqlglot/pull/4266) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4245](https://github.com/tobymao/sqlglot/issues/4245) opened by [@daniel769](https://github.com/daniel769)*
- [`222152e`](https://github.com/tobymao/sqlglot/commit/222152e32521dbc6de3384b18ab4c677239c6088) - **optimizer**: Add type hints for optimizer rules eliminate & merge subqueries *(PR [#4267](https://github.com/tobymao/sqlglot/pull/4267) by [@VaggelisD](https://github.com/VaggelisD))*

### :recycle: Refactors
- [`94013a2`](https://github.com/tobymao/sqlglot/commit/94013a21ca69b90da78dc47b16cd86503736597a) - simplify _expression_only_args helper in diff module *(PR [#4251](https://github.com/tobymao/sqlglot/pull/4251) by [@georgesittas](https://github.com/georgesittas))*
- [`41e2eba`](https://github.com/tobymao/sqlglot/commit/41e2eba1a01c1a5b784ad9dc6c5191f3d3bc0d74) - **Oracle**: simplify hint arg formatting *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`cfd692f`](https://github.com/tobymao/sqlglot/commit/cfd692ff28a59f413671aafbc8dcd61eab3558c3) - move SwapTable logic to the base Parser/Generator classes *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.25.1] - 2024-10-15
### :bug: Bug Fixes
- [`e6567ae`](https://github.com/tobymao/sqlglot/commit/e6567ae11650834874808a844a19836fbb9ee753) - small overload fix for ensure list taking None *(PR [#4248](https://github.com/tobymao/sqlglot/pull/4248) by [@benfdking](https://github.com/benfdking))*


## [v25.25.0] - 2024-10-14
### :boom: BREAKING CHANGES
- due to [`275b64b`](https://github.com/tobymao/sqlglot/commit/275b64b6a28722232a24870e443b249994220d54) - refactor set operation builders so they can work with N expressions *(PR [#4226](https://github.com/tobymao/sqlglot/pull/4226) by [@georgesittas](https://github.com/georgesittas))*:

  refactor set operation builders so they can work with N expressions (#4226)

- due to [`aee76da`](https://github.com/tobymao/sqlglot/commit/aee76da1cadec242f7428d23999f1752cb0708ca) - Native annotations for string functions *(PR [#4231](https://github.com/tobymao/sqlglot/pull/4231) by [@VaggelisD](https://github.com/VaggelisD))*:

  Native annotations for string functions (#4231)

- due to [`202aaa0`](https://github.com/tobymao/sqlglot/commit/202aaa0e7390142ee3ade41c28e2e77cde31f295) - Native annotations for string functions *(PR [#4234](https://github.com/tobymao/sqlglot/pull/4234) by [@VaggelisD](https://github.com/VaggelisD))*:

  Native annotations for string functions (#4234)

- due to [`5741180`](https://github.com/tobymao/sqlglot/commit/5741180e895eaaa75a07af388d36a0d2df97b28c) - produce exp.Column for the RHS of <value> IN <name> *(PR [#4239](https://github.com/tobymao/sqlglot/pull/4239) by [@georgesittas](https://github.com/georgesittas))*:

  produce exp.Column for the RHS of <value> IN <name> (#4239)

- due to [`4da2502`](https://github.com/tobymao/sqlglot/commit/4da25029b1c6f1425b4602f42da4fa1bcd3fccdb) - make Explode a UDTF subclass *(PR [#4242](https://github.com/tobymao/sqlglot/pull/4242) by [@georgesittas](https://github.com/georgesittas))*:

  make Explode a UDTF subclass (#4242)


### :sparkles: New Features
- [`163e943`](https://github.com/tobymao/sqlglot/commit/163e943cdaf449599640c198f69e73d2398eb323) - **tsql**: SPLIT_PART function and conversion to PARSENAME in tsql *(PR [#4211](https://github.com/tobymao/sqlglot/pull/4211) by [@daihuynh](https://github.com/daihuynh))*
- [`275b64b`](https://github.com/tobymao/sqlglot/commit/275b64b6a28722232a24870e443b249994220d54) - refactor set operation builders so they can work with N expressions *(PR [#4226](https://github.com/tobymao/sqlglot/pull/4226) by [@georgesittas](https://github.com/georgesittas))*
- [`3f6ba3e`](https://github.com/tobymao/sqlglot/commit/3f6ba3e69c9ba92429d2b3b00cac33f45518aa56) - **clickhouse**: Support varlen arrays for ARRAY JOIN *(PR [#4229](https://github.com/tobymao/sqlglot/pull/4229) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4227](https://github.com/tobymao/sqlglot/issues/4227) opened by [@brunorpinho](https://github.com/brunorpinho)*
- [`aee76da`](https://github.com/tobymao/sqlglot/commit/aee76da1cadec242f7428d23999f1752cb0708ca) - **bigquery**: Native annotations for string functions *(PR [#4231](https://github.com/tobymao/sqlglot/pull/4231) by [@VaggelisD](https://github.com/VaggelisD))*
- [`202aaa0`](https://github.com/tobymao/sqlglot/commit/202aaa0e7390142ee3ade41c28e2e77cde31f295) - **bigquery**: Native annotations for string functions *(PR [#4234](https://github.com/tobymao/sqlglot/pull/4234) by [@VaggelisD](https://github.com/VaggelisD))*
- [`eeae25e`](https://github.com/tobymao/sqlglot/commit/eeae25e03a883671f9d5e514f9bd3021fb6c0d32) - support EXPLAIN in mysql *(PR [#4235](https://github.com/tobymao/sqlglot/pull/4235) by [@xiaoyu-meng-mxy](https://github.com/xiaoyu-meng-mxy))*
- [`06748d9`](https://github.com/tobymao/sqlglot/commit/06748d93ccd232528003c37fdda25ae8163f3c18) - **mysql**: add support for operation modifiers like HIGH_PRIORITY *(PR [#4238](https://github.com/tobymao/sqlglot/pull/4238) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4236](https://github.com/tobymao/sqlglot/issues/4236) opened by [@asdfsx](https://github.com/asdfsx)*

### :bug: Bug Fixes
- [`dcdec95`](https://github.com/tobymao/sqlglot/commit/dcdec95f986426ae90469baca993b47ac390081b) - Make exp.Update a DML node *(PR [#4223](https://github.com/tobymao/sqlglot/pull/4223) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4221](https://github.com/tobymao/sqlglot/issues/4221) opened by [@rahul-ve](https://github.com/rahul-ve)*
- [`79caf51`](https://github.com/tobymao/sqlglot/commit/79caf519987718390a086bee19fdc89f6094496c) - **clickhouse**: rename BOOLEAN type to Bool fixes [#4230](https://github.com/tobymao/sqlglot/pull/4230) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b26a3f6`](https://github.com/tobymao/sqlglot/commit/b26a3f67b7113802ba1b4b3b211431e98258dc15) - satisfy mypy *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`5741180`](https://github.com/tobymao/sqlglot/commit/5741180e895eaaa75a07af388d36a0d2df97b28c) - **parser**: produce exp.Column for the RHS of <value> IN <name> *(PR [#4239](https://github.com/tobymao/sqlglot/pull/4239) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4237](https://github.com/tobymao/sqlglot/issues/4237) opened by [@rustyconover](https://github.com/rustyconover)*
- [`daa6e78`](https://github.com/tobymao/sqlglot/commit/daa6e78e4b810eff826f995aa52f9e38197f1b7e) - **optimizer**: handle subquery predicate substitution correctly in de morgan's rule *(PR [#4240](https://github.com/tobymao/sqlglot/pull/4240) by [@georgesittas](https://github.com/georgesittas))*
- [`c0a8355`](https://github.com/tobymao/sqlglot/commit/c0a83556acffcd77521f69bf51503a07310f749d) - **parser**: parse a column reference for the RHS of the IN clause *(PR [#4241](https://github.com/tobymao/sqlglot/pull/4241) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`0882f03`](https://github.com/tobymao/sqlglot/commit/0882f03d526f593b2d415e85b7d7a7c113721806) - Rename exp.RenameTable to exp.AlterRename *(PR [#4224](https://github.com/tobymao/sqlglot/pull/4224) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4222](https://github.com/tobymao/sqlglot/issues/4222) opened by [@s1101010110](https://github.com/s1101010110)*
- [`fd42b5c`](https://github.com/tobymao/sqlglot/commit/fd42b5cdaf9421abb11e71d82726536af09e3ae3) - Simplify PARSENAME <-> SPLIT_PART transpilation *(PR [#4225](https://github.com/tobymao/sqlglot/pull/4225) by [@VaggelisD](https://github.com/VaggelisD))*
- [`4da2502`](https://github.com/tobymao/sqlglot/commit/4da25029b1c6f1425b4602f42da4fa1bcd3fccdb) - make Explode a UDTF subclass *(PR [#4242](https://github.com/tobymao/sqlglot/pull/4242) by [@georgesittas](https://github.com/georgesittas))*


## [v25.24.5] - 2024-10-08
### :sparkles: New Features
- [`22a1684`](https://github.com/tobymao/sqlglot/commit/22a16848d80a2fa6d310f99d21f7d81f90eb9440) - **bigquery**: Native annotations for more math functions *(PR [#4212](https://github.com/tobymao/sqlglot/pull/4212) by [@VaggelisD](https://github.com/VaggelisD))*
- [`354cfff`](https://github.com/tobymao/sqlglot/commit/354cfff13ab30d01c6123fca74eed0669d238aa0) - add builder methods to exp.Update and add with_ arg to exp.update *(PR [#4217](https://github.com/tobymao/sqlglot/pull/4217) by [@brdbry](https://github.com/brdbry))*

### :bug: Bug Fixes
- [`2c513b7`](https://github.com/tobymao/sqlglot/commit/2c513b71c7d4b1ff5c7c4e12d6c38694210b1a12) - Attach CTE comments before commas *(PR [#4218](https://github.com/tobymao/sqlglot/pull/4218) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4216](https://github.com/tobymao/sqlglot/issues/4216) opened by [@ajfriend](https://github.com/ajfriend)*


## [v25.24.4] - 2024-10-04
### :bug: Bug Fixes
- [`484df7d`](https://github.com/tobymao/sqlglot/commit/484df7d50df5cb314943e1810db18a7d7d5bb3eb) - tsql union with limit *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.24.3] - 2024-10-03
### :sparkles: New Features
- [`25b18d2`](https://github.com/tobymao/sqlglot/commit/25b18d28e5ad7b3687e2848ff92a0a1fc17b06fa) - **trino**: Support JSON_QUERY *(PR [#4206](https://github.com/tobymao/sqlglot/pull/4206) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4200](https://github.com/tobymao/sqlglot/issues/4200) opened by [@Harmuth94](https://github.com/Harmuth94)*
- [`5781b45`](https://github.com/tobymao/sqlglot/commit/5781b455fa3ec495b65f3f3f4a959192389bd816) - **duckdb**: Add more Postgres operators *(PR [#4199](https://github.com/tobymao/sqlglot/pull/4199) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4189](https://github.com/tobymao/sqlglot/issues/4189) opened by [@rustyconover](https://github.com/rustyconover)*
- [`89c0703`](https://github.com/tobymao/sqlglot/commit/89c07039da402fb2ad77e00edb4f09079ecbb41d) - **bigquery**: Native math function annotations *(PR [#4201](https://github.com/tobymao/sqlglot/pull/4201) by [@VaggelisD](https://github.com/VaggelisD))*
- [`977d9e5`](https://github.com/tobymao/sqlglot/commit/977d9e5a854b58b4469be1af6aa14a5bf5a4b8c6) - allow supplying dialect in diff, conditionally copy ASTs *(PR [#4208](https://github.com/tobymao/sqlglot/pull/4208) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4203](https://github.com/tobymao/sqlglot/issues/4203) opened by [@mkmoisen](https://github.com/mkmoisen)*

### :bug: Bug Fixes
- [`332c74b`](https://github.com/tobymao/sqlglot/commit/332c74b881487cd9ce711ca3bd065a8992872098) - attach comments to subquery predicates properly, fix comment case *(PR [#4207](https://github.com/tobymao/sqlglot/pull/4207) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4205](https://github.com/tobymao/sqlglot/issues/4205) opened by [@mkmoisen](https://github.com/mkmoisen)*
- [`55da21d`](https://github.com/tobymao/sqlglot/commit/55da21dd043dfcbefa3653fe168eb9cae5dc5bf5) - Unexpected row deduplication using eliminate_full_outer_join *(PR [#4178](https://github.com/tobymao/sqlglot/pull/4178) by [@liaco](https://github.com/liaco))*


## [v25.24.2] - 2024-10-02
### :sparkles: New Features
- [`c8b7c1e`](https://github.com/tobymao/sqlglot/commit/c8b7c1ef7c6070a51638af18833c649a77e735cb) - **optimizer**: Fixture file for function annotations *(PR [#4182](https://github.com/tobymao/sqlglot/pull/4182) by [@VaggelisD](https://github.com/VaggelisD))*
- [`0adbbf7`](https://github.com/tobymao/sqlglot/commit/0adbbf7ad8f16700adc48c6757c07768199860d9) - **duckdb**: Parse ** and ^ operators as POW *(PR [#4193](https://github.com/tobymao/sqlglot/pull/4193) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4186](https://github.com/tobymao/sqlglot/issues/4186) opened by [@rustyconover](https://github.com/rustyconover)*
- [`4949906`](https://github.com/tobymao/sqlglot/commit/4949906e9dd0c3039a161e06ddb970f37067b88f) - **duckdb**: Parse ~~~ as GLOB *(PR [#4194](https://github.com/tobymao/sqlglot/pull/4194) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4187](https://github.com/tobymao/sqlglot/issues/4187) opened by [@rustyconover](https://github.com/rustyconover)*
- [`6ba2bb0`](https://github.com/tobymao/sqlglot/commit/6ba2bb03f973c30788508768c3ba716aa94b0299) - **oracle**: Add support for BULK COLLECT INTO *(PR [#4181](https://github.com/tobymao/sqlglot/pull/4181) by [@mkmoisen](https://github.com/mkmoisen))*
- [`0de59ce`](https://github.com/tobymao/sqlglot/commit/0de59cebe550b33ac34a92c1ded1d3f9b8f679c4) - mark `expressions` as unsupported in Into generator *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`475f7a3`](https://github.com/tobymao/sqlglot/commit/475f7a3c639c7b8c5f3af1b2e5fcce9174be39ec) - **redshift**: Add unsupported warnings for UNNEST *(PR [#4173](https://github.com/tobymao/sqlglot/pull/4173) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4169](https://github.com/tobymao/sqlglot/issues/4169) opened by [@bjabes](https://github.com/bjabes)*
- [`d38e023`](https://github.com/tobymao/sqlglot/commit/d38e023966c32b208fe5ae9843bbd716e2181521) - **spark**: Offset TRY_ELEMENT_AT by one *(PR [#4183](https://github.com/tobymao/sqlglot/pull/4183) by [@VaggelisD](https://github.com/VaggelisD))*
- [`3d1c643`](https://github.com/tobymao/sqlglot/commit/3d1c6430791dcce05f1a71f17311e294d9fc9d3d) - rename SHA function to SHA1 for DuckDB *(PR [#4191](https://github.com/tobymao/sqlglot/pull/4191) by [@rustyconover](https://github.com/rustyconover))*
- [`0388a51`](https://github.com/tobymao/sqlglot/commit/0388a519dba63636a9aac3e3272cdea0f0b8312d) - add support for UHUGEINT for duckdb *(PR [#4190](https://github.com/tobymao/sqlglot/pull/4190) by [@rustyconover](https://github.com/rustyconover))*
  - :arrow_lower_right: *fixes issue [#4184](https://github.com/tobymao/sqlglot/issues/4184) opened by [@rustyconover](https://github.com/rustyconover)*
- [`9eba00d`](https://github.com/tobymao/sqlglot/commit/9eba00dca517efe7df171b09ed916af3ea5e350d) - **duckdb**: Parse ~~ as LIKE *(PR [#4195](https://github.com/tobymao/sqlglot/pull/4195) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4188](https://github.com/tobymao/sqlglot/issues/4188) opened by [@rustyconover](https://github.com/rustyconover)*
- [`6a65973`](https://github.com/tobymao/sqlglot/commit/6a659736f3a176e335c68fdd07d8265c3d0421dc) - expand UPDATABLE_EXPRESSION_TYPES to account for Identifier changes *(PR [#4197](https://github.com/tobymao/sqlglot/pull/4197) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4192](https://github.com/tobymao/sqlglot/issues/4192) opened by [@mkmoisen](https://github.com/mkmoisen)*
- [`a6c28c6`](https://github.com/tobymao/sqlglot/commit/a6c28c63f4e44bb62ba8df30f1407c728eb215f2) - **sqlite**: generate StrPosition as INSTR *(PR [#4198](https://github.com/tobymao/sqlglot/pull/4198) by [@pruzko](https://github.com/pruzko))*
  - :arrow_lower_right: *fixes issue [#4196](https://github.com/tobymao/sqlglot/issues/4196) opened by [@pruzko](https://github.com/pruzko)*
- [`5a123a5`](https://github.com/tobymao/sqlglot/commit/5a123a54ecd033c0a104e33476b17d816a09caac) - **oracle**: retreat properly when parsing BULK COLLECT INTO *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`f935e42`](https://github.com/tobymao/sqlglot/commit/f935e42130724e032b294074f3b552f21e20bc57) - properly escape closing identifier delimiters *(PR [#4202](https://github.com/tobymao/sqlglot/pull/4202) by [@georgesittas](https://github.com/georgesittas))*


## [v25.24.1] - 2024-10-01
### :sparkles: New Features
- [`7af33a2`](https://github.com/tobymao/sqlglot/commit/7af33a2f74dd1300bcd45f1974b7fd28abe66b8e) - **spark**: Custom annotation for more string functions *(PR [#4156](https://github.com/tobymao/sqlglot/pull/4156) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`81df4e1`](https://github.com/tobymao/sqlglot/commit/81df4e104ff3d60e3c23d3ac321e719b1f0962c0) - **athena**: Case sensitivity in CTAS property names *(PR [#4171](https://github.com/tobymao/sqlglot/pull/4171) by [@erindru](https://github.com/erindru))*
- [`0703152`](https://github.com/tobymao/sqlglot/commit/0703152a25afced183dc5efd5f62311a48545420) - **bigquery**: Do not generate null ordering on agg funcs *(PR [#4172](https://github.com/tobymao/sqlglot/pull/4172) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4170](https://github.com/tobymao/sqlglot/issues/4170) opened by [@yjabri](https://github.com/yjabri)*


## [v25.24.0] - 2024-09-26
### :boom: BREAKING CHANGES
- due to [`3ab6dfb`](https://github.com/tobymao/sqlglot/commit/3ab6dfb486f18d036bfac6a90d5f81b0ce7a91ea) - Generalize COLUMNS(...) APPLY *(PR [#4161](https://github.com/tobymao/sqlglot/pull/4161) by [@VaggelisD](https://github.com/VaggelisD))*:

  Generalize COLUMNS(...) APPLY (#4161)


### :sparkles: New Features
- [`93cef30`](https://github.com/tobymao/sqlglot/commit/93cef30bc534a155bce06f35d441d20e5dd78cf6) - **postgres**: Support OVERLAY function *(PR [#4165](https://github.com/tobymao/sqlglot/pull/4165) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4159](https://github.com/tobymao/sqlglot/issues/4159) opened by [@s1101010110](https://github.com/s1101010110)*
- [`0a5444d`](https://github.com/tobymao/sqlglot/commit/0a5444dc822b7c53c008bc946eb3b54ca2147f3c) - expose a flag to automatically exclude Keep diff nodes *(PR [#4168](https://github.com/tobymao/sqlglot/pull/4168) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`9c17264`](https://github.com/tobymao/sqlglot/commit/9c172643aa3f3f0ffcc2e62242b62ba9c6141925) - **hive**: Enclose exp.Split with \E *(PR [#4163](https://github.com/tobymao/sqlglot/pull/4163) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4158](https://github.com/tobymao/sqlglot/issues/4158) opened by [@cpcloud](https://github.com/cpcloud)*
- [`3ab6dfb`](https://github.com/tobymao/sqlglot/commit/3ab6dfb486f18d036bfac6a90d5f81b0ce7a91ea) - **clickhouse**: Generalize COLUMNS(...) APPLY *(PR [#4161](https://github.com/tobymao/sqlglot/pull/4161) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4157](https://github.com/tobymao/sqlglot/issues/4157) opened by [@elchyn-cheliabiyeu](https://github.com/elchyn-cheliabiyeu)*

### :recycle: Refactors
- [`2540e50`](https://github.com/tobymao/sqlglot/commit/2540e50d2b0df12f940c68acc574e540d19546cf) - simplify check_deploy job *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`f6d3bdd`](https://github.com/tobymao/sqlglot/commit/f6d3bdd740d0fe128d4d5dd99833a6f71c890ed3) - update supported dialect count (21 -> 23) *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.23.2] - 2024-09-25
### :wrench: Chores
- [`eca05d3`](https://github.com/tobymao/sqlglot/commit/eca05d3b08645d7a984ee65b438282b35cb41960) - tweak should_deploy_rs script to avoid marking CI as failed *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.23.1] - 2024-09-25
### :wrench: Chores
- [`349b8f8`](https://github.com/tobymao/sqlglot/commit/349b8f81ed69a3708e1afd15816b3b58e2bf8b3f) - fetch all history to allow workflow script to skip sqlglotrs deployments *(PR [#4162](https://github.com/tobymao/sqlglot/pull/4162) by [@georgesittas](https://github.com/georgesittas))*


## [v25.23.0] - 2024-09-25
### :boom: BREAKING CHANGES
- due to [`da51ea5`](https://github.com/tobymao/sqlglot/commit/da51ea5c8f405859d877a25176e8e48ef8b4b112) - refactor exp.Chr *(PR [#4081](https://github.com/tobymao/sqlglot/pull/4081) by [@georgesittas](https://github.com/georgesittas))*:

  refactor exp.Chr (#4081)

- due to [`9c527b5`](https://github.com/tobymao/sqlglot/commit/9c527b549cc56db9d8f44579397d9f9fe1440573) - treat Nullable as an arg instead of a DataType.TYPE *(PR [#4094](https://github.com/tobymao/sqlglot/pull/4094) by [@georgesittas](https://github.com/georgesittas))*:

  treat Nullable as an arg instead of a DataType.TYPE (#4094)

- due to [`ba015dc`](https://github.com/tobymao/sqlglot/commit/ba015dc1102a4fe0c35cbfe6e3d23dc24263c20f) - add `returning` to merge expression builder *(PR [#4125](https://github.com/tobymao/sqlglot/pull/4125) by [@max-muoto](https://github.com/max-muoto))*:

  add `returning` to merge expression builder (#4125)

- due to [`77a514d`](https://github.com/tobymao/sqlglot/commit/77a514dd7cfa9feb847c429411809092e5578bad) - Parse VALUES & query modifiers in wrapped FROM clause *(PR [#4135](https://github.com/tobymao/sqlglot/pull/4135) by [@VaggelisD](https://github.com/VaggelisD))*:

  Parse VALUES & query modifiers in wrapped FROM clause (#4135)


### :sparkles: New Features
- [`5771d8d`](https://github.com/tobymao/sqlglot/commit/5771d8da94aff104206f93482e7b248d725f1843) - add merge expression builder *(PR [#4084](https://github.com/tobymao/sqlglot/pull/4084) by [@max-muoto](https://github.com/max-muoto))*
- [`1d52709`](https://github.com/tobymao/sqlglot/commit/1d5270915d14f3f92341d5057b88b58fff6c0d97) - **postgres**: Parse DO NOTHING and RETURNING in MERGE statement *(PR [#4087](https://github.com/tobymao/sqlglot/pull/4087) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4083](https://github.com/tobymao/sqlglot/issues/4083) opened by [@max-muoto](https://github.com/max-muoto)*
- [`1615bad`](https://github.com/tobymao/sqlglot/commit/1615bad98eeddc8e67e8002c3b1fe93bd7c3b690) - Add support for UUID function *(PR [#4089](https://github.com/tobymao/sqlglot/pull/4089) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5733600`](https://github.com/tobymao/sqlglot/commit/57336006795d32e9253a9df4813d3029d1d32ef1) - **bigquery**: transpile UUID type to STRING *(PR [#4093](https://github.com/tobymao/sqlglot/pull/4093) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4091](https://github.com/tobymao/sqlglot/issues/4091) opened by [@gigatexal](https://github.com/gigatexal)*
- [`75230f5`](https://github.com/tobymao/sqlglot/commit/75230f5970c240add1cff7349fc65fb67541fa34) - **bigquery**: add support for the MERGE ... THEN INSERT ROW syntax *(PR [#4096](https://github.com/tobymao/sqlglot/pull/4096) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4095](https://github.com/tobymao/sqlglot/issues/4095) opened by [@ericist](https://github.com/ericist)*
- [`f8d4dc4`](https://github.com/tobymao/sqlglot/commit/f8d4dc4bab90cd369eef090c23b81160a7ae78fc) - **parser**: add support for ALTER INDEX closes [#4105](https://github.com/tobymao/sqlglot/pull/4105) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`28c6f27`](https://github.com/tobymao/sqlglot/commit/28c6f27291d57d85917c62b387b86a598ee3c1d6) - **duckdb**: Support *COLUMNS() function *(PR [#4106](https://github.com/tobymao/sqlglot/pull/4106) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4101](https://github.com/tobymao/sqlglot/issues/4101) opened by [@aersam](https://github.com/aersam)*
- [`3cb0041`](https://github.com/tobymao/sqlglot/commit/3cb00417f45624f012e5ce8ababfe3250e813b80) - **snowflake**: Fix exp.Pivot FOR IN clause *(PR [#4109](https://github.com/tobymao/sqlglot/pull/4109) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4108](https://github.com/tobymao/sqlglot/issues/4108) opened by [@kharigardner](https://github.com/kharigardner)*
- [`7ac21a0`](https://github.com/tobymao/sqlglot/commit/7ac21a07c73cdf156ae4dc6a848b9f781b265d16) - **athena**: Improve DDL query support *(PR [#4099](https://github.com/tobymao/sqlglot/pull/4099) by [@erindru](https://github.com/erindru))*
- [`a34f8b6`](https://github.com/tobymao/sqlglot/commit/a34f8b6ff9b0aa8595214da75fe7cbfbc8285476) - **oracle**: support TRUNC without fmt argument fixes [#4116](https://github.com/tobymao/sqlglot/pull/4116) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`bd8e050`](https://github.com/tobymao/sqlglot/commit/bd8e050e7df905082fd32e26c4ee0c6e2d36c897) - **clickhouse**: support ON CLUSTER clause in DELETE *(PR [#4119](https://github.com/tobymao/sqlglot/pull/4119) by [@treysp](https://github.com/treysp))*
- [`1fac6a9`](https://github.com/tobymao/sqlglot/commit/1fac6a9f46e147a5583042d6f82deffd04cd58c9) - expose sqlglot.expressions.delete as a sqlglot module function *(PR [#4126](https://github.com/tobymao/sqlglot/pull/4126) by [@max-muoto](https://github.com/max-muoto))*
- [`4506b3b`](https://github.com/tobymao/sqlglot/commit/4506b3b58fde8e8fe711df8fd0c9c245a98ca86b) - **duckdb**: add support for the UNION type *(PR [#4128](https://github.com/tobymao/sqlglot/pull/4128) by [@georgesittas](https://github.com/georgesittas))*
- [`ba015dc`](https://github.com/tobymao/sqlglot/commit/ba015dc1102a4fe0c35cbfe6e3d23dc24263c20f) - add `returning` to merge expression builder *(PR [#4125](https://github.com/tobymao/sqlglot/pull/4125) by [@max-muoto](https://github.com/max-muoto))*
- [`3ec96ab`](https://github.com/tobymao/sqlglot/commit/3ec96ab7318dc5fc07802d31c825a95db7f5b303) - **clickhouse**: Add support for APPLY query modifier *(PR [#4141](https://github.com/tobymao/sqlglot/pull/4141) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4139](https://github.com/tobymao/sqlglot/issues/4139) opened by [@elchyn-cheliabiyeu](https://github.com/elchyn-cheliabiyeu)*
- [`04ddc54`](https://github.com/tobymao/sqlglot/commit/04ddc543159bc55e2cf8098cd96b2a5c881ebbc6) - **bigquery**: Support RANGE<T> type *(PR [#4148](https://github.com/tobymao/sqlglot/pull/4148) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4146](https://github.com/tobymao/sqlglot/issues/4146) opened by [@plaflamme](https://github.com/plaflamme)*
- [`17533ee`](https://github.com/tobymao/sqlglot/commit/17533ee6361c30731a9a14666ac952b44982b69d) - Add support for GRANT DDL *(PR [#4138](https://github.com/tobymao/sqlglot/pull/4138) by [@VaggelisD](https://github.com/VaggelisD))*
- [`1a240ec`](https://github.com/tobymao/sqlglot/commit/1a240ec1cbdaf15abab8df642e189b89de239e84) - Add SUBSTR Support *(PR [#4153](https://github.com/tobymao/sqlglot/pull/4153) by [@mwc360](https://github.com/mwc360))*

### :bug: Bug Fixes
- [`da51ea5`](https://github.com/tobymao/sqlglot/commit/da51ea5c8f405859d877a25176e8e48ef8b4b112) - **parser**: refactor exp.Chr *(PR [#4081](https://github.com/tobymao/sqlglot/pull/4081) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4080](https://github.com/tobymao/sqlglot/issues/4080) opened by [@EugeneTorap](https://github.com/EugeneTorap)*
- [`6294f9e`](https://github.com/tobymao/sqlglot/commit/6294f9e6d08111c6088f5ed9846e7a64f7724801) - **starrocks**: generate ARRAY_FILTER for exp.ArrayFilter *(PR [#4088](https://github.com/tobymao/sqlglot/pull/4088) by [@gauravsagar483](https://github.com/gauravsagar483))*
- [`1e02c02`](https://github.com/tobymao/sqlglot/commit/1e02c0221ea8445ccb5537b0a77e120c0b2c108c) - **mysql**: convert VARCHAR without size to TEXT for DDLs *(PR [#4092](https://github.com/tobymao/sqlglot/pull/4092) by [@georgesittas](https://github.com/georgesittas))*
- [`cb5bcff`](https://github.com/tobymao/sqlglot/commit/cb5bcfff1f96972e75681bb2411bca8b60a4bff1) - **clickhouse**: generate formatDateTime instead of DATE_FORMAT fixes [#4098](https://github.com/tobymao/sqlglot/pull/4098) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b10255e`](https://github.com/tobymao/sqlglot/commit/b10255eb8b6b73bf5084fdf6bffd5a7fa351b1ec) - **snowflake**: Manually escape single quotes in colon operator *(PR [#4104](https://github.com/tobymao/sqlglot/pull/4104) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4090](https://github.com/tobymao/sqlglot/issues/4090) opened by [@jussihe-rec](https://github.com/jussihe-rec)*
- [`67a9ad8`](https://github.com/tobymao/sqlglot/commit/67a9ad89abfce84f78dd1a34caa9dc8143233609) - Move JSON path escape to generation *(PR [#4110](https://github.com/tobymao/sqlglot/pull/4110) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4090](https://github.com/tobymao/sqlglot/issues/4090) opened by [@jussihe-rec](https://github.com/jussihe-rec)*
- [`06c76f7`](https://github.com/tobymao/sqlglot/commit/06c76f7471cdb679a7a7a35064204d94841fd929) - calling interval without unit *(commit by [@tobymao](https://github.com/tobymao))*
- [`66c3295`](https://github.com/tobymao/sqlglot/commit/66c32958a9e46642077813adf90079098e41c87e) - **optimizer**: Enable USING expansion with multiple joins *(PR [#4113](https://github.com/tobymao/sqlglot/pull/4113) by [@dg-hellotwin](https://github.com/dg-hellotwin))*
  - :arrow_lower_right: *fixes issue [#4112](https://github.com/tobymao/sqlglot/issues/4112) opened by [@dg-hellotwin](https://github.com/dg-hellotwin)*
- [`21f5bcd`](https://github.com/tobymao/sqlglot/commit/21f5bcd13eb9c567c711cec5879c4d08a052b91c) - parse struct(...)[] type properly *(PR [#4123](https://github.com/tobymao/sqlglot/pull/4123) by [@georgesittas](https://github.com/georgesittas))*
- [`22c456d`](https://github.com/tobymao/sqlglot/commit/22c456d032b457244b397598d4480ae22ad316bd) - Do not generate DISTINCT keyword in FILTER *(PR [#4130](https://github.com/tobymao/sqlglot/pull/4130) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4129](https://github.com/tobymao/sqlglot/issues/4129) opened by [@tekumara](https://github.com/tekumara)*
- [`089b77e`](https://github.com/tobymao/sqlglot/commit/089b77ec7efcd6decbf9a7be500e73dc88ba4dec) - **athena**: DDL fixes *(PR [#4132](https://github.com/tobymao/sqlglot/pull/4132) by [@erindru](https://github.com/erindru))*
- [`e6c9902`](https://github.com/tobymao/sqlglot/commit/e6c990225e2685c617dfd1594c83778036405f6b) - invalid regex *(commit by [@tobymao](https://github.com/tobymao))*
- [`77a514d`](https://github.com/tobymao/sqlglot/commit/77a514dd7cfa9feb847c429411809092e5578bad) - **parser**: Parse VALUES & query modifiers in wrapped FROM clause *(PR [#4135](https://github.com/tobymao/sqlglot/pull/4135) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4133](https://github.com/tobymao/sqlglot/issues/4133) opened by [@danxmoran](https://github.com/danxmoran)*
- [`8822d6c`](https://github.com/tobymao/sqlglot/commit/8822d6c1b3b62cfd76fd481db473bf8ea1c12b1a) - **parser**: handle brackets in column op json extract arrow parser *(PR [#4140](https://github.com/tobymao/sqlglot/pull/4140) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3151](https://github.com/TobikoData/sqlmesh/issues/3151) opened by [@markgraphene](https://github.com/markgraphene)*
- [`be0a4a8`](https://github.com/tobymao/sqlglot/commit/be0a4a85d41bfb617360bd9a59aa9e631e4d6d6c) - **bigquery**: Consume dashed identifiers only if they're connected *(PR [#4144](https://github.com/tobymao/sqlglot/pull/4144) by [@VaggelisD](https://github.com/VaggelisD))*
- [`0444819`](https://github.com/tobymao/sqlglot/commit/044481926d4b008027a2c7fb20501514ef507811) - **optimizer**: don't reorder subquery predicates in simplify *(PR [#4147](https://github.com/tobymao/sqlglot/pull/4147) by [@georgesittas](https://github.com/georgesittas))*
- [`89519bb`](https://github.com/tobymao/sqlglot/commit/89519bba99fc11f17e8e00bf8e3f6dde213e99be) - **clickhouse**: make ToTableProperty appear right after the DDL name *(PR [#4151](https://github.com/tobymao/sqlglot/pull/4151) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4150](https://github.com/tobymao/sqlglot/issues/4150) opened by [@tzinyama](https://github.com/tzinyama)*

### :recycle: Refactors
- [`9c527b5`](https://github.com/tobymao/sqlglot/commit/9c527b549cc56db9d8f44579397d9f9fe1440573) - treat Nullable as an arg instead of a DataType.TYPE *(PR [#4094](https://github.com/tobymao/sqlglot/pull/4094) by [@georgesittas](https://github.com/georgesittas))*
- [`2961049`](https://github.com/tobymao/sqlglot/commit/296104950f2e679aea37810a48eb490e170518d3) - implement decorator to easily mark args as unsupported *(PR [#4111](https://github.com/tobymao/sqlglot/pull/4111) by [@georgesittas](https://github.com/georgesittas))*
- [`7cf1d70`](https://github.com/tobymao/sqlglot/commit/7cf1d70e909ae319ff659e1455e6fcad1e8cf905) - **optimizer**: Optimize USING expansion *(PR [#4115](https://github.com/tobymao/sqlglot/pull/4115) by [@VaggelisD](https://github.com/VaggelisD))*

### :wrench: Chores
- [`75e6406`](https://github.com/tobymao/sqlglot/commit/75e640672eef0ddf752ee36dbc6f904f8e06510f) - **prql**: rewrite tests to use `validate_all()` *(PR [#4097](https://github.com/tobymao/sqlglot/pull/4097) by [@JJHCool](https://github.com/JJHCool))*
- [`e1f6ae3`](https://github.com/tobymao/sqlglot/commit/e1f6ae393fa2857dbbb9a14b03cbe39910207233) - **prql**: use validate_all instead of validate_identity *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`2dc0b86`](https://github.com/tobymao/sqlglot/commit/2dc0b8620e16b3cdf85f6bb6bea10c2527497933) - **optimizer**: rename helper function in expand_using *(PR [#4117](https://github.com/tobymao/sqlglot/pull/4117) by [@georgesittas](https://github.com/georgesittas))*
- [`fd8b8ba`](https://github.com/tobymao/sqlglot/commit/fd8b8ba7dedaee5d237b080db5c4f7e83ba079e9) - create ARRAY_TYPES set under DataType *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.22.0] - 2024-09-19
### :boom: BREAKING CHANGES
- due to [`ba015dc`](https://github.com/tobymao/sqlglot/commit/ba015dc1102a4fe0c35cbfe6e3d23dc24263c20f) - add `returning` to merge expression builder *(PR [#4125](https://github.com/tobymao/sqlglot/pull/4125) by [@max-muoto](https://github.com/max-muoto))*:

  add `returning` to merge expression builder (#4125)


### :sparkles: New Features
- [`1fac6a9`](https://github.com/tobymao/sqlglot/commit/1fac6a9f46e147a5583042d6f82deffd04cd58c9) - expose sqlglot.expressions.delete as a sqlglot module function *(PR [#4126](https://github.com/tobymao/sqlglot/pull/4126) by [@max-muoto](https://github.com/max-muoto))*
- [`4506b3b`](https://github.com/tobymao/sqlglot/commit/4506b3b58fde8e8fe711df8fd0c9c245a98ca86b) - **duckdb**: add support for the UNION type *(PR [#4128](https://github.com/tobymao/sqlglot/pull/4128) by [@georgesittas](https://github.com/georgesittas))*
- [`ba015dc`](https://github.com/tobymao/sqlglot/commit/ba015dc1102a4fe0c35cbfe6e3d23dc24263c20f) - add `returning` to merge expression builder *(PR [#4125](https://github.com/tobymao/sqlglot/pull/4125) by [@max-muoto](https://github.com/max-muoto))*

### :bug: Bug Fixes
- [`22c456d`](https://github.com/tobymao/sqlglot/commit/22c456d032b457244b397598d4480ae22ad316bd) - Do not generate DISTINCT keyword in FILTER *(PR [#4130](https://github.com/tobymao/sqlglot/pull/4130) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4129](https://github.com/tobymao/sqlglot/issues/4129) opened by [@tekumara](https://github.com/tekumara)*
- [`089b77e`](https://github.com/tobymao/sqlglot/commit/089b77ec7efcd6decbf9a7be500e73dc88ba4dec) - **athena**: DDL fixes *(PR [#4132](https://github.com/tobymao/sqlglot/pull/4132) by [@erindru](https://github.com/erindru))*
- [`e6c9902`](https://github.com/tobymao/sqlglot/commit/e6c990225e2685c617dfd1594c83778036405f6b) - invalid regex *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.21.3] - 2024-09-14
### :sparkles: New Features
- [`bd8e050`](https://github.com/tobymao/sqlglot/commit/bd8e050e7df905082fd32e26c4ee0c6e2d36c897) - **clickhouse**: support ON CLUSTER clause in DELETE *(PR [#4119](https://github.com/tobymao/sqlglot/pull/4119) by [@treysp](https://github.com/treysp))*

### :bug: Bug Fixes
- [`21f5bcd`](https://github.com/tobymao/sqlglot/commit/21f5bcd13eb9c567c711cec5879c4d08a052b91c) - parse struct(...)[] type properly *(PR [#4123](https://github.com/tobymao/sqlglot/pull/4123) by [@georgesittas](https://github.com/georgesittas))*


## [v25.21.2] - 2024-09-13
### :sparkles: New Features
- [`a34f8b6`](https://github.com/tobymao/sqlglot/commit/a34f8b6ff9b0aa8595214da75fe7cbfbc8285476) - **oracle**: support TRUNC without fmt argument fixes [#4116](https://github.com/tobymao/sqlglot/pull/4116) *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`66c3295`](https://github.com/tobymao/sqlglot/commit/66c32958a9e46642077813adf90079098e41c87e) - **optimizer**: Enable USING expansion with multiple joins *(PR [#4113](https://github.com/tobymao/sqlglot/pull/4113) by [@dg-hellotwin](https://github.com/dg-hellotwin))*
  - :arrow_lower_right: *fixes issue [#4112](https://github.com/tobymao/sqlglot/issues/4112) opened by [@dg-hellotwin](https://github.com/dg-hellotwin)*

### :recycle: Refactors
- [`7cf1d70`](https://github.com/tobymao/sqlglot/commit/7cf1d70e909ae319ff659e1455e6fcad1e8cf905) - **optimizer**: Optimize USING expansion *(PR [#4115](https://github.com/tobymao/sqlglot/pull/4115) by [@VaggelisD](https://github.com/VaggelisD))*

### :wrench: Chores
- [`2dc0b86`](https://github.com/tobymao/sqlglot/commit/2dc0b8620e16b3cdf85f6bb6bea10c2527497933) - **optimizer**: rename helper function in expand_using *(PR [#4117](https://github.com/tobymao/sqlglot/pull/4117) by [@georgesittas](https://github.com/georgesittas))*
- [`fd8b8ba`](https://github.com/tobymao/sqlglot/commit/fd8b8ba7dedaee5d237b080db5c4f7e83ba079e9) - create ARRAY_TYPES set under DataType *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.21.1] - 2024-09-13
### :bug: Bug Fixes
- [`06c76f7`](https://github.com/tobymao/sqlglot/commit/06c76f7471cdb679a7a7a35064204d94841fd929) - calling interval without unit *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.21.0] - 2024-09-12
### :boom: BREAKING CHANGES
- due to [`da51ea5`](https://github.com/tobymao/sqlglot/commit/da51ea5c8f405859d877a25176e8e48ef8b4b112) - refactor exp.Chr *(PR [#4081](https://github.com/tobymao/sqlglot/pull/4081) by [@georgesittas](https://github.com/georgesittas))*:

  refactor exp.Chr (#4081)

- due to [`9c527b5`](https://github.com/tobymao/sqlglot/commit/9c527b549cc56db9d8f44579397d9f9fe1440573) - treat Nullable as an arg instead of a DataType.TYPE *(PR [#4094](https://github.com/tobymao/sqlglot/pull/4094) by [@georgesittas](https://github.com/georgesittas))*:

  treat Nullable as an arg instead of a DataType.TYPE (#4094)


### :sparkles: New Features
- [`5771d8d`](https://github.com/tobymao/sqlglot/commit/5771d8da94aff104206f93482e7b248d725f1843) - add merge expression builder *(PR [#4084](https://github.com/tobymao/sqlglot/pull/4084) by [@max-muoto](https://github.com/max-muoto))*
- [`1d52709`](https://github.com/tobymao/sqlglot/commit/1d5270915d14f3f92341d5057b88b58fff6c0d97) - **postgres**: Parse DO NOTHING and RETURNING in MERGE statement *(PR [#4087](https://github.com/tobymao/sqlglot/pull/4087) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4083](https://github.com/tobymao/sqlglot/issues/4083) opened by [@max-muoto](https://github.com/max-muoto)*
- [`1615bad`](https://github.com/tobymao/sqlglot/commit/1615bad98eeddc8e67e8002c3b1fe93bd7c3b690) - Add support for UUID function *(PR [#4089](https://github.com/tobymao/sqlglot/pull/4089) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5733600`](https://github.com/tobymao/sqlglot/commit/57336006795d32e9253a9df4813d3029d1d32ef1) - **bigquery**: transpile UUID type to STRING *(PR [#4093](https://github.com/tobymao/sqlglot/pull/4093) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4091](https://github.com/tobymao/sqlglot/issues/4091) opened by [@gigatexal](https://github.com/gigatexal)*
- [`75230f5`](https://github.com/tobymao/sqlglot/commit/75230f5970c240add1cff7349fc65fb67541fa34) - **bigquery**: add support for the MERGE ... THEN INSERT ROW syntax *(PR [#4096](https://github.com/tobymao/sqlglot/pull/4096) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4095](https://github.com/tobymao/sqlglot/issues/4095) opened by [@ericist](https://github.com/ericist)*
- [`f8d4dc4`](https://github.com/tobymao/sqlglot/commit/f8d4dc4bab90cd369eef090c23b81160a7ae78fc) - **parser**: add support for ALTER INDEX closes [#4105](https://github.com/tobymao/sqlglot/pull/4105) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`28c6f27`](https://github.com/tobymao/sqlglot/commit/28c6f27291d57d85917c62b387b86a598ee3c1d6) - **duckdb**: Support *COLUMNS() function *(PR [#4106](https://github.com/tobymao/sqlglot/pull/4106) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4101](https://github.com/tobymao/sqlglot/issues/4101) opened by [@aersam](https://github.com/aersam)*
- [`3cb0041`](https://github.com/tobymao/sqlglot/commit/3cb00417f45624f012e5ce8ababfe3250e813b80) - **snowflake**: Fix exp.Pivot FOR IN clause *(PR [#4109](https://github.com/tobymao/sqlglot/pull/4109) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4108](https://github.com/tobymao/sqlglot/issues/4108) opened by [@kharigardner](https://github.com/kharigardner)*
- [`7ac21a0`](https://github.com/tobymao/sqlglot/commit/7ac21a07c73cdf156ae4dc6a848b9f781b265d16) - **athena**: Improve DDL query support *(PR [#4099](https://github.com/tobymao/sqlglot/pull/4099) by [@erindru](https://github.com/erindru))*

### :bug: Bug Fixes
- [`da51ea5`](https://github.com/tobymao/sqlglot/commit/da51ea5c8f405859d877a25176e8e48ef8b4b112) - **parser**: refactor exp.Chr *(PR [#4081](https://github.com/tobymao/sqlglot/pull/4081) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4080](https://github.com/tobymao/sqlglot/issues/4080) opened by [@EugeneTorap](https://github.com/EugeneTorap)*
- [`6294f9e`](https://github.com/tobymao/sqlglot/commit/6294f9e6d08111c6088f5ed9846e7a64f7724801) - **starrocks**: generate ARRAY_FILTER for exp.ArrayFilter *(PR [#4088](https://github.com/tobymao/sqlglot/pull/4088) by [@gauravsagar483](https://github.com/gauravsagar483))*
- [`1e02c02`](https://github.com/tobymao/sqlglot/commit/1e02c0221ea8445ccb5537b0a77e120c0b2c108c) - **mysql**: convert VARCHAR without size to TEXT for DDLs *(PR [#4092](https://github.com/tobymao/sqlglot/pull/4092) by [@georgesittas](https://github.com/georgesittas))*
- [`cb5bcff`](https://github.com/tobymao/sqlglot/commit/cb5bcfff1f96972e75681bb2411bca8b60a4bff1) - **clickhouse**: generate formatDateTime instead of DATE_FORMAT fixes [#4098](https://github.com/tobymao/sqlglot/pull/4098) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b10255e`](https://github.com/tobymao/sqlglot/commit/b10255eb8b6b73bf5084fdf6bffd5a7fa351b1ec) - **snowflake**: Manually escape single quotes in colon operator *(PR [#4104](https://github.com/tobymao/sqlglot/pull/4104) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4090](https://github.com/tobymao/sqlglot/issues/4090) opened by [@jussihe-rec](https://github.com/jussihe-rec)*
- [`67a9ad8`](https://github.com/tobymao/sqlglot/commit/67a9ad89abfce84f78dd1a34caa9dc8143233609) - Move JSON path escape to generation *(PR [#4110](https://github.com/tobymao/sqlglot/pull/4110) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4090](https://github.com/tobymao/sqlglot/issues/4090) opened by [@jussihe-rec](https://github.com/jussihe-rec)*

### :recycle: Refactors
- [`9c527b5`](https://github.com/tobymao/sqlglot/commit/9c527b549cc56db9d8f44579397d9f9fe1440573) - treat Nullable as an arg instead of a DataType.TYPE *(PR [#4094](https://github.com/tobymao/sqlglot/pull/4094) by [@georgesittas](https://github.com/georgesittas))*
- [`2961049`](https://github.com/tobymao/sqlglot/commit/296104950f2e679aea37810a48eb490e170518d3) - implement decorator to easily mark args as unsupported *(PR [#4111](https://github.com/tobymao/sqlglot/pull/4111) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`75e6406`](https://github.com/tobymao/sqlglot/commit/75e640672eef0ddf752ee36dbc6f904f8e06510f) - **prql**: rewrite tests to use `validate_all()` *(PR [#4097](https://github.com/tobymao/sqlglot/pull/4097) by [@JJHCool](https://github.com/JJHCool))*
- [`e1f6ae3`](https://github.com/tobymao/sqlglot/commit/e1f6ae393fa2857dbbb9a14b03cbe39910207233) - **prql**: use validate_all instead of validate_identity *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.20.1] - 2024-09-06
### :wrench: Chores
- [`8a357cc`](https://github.com/tobymao/sqlglot/commit/8a357ccbcf2301f6a8d60c237a6397bf6547de14) - bump sqlglotrs to 0.2.12 -- remove relative readme path *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.20.0] - 2024-09-06
### :boom: BREAKING CHANGES
- due to [`3e1af21`](https://github.com/tobymao/sqlglot/commit/3e1af21787ee81df5cbb5eb8b4b7f808b404c870) - Canonicalize exp.RegexpExtract group default value *(PR [#4051](https://github.com/tobymao/sqlglot/pull/4051) by [@VaggelisD](https://github.com/VaggelisD))*:

  Canonicalize exp.RegexpExtract group default value (#4051)

- due to [`c8e2eae`](https://github.com/tobymao/sqlglot/commit/c8e2eaecad0b3b0fff725512ef571de41c5be0a1) - do not canonicalize INTERVAL values to number literals *(commit by [@VaggelisD](https://github.com/VaggelisD))*:

  do not canonicalize INTERVAL values to number literals


### :sparkles: New Features
- [`d3ee5ea`](https://github.com/tobymao/sqlglot/commit/d3ee5ea6abd0dfb6e5216bf212e9e737c163eeb9) - **oracle**: parse TRUNC to facilitate transpilation closes [#4054](https://github.com/tobymao/sqlglot/pull/4054) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`143f176`](https://github.com/tobymao/sqlglot/commit/143f176a893f060eecc1fbf4a5b5c54d35a3acc7) - **clickhouse**: transpile oracle functions chr, lag, lead *(PR [#4053](https://github.com/tobymao/sqlglot/pull/4053) by [@sleshJdev](https://github.com/sleshJdev))*
- [`d89757e`](https://github.com/tobymao/sqlglot/commit/d89757e21665913d49a3ccc19deeea86ab59820c) - **postgres**: add support for the NOT VALID clause in ALTER TABLE fixes [#4077](https://github.com/tobymao/sqlglot/pull/4077) *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`3e1af21`](https://github.com/tobymao/sqlglot/commit/3e1af21787ee81df5cbb5eb8b4b7f808b404c870) - Canonicalize exp.RegexpExtract group default value *(PR [#4051](https://github.com/tobymao/sqlglot/pull/4051) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4039](https://github.com/tobymao/sqlglot/issues/4039) opened by [@hellozepp](https://github.com/hellozepp)*
- [`75cafad`](https://github.com/tobymao/sqlglot/commit/75cafad45fec02eb27f42676ddca4d1777e800f7) - **starrocks**: Move the parse distribute and duplicate to Parser *(PR [#4062](https://github.com/tobymao/sqlglot/pull/4062) by [@hellozepp](https://github.com/hellozepp))*
- [`74352d5`](https://github.com/tobymao/sqlglot/commit/74352d523333e5eff464f97b49a1bcfb11ec291b) - **tsql**: use plus operator for string concat to support more systems that use tsql *(PR [#4067](https://github.com/tobymao/sqlglot/pull/4067) by [@cpcloud](https://github.com/cpcloud))*
  - :arrow_lower_right: *fixes issue [#4066](https://github.com/tobymao/sqlglot/issues/4066) opened by [@cpcloud](https://github.com/cpcloud)*
- [`c8e2eae`](https://github.com/tobymao/sqlglot/commit/c8e2eaecad0b3b0fff725512ef571de41c5be0a1) - do not canonicalize INTERVAL values to number literals *(commit by [@VaggelisD](https://github.com/VaggelisD))*
- [`532a024`](https://github.com/tobymao/sqlglot/commit/532a024e1a1dbc422e603dc0336149362c5763df) - **snowflake, bigquery**: Remove exp.Trim generation *(PR [#4070](https://github.com/tobymao/sqlglot/pull/4070) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3095](https://github.com/TobikoData/sqlmesh/issues/3095) opened by [@plaflamme](https://github.com/plaflamme)*
- [`ad7e582`](https://github.com/tobymao/sqlglot/commit/ad7e582504955c5dba84566994e81873a46d1c28) - **athena**: Apply correct quoting to queries depending on type (DML or DDL) *(PR [#4073](https://github.com/tobymao/sqlglot/pull/4073) by [@erindru](https://github.com/erindru))*
- [`cc5b877`](https://github.com/tobymao/sqlglot/commit/cc5b8774c469250fd403ca3379f1c2dcab9d4017) - **parser**: Wrap column constraints in _parse_column_def() *(PR [#4078](https://github.com/tobymao/sqlglot/pull/4078) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4065](https://github.com/tobymao/sqlglot/issues/4065) opened by [@ajuszczak](https://github.com/ajuszczak)*
- [`4eb384a`](https://github.com/tobymao/sqlglot/commit/4eb384a799b3ad0f152893eb6217131a3a698ff1) - **clickhouse**: Remove CURRENT_TIMESTAMP from NO_PAREN_FUNCTIONS *(PR [#4079](https://github.com/tobymao/sqlglot/pull/4079) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4076](https://github.com/tobymao/sqlglot/issues/4076) opened by [@hellozepp](https://github.com/hellozepp)*

### :recycle: Refactors
- [`534f882`](https://github.com/tobymao/sqlglot/commit/534f88280a895d9f7503e48eedf600628d34aa82) - **clickhouse**: clean up chr, lag, lead generation *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`b986ebe`](https://github.com/tobymao/sqlglot/commit/b986ebe99c95ce6f76a76bfd0ea79ee4ac3757f0) - fill in more details for sqlglotrs pypi page *(PR [#4071](https://github.com/tobymao/sqlglot/pull/4071) by [@georgesittas](https://github.com/georgesittas))*
- [`0310926`](https://github.com/tobymao/sqlglot/commit/0310926297b18714a02873b649061b50c7080ac9) - bump sqlglotrs to 0.2.11 (update pypi details) *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.19.0] - 2024-09-03
### :boom: BREAKING CHANGES
- due to [`6da9328`](https://github.com/tobymao/sqlglot/commit/6da932889c2d60c82be118842f4edee031009f8a) - refactor SET OPERATION handling to set correct defaults *(PR [#4009](https://github.com/tobymao/sqlglot/pull/4009) by [@georgesittas](https://github.com/georgesittas))*:

  refactor SET OPERATION handling to set correct defaults (#4009)

- due to [`4b69d18`](https://github.com/tobymao/sqlglot/commit/4b69d18e8e23c9ba2b0a886be497df9c1071f26c) - use TO_GEOGRAPHY, TO_GEOMETRY instead of casts *(PR [#4017](https://github.com/tobymao/sqlglot/pull/4017) by [@georgesittas](https://github.com/georgesittas))*:

  use TO_GEOGRAPHY, TO_GEOMETRY instead of casts (#4017)

- due to [`0985907`](https://github.com/tobymao/sqlglot/commit/098590718104b9a6e9c0340fbe05fd89759c142b) - Add UnsupportedError to unnest_to_explode transform *(PR [#4016](https://github.com/tobymao/sqlglot/pull/4016) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add UnsupportedError to unnest_to_explode transform (#4016)

- due to [`7d63d23`](https://github.com/tobymao/sqlglot/commit/7d63d235c1f8ce7a76db3d31f11050dc65c0fef1) - Support JSON_EXISTS, refactor ON handling *(PR [#4032](https://github.com/tobymao/sqlglot/pull/4032) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support JSON_EXISTS, refactor ON handling (#4032)


### :sparkles: New Features
- [`f550ba1`](https://github.com/tobymao/sqlglot/commit/f550ba1068eaa4be45c19b4a3ea11baad48b27c1) - **presto**: support [ SECURITY { DEFINER | INVOKER } ] *(PR [#4008](https://github.com/tobymao/sqlglot/pull/4008) by [@usmanovbf](https://github.com/usmanovbf))*
- [`dedd757`](https://github.com/tobymao/sqlglot/commit/dedd75790ecc2549fa7b28b3612125ca2aaeb762) - **oracle**: Parse multitable inserts *(PR [#4000](https://github.com/tobymao/sqlglot/pull/4000) by [@usefulalgorithm](https://github.com/usefulalgorithm))*
- [`0985907`](https://github.com/tobymao/sqlglot/commit/098590718104b9a6e9c0340fbe05fd89759c142b) - Add UnsupportedError to unnest_to_explode transform *(PR [#4016](https://github.com/tobymao/sqlglot/pull/4016) by [@VaggelisD](https://github.com/VaggelisD))*
- [`8f5fccf`](https://github.com/tobymao/sqlglot/commit/8f5fccfca8502e0fe00420662825845cc640a1cb) - **presto**: generate non-iso DayOfWeek *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`7d63d23`](https://github.com/tobymao/sqlglot/commit/7d63d235c1f8ce7a76db3d31f11050dc65c0fef1) - **oracle**: Support JSON_EXISTS, refactor ON handling *(PR [#4032](https://github.com/tobymao/sqlglot/pull/4032) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4026](https://github.com/tobymao/sqlglot/issues/4026) opened by [@ashishshukla19](https://github.com/ashishshukla19)*
- [`85cc7ad`](https://github.com/tobymao/sqlglot/commit/85cc7ad68599dde59ffab460d49010f167cab85d) - **duckdb**: Transpile BQ's exp.ArrayToString *(PR [#4034](https://github.com/tobymao/sqlglot/pull/4034) by [@VaggelisD](https://github.com/VaggelisD))*
- [`7f2c7f1`](https://github.com/tobymao/sqlglot/commit/7f2c7f17f5d79c3ea93b43cdeacdb5339955c9a8) - Support for NORMALIZE() function *(PR [#4041](https://github.com/tobymao/sqlglot/pull/4041) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#4037](https://github.com/tobymao/sqlglot/issues/4037) opened by [@jasonrosendale](https://github.com/jasonrosendale)*
- [`a1b9803`](https://github.com/tobymao/sqlglot/commit/a1b980327ff94519a4cba1e0e48066c0ea51d359) - support COMPRESS column constraint wihout a value *(PR [#4045](https://github.com/tobymao/sqlglot/pull/4045) by [@thomascjohnson](https://github.com/thomascjohnson))*

### :bug: Bug Fixes
- [`8583772`](https://github.com/tobymao/sqlglot/commit/85837729e746743755294727d0394534834f4c4c) - **tsql**: Use count_big instead of count *(PR [#3996](https://github.com/tobymao/sqlglot/pull/3996) by [@colin-ho](https://github.com/colin-ho))*
  - :arrow_lower_right: *fixes issue [#3995](https://github.com/tobymao/sqlglot/issues/3995) opened by [@colin-ho](https://github.com/colin-ho)*
- [`4b7ca2b`](https://github.com/tobymao/sqlglot/commit/4b7ca2be353e7432b84384ff9cfd43f3c43438e0) - **spark**: Custom annotation for SUBSTRING() *(PR [#4004](https://github.com/tobymao/sqlglot/pull/4004) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4002](https://github.com/tobymao/sqlglot/issues/4002) opened by [@racevedoo](https://github.com/racevedoo)*
- [`cb172db`](https://github.com/tobymao/sqlglot/commit/cb172dbe4d13fd3badad352ea79d2fd6e5271576) - **clickhouse**: ensure that ALL and DISTINCT are rendered for except and intersect *(PR [#4007](https://github.com/tobymao/sqlglot/pull/4007) by [@cpcloud](https://github.com/cpcloud))*
  - :arrow_lower_right: *fixes issue [#4005](https://github.com/tobymao/sqlglot/issues/4005) opened by [@cpcloud](https://github.com/cpcloud)*
- [`829fdcb`](https://github.com/tobymao/sqlglot/commit/829fdcb1dbe52710269823bda93e3e49c02dbf63) - **starrocks**: exp.Unnest transpilation *(PR [#3999](https://github.com/tobymao/sqlglot/pull/3999) by [@hellozepp](https://github.com/hellozepp))*
  - :arrow_lower_right: *fixes issue [#3962](https://github.com/tobymao/sqlglot/issues/3962) opened by [@hellozepp](https://github.com/hellozepp)*
- [`6da9328`](https://github.com/tobymao/sqlglot/commit/6da932889c2d60c82be118842f4edee031009f8a) - refactor SET OPERATION handling to set correct defaults *(PR [#4009](https://github.com/tobymao/sqlglot/pull/4009) by [@georgesittas](https://github.com/georgesittas))*
- [`23a928e`](https://github.com/tobymao/sqlglot/commit/23a928edc1d204a13516e0db38336774962a135e) - **mysql**: Preserve roundtrip of %a, %W time formats *(PR [#4014](https://github.com/tobymao/sqlglot/pull/4014) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#4011](https://github.com/tobymao/sqlglot/issues/4011) opened by [@hellozepp](https://github.com/hellozepp)*
- [`2d4483c`](https://github.com/tobymao/sqlglot/commit/2d4483c0f79c5c72438a7093c938b1f178e5d48a) - don't log warning in to_json_path conditionally *(PR [#4015](https://github.com/tobymao/sqlglot/pull/4015) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4013](https://github.com/tobymao/sqlglot/issues/4013) opened by [@hellozepp](https://github.com/hellozepp)*
- [`4b69d18`](https://github.com/tobymao/sqlglot/commit/4b69d18e8e23c9ba2b0a886be497df9c1071f26c) - **snowflake**: use TO_GEOGRAPHY, TO_GEOMETRY instead of casts *(PR [#4017](https://github.com/tobymao/sqlglot/pull/4017) by [@georgesittas](https://github.com/georgesittas))*
- [`1108426`](https://github.com/tobymao/sqlglot/commit/1108426a0eb23bbcaec8bed946f1dae6682bc1dd) - **optimizer**: annotate unary expressions correctly *(PR [#4019](https://github.com/tobymao/sqlglot/pull/4019) by [@georgesittas](https://github.com/georgesittas))*
- [`5fad18c`](https://github.com/tobymao/sqlglot/commit/5fad18c6cb5f62630cdfa2616231436586c41d67) - **presto**: exp.DayOfWeek *(PR [#4024](https://github.com/tobymao/sqlglot/pull/4024) by [@hellozepp](https://github.com/hellozepp))*
- [`ea9a494`](https://github.com/tobymao/sqlglot/commit/ea9a4948a3e1619b885fc0b1522a7382d68c9cbe) - **parser**: consume STREAM in _parse_select only if it's a VAR, closes [#4029](https://github.com/tobymao/sqlglot/pull/4029) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`c239a74`](https://github.com/tobymao/sqlglot/commit/c239a741233bf858ce686d2d32a657cbedb49699) - transpile null exclusion for ARRAY_AGG *(PR [#4033](https://github.com/tobymao/sqlglot/pull/4033) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#4031](https://github.com/tobymao/sqlglot/issues/4031) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`60a8f16`](https://github.com/tobymao/sqlglot/commit/60a8f16b5386fe334b6e15afa967ad7bdd2a83de) - **parser**: don't consume strings in match_text_seq *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a726583`](https://github.com/tobymao/sqlglot/commit/a726583995716f815946b4c81c07a916ade727b7) - **parser**: don't consume strings in match_texts *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`551d32f`](https://github.com/tobymao/sqlglot/commit/551d32fdebfc78506b77e4f6e6882d2a8cbd457c) - **postgres**: Support for DROP INDEX CONCURRENTLY. *(PR [#4040](https://github.com/tobymao/sqlglot/pull/4040) by [@EdgyEdgemond](https://github.com/EdgyEdgemond))*
  - :arrow_lower_right: *fixes issue [#3783](https://github.com/tobymao/sqlglot/issues/3783) opened by [@EdgyEdgemond](https://github.com/EdgyEdgemond)*
- [`f55647d`](https://github.com/tobymao/sqlglot/commit/f55647d9d9c088880c0c16efff23ef8d22c2be44) - **starrocks**: exp.Create transpilation *(PR [#4023](https://github.com/tobymao/sqlglot/pull/4023) by [@hellozepp](https://github.com/hellozepp))*
  - :arrow_lower_right: *fixes issue [#3997](https://github.com/tobymao/sqlglot/issues/3997) opened by [@hellozepp](https://github.com/hellozepp)*
- [`bf0f5fa`](https://github.com/tobymao/sqlglot/commit/bf0f5fa1ab44daa74102b0f16ae16f905b175fbc) - **parser**: Ensure exp.Coalesce expressions is a list *(PR [#4050](https://github.com/tobymao/sqlglot/pull/4050) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3080](https://github.com/TobikoData/sqlmesh/issues/3080) opened by [@Ziemin](https://github.com/Ziemin)*

### :recycle: Refactors
- [`6494776`](https://github.com/tobymao/sqlglot/commit/6494776a45ae4975cee21f70b5f383d29530d155) - simplify multi-insert generation, fix pretty mode *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b11c73e`](https://github.com/tobymao/sqlglot/commit/b11c73e38aa495715c327f44586714e19f699c9c) - clean up starrocks DISTRIBUTED BY property generation *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`c35a62c`](https://github.com/tobymao/sqlglot/commit/c35a62cdbc29c796bf0728d3f26e5ae5474881a8) - set the license for sqlglotrs *(PR [#4048](https://github.com/tobymao/sqlglot/pull/4048) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#4047](https://github.com/tobymao/sqlglot/issues/4047) opened by [@chriscc2](https://github.com/chriscc2)*
- [`9b7eb2e`](https://github.com/tobymao/sqlglot/commit/9b7eb2e40e4bec4d18664f09e01c1165122dd43f) - bump sqlglotrs to v0.2.10 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.18.0] - 2024-08-28
### :boom: BREAKING CHANGES
- due to [`22bb9a0`](https://github.com/tobymao/sqlglot/commit/22bb9a0e5c64ae344c9e25ed34200ed743e7b8f0) - stop normalizing qualified anonymous functions *(PR [#3969](https://github.com/tobymao/sqlglot/pull/3969) by [@georgesittas](https://github.com/georgesittas))*:

  stop normalizing qualified anonymous functions (#3969)

- due to [`8aec682`](https://github.com/tobymao/sqlglot/commit/8aec68253b10dcbfe7cc5b3d6e1145ae714ca346) - mysql/tsql datetime precision, formatting, exp.AtTimeZone *(PR [#3951](https://github.com/tobymao/sqlglot/pull/3951) by [@erindru](https://github.com/erindru))*:

  mysql/tsql datetime precision, formatting, exp.AtTimeZone (#3951)

- due to [`2f3626a`](https://github.com/tobymao/sqlglot/commit/2f3626a4fc20c46411cd91bf8beda2bdd103ca4a) - Generation of exp.SHA2, exp.Transform, exp.IgnoreNulls *(PR [#3980](https://github.com/tobymao/sqlglot/pull/3980) by [@VaggelisD](https://github.com/VaggelisD))*:

  Generation of exp.SHA2, exp.Transform, exp.IgnoreNulls (#3980)

- due to [`905b722`](https://github.com/tobymao/sqlglot/commit/905b7226ae4a6dc505fe303bb4df3818cb586826) - preserve each distinct CUBE/ROLLUP/GROUPING SET clause *(PR [#3985](https://github.com/tobymao/sqlglot/pull/3985) by [@georgesittas](https://github.com/georgesittas))*:

  preserve each distinct CUBE/ROLLUP/GROUPING SET clause (#3985)


### :sparkles: New Features
- [`48b214d`](https://github.com/tobymao/sqlglot/commit/48b214da7e39d36938d12059deb827d0a5f6a5a2) - **postgres**: Support for IS JSON predicate *(PR [#3971](https://github.com/tobymao/sqlglot/pull/3971) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3965](https://github.com/tobymao/sqlglot/issues/3965) opened by [@faisal-ksolves](https://github.com/faisal-ksolves)*
- [`f7e4e4a`](https://github.com/tobymao/sqlglot/commit/f7e4e4adc64aaef73d23c2550a4bfa9958d4851b) - **duckdb**: add support for the GLOB table function closes [#3973](https://github.com/tobymao/sqlglot/pull/3973) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a0d1377`](https://github.com/tobymao/sqlglot/commit/a0d137787885627aae07f11a9c18a4cc133baa0a) - **spark**: add support for table statement in INSERT *(PR [#3986](https://github.com/tobymao/sqlglot/pull/3986) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3984](https://github.com/tobymao/sqlglot/issues/3984) opened by [@madeirak](https://github.com/madeirak)*
- [`f5bfd67`](https://github.com/tobymao/sqlglot/commit/f5bfd67341518d0ecb1c3693e0b41ed5c1cf0596) - **mysql**: Parse JSON_VALUE() *(PR [#3987](https://github.com/tobymao/sqlglot/pull/3987) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3983](https://github.com/tobymao/sqlglot/issues/3983) opened by [@ashishshukla19](https://github.com/ashishshukla19)*
- [`79e92ad`](https://github.com/tobymao/sqlglot/commit/79e92ad565c42098ff7b7921fe04e6aac7859dd8) - **spark**: Default naming of STRUCT fields *(PR [#3991](https://github.com/tobymao/sqlglot/pull/3991) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3988](https://github.com/tobymao/sqlglot/issues/3988) opened by [@dor-bernstein](https://github.com/dor-bernstein)*

### :bug: Bug Fixes
- [`22bb9a0`](https://github.com/tobymao/sqlglot/commit/22bb9a0e5c64ae344c9e25ed34200ed743e7b8f0) - stop normalizing qualified anonymous functions *(PR [#3969](https://github.com/tobymao/sqlglot/pull/3969) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3967](https://github.com/tobymao/sqlglot/issues/3967) opened by [@cpcloud](https://github.com/cpcloud)*
- [`8aec682`](https://github.com/tobymao/sqlglot/commit/8aec68253b10dcbfe7cc5b3d6e1145ae714ca346) - mysql/tsql datetime precision, formatting, exp.AtTimeZone *(PR [#3951](https://github.com/tobymao/sqlglot/pull/3951) by [@erindru](https://github.com/erindru))*
- [`d37a5bb`](https://github.com/tobymao/sqlglot/commit/d37a5bbfcd5732aa64a24bd83dde4abcac8b0bed) - **snowflake**: handle DIV0 case where divident is null *(PR [#3975](https://github.com/tobymao/sqlglot/pull/3975) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3974](https://github.com/tobymao/sqlglot/issues/3974) opened by [@Nathan-Fenner](https://github.com/Nathan-Fenner)*
- [`b2f877b`](https://github.com/tobymao/sqlglot/commit/b2f877ba5fc9ec9fdafad74196dda1631fdfc0c1) - **oracle**: Use LTRIM/RTRIM unless BOTH is specified *(PR [#3977](https://github.com/tobymao/sqlglot/pull/3977) by [@VaggelisD](https://github.com/VaggelisD))*
- [`201b51a`](https://github.com/tobymao/sqlglot/commit/201b51a860d4db2b2e49e04f6534b7ad22ae287c) - **sqlite**: Make IS parser more lenient *(PR [#3981](https://github.com/tobymao/sqlglot/pull/3981) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3978](https://github.com/tobymao/sqlglot/issues/3978) opened by [@focafull](https://github.com/focafull)*
- [`2f3626a`](https://github.com/tobymao/sqlglot/commit/2f3626a4fc20c46411cd91bf8beda2bdd103ca4a) - **duckdb**: Generation of exp.SHA2, exp.Transform, exp.IgnoreNulls *(PR [#3980](https://github.com/tobymao/sqlglot/pull/3980) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3972](https://github.com/tobymao/sqlglot/issues/3972) opened by [@dor-bernstein](https://github.com/dor-bernstein)*
- [`905b722`](https://github.com/tobymao/sqlglot/commit/905b7226ae4a6dc505fe303bb4df3818cb586826) - **parser**: preserve each distinct CUBE/ROLLUP/GROUPING SET clause *(PR [#3985](https://github.com/tobymao/sqlglot/pull/3985) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3979](https://github.com/tobymao/sqlglot/issues/3979) opened by [@cpcloud](https://github.com/cpcloud)*
- [`ee9dc39`](https://github.com/tobymao/sqlglot/commit/ee9dc399134ad86720abe480ee2565de822336cf) - Fix binding of TABLESAMPLE to exp.Subquery instead of top-level exp.Select *(PR [#3994](https://github.com/tobymao/sqlglot/pull/3994) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3992](https://github.com/tobymao/sqlglot/issues/3992) opened by [@cpcloud](https://github.com/cpcloud)*


## [v25.17.0] - 2024-08-26
### :boom: BREAKING CHANGES
- due to [`0a9ba05`](https://github.com/tobymao/sqlglot/commit/0a9ba0536235e10aed02d4ff5e571e435a00febc) - 0 is falsey *(commit by [@tobymao](https://github.com/tobymao))*:

  0 is falsey


### :bug: Bug Fixes
- [`42b725e`](https://github.com/tobymao/sqlglot/commit/42b725e4821a1426fe7c93f9fecbd4ec372accc9) - flaky test closes [#3961](https://github.com/tobymao/sqlglot/pull/3961) *(commit by [@tobymao](https://github.com/tobymao))*
- [`cc29921`](https://github.com/tobymao/sqlglot/commit/cc299217f5d31a0406ba3c4778bb1ce581fe3f4a) - Parse LTRIM/RTRIM functions as positional exp.Trim *(PR [#3958](https://github.com/tobymao/sqlglot/pull/3958) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3957](https://github.com/tobymao/sqlglot/issues/3957) opened by [@sleshJdev](https://github.com/sleshJdev)*
- [`678e692`](https://github.com/tobymao/sqlglot/commit/678e6926fdbefb16efbbcaef9cd6c5ca284af54a) - make sample an arg of table, not a wrapper *(PR [#3963](https://github.com/tobymao/sqlglot/pull/3963) by [@barakalon](https://github.com/barakalon))*
- [`0a9ba05`](https://github.com/tobymao/sqlglot/commit/0a9ba0536235e10aed02d4ff5e571e435a00febc) - 0 is falsey *(commit by [@tobymao](https://github.com/tobymao))*
- [`c1ac987`](https://github.com/tobymao/sqlglot/commit/c1ac9872a6f77acd52546edbc9da53e350ebf080) - **starrocks**: exp.Array generation, exp.Unnest alias *(PR [#3964](https://github.com/tobymao/sqlglot/pull/3964) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3962](https://github.com/tobymao/sqlglot/issues/3962) opened by [@hellozepp](https://github.com/hellozepp)*


## [v25.16.1] - 2024-08-23
### :bug: Bug Fixes
- [`c4e5be7`](https://github.com/tobymao/sqlglot/commit/c4e5be7d3f4d7a9075d11dc56ece02774f32e749) - include dialect when parsing inside cast *(PR [#3960](https://github.com/tobymao/sqlglot/pull/3960) by [@eakmanrq](https://github.com/eakmanrq))*

### :wrench: Chores
- [`794dc4c`](https://github.com/tobymao/sqlglot/commit/794dc4cea3c4298c8986ade8e0fee88479851b34) - update readme to include onboarding doc *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.16.0] - 2024-08-22
### :boom: BREAKING CHANGES
- due to [`f68d155`](https://github.com/tobymao/sqlglot/commit/f68d155c38a79a6527685c37f8de8773ce790bca) - exp.Merge, for Trino and Postgres, dont strip the target alias from then WHEN MATCHED condition to prevent an ambiguous column error *(PR [#3940](https://github.com/tobymao/sqlglot/pull/3940) by [@erindru](https://github.com/erindru))*:

  exp.Merge, for Trino and Postgres, dont strip the target alias from then WHEN MATCHED condition to prevent an ambiguous column error (#3940)

- due to [`667f7d9`](https://github.com/tobymao/sqlglot/commit/667f7d9e94e14ff619998d2001b6116d363f2a1f) - attach INTERPOLATE expressions to WithFill *(PR [#3944](https://github.com/tobymao/sqlglot/pull/3944) by [@georgesittas](https://github.com/georgesittas))*:

  attach INTERPOLATE expressions to WithFill (#3944)

- due to [`145fdbf`](https://github.com/tobymao/sqlglot/commit/145fdbf6bb02fa1c55087bfd9f6b3a15fbd4b684) - Redshift date format *(PR [#3942](https://github.com/tobymao/sqlglot/pull/3942) by [@erindru](https://github.com/erindru))*:

  Redshift date format (#3942)

- due to [`a84a21a`](https://github.com/tobymao/sqlglot/commit/a84a21aaef0e65754e67ecebdfcbf7136c77acc7) - Add timezone support to exp.TimeStrToTime *(PR [#3938](https://github.com/tobymao/sqlglot/pull/3938) by [@erindru](https://github.com/erindru))*:

  Add timezone support to exp.TimeStrToTime (#3938)


### :sparkles: New Features
- [`a84a21a`](https://github.com/tobymao/sqlglot/commit/a84a21aaef0e65754e67ecebdfcbf7136c77acc7) - Add timezone support to exp.TimeStrToTime *(PR [#3938](https://github.com/tobymao/sqlglot/pull/3938) by [@erindru](https://github.com/erindru))*
- [`70a052a`](https://github.com/tobymao/sqlglot/commit/70a052a672d0c72a3e53b19316defb01144f2907) - transpile from_iso8601_timestamp from presto/trino to duckdb *(PR [#3956](https://github.com/tobymao/sqlglot/pull/3956) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`f68d155`](https://github.com/tobymao/sqlglot/commit/f68d155c38a79a6527685c37f8de8773ce790bca) - exp.Merge, for Trino and Postgres, dont strip the target alias from then WHEN MATCHED condition to prevent an ambiguous column error *(PR [#3940](https://github.com/tobymao/sqlglot/pull/3940) by [@erindru](https://github.com/erindru))*
- [`0458dc0`](https://github.com/tobymao/sqlglot/commit/0458dc0fa1978388336b9fa459b28508d7b40f9e) - **optimizer**: expand alias refs recursive CTE edge case patch *(PR [#3943](https://github.com/tobymao/sqlglot/pull/3943) by [@georgesittas](https://github.com/georgesittas))*
- [`145fdbf`](https://github.com/tobymao/sqlglot/commit/145fdbf6bb02fa1c55087bfd9f6b3a15fbd4b684) - Redshift date format *(PR [#3942](https://github.com/tobymao/sqlglot/pull/3942) by [@erindru](https://github.com/erindru))*
- [`6233c2c`](https://github.com/tobymao/sqlglot/commit/6233c2c75ab3a3bc0dfbf28d3fa8adc1be719281) - **parser**: Support sqls with DESCRIBE partition  *(PR [#3945](https://github.com/tobymao/sqlglot/pull/3945) by [@gp1105739](https://github.com/gp1105739))*
  - :arrow_lower_right: *fixes issue [#3941](https://github.com/tobymao/sqlglot/issues/3941) opened by [@gp1105739](https://github.com/gp1105739)*
- [`85cd6e5`](https://github.com/tobymao/sqlglot/commit/85cd6e507b73be89d2d9b2c88c7370a14b813b5c) - **bigquery**: Map %e to %-d *(PR [#3946](https://github.com/tobymao/sqlglot/pull/3946) by [@VaggelisD](https://github.com/VaggelisD))*
- [`1ba0f03`](https://github.com/tobymao/sqlglot/commit/1ba0f03fbfe5dadc3411c7ff26e6dfbef852491a) - **duckdb**: TIME does not support modifiers *(PR [#3947](https://github.com/tobymao/sqlglot/pull/3947) by [@georgesittas](https://github.com/georgesittas))*
- [`d5d3615`](https://github.com/tobymao/sqlglot/commit/d5d361571cd463869e2243d257f9b6ad0615c070) - **optimizer**: convert TsOrDsToDate to Cast more conservatively *(PR [#3949](https://github.com/tobymao/sqlglot/pull/3949) by [@barakalon](https://github.com/barakalon))*
- [`fb6edc7`](https://github.com/tobymao/sqlglot/commit/fb6edc774539704b48e7d2805ef3211636af18aa) - oracle/snowflake comments closes [#3950](https://github.com/tobymao/sqlglot/pull/3950) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1284fd0`](https://github.com/tobymao/sqlglot/commit/1284fd0a64890d3548af7ed0a0cc05bb6166ccb2) - **oracle**: Revert NVL() being parsed into exp.Anonymous *(PR [#3954](https://github.com/tobymao/sqlglot/pull/3954) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3952](https://github.com/tobymao/sqlglot/issues/3952) opened by [@sleshJdev](https://github.com/sleshJdev)*
- [`c99f8d5`](https://github.com/tobymao/sqlglot/commit/c99f8d5bda79f16fb0d71ae73127cc826860e104) - **duckdb**: Fix exp.Unnest generation for BQ's nested arrays *(PR [#3931](https://github.com/tobymao/sqlglot/pull/3931) by [@VaggelisD](https://github.com/VaggelisD))*

### :recycle: Refactors
- [`f16b0e7`](https://github.com/tobymao/sqlglot/commit/f16b0e7203ad60f0ce50861c4d78176ca53eb2cf) - iteratively generate binary expressions *(PR [#3926](https://github.com/tobymao/sqlglot/pull/3926) by [@MatMoore](https://github.com/MatMoore))*
- [`667f7d9`](https://github.com/tobymao/sqlglot/commit/667f7d9e94e14ff619998d2001b6116d363f2a1f) - **clickhouse**: attach INTERPOLATE expressions to WithFill *(PR [#3944](https://github.com/tobymao/sqlglot/pull/3944) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`c697357`](https://github.com/tobymao/sqlglot/commit/c6973572dfd953b5539bb4e9dcba402c0c3c6acf) - slightly refactor Generator.binary, add stress test *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6a5f619`](https://github.com/tobymao/sqlglot/commit/6a5f6199f6da0053fa4564e71a17e3b9f91f0496) - New doc - Onboarding Doc *(PR [#3902](https://github.com/tobymao/sqlglot/pull/3902) by [@VaggelisD](https://github.com/VaggelisD))*


## [v25.15.0] - 2024-08-19
### :boom: BREAKING CHANGES
- due to [`a668655`](https://github.com/tobymao/sqlglot/commit/a668655440815605a566c52b65b28decdfb551eb) - preserve SYSDATE *(PR [#3935](https://github.com/tobymao/sqlglot/pull/3935) by [@georgesittas](https://github.com/georgesittas))*:

  preserve SYSDATE (#3935)


### :sparkles: New Features
- [`be11f4c`](https://github.com/tobymao/sqlglot/commit/be11f4c57c7842f69950bafc3225fb9c139af014) - **clickhouse**: add support for "@"-style parameters *(PR [#3939](https://github.com/tobymao/sqlglot/pull/3939) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`a668655`](https://github.com/tobymao/sqlglot/commit/a668655440815605a566c52b65b28decdfb551eb) - **oracle**: preserve SYSDATE *(PR [#3935](https://github.com/tobymao/sqlglot/pull/3935) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3934](https://github.com/tobymao/sqlglot/issues/3934) opened by [@Hal-H2Apps](https://github.com/Hal-H2Apps)*
- [`b824f8a`](https://github.com/tobymao/sqlglot/commit/b824f8a4148ace01750db301daf4a663dc03b580) - **parser**: allow complex expressions for UNPIVOT alias *(PR [#3937](https://github.com/tobymao/sqlglot/pull/3937) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3936](https://github.com/tobymao/sqlglot/issues/3936) opened by [@dbittenbender](https://github.com/dbittenbender)*

### :recycle: Refactors
- [`f4c34d3`](https://github.com/tobymao/sqlglot/commit/f4c34d37c5773c37a13437c7e0e7eb27b4e98877) - move "MINUS": TokenType.EXCEPT to hive instead of spark *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.14.0] - 2024-08-19
### :boom: BREAKING CHANGES
- due to [`605f1b2`](https://github.com/tobymao/sqlglot/commit/605f1b217d5d1de654cfe2fa1b51435a1a71ae62) - use creatable kind mapping dict for schema<-->database substitution *(PR [#3924](https://github.com/tobymao/sqlglot/pull/3924) by [@treysp](https://github.com/treysp))*:

  use creatable kind mapping dict for schema<-->database substitution (#3924)

- due to [`f418caa`](https://github.com/tobymao/sqlglot/commit/f418caafa8ed317f9e360c6c8f01bdac596258e5) - skip nullable comparison in is_type by default *(PR [#3927](https://github.com/tobymao/sqlglot/pull/3927) by [@georgesittas](https://github.com/georgesittas))*:

  skip nullable comparison in is_type by default (#3927)


### :sparkles: New Features
- [`f418caa`](https://github.com/tobymao/sqlglot/commit/f418caafa8ed317f9e360c6c8f01bdac596258e5) - skip nullable comparison in is_type by default *(PR [#3927](https://github.com/tobymao/sqlglot/pull/3927) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`605f1b2`](https://github.com/tobymao/sqlglot/commit/605f1b217d5d1de654cfe2fa1b51435a1a71ae62) - **clickhouse**: use creatable kind mapping dict for schema<-->database substitution *(PR [#3924](https://github.com/tobymao/sqlglot/pull/3924) by [@treysp](https://github.com/treysp))*


## [v25.13.0] - 2024-08-17
### :boom: BREAKING CHANGES
- due to [`102f5d4`](https://github.com/tobymao/sqlglot/commit/102f5d48279ac1a7a1851737f55a13bd08512f3d) - infer set op types more accurately *(PR [#3918](https://github.com/tobymao/sqlglot/pull/3918) by [@georgesittas](https://github.com/georgesittas))*:

  infer set op types more accurately (#3918)

- due to [`46496a6`](https://github.com/tobymao/sqlglot/commit/46496a6af80bd49d36ef8d265800679d2b07c4db) - improve transpilation of nullable/non-nullable data types *(PR [#3921](https://github.com/tobymao/sqlglot/pull/3921) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilation of nullable/non-nullable data types (#3921)


### :bug: Bug Fixes
- [`c74a8fd`](https://github.com/tobymao/sqlglot/commit/c74a8fd2acd859f5947f27a8f091f13fba1d39e4) - **clickhouse**: make try_cast toXXXOrNull() functions case-specific *(PR [#3917](https://github.com/tobymao/sqlglot/pull/3917) by [@treysp](https://github.com/treysp))*
- [`102f5d4`](https://github.com/tobymao/sqlglot/commit/102f5d48279ac1a7a1851737f55a13bd08512f3d) - **optimizer**: infer set op types more accurately *(PR [#3918](https://github.com/tobymao/sqlglot/pull/3918) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3916](https://github.com/tobymao/sqlglot/issues/3916) opened by [@racevedoo](https://github.com/racevedoo)*

### :recycle: Refactors
- [`1d436d4`](https://github.com/tobymao/sqlglot/commit/1d436d45b4469bb8195dd3597319b6fc5c3f2344) - **clickhouse**: transpile TRY_CAST(x AS T) to CAST(x AS Nullable(T)) *(PR [#3919](https://github.com/tobymao/sqlglot/pull/3919) by [@georgesittas](https://github.com/georgesittas))*
- [`46496a6`](https://github.com/tobymao/sqlglot/commit/46496a6af80bd49d36ef8d265800679d2b07c4db) - **clickhouse**: improve transpilation of nullable/non-nullable data types *(PR [#3921](https://github.com/tobymao/sqlglot/pull/3921) by [@georgesittas](https://github.com/georgesittas))*


## [v25.12.0] - 2024-08-15
### :boom: BREAKING CHANGES
- due to [`e8e70f3`](https://github.com/tobymao/sqlglot/commit/e8e70f3a6cc2ca24de2afe622bbcbccb1ac8aeb3) - treat DATABASE kind as SCHEMA (and conversely) in exp.Create *(PR [#3912](https://github.com/tobymao/sqlglot/pull/3912) by [@georgesittas](https://github.com/georgesittas))*:

  treat DATABASE kind as SCHEMA (and conversely) in exp.Create (#3912)


### :sparkles: New Features
- [`9a66903`](https://github.com/tobymao/sqlglot/commit/9a66903975f16a09d84337a8405bf70945706412) - **clickhouse**: add support for TryCast generation *(PR [#3913](https://github.com/tobymao/sqlglot/pull/3913) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`7965cac`](https://github.com/tobymao/sqlglot/commit/7965cace1d9632c865cae257781072b0932b709d) - **clickhouse**: wrap query in CTAS when COMMENT prop is present *(PR [#3911](https://github.com/tobymao/sqlglot/pull/3911) by [@georgesittas](https://github.com/georgesittas))*
- [`e8e70f3`](https://github.com/tobymao/sqlglot/commit/e8e70f3a6cc2ca24de2afe622bbcbccb1ac8aeb3) - **clickhouse**: treat DATABASE kind as SCHEMA (and conversely) in exp.Create *(PR [#3912](https://github.com/tobymao/sqlglot/pull/3912) by [@georgesittas](https://github.com/georgesittas))*


## [v25.11.3] - 2024-08-14
### :bug: Bug Fixes
- [`57f7aa9`](https://github.com/tobymao/sqlglot/commit/57f7aa9108ed38c0e83ef5bf4fac900434fac777) - **clickhouse**: COMMENT property in CTAS needs to come last *(PR [#3910](https://github.com/tobymao/sqlglot/pull/3910) by [@georgesittas](https://github.com/georgesittas))*


## [v25.11.2] - 2024-08-14
### :bug: Bug Fixes
- [`c22f411`](https://github.com/tobymao/sqlglot/commit/c22f41129985ecfd3b3906b9594ca1692b91708c) - **clickhouse**: ensure we generate the Table in creatable_sql if it represents a db ref *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`19eee93`](https://github.com/tobymao/sqlglot/commit/19eee93c8027e6c612611d3b54980e193e0b6f49) - various fixups for unnest(generatedatearray) transpilation *(PR [#3906](https://github.com/tobymao/sqlglot/pull/3906) by [@georgesittas](https://github.com/georgesittas))*


## [v25.11.1] - 2024-08-13
### :sparkles: New Features
- [`790c1b1`](https://github.com/tobymao/sqlglot/commit/790c1b141d4bc2206df017c70416b589932886a4) - **clickhouse**: support PARTITION BY, SETTINGS in Insert expression *(PR [#3904](https://github.com/tobymao/sqlglot/pull/3904) by [@georgesittas](https://github.com/georgesittas))*


## [v25.11.0] - 2024-08-13
### :boom: BREAKING CHANGES
- due to [`0428c37`](https://github.com/tobymao/sqlglot/commit/0428c37e11f42be8eba352e69c1d2e7425824d38) - Support ALTER VIEW AS SELECT *(PR [#3873](https://github.com/tobymao/sqlglot/pull/3873) by [@xiaohui-sun](https://github.com/xiaohui-sun))*:

  Support ALTER VIEW AS SELECT (#3873)

- due to [`a666117`](https://github.com/tobymao/sqlglot/commit/a666117dcb887031f5995c50d687405b9c145fbd) - parse v NOT IN (subquery) as v <> ALL (subquery) *(PR [#3891](https://github.com/tobymao/sqlglot/pull/3891) by [@georgesittas](https://github.com/georgesittas))*:

  parse v NOT IN (subquery) as v <> ALL (subquery) (#3891)

- due to [`d968932`](https://github.com/tobymao/sqlglot/commit/d968932ef742e97ccf3ec6cdca0bc3319830f0a9) - treat identifiers as case-sensitive, handle EMPTY table property, generate DateStrToDate *(PR [#3895](https://github.com/tobymao/sqlglot/pull/3895) by [@jwhitaker-gridcog](https://github.com/jwhitaker-gridcog))*:

  treat identifiers as case-sensitive, handle EMPTY table property, generate DateStrToDate (#3895)

- due to [`1d7319a`](https://github.com/tobymao/sqlglot/commit/1d7319a8425aace6c11f59552fdd19bdbf5efd03) - transpile Unnest(GenerateDateArray(...)) to various dialects *(PR [#3899](https://github.com/tobymao/sqlglot/pull/3899) by [@georgesittas](https://github.com/georgesittas))*:

  transpile Unnest(GenerateDateArray(...)) to various dialects (#3899)


### :sparkles: New Features
- [`0428c37`](https://github.com/tobymao/sqlglot/commit/0428c37e11f42be8eba352e69c1d2e7425824d38) - **parser**: Support ALTER VIEW AS SELECT *(PR [#3873](https://github.com/tobymao/sqlglot/pull/3873) by [@xiaohui-sun](https://github.com/xiaohui-sun))*
- [`8a48458`](https://github.com/tobymao/sqlglot/commit/8a48458e20e6d0833638e750565da138bdcd5d55) - **athena**: parse UNLOAD into exp.Command closes [#3896](https://github.com/tobymao/sqlglot/pull/3896) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6f1527f`](https://github.com/tobymao/sqlglot/commit/6f1527fd3ebd16f49edb351f050a1db687824530) - **bigquery**: transpile format_datetime, datetime_trunc to duckdb *(PR [#3894](https://github.com/tobymao/sqlglot/pull/3894) by [@skadel](https://github.com/skadel))*
- [`1d7319a`](https://github.com/tobymao/sqlglot/commit/1d7319a8425aace6c11f59552fdd19bdbf5efd03) - transpile Unnest(GenerateDateArray(...)) to various dialects *(PR [#3899](https://github.com/tobymao/sqlglot/pull/3899) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`2cac14f`](https://github.com/tobymao/sqlglot/commit/2cac14f480dcaf458b1eb36b694770ce24f56e61) - generate set ops in ALTER VIEW AS statement *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a666117`](https://github.com/tobymao/sqlglot/commit/a666117dcb887031f5995c50d687405b9c145fbd) - **snowflake**: parse v NOT IN (subquery) as v <> ALL (subquery) *(PR [#3891](https://github.com/tobymao/sqlglot/pull/3891) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3890](https://github.com/tobymao/sqlglot/issues/3890) opened by [@ajuszczak](https://github.com/ajuszczak)*
- [`924a4af`](https://github.com/tobymao/sqlglot/commit/924a4af146952e84688fdccb7b63883fcd7fb255) - **oracle**: preserve function-style MOD syntax fixes [#3897](https://github.com/tobymao/sqlglot/pull/3897) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`d968932`](https://github.com/tobymao/sqlglot/commit/d968932ef742e97ccf3ec6cdca0bc3319830f0a9) - **clickhouse**: treat identifiers as case-sensitive, handle EMPTY table property, generate DateStrToDate *(PR [#3895](https://github.com/tobymao/sqlglot/pull/3895) by [@jwhitaker-gridcog](https://github.com/jwhitaker-gridcog))*
- [`3e5e730`](https://github.com/tobymao/sqlglot/commit/3e5e7300ec184024f871669db48d68476b3fa4df) - **clickhouse**: generate exp.Values correctly, handle `FORMAT Values` *(PR [#3900](https://github.com/tobymao/sqlglot/pull/3900) by [@georgesittas](https://github.com/georgesittas))*
- [`bea3c08`](https://github.com/tobymao/sqlglot/commit/bea3c08e46a020d8545b702c77f0db18c99f1c55) - **parser**: improve performance of OUTER/CROSS APPLY parsing *(PR [#3901](https://github.com/tobymao/sqlglot/pull/3901) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3898](https://github.com/tobymao/sqlglot/issues/3898) opened by [@ewhitley](https://github.com/ewhitley)*


## [v25.10.0] - 2024-08-08
### :boom: BREAKING CHANGES
- due to [`3eb46db`](https://github.com/tobymao/sqlglot/commit/3eb46db5c429f50b5bb6c0c5517a5f7c1084b5ea) - switch off CSV file schema inference by default *(PR [#3879](https://github.com/tobymao/sqlglot/pull/3879) by [@georgesittas](https://github.com/georgesittas))*:

  switch off CSV file schema inference by default (#3879)


### :sparkles: New Features
- [`3e4fcf7`](https://github.com/tobymao/sqlglot/commit/3e4fcf7e8f6a322c14470de6c5dbba152bc9b2fe) - **databricks**: Add support for STREAMING tables *(PR [#3878](https://github.com/tobymao/sqlglot/pull/3878) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3876](https://github.com/tobymao/sqlglot/issues/3876) opened by [@ericvergnaud](https://github.com/ericvergnaud)*
- [`528f690`](https://github.com/tobymao/sqlglot/commit/528f6908001db2f132edfa3c61c21815f7e9dc2f) - **duckdb**: Transpile Snowflake's CONVERT_TIMEZONE 3-arg version *(PR [#3883](https://github.com/tobymao/sqlglot/pull/3883) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3875](https://github.com/tobymao/sqlglot/issues/3875) opened by [@milonimrod](https://github.com/milonimrod)*
- [`411f62a`](https://github.com/tobymao/sqlglot/commit/411f62ad27f8cbe0d9a429e0cafdf4bd9eb2749f) - **bigquery**: Support for GENERATE_TIMESTAMP_ARRAY, DDB transpilation *(PR [#3888](https://github.com/tobymao/sqlglot/pull/3888) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`7169e6e`](https://github.com/tobymao/sqlglot/commit/7169e6ef52d24754059b9ee4324398d22ddff0da) - **bigquery**: ensure Funcs are preserved when used as Tables *(PR [#3877](https://github.com/tobymao/sqlglot/pull/3877) by [@georgesittas](https://github.com/georgesittas))*
- [`62ceed2`](https://github.com/tobymao/sqlglot/commit/62ceed2fa3cd7b41919839d837b860f3814fa769) - **redshift**: parse first arg in DATE_PART into a Var fixes [#3882](https://github.com/tobymao/sqlglot/pull/3882) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`2ad9bfe`](https://github.com/tobymao/sqlglot/commit/2ad9bfef71ae707b83f604f16b47aa583d082c3b) - **snowflake**: support table qualification in USING clause *(PR [#3885](https://github.com/tobymao/sqlglot/pull/3885) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3881](https://github.com/tobymao/sqlglot/issues/3881) opened by [@dlahyani](https://github.com/dlahyani)*
- [`ef16b1d`](https://github.com/tobymao/sqlglot/commit/ef16b1da6b43647a0ca08d69eaf3610e3b72671f) - Fix COLLATE's RHS parsing *(PR [#3887](https://github.com/tobymao/sqlglot/pull/3887) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3880](https://github.com/tobymao/sqlglot/issues/3880) opened by [@ewhitley](https://github.com/ewhitley)*

### :recycle: Refactors
- [`3eb46db`](https://github.com/tobymao/sqlglot/commit/3eb46db5c429f50b5bb6c0c5517a5f7c1084b5ea) - **optimizer**: switch off CSV file schema inference by default *(PR [#3879](https://github.com/tobymao/sqlglot/pull/3879) by [@georgesittas](https://github.com/georgesittas))*


## [v25.9.0] - 2024-08-05
### :boom: BREAKING CHANGES
- due to [`64e187c`](https://github.com/tobymao/sqlglot/commit/64e187c52cd9725ba79e6afbd444382eba9e5827) - transpile postgres impliclitly exploding GENERATE_SERIES proje… *(PR [#3853](https://github.com/tobymao/sqlglot/pull/3853) by [@georgesittas](https://github.com/georgesittas))*:

  transpile postgres impliclitly exploding GENERATE_SERIES proje… (#3853)

- due to [`e53e7cc`](https://github.com/tobymao/sqlglot/commit/e53e7cc02a224563d0a61b0a39298d606b9bac80) - Generation of exp.ArrayConcat for 2-arg based dialects *(PR [#3864](https://github.com/tobymao/sqlglot/pull/3864) by [@VaggelisD](https://github.com/VaggelisD))*:

  Generation of exp.ArrayConcat for 2-arg based dialects (#3864)

- due to [`659b8bf`](https://github.com/tobymao/sqlglot/commit/659b8bf12e396856d1562ee4678b4f687629e081) - Support for BQ's exp.GenerateDateArray generation *(PR [#3865](https://github.com/tobymao/sqlglot/pull/3865) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support for BQ's exp.GenerateDateArray generation (#3865)


### :sparkles: New Features
- [`6afed2a`](https://github.com/tobymao/sqlglot/commit/6afed2aecc0ce186ff6c484b1ad32ac6a2fb61bc) - **duckdb**: Support for exp.TimeDiff generation *(PR [#3856](https://github.com/tobymao/sqlglot/pull/3856) by [@VaggelisD](https://github.com/VaggelisD))*
- [`64e187c`](https://github.com/tobymao/sqlglot/commit/64e187c52cd9725ba79e6afbd444382eba9e5827) - transpile postgres impliclitly exploding GENERATE_SERIES proje… *(PR [#3853](https://github.com/tobymao/sqlglot/pull/3853) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3818](https://github.com/tobymao/sqlglot/issues/3818) opened by [@wojciechowski-p](https://github.com/wojciechowski-p)*
- [`8a948c8`](https://github.com/tobymao/sqlglot/commit/8a948c805f7534e266557e1aa08bee0982340685) - **teradata**: Parse RENAME TABLE as Command *(PR [#3863](https://github.com/tobymao/sqlglot/pull/3863) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3861](https://github.com/tobymao/sqlglot/issues/3861) opened by [@EdouardW](https://github.com/EdouardW)*
- [`659b8bf`](https://github.com/tobymao/sqlglot/commit/659b8bf12e396856d1562ee4678b4f687629e081) - **duckdb**: Support for BQ's exp.GenerateDateArray generation *(PR [#3865](https://github.com/tobymao/sqlglot/pull/3865) by [@VaggelisD](https://github.com/VaggelisD))*
- [`734f54b`](https://github.com/tobymao/sqlglot/commit/734f54bb6ec697a5213f046fbb1e8174b2c31115) - **snowflake**: add support for a a couple of missing clauses in PIVOT clause *(PR [#3867](https://github.com/tobymao/sqlglot/pull/3867) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`8710763`](https://github.com/tobymao/sqlglot/commit/87107631378b0972115a01cc0bb99dbfc44a66d7) - **presto**: map %W to %A in the TIME_MAPPING *(PR [#3855](https://github.com/tobymao/sqlglot/pull/3855) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3854](https://github.com/tobymao/sqlglot/issues/3854) opened by [@ddelzell](https://github.com/ddelzell)*
- [`532f3c8`](https://github.com/tobymao/sqlglot/commit/532f3c8714220058170790b13977cc66760841dc) - **duckdb**: Add implicit casts to DATE_DIFF *(PR [#3857](https://github.com/tobymao/sqlglot/pull/3857) by [@VaggelisD](https://github.com/VaggelisD))*
- [`299c4a5`](https://github.com/tobymao/sqlglot/commit/299c4a559dd04047d5a4c4691f8965972842fe7d) - **clickhouse**: Fix SETTINGS parsing *(PR [#3859](https://github.com/tobymao/sqlglot/pull/3859) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3858](https://github.com/tobymao/sqlglot/issues/3858) opened by [@obazna](https://github.com/obazna)*
- [`810d23d`](https://github.com/tobymao/sqlglot/commit/810d23d4e42f9a7de83015ec425dff9223598219) - **parser**: make assignment parsing more lenient by allowing keyword in LHS *(PR [#3866](https://github.com/tobymao/sqlglot/pull/3866) by [@georgesittas](https://github.com/georgesittas))*
- [`e53e7cc`](https://github.com/tobymao/sqlglot/commit/e53e7cc02a224563d0a61b0a39298d606b9bac80) - Generation of exp.ArrayConcat for 2-arg based dialects *(PR [#3864](https://github.com/tobymao/sqlglot/pull/3864) by [@VaggelisD](https://github.com/VaggelisD))*
- [`813f127`](https://github.com/tobymao/sqlglot/commit/813f127b293e7087d174f3f632b65ba7b24bc9e3) - **duckdb**: Allow DESCRIBE as a _parse_select() path *(PR [#3871](https://github.com/tobymao/sqlglot/pull/3871) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3869](https://github.com/tobymao/sqlglot/issues/3869) opened by [@cpcloud](https://github.com/cpcloud)*
- [`6ff0c01`](https://github.com/tobymao/sqlglot/commit/6ff0c01a5b8b19e3090b8cf08aabbb4b27425abb) - Fixed size array parsing *(PR [#3870](https://github.com/tobymao/sqlglot/pull/3870) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3868](https://github.com/tobymao/sqlglot/issues/3868) opened by [@tekumara](https://github.com/tekumara)*


## [v25.8.1] - 2024-07-30
### :bug: Bug Fixes
- [`a295b3a`](https://github.com/tobymao/sqlglot/commit/a295b3adbef0eff0b3f6c3b8b97b1eaa8c13f144) - **tsql**: regression related to CTEs in CREATE VIEW AS statements *(PR [#3852](https://github.com/tobymao/sqlglot/pull/3852) by [@georgesittas](https://github.com/georgesittas))*


## [v25.8.0] - 2024-07-29
### :sparkles: New Features
- [`e37d63a`](https://github.com/tobymao/sqlglot/commit/e37d63a17d4709135c1de7876b2898cf7bd2e641) - **bigquery**: add support for BYTEINT closes [#3838](https://github.com/tobymao/sqlglot/pull/3838) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`4c912cd`](https://github.com/tobymao/sqlglot/commit/4c912cd2302874b8abeed3cafa93ff3771b8dcba) - **clickhouse**: improve parsing/transpilation of StrToDate *(PR [#3839](https://github.com/tobymao/sqlglot/pull/3839) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3837](https://github.com/tobymao/sqlglot/issues/3837) opened by [@ace-xc](https://github.com/ace-xc)*
- [`45f45ea`](https://github.com/tobymao/sqlglot/commit/45f45eaaac5a9130168dddaef4713542886a83cb) - **duckdb**: add support for SUMMARIZE *(PR [#3840](https://github.com/tobymao/sqlglot/pull/3840) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3823](https://github.com/tobymao/sqlglot/issues/3823) opened by [@cpcloud](https://github.com/cpcloud)*

### :bug: Bug Fixes
- [`57ecc84`](https://github.com/tobymao/sqlglot/commit/57ecc8465a3c4d1e0ab1db71dc185c80efc5d0aa) - **duckdb**: wrap left IN clause json extract arrow operand fixes [#3836](https://github.com/tobymao/sqlglot/pull/3836) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`2ffb070`](https://github.com/tobymao/sqlglot/commit/2ffb07070952cde7ac9a1883cbf9b4c477c55abb) - **duckdb**: allow fixed length array casts closes [#3841](https://github.com/tobymao/sqlglot/pull/3841) *(PR [#3842](https://github.com/tobymao/sqlglot/pull/3842) by [@tobymao](https://github.com/tobymao))*
- [`d71eb4e`](https://github.com/tobymao/sqlglot/commit/d71eb4ebc2a0f82c567b32de51298f0d82f400a1) - pretty gen for tuples *(commit by [@tobymao](https://github.com/tobymao))*
- [`12ae9cd`](https://github.com/tobymao/sqlglot/commit/12ae9cdc1c1f52735f8c60488b5d98a4872bf764) - **tsql**: handle JSON_QUERY with a single argument *(PR [#3847](https://github.com/tobymao/sqlglot/pull/3847) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3843](https://github.com/tobymao/sqlglot/issues/3843) opened by [@zachary62](https://github.com/zachary62)*
- [`f8ca6b4`](https://github.com/tobymao/sqlglot/commit/f8ca6b4048ee22585cd7635f83b25fe2df9bd748) - **tsql**: bubble up exp.Create CTEs to improve transpilability *(PR [#3848](https://github.com/tobymao/sqlglot/pull/3848) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3844](https://github.com/tobymao/sqlglot/issues/3844) opened by [@zachary62](https://github.com/zachary62)*
- [`89976c1`](https://github.com/tobymao/sqlglot/commit/89976c1dbb61bdfe3bbb98702b18365e90a69acb) - **parser**: allow 'cube' to be used for identifiers *(PR [#3850](https://github.com/tobymao/sqlglot/pull/3850) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`d00ea9c`](https://github.com/tobymao/sqlglot/commit/d00ea9c4d39f686fabbe864e88cfe5c071fd4f66) - exclude boolean args in Generator.format_args *(PR [#3849](https://github.com/tobymao/sqlglot/pull/3849) by [@georgesittas](https://github.com/georgesittas))*


## [v25.7.1] - 2024-07-25
### :bug: Bug Fixes
- [`ae95c18`](https://github.com/tobymao/sqlglot/commit/ae95c18f636d34c7f92b48cd5970f4fa6ad81b08) - alter table add columns closes [#3835](https://github.com/tobymao/sqlglot/pull/3835) *(commit by [@tobymao](https://github.com/tobymao))*
- [`9b5839d`](https://github.com/tobymao/sqlglot/commit/9b5839d7fb04f78c9ef50b112cd9d4d24558c912) - make ast consistent *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.7.0] - 2024-07-25
### :sparkles: New Features
- [`ba0aa50`](https://github.com/tobymao/sqlglot/commit/ba0aa50072f623c299eb4d2dbb69993541fff27b) - **duckdb**: Transpile BQ's exp.DatetimeAdd, exp.DatetimeSub *(PR [#3777](https://github.com/tobymao/sqlglot/pull/3777) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5da91fb`](https://github.com/tobymao/sqlglot/commit/5da91fb50d0f8029ddda16040ebd316c1a651e2d) - **postgres**: Support for CREATE INDEX CONCURRENTLY *(PR [#3787](https://github.com/tobymao/sqlglot/pull/3787) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3783](https://github.com/tobymao/sqlglot/issues/3783) opened by [@EdgyEdgemond](https://github.com/EdgyEdgemond)*
- [`00722eb`](https://github.com/tobymao/sqlglot/commit/00722eb41795e7454d0ecb4c3d0e1caf96a19465) - Move ANNOTATORS to Dialect for dialect-aware annotation *(PR [#3786](https://github.com/tobymao/sqlglot/pull/3786) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3778](https://github.com/tobymao/sqlglot/issues/3778) opened by [@ddelzell](https://github.com/ddelzell)*
- [`a6d84fb`](https://github.com/tobymao/sqlglot/commit/a6d84fbd9b4120f42b31bb01d4bf3e6258e51562) - **postgres**: Parse TO_DATE as exp.StrToDate *(PR [#3799](https://github.com/tobymao/sqlglot/pull/3799) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3797](https://github.com/tobymao/sqlglot/issues/3797) opened by [@dioptre](https://github.com/dioptre)*
- [`3582644`](https://github.com/tobymao/sqlglot/commit/358264478e5449b7e4ebddce1cc463d140f266f5) - **hive, spark, db**: Support for exp.GenerateSeries *(PR [#3798](https://github.com/tobymao/sqlglot/pull/3798) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3793](https://github.com/tobymao/sqlglot/issues/3793) opened by [@wojciechowski-p](https://github.com/wojciechowski-p)*
- [`80b4a12`](https://github.com/tobymao/sqlglot/commit/80b4a12b779b661e42d31cf75ead8aff25257f8a) - **tsql**: Support for COLUMNSTORE option on CREATE INDEX *(PR [#3805](https://github.com/tobymao/sqlglot/pull/3805) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3801](https://github.com/tobymao/sqlglot/issues/3801) opened by [@na399](https://github.com/na399)*
- [`bf6c126`](https://github.com/tobymao/sqlglot/commit/bf6c12687f3ed032ea7be40875c19fc00e5927ad) - **databricks**: Support USE CATALOG *(PR [#3812](https://github.com/tobymao/sqlglot/pull/3812) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3811](https://github.com/tobymao/sqlglot/issues/3811) opened by [@grusin-db](https://github.com/grusin-db)*
- [`624d411`](https://github.com/tobymao/sqlglot/commit/624d4115e3ee4b8db2dbf2970bf0047e14b23e92) - **snowflake**: Support for OBJECT_INSERT, transpile to DDB *(PR [#3807](https://github.com/tobymao/sqlglot/pull/3807) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3802](https://github.com/tobymao/sqlglot/issues/3802) opened by [@buremba](https://github.com/buremba)*
- [`5b393fb`](https://github.com/tobymao/sqlglot/commit/5b393fb4d2db47b9229ca12a03aba82cdd510615) - **postgres**: Add missing constraint options *(PR [#3816](https://github.com/tobymao/sqlglot/pull/3816) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3814](https://github.com/tobymao/sqlglot/issues/3814) opened by [@DTovstohan](https://github.com/DTovstohan)*

### :bug: Bug Fixes
- [`898f523`](https://github.com/tobymao/sqlglot/commit/898f523a8db9f73b59055f1e38cf4acb07157f00) - **duckdb**: Wrap JSON_EXTRACT if it's subscripted *(PR [#3785](https://github.com/tobymao/sqlglot/pull/3785) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3782](https://github.com/tobymao/sqlglot/issues/3782) opened by [@egan8888](https://github.com/egan8888)*
- [`db3748d`](https://github.com/tobymao/sqlglot/commit/db3748d56b138a6427d6f4fc3e32c895ffb993fa) - **mysql**: don't wrap VALUES clause *(PR [#3792](https://github.com/tobymao/sqlglot/pull/3792) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3789](https://github.com/tobymao/sqlglot/issues/3789) opened by [@stephenprater](https://github.com/stephenprater)*
- [`44d6506`](https://github.com/tobymao/sqlglot/commit/44d650637d5d7a662b57ec1d8ca74dffe0f7ad73) - with as comments closes [#3794](https://github.com/tobymao/sqlglot/pull/3794) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8ca6a61`](https://github.com/tobymao/sqlglot/commit/8ca6a613692e7339717c449ba6966d7c2911b584) - **tsql**: Fix roundtrip of exp.Stddev *(PR [#3806](https://github.com/tobymao/sqlglot/pull/3806) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3804](https://github.com/tobymao/sqlglot/issues/3804) opened by [@JonaGeishauser](https://github.com/JonaGeishauser)*
- [`8551063`](https://github.com/tobymao/sqlglot/commit/855106377c97ee313b45046041fafabb2810dab2) - **duckdb**: Fix STRUCT_PACK -> ROW due to is_struct_cast *(PR [#3809](https://github.com/tobymao/sqlglot/pull/3809) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3808](https://github.com/tobymao/sqlglot/issues/3808) opened by [@aersam](https://github.com/aersam)*
- [`98f80ed`](https://github.com/tobymao/sqlglot/commit/98f80eda3863b5ff40d566330e6ab35a99f569ca) - **clickhouse**: allow like as an identifier closes [#3813](https://github.com/tobymao/sqlglot/pull/3813) *(commit by [@tobymao](https://github.com/tobymao))*
- [`556ba35`](https://github.com/tobymao/sqlglot/commit/556ba35e4ce9efa51561ef0578bfb24a51ce4dcd) - allow parse_identifier to handle single quotes *(commit by [@tobymao](https://github.com/tobymao))*
- [`f9810d2`](https://github.com/tobymao/sqlglot/commit/f9810d213f3992881fc13291a681da6553701083) - **snowflake**: Don't consume LPAREN when parsing staged file path *(PR [#3815](https://github.com/tobymao/sqlglot/pull/3815) by [@VaggelisD](https://github.com/VaggelisD))*
- [`416f4a1`](https://github.com/tobymao/sqlglot/commit/416f4a1b6a04b858ff8ed94509aacd9bacca145b) - **postgres**: Fix COLLATE column constraint *(PR [#3820](https://github.com/tobymao/sqlglot/pull/3820) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3817](https://github.com/tobymao/sqlglot/issues/3817) opened by [@DTovstohan](https://github.com/DTovstohan)*
- [`69b9395`](https://github.com/tobymao/sqlglot/commit/69b93953c35bd7f1d53cf15d9937117edb38f512) - Do not preemptively consume SELECT [ALL] if ALL is connected *(PR [#3822](https://github.com/tobymao/sqlglot/pull/3822) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3819](https://github.com/tobymao/sqlglot/issues/3819) opened by [@nfx](https://github.com/nfx)*
- [`1c19abe`](https://github.com/tobymao/sqlglot/commit/1c19abe5b3f3187a2e0ba420cf8c5e5b5ecc788e) - **presto, trino**: Fix StrToUnix transpilation *(PR [#3824](https://github.com/tobymao/sqlglot/pull/3824) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3796](https://github.com/tobymao/sqlglot/issues/3796) opened by [@ddelzell](https://github.com/ddelzell)*


## [v25.6.1] - 2024-07-18
### :bug: Bug Fixes
- [`19370d5`](https://github.com/tobymao/sqlglot/commit/19370d5d16b555e25def503323ec3dc4e5d40e6c) - **postgres**: Decouple UNIQUE from DEFAULT constraints *(PR [#3775](https://github.com/tobymao/sqlglot/pull/3775) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3774](https://github.com/tobymao/sqlglot/issues/3774) opened by [@EdgyEdgemond](https://github.com/EdgyEdgemond)*
- [`e99146b`](https://github.com/tobymao/sqlglot/commit/e99146b0989599772c020905f69496ea80e7e2e5) - make copy a dml statement for qualify_tables *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.6.0] - 2024-07-17
### :boom: BREAKING CHANGES
- due to [`89fc63c`](https://github.com/tobymao/sqlglot/commit/89fc63c5831dc5d63feff9e39fea1e90d65e9a09) - QUALIFY comes after WINDOW clause in queries *(PR [#3745](https://github.com/tobymao/sqlglot/pull/3745) by [@georgesittas](https://github.com/georgesittas))*:

  QUALIFY comes after WINDOW clause in queries (#3745)

- due to [`a2a6efb`](https://github.com/tobymao/sqlglot/commit/a2a6efb45dc0f380747aa4afdaa19122389f3c28) - Canonicalize struct & array inline constructor *(PR [#3751](https://github.com/tobymao/sqlglot/pull/3751) by [@VaggelisD](https://github.com/VaggelisD))*:

  Canonicalize struct & array inline constructor (#3751)


### :sparkles: New Features
- [`e9c4bbb`](https://github.com/tobymao/sqlglot/commit/e9c4bbbb0d0a03d1b1efaad9abe0068b3b7efa9d) - Support for ORDER BY ALL *(PR [#3756](https://github.com/tobymao/sqlglot/pull/3756) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3755](https://github.com/tobymao/sqlglot/issues/3755) opened by [@Hunterlige](https://github.com/Hunterlige)*
- [`4a843e6`](https://github.com/tobymao/sqlglot/commit/4a843e6cca7bcc0d9956fe975dbc77e67038f1b8) - **postgres**: Support FROM ROWS FROM (...) *(PR [#3753](https://github.com/tobymao/sqlglot/pull/3753) by [@VaggelisD](https://github.com/VaggelisD))*
- [`321051a`](https://github.com/tobymao/sqlglot/commit/321051aef30f11f2778444040a2078633e617144) - **presto, trino**: Add support for exp.TimestampAdd *(PR [#3765](https://github.com/tobymao/sqlglot/pull/3765) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3762](https://github.com/tobymao/sqlglot/issues/3762) opened by [@ddelzell](https://github.com/ddelzell)*
- [`82a1bb4`](https://github.com/tobymao/sqlglot/commit/82a1bb42856d628651bb5f1ef9aa8f440736c450) - Support for RPAD & LPAD functions *(PR [#3757](https://github.com/tobymao/sqlglot/pull/3757) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`89fc63c`](https://github.com/tobymao/sqlglot/commit/89fc63c5831dc5d63feff9e39fea1e90d65e9a09) - **duckdb, clickhouse**: QUALIFY comes after WINDOW clause in queries *(PR [#3745](https://github.com/tobymao/sqlglot/pull/3745) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3744](https://github.com/tobymao/sqlglot/issues/3744) opened by [@taylorbarstow](https://github.com/taylorbarstow)*
- [`15ca924`](https://github.com/tobymao/sqlglot/commit/15ca924ac6e8a72396a882c394856e466cae9ac3) - **optimizer**: Fix expansion of SELECT * REPLACE, RENAME *(PR [#3742](https://github.com/tobymao/sqlglot/pull/3742) by [@VaggelisD](https://github.com/VaggelisD))*
- [`0363fef`](https://github.com/tobymao/sqlglot/commit/0363fefd3ddd490ddddae47f7eb0192f0ff3cc5e) - attach comments to Commands *(PR [#3758](https://github.com/tobymao/sqlglot/pull/3758) by [@georgesittas](https://github.com/georgesittas))*
- [`a2a6efb`](https://github.com/tobymao/sqlglot/commit/a2a6efb45dc0f380747aa4afdaa19122389f3c28) - **bigquery**: Canonicalize struct & array inline constructor *(PR [#3751](https://github.com/tobymao/sqlglot/pull/3751) by [@VaggelisD](https://github.com/VaggelisD))*
- [`5df3f52`](https://github.com/tobymao/sqlglot/commit/5df3f5292488df6a8e21abf3b49086c823797e78) - Remove number matching from COLON placeholder parser *(PR [#3761](https://github.com/tobymao/sqlglot/pull/3761) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3759](https://github.com/tobymao/sqlglot/issues/3759) opened by [@egan8888](https://github.com/egan8888)*
- [`0606af6`](https://github.com/tobymao/sqlglot/commit/0606af66dba7c290fee65926dcb74baad82c84ac) - **duckdb**: Transpile UDFs from Databricks *(PR [#3768](https://github.com/tobymao/sqlglot/pull/3768) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3764](https://github.com/tobymao/sqlglot/issues/3764) opened by [@aersam](https://github.com/aersam)*
- [`dcc783a`](https://github.com/tobymao/sqlglot/commit/dcc783aad7c2e7184224e90fed7710eb08ddc76a) - **clickhouse**: Allow TokenType.SELECT as a Tuple field identifier *(PR [#3766](https://github.com/tobymao/sqlglot/pull/3766) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3763](https://github.com/tobymao/sqlglot/issues/3763) opened by [@cpcloud](https://github.com/cpcloud)*
- [`b42b7ac`](https://github.com/tobymao/sqlglot/commit/b42b7ac5bb1785a9028235c1557b9842ea1d7524) - extract from time/date *(commit by [@tobymao](https://github.com/tobymao))*


## [v25.5.1] - 2024-07-05
### :bug: Bug Fixes
- [`2bdde22`](https://github.com/tobymao/sqlglot/commit/2bdde2221b8017791ce4cc619abb2706464ca408) - **optimizer**: only qualify coalesced USING columns if they exist in table schemas *(PR [#3740](https://github.com/tobymao/sqlglot/pull/3740) by [@georgesittas](https://github.com/georgesittas))*


## [v25.5.0] - 2024-07-04
### :boom: BREAKING CHANGES
- due to [`8335ba1`](https://github.com/tobymao/sqlglot/commit/8335ba10e60c7c63881d7559a6f1fada11b0e55d) - preserve EXTRACT(date_part FROM datetime) calls *(PR [#3729](https://github.com/tobymao/sqlglot/pull/3729) by [@georgesittas](https://github.com/georgesittas))*:

  preserve EXTRACT(date_part FROM datetime) calls (#3729)

- due to [`fb066a6`](https://github.com/tobymao/sqlglot/commit/fb066a6167e1f887bd8c1a1369d063fe70f36a8a) - Decouple NVL() from COALESCE() *(PR [#3734](https://github.com/tobymao/sqlglot/pull/3734) by [@VaggelisD](https://github.com/VaggelisD))*:

  Decouple NVL() from COALESCE() (#3734)


### :sparkles: New Features
- [`0c03299`](https://github.com/tobymao/sqlglot/commit/0c032992fac622ebaee114cd9f6e405be1820054) - **teradata**: random lower upper closes [#3721](https://github.com/tobymao/sqlglot/pull/3721) *(commit by [@tobymao](https://github.com/tobymao))*
- [`37b6e2d`](https://github.com/tobymao/sqlglot/commit/37b6e2d806f6da1338c75803919c602f8705acac) - **snowflake**: add support for VECTOR(type, size) *(PR [#3724](https://github.com/tobymao/sqlglot/pull/3724) by [@georgesittas](https://github.com/georgesittas))*
- [`1e07c4d`](https://github.com/tobymao/sqlglot/commit/1e07c4d29a43192fb57c120f3b9c1c2fa27d0fa6) - **presto, trino**: Configurable transpilation of Snowflake VARIANT *(PR [#3725](https://github.com/tobymao/sqlglot/pull/3725) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3713](https://github.com/tobymao/sqlglot/issues/3713) opened by [@Leonti](https://github.com/Leonti)*
- [`e5a53aa`](https://github.com/tobymao/sqlglot/commit/e5a53aaa015806574cd3c4bbe46b5788e960903e) - **snowflake**: Support for FROM CHANGES *(PR [#3731](https://github.com/tobymao/sqlglot/pull/3731) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3730](https://github.com/tobymao/sqlglot/issues/3730) opened by [@achicoine-coveo](https://github.com/achicoine-coveo)*
- [`820d664`](https://github.com/tobymao/sqlglot/commit/820d66430bb23bff88d0057b22842d313e1431c5) - **presto**: wrap md5 string arguments in to_utf8 *(PR [#3732](https://github.com/tobymao/sqlglot/pull/3732) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2855](https://github.com/TobikoData/sqlmesh/issues/2855) opened by [@MikeWallis42](https://github.com/MikeWallis42)*
- [`912bc84`](https://github.com/tobymao/sqlglot/commit/912bc84791008ecce545cfbd3b0c9d4362131eb3) - **spark, databricks**: Support view schema binding options *(PR [#3739](https://github.com/tobymao/sqlglot/pull/3739) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3738](https://github.com/tobymao/sqlglot/issues/3738) opened by [@aersam](https://github.com/aersam)*

### :bug: Bug Fixes
- [`3454f86`](https://github.com/tobymao/sqlglot/commit/3454f861f22f680f6b8c18cca466154d3b9fe8d1) - **teradata**: use timestamp with time zone over timestamptz *(PR [#3723](https://github.com/tobymao/sqlglot/pull/3723) by [@mtagle](https://github.com/mtagle))*
  - :arrow_lower_right: *fixes issue [#3722](https://github.com/tobymao/sqlglot/issues/3722) opened by [@mtagle](https://github.com/mtagle)*
- [`f4a2872`](https://github.com/tobymao/sqlglot/commit/f4a28721fd33edb3178c1d99746209dadfbba487) - **clickhouse**: switch off table alias columns generation *(PR [#3727](https://github.com/tobymao/sqlglot/pull/3727) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3726](https://github.com/tobymao/sqlglot/issues/3726) opened by [@cpcloud](https://github.com/cpcloud)*
- [`8335ba1`](https://github.com/tobymao/sqlglot/commit/8335ba10e60c7c63881d7559a6f1fada11b0e55d) - **clickhouse**: preserve EXTRACT(date_part FROM datetime) calls *(PR [#3729](https://github.com/tobymao/sqlglot/pull/3729) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3728](https://github.com/tobymao/sqlglot/issues/3728) opened by [@cpcloud](https://github.com/cpcloud)*
- [`fb066a6`](https://github.com/tobymao/sqlglot/commit/fb066a6167e1f887bd8c1a1369d063fe70f36a8a) - **oracle**: Decouple NVL() from COALESCE() *(PR [#3734](https://github.com/tobymao/sqlglot/pull/3734) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3733](https://github.com/tobymao/sqlglot/issues/3733) opened by [@Hal-H2Apps](https://github.com/Hal-H2Apps)*
- [`c790c3b`](https://github.com/tobymao/sqlglot/commit/c790c3b1fa274d7b0faf9f75e7dbc62bc4f55c67) - **tsql**: parse rhs of x::varchar(max) into a type *(PR [#3737](https://github.com/tobymao/sqlglot/pull/3737) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`84416d2`](https://github.com/tobymao/sqlglot/commit/84416d207a2e397aba12a4138fcbd1fab382c22d) - **teradata**: clean up CurrentTimestamp generation logic *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.4.1] - 2024-06-29
### :bug: Bug Fixes
- [`6bf9853`](https://github.com/tobymao/sqlglot/commit/6bf9853fd0b26d5a4e93e37447c3b275cd108872) - **tsql**: cast shorthand closes [#3760](https://github.com/tobymao/sqlglot/pull/3760) *(PR [#3720](https://github.com/tobymao/sqlglot/pull/3720) by [@tobymao](https://github.com/tobymao))*


## [v25.4.0] - 2024-06-28
### :boom: BREAKING CHANGES
- due to [`9fb1d79`](https://github.com/tobymao/sqlglot/commit/9fb1d79398769edb452e075eb3b6416e69f239bf) - extract unit should be a var, not a column *(PR [#3712](https://github.com/tobymao/sqlglot/pull/3712) by [@tobymao](https://github.com/tobymao))*:

  extract unit should be a var, not a column (#3712)

- due to [`ae1816f`](https://github.com/tobymao/sqlglot/commit/ae1816fc71a5a164d1aae6644a9c3bc4cec484d2) - simplify no longer removes neg, add to_py *(PR [#3714](https://github.com/tobymao/sqlglot/pull/3714) by [@tobymao](https://github.com/tobymao))*:

  simplify no longer removes neg, add to_py (#3714)

- due to [`beaf9cc`](https://github.com/tobymao/sqlglot/commit/beaf9cc1f07ff4223f99c84ad6645d3f29af5801) - coalesce left-hand side of join conditions produced by expanding USING *(PR [#3715](https://github.com/tobymao/sqlglot/pull/3715) by [@georgesittas](https://github.com/georgesittas))*:

  coalesce left-hand side of join conditions produced by expanding USING (#3715)


### :sparkles: New Features
- [`97739fe`](https://github.com/tobymao/sqlglot/commit/97739fe692a883a45247d92b2a3efaed33c4b5bf) - add Select expression parser *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1c2279c`](https://github.com/tobymao/sqlglot/commit/1c2279c0659d5cbe30c19afee85308ef7bf4c9c5) - **duckdb**: Transpile exp.Length from other dialects *(PR [#3708](https://github.com/tobymao/sqlglot/pull/3708) by [@VaggelisD](https://github.com/VaggelisD))*
- [`23dac71`](https://github.com/tobymao/sqlglot/commit/23dac7147883d559acca7d21e3600c28576ec950) - **snowflake**: add support for CONNECT_BY_ROOT expression *(PR [#3717](https://github.com/tobymao/sqlglot/pull/3717) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3716](https://github.com/tobymao/sqlglot/issues/3716) opened by [@niklaslorenzatalligator](https://github.com/niklaslorenzatalligator)*
- [`4f050e0`](https://github.com/tobymao/sqlglot/commit/4f050e0aefcde8fb3c65abaf49c6aa4e2bbe5e2b) - transpile BigQuery's SAFE_CAST with FORMAT to DuckDB *(PR [#3718](https://github.com/tobymao/sqlglot/pull/3718) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2837](https://github.com/TobikoData/sqlmesh/issues/2837) opened by [@hustic](https://github.com/hustic)*

### :bug: Bug Fixes
- [`3a86d7e`](https://github.com/tobymao/sqlglot/commit/3a86d7e4ec02e96326021c417dc972b64076567f) - non deterministic aggs in planner closes [#3709](https://github.com/tobymao/sqlglot/pull/3709) *(commit by [@tobymao](https://github.com/tobymao))*
- [`3b8568d`](https://github.com/tobymao/sqlglot/commit/3b8568d37792c1916f05faf5df8af1841144b338) - **clickhouse**: extract closes [#3711](https://github.com/tobymao/sqlglot/pull/3711) *(commit by [@tobymao](https://github.com/tobymao))*
- [`9fb1d79`](https://github.com/tobymao/sqlglot/commit/9fb1d79398769edb452e075eb3b6416e69f239bf) - extract unit should be a var, not a column *(PR [#3712](https://github.com/tobymao/sqlglot/pull/3712) by [@tobymao](https://github.com/tobymao))*
- [`ae1816f`](https://github.com/tobymao/sqlglot/commit/ae1816fc71a5a164d1aae6644a9c3bc4cec484d2) - simplify no longer removes neg, add to_py *(PR [#3714](https://github.com/tobymao/sqlglot/pull/3714) by [@tobymao](https://github.com/tobymao))*
- [`beaf9cc`](https://github.com/tobymao/sqlglot/commit/beaf9cc1f07ff4223f99c84ad6645d3f29af5801) - **optimizer**: coalesce left-hand side of join conditions produced by expanding USING *(PR [#3715](https://github.com/tobymao/sqlglot/pull/3715) by [@georgesittas](https://github.com/georgesittas))*


## [v25.3.3] - 2024-06-26
### :recycle: Refactors
- [`972ce7d`](https://github.com/tobymao/sqlglot/commit/972ce7d27d9f083d8ef02ded9278e320da3aa0b6) - control ParseJSON generation logic with a flag *(PR [#3707](https://github.com/tobymao/sqlglot/pull/3707) by [@georgesittas](https://github.com/georgesittas))*


## [v25.3.2] - 2024-06-26
### :sparkles: New Features
- [`a1327c7`](https://github.com/tobymao/sqlglot/commit/a1327c7f4ae74ae25617cd448448ae89c915c744) - **tsql**: Add support for scope qualifier operator *(PR [#3703](https://github.com/tobymao/sqlglot/pull/3703) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#2817](https://github.com/TobikoData/sqlmesh/issues/2817) opened by [@elauser](https://github.com/elauser)*

### :bug: Bug Fixes
- [`842a9f0`](https://github.com/tobymao/sqlglot/commit/842a9f0cf6fd49cf1d6ed31a5ad9b40eaa483bff) - **parser**: preserve Cast expression when it's 'safe' and has a format *(PR [#3705](https://github.com/tobymao/sqlglot/pull/3705) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2827](https://github.com/TobikoData/sqlmesh/issues/2827) opened by [@hustic](https://github.com/hustic)*
- [`fc0411d`](https://github.com/tobymao/sqlglot/commit/fc0411dc6236c040ce12c036e1ce1165a5143fa1) - **parser**: explicitly check for identifiers in _parse_types *(PR [#3704](https://github.com/tobymao/sqlglot/pull/3704) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2826](https://github.com/TobikoData/sqlmesh/issues/2826) opened by [@plaflamme](https://github.com/plaflamme)*

### :recycle: Refactors
- [`e9236e3`](https://github.com/tobymao/sqlglot/commit/e9236e36c94464af21c7e2f35a083eef316feab1) - add EXPAND_ALIAS_REFS_ONLY_IN_GROUP_BY dialect constant *(PR [#3702](https://github.com/tobymao/sqlglot/pull/3702) by [@georgesittas](https://github.com/georgesittas))*
- [`92c6ebb`](https://github.com/tobymao/sqlglot/commit/92c6ebb8f703486cf3132c9d2c3c58568c10aea4) - **tsql**: make ScopeResolution round-trippable *(PR [#3706](https://github.com/tobymao/sqlglot/pull/3706) by [@georgesittas](https://github.com/georgesittas))*


## [v25.3.1] - 2024-06-25
### :sparkles: New Features
- [`4ed02b0`](https://github.com/tobymao/sqlglot/commit/4ed02b0a24eeabf813525ba09d646763970dd33b) - transpile TRY_PARSE_JSON Snowflake -> DuckDB *(PR [#3696](https://github.com/tobymao/sqlglot/pull/3696) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3690](https://github.com/tobymao/sqlglot/issues/3690) opened by [@achicoine-coveo](https://github.com/achicoine-coveo)*
- [`60fa5e3`](https://github.com/tobymao/sqlglot/commit/60fa5e3f8a6eab3abb12064366a6bde907d9e9de) - **snowflake**: add support for dynamic table DDL *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`442c61d`](https://github.com/tobymao/sqlglot/commit/442c61defe05f4c168a7909d0a5fc5c043a2d2b4) - **tokenizer**: don't treat escapes in raw strings as such for some dialects *(PR [#3689](https://github.com/tobymao/sqlglot/pull/3689) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3686](https://github.com/tobymao/sqlglot/issues/3686) opened by [@aersam](https://github.com/aersam)*
- [`f3e928e`](https://github.com/tobymao/sqlglot/commit/f3e928e771e1973a13afe09e4dc295ad492b783f) - **parser**: make parse_var_or_string more lenient *(PR [#3695](https://github.com/tobymao/sqlglot/pull/3695) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3693](https://github.com/tobymao/sqlglot/issues/3693) opened by [@WSKINGS](https://github.com/WSKINGS)*
- [`806a7e4`](https://github.com/tobymao/sqlglot/commit/806a7e421a9b5a54a2859d7bb4c3ea131a4a8640) - remove tokenizer cache for multi-threading *(commit by [@tobymao](https://github.com/tobymao))*
- [`3fba603`](https://github.com/tobymao/sqlglot/commit/3fba6035ac0263beab73ab62013a64a56dea9165) - don't treat /*+ as a HINT token in dialects that don't support hints *(PR [#3697](https://github.com/tobymao/sqlglot/pull/3697) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3692](https://github.com/tobymao/sqlglot/issues/3692) opened by [@sandband](https://github.com/sandband)*
- [`e5d534c`](https://github.com/tobymao/sqlglot/commit/e5d534ce96381f42f26d43c4fcab7eff23946c90) - **optimizer**: Force early alias expansion in BQ & CH *(PR [#3699](https://github.com/tobymao/sqlglot/pull/3699) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3687](https://github.com/tobymao/sqlglot/issues/3687) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`1cfb1ff`](https://github.com/tobymao/sqlglot/commit/1cfb1ff850fb4fcf69fc5962e01c879ce51bec8b) - proper parsing of unit in spark/databricks date_diff *(PR [#3701](https://github.com/tobymao/sqlglot/pull/3701) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3700](https://github.com/tobymao/sqlglot/issues/3700) opened by [@cheesefactory](https://github.com/cheesefactory)*

### :wrench: Chores
- [`8b16199`](https://github.com/tobymao/sqlglot/commit/8b16199af3743aee292df5429e1f0087704e1cbc) - bump sqlglotrs to v0.2.8 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.3.0] - 2024-06-21
### :boom: BREAKING CHANGES
- due to [`84d820f`](https://github.com/tobymao/sqlglot/commit/84d820f96b161fdd5b00f265890b5c75c65a36f0) - Time/Datetime/Timestamp function additions *(PR [#3666](https://github.com/tobymao/sqlglot/pull/3666) by [@VaggelisD](https://github.com/VaggelisD))*:

  Time/Datetime/Timestamp function additions (#3666)

- due to [`acbc81d`](https://github.com/tobymao/sqlglot/commit/acbc81d47a2e721c4334ac86b5e17177429cd1c6) - Preserve JSON/VARIANT path with operators *(PR [#3678](https://github.com/tobymao/sqlglot/pull/3678) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve JSON/VARIANT path with operators (#3678)


### :sparkles: New Features
- [`84d820f`](https://github.com/tobymao/sqlglot/commit/84d820f96b161fdd5b00f265890b5c75c65a36f0) - **bigquery**: Time/Datetime/Timestamp function additions *(PR [#3666](https://github.com/tobymao/sqlglot/pull/3666) by [@VaggelisD](https://github.com/VaggelisD))*
- [`d46ad95`](https://github.com/tobymao/sqlglot/commit/d46ad95bb623f1931d9e373d8444d9ed947362c5) - **tokenizer**: add support for nested comments *(PR [#3670](https://github.com/tobymao/sqlglot/pull/3670) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3668](https://github.com/tobymao/sqlglot/issues/3668) opened by [@aersam](https://github.com/aersam)*
- [`ac0e89c`](https://github.com/tobymao/sqlglot/commit/ac0e89c4401f2f278d32c3e956670b262ab21ce7) - **snowflake**: add SECURE post table property fixes [#3677](https://github.com/tobymao/sqlglot/pull/3677) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`acbc81d`](https://github.com/tobymao/sqlglot/commit/acbc81d47a2e721c4334ac86b5e17177429cd1c6) - **databricks**: Preserve JSON/VARIANT path with operators *(PR [#3678](https://github.com/tobymao/sqlglot/pull/3678) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3673](https://github.com/tobymao/sqlglot/issues/3673) opened by [@aersam](https://github.com/aersam)*
- [`07158c7`](https://github.com/tobymao/sqlglot/commit/07158c77ae7879aa83b7982cefb4ec9d01c11857) - **clickhouse**: Fix roundtrips of DATE/TIMESTAMP functions *(PR [#3683](https://github.com/tobymao/sqlglot/pull/3683) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3679](https://github.com/tobymao/sqlglot/issues/3679) opened by [@TacoBel42](https://github.com/TacoBel42)*

### :bug: Bug Fixes
- [`79aea2a`](https://github.com/tobymao/sqlglot/commit/79aea2affece72acfac52b3ac85cf740d55ccff0) - **doris**: ensure LAG/LEAD are generated with three arguments *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`08fb2ec`](https://github.com/tobymao/sqlglot/commit/08fb2ecf808f25eae74b579f8e5c4369edc7c604) - **parser**: check if FROM exists when making implicit unnest explicit fixes [#3671](https://github.com/tobymao/sqlglot/pull/3671) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`049fc5a`](https://github.com/tobymao/sqlglot/commit/049fc5a430ad6fa2998fd94d6e20b23da3b545c3) - **bigquery**: handle the case-sensitive strategy in normalize_identifier *(PR [#3667](https://github.com/tobymao/sqlglot/pull/3667) by [@georgesittas](https://github.com/georgesittas))*
- [`9e1b6aa`](https://github.com/tobymao/sqlglot/commit/9e1b6aa5d9e2abb141143327c835c8f3b4bbcb0f) - **parser**: handle another edge case in struct field type parser *(PR [#3682](https://github.com/tobymao/sqlglot/pull/3682) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3680](https://github.com/tobymao/sqlglot/issues/3680) opened by [@plaflamme](https://github.com/plaflamme)*
- [`a1a0278`](https://github.com/tobymao/sqlglot/commit/a1a02782f22b471ee3c896d57f15237dc86565d1) - jsonbcontains default gen *(commit by [@tobymao](https://github.com/tobymao))*
- [`bf44942`](https://github.com/tobymao/sqlglot/commit/bf44942a7d35eb83685ad3aa2b360c7105a9f5b7) - **oracle**: Fix default NULL_ORDERING *(PR [#3688](https://github.com/tobymao/sqlglot/pull/3688) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3684](https://github.com/tobymao/sqlglot/issues/3684) opened by [@ncclementi](https://github.com/ncclementi)*

### :wrench: Chores
- [`7ae99fe`](https://github.com/tobymao/sqlglot/commit/7ae99fe8284cf2e60819b3992bc79a020dfd00c5) - bump sqlglotrs to 0.2.7 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.2.0] - 2024-06-17
### :boom: BREAKING CHANGES
- due to [`d331e56`](https://github.com/tobymao/sqlglot/commit/d331e56aad7784a122dc36d7bffe5cf0565e38d1) - Normalize time units in their full singular form *(PR [#3652](https://github.com/tobymao/sqlglot/pull/3652) by [@VaggelisD](https://github.com/VaggelisD))*:

  Normalize time units in their full singular form (#3652)

- due to [`468123e`](https://github.com/tobymao/sqlglot/commit/468123e4b7612287e128529de62f3a88f4e1d579) - create SetOperation class *(PR [#3661](https://github.com/tobymao/sqlglot/pull/3661) by [@georgesittas](https://github.com/georgesittas))*:

  create SetOperation class (#3661)


### :sparkles: New Features
- [`e7a158b`](https://github.com/tobymao/sqlglot/commit/e7a158b6f0990db00a4890dfb456de6112f50fd2) - set misc. dialect settings if available *(PR [#3649](https://github.com/tobymao/sqlglot/pull/3649) by [@georgesittas](https://github.com/georgesittas))*
- [`ff3dabc`](https://github.com/tobymao/sqlglot/commit/ff3dabc75f9a03627caa988b85f88be04a6c70a4) - **tsql**: index on closes [#3658](https://github.com/tobymao/sqlglot/pull/3658) *(commit by [@tobymao](https://github.com/tobymao))*
- [`fb4d908`](https://github.com/tobymao/sqlglot/commit/fb4d9080a042d40455bcf631ca6a0afaacb19683) - **tsql**: clustered index closes [#3659](https://github.com/tobymao/sqlglot/pull/3659) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`88c4965`](https://github.com/tobymao/sqlglot/commit/88c49651ecc9d55967f5c8056352de0f0981989f) - **mysql**: delete redundant keywords *(PR [#3646](https://github.com/tobymao/sqlglot/pull/3646) by [@Toms1999](https://github.com/Toms1999))*
- [`4c82c0d`](https://github.com/tobymao/sqlglot/commit/4c82c0d01086e0a622a1448d25f51b0e760d053f) - Parse UNNEST as a function in base dialect *(PR [#3650](https://github.com/tobymao/sqlglot/pull/3650) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3647](https://github.com/tobymao/sqlglot/issues/3647) opened by [@ronnix](https://github.com/ronnix)*
- [`d331e56`](https://github.com/tobymao/sqlglot/commit/d331e56aad7784a122dc36d7bffe5cf0565e38d1) - **redshift**: Normalize time units in their full singular form *(PR [#3652](https://github.com/tobymao/sqlglot/pull/3652) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3651](https://github.com/tobymao/sqlglot/issues/3651) opened by [@vidit-wisdom](https://github.com/vidit-wisdom)*
- [`a06ee36`](https://github.com/tobymao/sqlglot/commit/a06ee3695d4d23626c1ef0700b373fc84600d374) - **parser**: edge case in _parse_types *(PR [#3656](https://github.com/tobymao/sqlglot/pull/3656) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3655](https://github.com/tobymao/sqlglot/issues/3655) opened by [@dangoldin](https://github.com/dangoldin)*
- [`a739741`](https://github.com/tobymao/sqlglot/commit/a739741dca5eefd7d4a2c450dd4506cb951d7efb) - teradata warning *(commit by [@tobymao](https://github.com/tobymao))*
- [`868f30d`](https://github.com/tobymao/sqlglot/commit/868f30d1ff46ec9b8a048bb79fbb511f458fd769) - improve schema error handling *(PR [#3663](https://github.com/tobymao/sqlglot/pull/3663) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3662](https://github.com/tobymao/sqlglot/issues/3662) opened by [@dexhorthy](https://github.com/dexhorthy)*

### :recycle: Refactors
- [`e8cab58`](https://github.com/tobymao/sqlglot/commit/e8cab58c4c44e84ee21d11e8554ee7aed5dc5901) - clean up join mark elimination rule *(PR [#3653](https://github.com/tobymao/sqlglot/pull/3653) by [@georgesittas](https://github.com/georgesittas))*
- [`468123e`](https://github.com/tobymao/sqlglot/commit/468123e4b7612287e128529de62f3a88f4e1d579) - create SetOperation class *(PR [#3661](https://github.com/tobymao/sqlglot/pull/3661) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3660](https://github.com/tobymao/sqlglot/issues/3660) opened by [@sorgfresser](https://github.com/sorgfresser)*


## [v25.1.0] - 2024-06-12
### :boom: BREAKING CHANGES
- due to [`d6cfb41`](https://github.com/tobymao/sqlglot/commit/d6cfb41d63893eadf23a81adf413952f3bd4f0ad) - Support for DATE_ADD functions *(PR [#3609](https://github.com/tobymao/sqlglot/pull/3609) by [@VaggelisD](https://github.com/VaggelisD))*:

  Support for DATE_ADD functions (#3609)


### :sparkles: New Features
- [`d6cfb41`](https://github.com/tobymao/sqlglot/commit/d6cfb41d63893eadf23a81adf413952f3bd4f0ad) - **spark, databricks**: Support for DATE_ADD functions *(PR [#3609](https://github.com/tobymao/sqlglot/pull/3609) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3607](https://github.com/tobymao/sqlglot/issues/3607) opened by [@justinbt21](https://github.com/justinbt21)*
- [`4b30b87`](https://github.com/tobymao/sqlglot/commit/4b30b872b6db73da51e81ef72e1f3bf8763b652b) - **postgres**: Support DIV() func for integer division *(PR [#3602](https://github.com/tobymao/sqlglot/pull/3602) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3601](https://github.com/tobymao/sqlglot/issues/3601) opened by [@andrrreasss](https://github.com/andrrreasss)*
- [`ee9b01d`](https://github.com/tobymao/sqlglot/commit/ee9b01d5631f8f0942b61dfaf0632ae0ac2543bb) - **mysql**: support ADD INDEX/KEY/UNIQUE in ALTER TABLE *(PR [#3621](https://github.com/tobymao/sqlglot/pull/3621) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3615](https://github.com/tobymao/sqlglot/issues/3615) opened by [@hubg398](https://github.com/hubg398)*
- [`c49cefa`](https://github.com/tobymao/sqlglot/commit/c49cefafaf5e9e51778ab85499fde29600d66ed7) - **mysql**: support STRAIGHT_JOIN *(PR [#3623](https://github.com/tobymao/sqlglot/pull/3623) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3622](https://github.com/tobymao/sqlglot/issues/3622) opened by [@gabocic](https://github.com/gabocic)*
- [`e998308`](https://github.com/tobymao/sqlglot/commit/e998308be079bca343af053b99e3826606811df5) - eliminate join marks *(PR [#3580](https://github.com/tobymao/sqlglot/pull/3580) by [@mrhopko](https://github.com/mrhopko))*
- [`227e054`](https://github.com/tobymao/sqlglot/commit/227e0544ede5dfe3063f3497e865be6e383db524) - **oracle**: support unicode strings u'...' *(PR [#3641](https://github.com/tobymao/sqlglot/pull/3641) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3638](https://github.com/tobymao/sqlglot/issues/3638) opened by [@deebify](https://github.com/deebify)*
- [`6df5757`](https://github.com/tobymao/sqlglot/commit/6df5757d7714269c035b3a3a015c81bde436f2bb) - bq datetime -> timestampfromparts *(PR [#3642](https://github.com/tobymao/sqlglot/pull/3642) by [@tobymao](https://github.com/tobymao))*
- [`6abd2c9`](https://github.com/tobymao/sqlglot/commit/6abd2c943896e65b6c9bb5343304dcd8f01b425e) - **oracle**: Support for WITH READ ONLY / CHECK OPTION *(PR [#3639](https://github.com/tobymao/sqlglot/pull/3639) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3635](https://github.com/tobymao/sqlglot/issues/3635) opened by [@deebify](https://github.com/deebify)*

### :bug: Bug Fixes
- [`514b3a5`](https://github.com/tobymao/sqlglot/commit/514b3a52384fc9164bc5c63fda6b779d68e427b0) - **redshift**: add support for Oracle style outer join markers [#3611](https://github.com/tobymao/sqlglot/pull/3611) *(PR [#3612](https://github.com/tobymao/sqlglot/pull/3612) by [@sandband](https://github.com/sandband))*
  - :arrow_lower_right: *fixes issue [#3611](https://github.com/tobymao/sqlglot/issues/3611) opened by [@sandband](https://github.com/sandband)*
- [`6a607d3`](https://github.com/tobymao/sqlglot/commit/6a607d3fa604be7fdbd51e7de06aeedae73039b7) - unnest should also be a function *(commit by [@tobymao](https://github.com/tobymao))*
- [`0e1a1fb`](https://github.com/tobymao/sqlglot/commit/0e1a1fb31de5fefc16a978162d6c6dd4141e1c4d) - **optimizer**: don't use datetrunc type for right side *(PR [#3614](https://github.com/tobymao/sqlglot/pull/3614) by [@barakalon](https://github.com/barakalon))*
- [`d96459f`](https://github.com/tobymao/sqlglot/commit/d96459f18b308466fbbfd9fcbe658e33ec931f1e) - **postgres**: sha256 support *(commit by [@tobymao](https://github.com/tobymao))*
- [`05fe847`](https://github.com/tobymao/sqlglot/commit/05fe847aeb6525836d4eadb908c65a50755dc0c5) - **snowflake**: support fqns in masking/projection policy constraint *(PR [#3620](https://github.com/tobymao/sqlglot/pull/3620) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3617](https://github.com/tobymao/sqlglot/issues/3617) opened by [@kosta-foundational](https://github.com/kosta-foundational)*
- [`caa3051`](https://github.com/tobymao/sqlglot/commit/caa305161893079f87d4d51d9042b5103a850be4) - **snowflake**: Allow SELECT keyword as JSON path key *(PR [#3627](https://github.com/tobymao/sqlglot/pull/3627) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3619](https://github.com/tobymao/sqlglot/issues/3619) opened by [@kosta-foundational](https://github.com/kosta-foundational)*
- [`96efb64`](https://github.com/tobymao/sqlglot/commit/96efb6458ad5c6b92990d8ea69545e60b2eaa8a5) - **tokenizer**: properly handle tags that need to be identifiers in heredocs *(PR [#3630](https://github.com/tobymao/sqlglot/pull/3630) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3618](https://github.com/tobymao/sqlglot/issues/3618) opened by [@bigluck](https://github.com/bigluck)*
- [`4f8edba`](https://github.com/tobymao/sqlglot/commit/4f8edba78d070e2d4b50da56ddb5ed139120c587) - **oracle**: Allow optional format in TO_DATE *(PR [#3637](https://github.com/tobymao/sqlglot/pull/3637) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3636](https://github.com/tobymao/sqlglot/issues/3636) opened by [@deebify](https://github.com/deebify)*
- [`d8c6153`](https://github.com/tobymao/sqlglot/commit/d8c61534f2b11287af22eb70948dfb735cd778bc) - **oracle**: don't apply eliminate_join_markers at parse time *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1afe6ac`](https://github.com/tobymao/sqlglot/commit/1afe6ac62b9c827a001c5a6ab917304c5756fb09) - don't generate neq(0) if subquery predicate in ensure_bools *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`fc050bd`](https://github.com/tobymao/sqlglot/commit/fc050bddf937509961cfd83e9fa86ed7e931da11) - **sqlite**: Fix transpilation of GENERATED AS IDENTITY *(PR [#3634](https://github.com/tobymao/sqlglot/pull/3634) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3632](https://github.com/tobymao/sqlglot/issues/3632) opened by [@lelandbatey](https://github.com/lelandbatey)*
- [`47472d9`](https://github.com/tobymao/sqlglot/commit/47472d9c0a27070fd5f4f9b8c12a8bd8c86b1de1) - **duckdb**: get rid of TEXT length to facilitate transpilation *(PR [#3633](https://github.com/tobymao/sqlglot/pull/3633) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`6194c0f`](https://github.com/tobymao/sqlglot/commit/6194c0f37fd322ee2c33ebe30dcee6c836a66943) - clean up logic related to join marker parsing/generation *(PR [#3613](https://github.com/tobymao/sqlglot/pull/3613) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`5396a8e`](https://github.com/tobymao/sqlglot/commit/5396a8e6ea29876c824b741c2812ad15f4768e4c) - fix SQLFrame casing *(PR [#3616](https://github.com/tobymao/sqlglot/pull/3616) by [@eakmanrq](https://github.com/eakmanrq))*
- [`0397d6f`](https://github.com/tobymao/sqlglot/commit/0397d6f7638c658528cdfef3c85f89afc7fc8952) - bump sqlglotrs to v0.2.6 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.0.3] - 2024-06-06
### :sparkles: New Features
- [`97f8d1a`](https://github.com/tobymao/sqlglot/commit/97f8d1a05801bcd7fd237dac0470c232d3106ca4) - add materialize dialect *(PR [#3577](https://github.com/tobymao/sqlglot/pull/3577) by [@bobbyiliev](https://github.com/bobbyiliev))*
- [`bde5a8d`](https://github.com/tobymao/sqlglot/commit/bde5a8de346125704f757ed6a2de444905fe146e) - add risingwave dialect *(PR [#3598](https://github.com/tobymao/sqlglot/pull/3598) by [@neverchanje](https://github.com/neverchanje))*

### :recycle: Refactors
- [`5140817`](https://github.com/tobymao/sqlglot/commit/51408172ce940b6ab0ad783d98e632d972da6a0a) - **risingwave**: clean up initial implementation of RisingWave *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`f920014`](https://github.com/tobymao/sqlglot/commit/f920014709c2d3ccb7ec18fb622ecd6b6ee0afcd) - **materialize**: clean up initial implementation of Materialize *(PR [#3608](https://github.com/tobymao/sqlglot/pull/3608) by [@georgesittas](https://github.com/georgesittas))*


## [v25.0.2] - 2024-06-05
### :sparkles: New Features
- [`472058d`](https://github.com/tobymao/sqlglot/commit/472058daccf8dc2a7f7f4b7082309a06802017a5) - **bigquery**: add support for GAP_FILL function *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v25.0.0] - 2024-06-05
### :bug: Bug Fixes
- [`f7081c4`](https://github.com/tobymao/sqlglot/commit/f7081c455cf2f61af61dcfd0859a1bf272b84258) - builder other props closes [#3588](https://github.com/tobymao/sqlglot/pull/3588) *(commit by [@tobymao](https://github.com/tobymao))*
- [`13009ca`](https://github.com/tobymao/sqlglot/commit/13009ca5c14d81b7a07311a38f329b967f909926) - **doris**: use CSV syntax for GROUP_CONCAT *(PR [#3592](https://github.com/tobymao/sqlglot/pull/3592) by [@Toms1999](https://github.com/Toms1999))*
- [`bf7fd12`](https://github.com/tobymao/sqlglot/commit/bf7fd12f9a19bf91dd89f76cf376bf6004d83dc0) - no_ilike_sql to lower both sides *(PR [#3593](https://github.com/tobymao/sqlglot/pull/3593) by [@barakalon](https://github.com/barakalon))*
- [`8d87568`](https://github.com/tobymao/sqlglot/commit/8d875681403a43282e1f414ca90f3cf955f26027) - stop normalization_distance early *(PR [#3594](https://github.com/tobymao/sqlglot/pull/3594) by [@barakalon](https://github.com/barakalon))*
- [`3e38912`](https://github.com/tobymao/sqlglot/commit/3e38912cd0de2e3939221b6ad8ae194e68cfe288) - **duckdb**: add reserved keywords *(PR [#3597](https://github.com/tobymao/sqlglot/pull/3597) by [@georgesittas](https://github.com/georgesittas))*
- [`5683d5f`](https://github.com/tobymao/sqlglot/commit/5683d5fe7eeae8f70751de962644c0981c21c7fc) - **hive**: generate TRUNC for TimestampTrunc *(PR [#3600](https://github.com/tobymao/sqlglot/pull/3600) by [@Toms1999](https://github.com/Toms1999))*
- [`ff55ec1`](https://github.com/tobymao/sqlglot/commit/ff55ec1ca8c259f3c304aa7f6039c033f1fe728c) - **hive**: generate string unit for TRUNC, parse it into TimestampTrunc too *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`12b6aa7`](https://github.com/tobymao/sqlglot/commit/12b6aa7006bbf005c750070d9e266153057ff281) - **snowflake**: Fix COPY INTO with subquery *(PR [#3605](https://github.com/tobymao/sqlglot/pull/3605) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3604](https://github.com/tobymao/sqlglot/issues/3604) opened by [@ajuszczak](https://github.com/ajuszczak)*
- [`061be9b`](https://github.com/tobymao/sqlglot/commit/061be9bda9e03b17590a0ac58fa2fec0540e2e77) - optimize absorb_and_eliminate and remove_complements *(PR [#3595](https://github.com/tobymao/sqlglot/pull/3595) by [@barakalon](https://github.com/barakalon))*

### :wrench: Chores
- [`7dd244b`](https://github.com/tobymao/sqlglot/commit/7dd244b6a57e4e8cc9d07cbaf3e89c60fa665a69) - **hive**: test TRUNC roundtrip *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.1.2] - 2024-06-04
### :sparkles: New Features
- [`158ca97`](https://github.com/tobymao/sqlglot/commit/158ca9724c23e7a58f6782719b477f2adb57acae) - **duckdb**: transpile TIMESTAMPNTZ into TIMESTAMP *(PR [#3587](https://github.com/tobymao/sqlglot/pull/3587) by [@georgesittas](https://github.com/georgesittas))*


## [v24.1.1] - 2024-06-03
### :bug: Bug Fixes
- [`60d9085`](https://github.com/tobymao/sqlglot/commit/60d9085a4ec2d0c39aa904bf81b7e15b5bac8ea5) - **postgres**: collate with identifier closes [#3578](https://github.com/tobymao/sqlglot/pull/3578) *(commit by [@tobymao](https://github.com/tobymao))*
- [`317e3a9`](https://github.com/tobymao/sqlglot/commit/317e3a96a49f439aa06af31abb06990f9a1b0d63) - **bigquery**: expanding positional refs with ambiguous references *(PR [#3585](https://github.com/tobymao/sqlglot/pull/3585) by [@tobymao](https://github.com/tobymao))*
- [`5e321f1`](https://github.com/tobymao/sqlglot/commit/5e321f15ac4e54c78b9f90475e1bac4a94eaa48d) - div aliases closes [#3583](https://github.com/tobymao/sqlglot/pull/3583) *(PR [#3586](https://github.com/tobymao/sqlglot/pull/3586) by [@tobymao](https://github.com/tobymao))*


## [v24.1.0] - 2024-05-30
### :boom: BREAKING CHANGES
- due to [`0788c94`](https://github.com/tobymao/sqlglot/commit/0788c944a85d7323b61109ee1ccb5859e3d08404) - Expand stars on BigQuery's tbl.struct_col.* selections *(PR [#3531](https://github.com/tobymao/sqlglot/pull/3531) by [@VaggelisD](https://github.com/VaggelisD))*:

  Expand stars on BigQuery's tbl.struct_col.* selections (#3531)

- due to [`3e71393`](https://github.com/tobymao/sqlglot/commit/3e71393cb8e201a75321fbc179289eb15b1dc6ce) - Refactor struct star expansion in BQ *(PR [#3576](https://github.com/tobymao/sqlglot/pull/3576) by [@VaggelisD](https://github.com/VaggelisD))*:

  Refactor struct star expansion in BQ (#3576)


### :sparkles: New Features
- [`0788c94`](https://github.com/tobymao/sqlglot/commit/0788c944a85d7323b61109ee1ccb5859e3d08404) - **optimizer**: Expand stars on BigQuery's tbl.struct_col.* selections *(PR [#3531](https://github.com/tobymao/sqlglot/pull/3531) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3484](https://github.com/tobymao/sqlglot/issues/3484) opened by [@Bladieblah](https://github.com/Bladieblah)*

### :bug: Bug Fixes
- [`14d63ee`](https://github.com/tobymao/sqlglot/commit/14d63ee8172ddc972d6677071cae3880c748c3aa) - bubble up Identifier comments to TableAliases *(PR [#3571](https://github.com/tobymao/sqlglot/pull/3571) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3570](https://github.com/tobymao/sqlglot/issues/3570) opened by [@fangxingli](https://github.com/fangxingli)*
- [`ba90c22`](https://github.com/tobymao/sqlglot/commit/ba90c22921448ef6b5a0497a9a48918d0e8a9654) - **snowflake**: COPY Postfix *(PR [#3564](https://github.com/tobymao/sqlglot/pull/3564) by [@VaggelisD](https://github.com/VaggelisD))*
- [`3e71393`](https://github.com/tobymao/sqlglot/commit/3e71393cb8e201a75321fbc179289eb15b1dc6ce) - **optimizer**: Refactor struct star expansion in BQ *(PR [#3576](https://github.com/tobymao/sqlglot/pull/3576) by [@VaggelisD](https://github.com/VaggelisD))*

### :recycle: Refactors
- [`1e1dc3f`](https://github.com/tobymao/sqlglot/commit/1e1dc3fea8c5fc1f86fefe6af384e38c8531f2d2) - **optimizer**: minor improvements in the struct star expansion *(PR [#3568](https://github.com/tobymao/sqlglot/pull/3568) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`b12ea8c`](https://github.com/tobymao/sqlglot/commit/b12ea8c126d5debef59e9d9bcbbc6fd5ecf56682) - minor style changes related to COPY INTO *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.0.3] - 2024-05-29
### :bug: Bug Fixes
- [`fb8db9f`](https://github.com/tobymao/sqlglot/commit/fb8db9f2219cfd578fda5c3f51737c180d5aecc6) - **parser**: edge case where TYPE_CONVERTERS leads to type instead of column *(PR [#3566](https://github.com/tobymao/sqlglot/pull/3566) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3565](https://github.com/tobymao/sqlglot/issues/3565) opened by [@galunto](https://github.com/galunto)*
- [`aac8570`](https://github.com/tobymao/sqlglot/commit/aac85705c43edfcd1ebb552573f496c14dce519b) - use index2 instead of self._index in _parse_type index difference *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.0.2] - 2024-05-28
### :sparkles: New Features
- [`078471d`](https://github.com/tobymao/sqlglot/commit/078471d3643da418c91b71dc7bfce5453b924028) - **mysql,doris**: improve transpilation of INTERVAL (plural to singular) *(PR [#3543](https://github.com/tobymao/sqlglot/pull/3543) by [@Toms1999](https://github.com/Toms1999))*
- [`fe56e64`](https://github.com/tobymao/sqlglot/commit/fe56e64aff775002c52843b6b9df973d96349400) - **postgres**: add support for col int[size] column def syntax *(PR [#3548](https://github.com/tobymao/sqlglot/pull/3548) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3544](https://github.com/tobymao/sqlglot/issues/3544) opened by [@judahrand](https://github.com/judahrand)*
  - :arrow_lower_right: *addresses issue [#3545](https://github.com/tobymao/sqlglot/issues/3545) opened by [@judahrand](https://github.com/judahrand)*
- [`188dce8`](https://github.com/tobymao/sqlglot/commit/188dce8ae98f23b5741882c698109563445f11f6) - **snowflake**: add support for WITH-prefixed column constraints *(PR [#3549](https://github.com/tobymao/sqlglot/pull/3549) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3537](https://github.com/tobymao/sqlglot/issues/3537) opened by [@barino86](https://github.com/barino86)*
- [`712d247`](https://github.com/tobymao/sqlglot/commit/712d24704f1be9e54fd6385d6fdbd05173b007aa) - add support for ALTER COLUMN DROP NOT NULL *(PR [#3550](https://github.com/tobymao/sqlglot/pull/3550) by [@noklam](https://github.com/noklam))*
  - :arrow_lower_right: *addresses issue [#3534](https://github.com/tobymao/sqlglot/issues/3534) opened by [@barino86](https://github.com/barino86)*
- [`7c323bd`](https://github.com/tobymao/sqlglot/commit/7c323bde83f1804d7a1e98fcf94e6832385a03d6) - add option in schema's find method to ensure types are DataTypes *(PR [#3560](https://github.com/tobymao/sqlglot/pull/3560) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`1a8a16b`](https://github.com/tobymao/sqlglot/commit/1a8a16b459c7fe20fc2c689ad601b5beac57a206) - **clickhouse**: improve struct type parsing *(PR [#3547](https://github.com/tobymao/sqlglot/pull/3547) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3546](https://github.com/tobymao/sqlglot/issues/3546) opened by [@cpcloud](https://github.com/cpcloud)*
- [`970d3b0`](https://github.com/tobymao/sqlglot/commit/970d3b03750d58ec236ce205bc250616e1fb1349) - **postgres**: setting un-suffixed FLOAT as DOUBLE ([#3551](https://github.com/tobymao/sqlglot/pull/3551)) *(PR [#3552](https://github.com/tobymao/sqlglot/pull/3552) by [@sandband](https://github.com/sandband))*
  - :arrow_lower_right: *fixes issue [#3551](https://github.com/tobymao/sqlglot/issues/3551) opened by [@sandband](https://github.com/sandband)*
- [`e1a9a8b`](https://github.com/tobymao/sqlglot/commit/e1a9a8b6b7fbd44e62cee626540f90425d22d50c) - **redshift**: add support for MINUS operator [#3553](https://github.com/tobymao/sqlglot/pull/3553) *(PR [#3555](https://github.com/tobymao/sqlglot/pull/3555) by [@sandband](https://github.com/sandband))*
- [`beb0269`](https://github.com/tobymao/sqlglot/commit/beb0269b39e848897eaf56e1966d342db72e5c7c) - **tsql**: adapt TimeStrToTime to avoid superfluous casts *(PR [#3558](https://github.com/tobymao/sqlglot/pull/3558) by [@Themiscodes](https://github.com/Themiscodes))*
- [`eae3c51`](https://github.com/tobymao/sqlglot/commit/eae3c5165c16b61c7b524a55776bdb1127005c7d) - use regex to split interval strings *(PR [#3556](https://github.com/tobymao/sqlglot/pull/3556) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3554](https://github.com/tobymao/sqlglot/issues/3554) opened by [@kevinjqiu](https://github.com/kevinjqiu)*

### :recycle: Refactors
- [`a67de5f`](https://github.com/tobymao/sqlglot/commit/a67de5faaa88c1fb5d9857a69c9df06506520cbc) - get rid of redundant dict_depth check in schema find *(PR [#3561](https://github.com/tobymao/sqlglot/pull/3561) by [@georgesittas](https://github.com/georgesittas))*
- [`89a8984`](https://github.com/tobymao/sqlglot/commit/89a8984b8db3817d934b4395e190f3848b1ee77a) - move UNESCAPED_SEQUENCES out of the _Dialect metaclass *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`893addf`](https://github.com/tobymao/sqlglot/commit/893addf9d07602ec3a77097f38d696b6760c6038) - add SET NOT NULL test *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.0.1] - 2024-05-23
### :boom: BREAKING CHANGES
- due to [`80c622e`](https://github.com/tobymao/sqlglot/commit/80c622e0c252ef3be9e469c1cf116c1cd4eaef94) - add reserved keywords fixes [#3526](https://github.com/tobymao/sqlglot/pull/3526) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  add reserved keywords fixes #3526


### :sparkles: New Features
- [`a255610`](https://github.com/tobymao/sqlglot/commit/a2556101c8d04907ae49252def84c55d2daf78b2) - add StringToArray expression (postgres), improve its transpilation *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`8f46d48`](https://github.com/tobymao/sqlglot/commit/8f46d48d4ef4e6be022aff5739992f149519c19d) - **redshift**: transpile SPLIT_TO_STRING *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`80c622e`](https://github.com/tobymao/sqlglot/commit/80c622e0c252ef3be9e469c1cf116c1cd4eaef94) - **doris**: add reserved keywords fixes [#3526](https://github.com/tobymao/sqlglot/pull/3526) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`ebf5fc7`](https://github.com/tobymao/sqlglot/commit/ebf5fc70d8936b5e1522a3ae1b9e231cefe49623) - **hive**: generate correct names for weekofyear, dayofmonth, dayofweek *(PR [#3533](https://github.com/tobymao/sqlglot/pull/3533) by [@oshyun](https://github.com/oshyun))*
  - :arrow_lower_right: *fixes issue [#3532](https://github.com/tobymao/sqlglot/issues/3532) opened by [@oshyun](https://github.com/oshyun)*
- [`3fe3c2c`](https://github.com/tobymao/sqlglot/commit/3fe3c2c0a3e5f465a0c62261c5a0ba6faf8f0846) - **parser**: make _parse_type less aggressive, only parse column as last resort *(PR [#3541](https://github.com/tobymao/sqlglot/pull/3541) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3539](https://github.com/tobymao/sqlglot/issues/3539) opened by [@crash-g](https://github.com/crash-g)*
  - :arrow_lower_right: *fixes issue [#3540](https://github.com/tobymao/sqlglot/issues/3540) opened by [@crash-g](https://github.com/crash-g)*
- [`8afff02`](https://github.com/tobymao/sqlglot/commit/8afff028977593789abe31c6168a93b7e32ac890) - **tsql**: preserve REPLICATE roundtrip *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v24.0.0] - 2024-05-21
### :boom: BREAKING CHANGES
- due to [`a077f17`](https://github.com/tobymao/sqlglot/commit/a077f17d10200980769ff69dd9044c95d6d718f2) - add reserved keywords *(PR [#3525](https://github.com/tobymao/sqlglot/pull/3525) by [@georgesittas](https://github.com/georgesittas))*:

  add reserved keywords (#3525)


### :sparkles: New Features
- [`d958bba`](https://github.com/tobymao/sqlglot/commit/d958bba8494b8bca9cf3ffef0384690bafd78393) - **snowflake**: add support for CREATE WAREHOUSE *(PR [#3510](https://github.com/tobymao/sqlglot/pull/3510) by [@yingw787](https://github.com/yingw787))*
  - :arrow_lower_right: *addresses issue [#3502](https://github.com/tobymao/sqlglot/issues/3502) opened by [@yingw787](https://github.com/yingw787)*
- [`2105300`](https://github.com/tobymao/sqlglot/commit/21053004dbb4c6dc3bcb078c4ab93f267e2c63b2) - **databricks**: Enable hex string literals *(PR [#3522](https://github.com/tobymao/sqlglot/pull/3522) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3521](https://github.com/tobymao/sqlglot/issues/3521) opened by [@aersam](https://github.com/aersam)*
- [`1ef3bb6`](https://github.com/tobymao/sqlglot/commit/1ef3bb6ab49eff66a50c4d3983f19292b6979e98) - **snowflake**: Add support for `CREATE STREAMLIT` *(PR [#3519](https://github.com/tobymao/sqlglot/pull/3519) by [@yingw787](https://github.com/yingw787))*
  - :arrow_lower_right: *addresses issue [#3516](https://github.com/tobymao/sqlglot/issues/3516) opened by [@yingw787](https://github.com/yingw787)*

### :bug: Bug Fixes
- [`5cecbfa`](https://github.com/tobymao/sqlglot/commit/5cecbfa63a770c4d623f4a5f76d1a7a5f59d087d) - unnest identifier closes [#3512](https://github.com/tobymao/sqlglot/pull/3512) *(commit by [@tobymao](https://github.com/tobymao))*
- [`33ab353`](https://github.com/tobymao/sqlglot/commit/33ab3536d68203f4fceee63507b5c73076d48ed7) - **snowflake**: parse certain DB_CREATABLES as identifiers *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`d468f92`](https://github.com/tobymao/sqlglot/commit/d468f92a16decabdf847d7de19f82d65d1939d92) - **doris**: dont generate arrows for JSONExtract* closes [#3513](https://github.com/tobymao/sqlglot/pull/3513) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`bfb9f98`](https://github.com/tobymao/sqlglot/commit/bfb9f983d35e080ec1f8c171a65d576af873c0ea) - **postgres**: parse @> into ArrayContainsAll, improve transpilation *(PR [#3515](https://github.com/tobymao/sqlglot/pull/3515) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3511](https://github.com/tobymao/sqlglot/issues/3511) opened by [@Toms1999](https://github.com/Toms1999)*
- [`4def45b`](https://github.com/tobymao/sqlglot/commit/4def45bb553f6fbc65dcf0fa3d6e8c3f5ec000ea) - make UDF DDL property parsing more lenient closes [#3517](https://github.com/tobymao/sqlglot/pull/3517) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a077f17`](https://github.com/tobymao/sqlglot/commit/a077f17d10200980769ff69dd9044c95d6d718f2) - **mysql**: add reserved keywords *(PR [#3525](https://github.com/tobymao/sqlglot/pull/3525) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3520](https://github.com/tobymao/sqlglot/issues/3520) opened by [@Toms1999](https://github.com/Toms1999)*
  - :arrow_lower_right: *fixes issue [#3524](https://github.com/tobymao/sqlglot/issues/3524) opened by [@Toms1999](https://github.com/Toms1999)*

### :wrench: Chores
- [`358f30c`](https://github.com/tobymao/sqlglot/commit/358f30cc02959275c53a2ee9eccde04ddc6a74a5) - remove redundant postgres JSONB token mapping *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.17.0] - 2024-05-19
### :boom: BREAKING CHANGES
- due to [`77d21d9`](https://github.com/tobymao/sqlglot/commit/77d21d9379c3f130b803ea651ec3d36256bb84a4) - parse : operator as JSONExtract (similar to Snowflake) *(PR [#3508](https://github.com/tobymao/sqlglot/pull/3508) by [@georgesittas](https://github.com/georgesittas))*:

  parse : operator as JSONExtract (similar to Snowflake) (#3508)


### :sparkles: New Features
- [`1125662`](https://github.com/tobymao/sqlglot/commit/11256629d74c4721ed13ed534509d266e260dde6) - add support for snowflake lambdas with type annotations closes … *(PR [#3506](https://github.com/tobymao/sqlglot/pull/3506) by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`77d21d9`](https://github.com/tobymao/sqlglot/commit/77d21d9379c3f130b803ea651ec3d36256bb84a4) - **databricks**: parse : operator as JSONExtract (similar to Snowflake) *(PR [#3508](https://github.com/tobymao/sqlglot/pull/3508) by [@georgesittas](https://github.com/georgesittas))*


## [v23.16.0] - 2024-05-18
### :boom: BREAKING CHANGES
- due to [`e281db8`](https://github.com/tobymao/sqlglot/commit/e281db8784682649be305e9a05c45211402f107c) - Add ALTER TABLE SET *(PR [#3485](https://github.com/tobymao/sqlglot/pull/3485) by [@VaggelisD](https://github.com/VaggelisD))*:

  Add ALTER TABLE SET (#3485)


### :sparkles: New Features
- [`e281db8`](https://github.com/tobymao/sqlglot/commit/e281db8784682649be305e9a05c45211402f107c) - Add ALTER TABLE SET *(PR [#3485](https://github.com/tobymao/sqlglot/pull/3485) by [@VaggelisD](https://github.com/VaggelisD))*
- [`9aee21b`](https://github.com/tobymao/sqlglot/commit/9aee21b88e73809e2cdc4e48f04e16edcf1141d7) - add RETURNS NULL ON NULL and STRICT properties *(PR [#3504](https://github.com/tobymao/sqlglot/pull/3504) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3503](https://github.com/tobymao/sqlglot/issues/3503) opened by [@krzysztof-kwitt](https://github.com/krzysztof-kwitt)*

### :wrench: Chores
- [`0896d11`](https://github.com/tobymao/sqlglot/commit/0896d113b94aaea82e90dd04cdf917dfa546d08e) - lint *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.10] - 2024-05-17
### :sparkles: New Features
- [`89c1d3a`](https://github.com/tobymao/sqlglot/commit/89c1d3a4dd3387576c384413b3a8991a2dd030de) - **clickhouse**: support generate TimestampTrunc, Variance, Stddev *(PR [#3489](https://github.com/tobymao/sqlglot/pull/3489) by [@longxiaofei](https://github.com/longxiaofei))*

### :bug: Bug Fixes
- [`03879bb`](https://github.com/tobymao/sqlglot/commit/03879bb3249ee83cce34d629f1016575d3b932e3) - **postgres**: date_trunc supports time zone *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6e7f37a`](https://github.com/tobymao/sqlglot/commit/6e7f37af86a4f36ec47ea4ef3519e5c97376e090) - copy into pretty printing and default dialect *(PR [#3496](https://github.com/tobymao/sqlglot/pull/3496) by [@tobymao](https://github.com/tobymao))*
- [`e8600e2`](https://github.com/tobymao/sqlglot/commit/e8600e24370a131a0b375a1a9943fdf590968198) - property eq needs highest precedence *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.9] - 2024-05-17
### :boom: BREAKING CHANGES
- due to [`846d5cd`](https://github.com/tobymao/sqlglot/commit/846d5cd2fe85f836f5ad888e783fedfa2108d579) - set default precision / width for DECIMAL type *(PR [#3472](https://github.com/tobymao/sqlglot/pull/3472) by [@georgesittas](https://github.com/georgesittas))*:

  set default precision / width for DECIMAL type (#3472)

- due to [`e3ff67b`](https://github.com/tobymao/sqlglot/commit/e3ff67b0327a217a0523f82e6a11940feab1a8ac) - preserve star clauses (EXCLUDE, RENAME, REPLACE) *(PR [#3477](https://github.com/tobymao/sqlglot/pull/3477) by [@georgesittas](https://github.com/georgesittas))*:

  preserve star clauses (EXCLUDE, RENAME, REPLACE) (#3477)

- due to [`b417c80`](https://github.com/tobymao/sqlglot/commit/b417c80b4208df1b97363db53af42158aa97bbd6) - parse TININT into UTINYINT to improve transpilation *(PR [#3486](https://github.com/tobymao/sqlglot/pull/3486) by [@georgesittas](https://github.com/georgesittas))*:

  parse TININT into UTINYINT to improve transpilation (#3486)

- due to [`54e31af`](https://github.com/tobymao/sqlglot/commit/54e31af7d86138662c9619d50b4ae2e68e04942b) - add DECLARE statement parsing *(PR [#3462](https://github.com/tobymao/sqlglot/pull/3462) by [@jlucas-fsp](https://github.com/jlucas-fsp))*:

  add DECLARE statement parsing (#3462)

- due to [`7287bb9`](https://github.com/tobymao/sqlglot/commit/7287bb9bf578b2b3afaf25647f505b9d73040dc7) - nested cte ordering closes [#3488](https://github.com/tobymao/sqlglot/pull/3488) *(commit by [@tobymao](https://github.com/tobymao))*:

  nested cte ordering closes #3488


### :sparkles: New Features
- [`2c29bf3`](https://github.com/tobymao/sqlglot/commit/2c29bf3b7a163b88754c4593996bbba9b3c791b6) - **snowflake**: add support for CREATE TAG DDL statement *(PR [#3473](https://github.com/tobymao/sqlglot/pull/3473) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3468](https://github.com/tobymao/sqlglot/issues/3468) opened by [@tekumara](https://github.com/tekumara)*
- [`2433993`](https://github.com/tobymao/sqlglot/commit/24339934167e651f2afd6966024e4d96ef55c677) - **transpiler**: handle different hex behavior for dialects *(PR [#3463](https://github.com/tobymao/sqlglot/pull/3463) by [@viplazylmht](https://github.com/viplazylmht))*
  - :arrow_lower_right: *addresses issue [#3460](https://github.com/tobymao/sqlglot/issues/3460) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`0009e09`](https://github.com/tobymao/sqlglot/commit/0009e09b1a7f94f85985670a09bb0be92c673b46) - add epoch_ms of duckdb to other dialects *(PR [#3471](https://github.com/tobymao/sqlglot/pull/3471) by [@longxiaofei](https://github.com/longxiaofei))*
- [`461215b`](https://github.com/tobymao/sqlglot/commit/461215b259de98125ea6b09d7bd875edb3ccce75) - **clickhouse**: add support for PROJECTION in CREATE TABLE statement *(PR [#3465](https://github.com/tobymao/sqlglot/pull/3465) by [@GaliFFun](https://github.com/GaliFFun))*
- [`54e31af`](https://github.com/tobymao/sqlglot/commit/54e31af7d86138662c9619d50b4ae2e68e04942b) - **tsql**: add DECLARE statement parsing *(PR [#3462](https://github.com/tobymao/sqlglot/pull/3462) by [@jlucas-fsp](https://github.com/jlucas-fsp))*
- [`c811adb`](https://github.com/tobymao/sqlglot/commit/c811adb73e6f83265fedc26274c7d4b40f8a1c85) - snowflake array_construct_compact to spark *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`58d5f2b`](https://github.com/tobymao/sqlglot/commit/58d5f2bece42acdda5f8c08d30e6f61a5e538d4c) - **presto**: fix parsing and generating hash functions presto/trino *(PR [#3459](https://github.com/tobymao/sqlglot/pull/3459) by [@viplazylmht](https://github.com/viplazylmht))*
  - :arrow_lower_right: *fixes issue [#3458](https://github.com/tobymao/sqlglot/issues/3458) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`065281e`](https://github.com/tobymao/sqlglot/commit/065281e28be75597f3f97cee22995423ed483660) - **optimizer**: fix multiple bugs in unnest_subqueries, clean up test suite *(PR [#3464](https://github.com/tobymao/sqlglot/pull/3464) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3448](https://github.com/tobymao/sqlglot/issues/3448) opened by [@yesemsanthoshkumar](https://github.com/yesemsanthoshkumar)*
- [`80ba1e8`](https://github.com/tobymao/sqlglot/commit/80ba1e8786a6347b8f20f340c185a0b41d017c73) - preserve quotes for projections produced by the eliminate_qualify rule *(PR [#3470](https://github.com/tobymao/sqlglot/pull/3470) by [@aersam](https://github.com/aersam))*
  - :arrow_lower_right: *fixes issue [#3467](https://github.com/tobymao/sqlglot/issues/3467) opened by [@aersam](https://github.com/aersam)*
- [`3bc1fbe`](https://github.com/tobymao/sqlglot/commit/3bc1fbed40d9d0d05f189ca60fdc7af19b815e8b) - make quoting of alias_or_name in eliminate_qualify more robust *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`1843e9b`](https://github.com/tobymao/sqlglot/commit/1843e9b825da6e97bda8c7b4fffce40baf199af1) - allow parameters in user-defined types *(PR [#3474](https://github.com/tobymao/sqlglot/pull/3474) by [@georgesittas](https://github.com/georgesittas))*
- [`e004d2a`](https://github.com/tobymao/sqlglot/commit/e004d2a3d88ea77d34ecdb8290df1e73511e6b6c) - **duckdb**: preserve precedence of json extraction when converting to arrow syntax *(PR [#3478](https://github.com/tobymao/sqlglot/pull/3478) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3476](https://github.com/tobymao/sqlglot/issues/3476) opened by [@asiunov](https://github.com/asiunov)*
- [`e3ff67b`](https://github.com/tobymao/sqlglot/commit/e3ff67b0327a217a0523f82e6a11940feab1a8ac) - **snowflake**: preserve star clauses (EXCLUDE, RENAME, REPLACE) *(PR [#3477](https://github.com/tobymao/sqlglot/pull/3477) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3475](https://github.com/tobymao/sqlglot/issues/3475) opened by [@asiunov](https://github.com/asiunov)*
- [`428fd61`](https://github.com/tobymao/sqlglot/commit/428fd61574e10be9afab23ac711758b229cc174f) - **mysql**: generate CONCAT for DPipe *(PR [#3482](https://github.com/tobymao/sqlglot/pull/3482) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3481](https://github.com/tobymao/sqlglot/issues/3481) opened by [@Toms1999](https://github.com/Toms1999)*
- [`b417c80`](https://github.com/tobymao/sqlglot/commit/b417c80b4208df1b97363db53af42158aa97bbd6) - **tsql**: parse TININT into UTINYINT to improve transpilation *(PR [#3486](https://github.com/tobymao/sqlglot/pull/3486) by [@georgesittas](https://github.com/georgesittas))*
- [`a3ff49e`](https://github.com/tobymao/sqlglot/commit/a3ff49e93f2c6752f512192ca8b6b6ad18fc925a) - **presto**: fix DELETE DML statement for presto/trino *(PR [#3466](https://github.com/tobymao/sqlglot/pull/3466) by [@viplazylmht](https://github.com/viplazylmht))*
- [`7287bb9`](https://github.com/tobymao/sqlglot/commit/7287bb9bf578b2b3afaf25647f505b9d73040dc7) - nested cte ordering closes [#3488](https://github.com/tobymao/sqlglot/pull/3488) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5b64475`](https://github.com/tobymao/sqlglot/commit/5b64475bfd2d6a0ddcb3d0adb60d06dca62421a0) - allow rollup to be used as an identifier *(PR [#3495](https://github.com/tobymao/sqlglot/pull/3495) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3494](https://github.com/tobymao/sqlglot/issues/3494) opened by [@dangoldin](https://github.com/dangoldin)*

### :recycle: Refactors
- [`846d5cd`](https://github.com/tobymao/sqlglot/commit/846d5cd2fe85f836f5ad888e783fedfa2108d579) - **snowflake**: set default precision / width for DECIMAL type *(PR [#3472](https://github.com/tobymao/sqlglot/pull/3472) by [@georgesittas](https://github.com/georgesittas))*
- [`930f923`](https://github.com/tobymao/sqlglot/commit/930f923c6da182be33ad4c912b64ec052a63af30) - clean up Hex / LowerHex implementation *(PR [#3483](https://github.com/tobymao/sqlglot/pull/3483) by [@georgesittas](https://github.com/georgesittas))*
- [`883fcd7`](https://github.com/tobymao/sqlglot/commit/883fcd78645539a275b66472f0bd1dfe1d3d4401) - **presto**: make DELETE transpilation more robust *(PR [#3487](https://github.com/tobymao/sqlglot/pull/3487) by [@georgesittas](https://github.com/georgesittas))*
- [`49f7f85`](https://github.com/tobymao/sqlglot/commit/49f7f857634ae85547c805ac53911895407dd7cb) - **tsql**: handle TABLE <schema> more gracefully for DeclareItem *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.15.8] - 2024-05-11
### :boom: BREAKING CHANGES
- due to [`510f8b5`](https://github.com/tobymao/sqlglot/commit/510f8b5726c59a13284e9482dc47d488559e6c9e) - improve transpilation of TABLESAMPLE clause *(PR [#3457](https://github.com/tobymao/sqlglot/pull/3457) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilation of TABLESAMPLE clause (#3457)


### :sparkles: New Features
- [`510f8b5`](https://github.com/tobymao/sqlglot/commit/510f8b5726c59a13284e9482dc47d488559e6c9e) - improve transpilation of TABLESAMPLE clause *(PR [#3457](https://github.com/tobymao/sqlglot/pull/3457) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3456](https://github.com/tobymao/sqlglot/issues/3456) opened by [@whummer](https://github.com/whummer)*
- [`e28c959`](https://github.com/tobymao/sqlglot/commit/e28c959bf44208bdb3821b38c13fde59f1944fbb) - make create table cmd parsing less aggressive so that they can be used in sqlmesh @if macros *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.7] - 2024-05-11
### :wrench: Chores
- [`c3bb3da`](https://github.com/tobymao/sqlglot/commit/c3bb3da670d06cb2eef545a909635224b6e7c205) - change python-version to 3.11 for build-rs *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.15.6] - 2024-05-11
### :wrench: Chores
- [`cd8f568`](https://github.com/tobymao/sqlglot/commit/cd8f568dba53efe6b9883035c48a67134016e612) - fix rust deployment workflow bug *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.15.3] - 2024-05-10
### :wrench: Chores
- [`130255e`](https://github.com/tobymao/sqlglot/commit/130255ebc927c48b3d3e479e17c38269bd7d8056) - update rust *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.2] - 2024-05-10
### :sparkles: New Features
- [`116172a`](https://github.com/tobymao/sqlglot/commit/116172a41119e72aaf618a83761f73d52f0440d2) - add support for ON property in ALTER and DROP statements *(PR [#3450](https://github.com/tobymao/sqlglot/pull/3450) by [@GaliFFun](https://github.com/GaliFFun))*
- [`aa104fd`](https://github.com/tobymao/sqlglot/commit/aa104fd2ccd73a13ca60fa3de3296ed4c007e8da) - add semi colon comments *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`2c62267`](https://github.com/tobymao/sqlglot/commit/2c62267e2ae908d10d8164f080bc66a133596bf6) - **bigquery**: fix SHA1 generator *(PR [#3453](https://github.com/tobymao/sqlglot/pull/3453) by [@viplazylmht](https://github.com/viplazylmht))*
  - :arrow_lower_right: *fixes issue [#3451](https://github.com/tobymao/sqlglot/issues/3451) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`fb3dea9`](https://github.com/tobymao/sqlglot/commit/fb3dea9a803157b4684cd62e2ef0b6a6b612f7e1) - **clickhouse**: fix parsing and generating hash functions *(PR [#3454](https://github.com/tobymao/sqlglot/pull/3454) by [@viplazylmht](https://github.com/viplazylmht))*
  - :arrow_lower_right: *fixes issue [#3452](https://github.com/tobymao/sqlglot/issues/3452) opened by [@viplazylmht](https://github.com/viplazylmht)*
- [`b76dfda`](https://github.com/tobymao/sqlglot/commit/b76dfda7b4122a59c52bcbb445cffc6617e68b8c) - **snowflake**: COPY Subquery postfix *(PR [#3449](https://github.com/tobymao/sqlglot/pull/3449) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3434](https://github.com/tobymao/sqlglot/issues/3434) opened by [@whummer](https://github.com/whummer)*

### :wrench: Chores
- [`684df5f`](https://github.com/tobymao/sqlglot/commit/684df5f7e11bb89def9ff71da0913de222bdaf3c) - remove unnecessary set_op *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.1] - 2024-05-10
### :bug: Bug Fixes
- [`33ac4fc`](https://github.com/tobymao/sqlglot/commit/33ac4fca3e5f162500ddde529cd69c338a6fecc5) - add create view tsql *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.15.0] - 2024-05-09
### :boom: BREAKING CHANGES
- due to [`9338ebc`](https://github.com/tobymao/sqlglot/commit/9338ebc6dc9635f12639b562ee2af140cf708b6b) - tsql drop view no catalog *(commit by [@tobymao](https://github.com/tobymao))*:

  tsql drop view no catalog


### :sparkles: New Features
- [`80670bb`](https://github.com/tobymao/sqlglot/commit/80670bbd1e062cc476dcee17d0b9972ff7dc0424) - **snowflake**: Support for APPROX_PERCENTILE *(PR [#3426](https://github.com/tobymao/sqlglot/pull/3426) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3424](https://github.com/tobymao/sqlglot/issues/3424) opened by [@baruchoxman](https://github.com/baruchoxman)*
- [`b46c5b3`](https://github.com/tobymao/sqlglot/commit/b46c5b3ddaed359fb59264f00d7033c7b36bd9a4) - **clickhouse**: add support for partition expression *(PR [#3428](https://github.com/tobymao/sqlglot/pull/3428) by [@GaliFFun](https://github.com/GaliFFun))*
- [`07badc9`](https://github.com/tobymao/sqlglot/commit/07badc9d155cfd6d0c70e4419ed763b8c52b4973) - **clickhouse**: add support for ALTER TABLE REPLACE PARTITION statement *(PR [#3441](https://github.com/tobymao/sqlglot/pull/3441) by [@GaliFFun](https://github.com/GaliFFun))*
- [`baf39e7`](https://github.com/tobymao/sqlglot/commit/baf39e78cdebf5478b59f83120c43b39b27d1a31) - **redshift**: improve ALTER TABLE .. ALTER .. support *(PR [#3444](https://github.com/tobymao/sqlglot/pull/3444) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`e8014e2`](https://github.com/tobymao/sqlglot/commit/e8014e2a479c37ef75510e7d5ca90ed30522ce60) - **mysql**: Parse REPLACE statement as Command *(PR [#3425](https://github.com/tobymao/sqlglot/pull/3425) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3423](https://github.com/tobymao/sqlglot/issues/3423) opened by [@DyCheer](https://github.com/DyCheer)*
- [`273731f`](https://github.com/tobymao/sqlglot/commit/273731fd8cba4d6bda0d7ce109f25c49de0ec95c) - **snowflake**: parse CREATE SEQUENCE with commas *(PR [#3436](https://github.com/tobymao/sqlglot/pull/3436) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3435](https://github.com/tobymao/sqlglot/issues/3435) opened by [@whummer](https://github.com/whummer)*
- [`761ba6f`](https://github.com/tobymao/sqlglot/commit/761ba6fb507158d4e5ea51ca396809be91c11ebf) - don't generate connector comments when comments=False closes [#3439](https://github.com/tobymao/sqlglot/pull/3439) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a2a6eaa`](https://github.com/tobymao/sqlglot/commit/a2a6eaa5d7ace2879ded7c3a4cf4192b75c07f26) - handle empty string in connector comment padding *(PR [#3437](https://github.com/tobymao/sqlglot/pull/3437) by [@uncledata](https://github.com/uncledata))*
- [`1bc0ce5`](https://github.com/tobymao/sqlglot/commit/1bc0ce57eca5e401a4c39237b52ee722bdfb46af) - func to binary MOD generation *(PR [#3440](https://github.com/tobymao/sqlglot/pull/3440) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3431](https://github.com/tobymao/sqlglot/issues/3431) opened by [@daniel769](https://github.com/daniel769)*
- [`5cfb29c`](https://github.com/tobymao/sqlglot/commit/5cfb29c7ff6015e39d7fd5b94ed2aa66436e33ae) - **bigquery**: MOD edge case *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`9825c4c`](https://github.com/tobymao/sqlglot/commit/9825c4cb616af07a048109c499666081bc6e4eba) - improve error handling for nested schema levels *(PR [#3445](https://github.com/tobymao/sqlglot/pull/3445) by [@tobymao](https://github.com/tobymao))*
- [`c309def`](https://github.com/tobymao/sqlglot/commit/c309defa450f755dbed1d1b6f276b4b1765166e2) - **duckdb**: use name sequence instead of single _t for unnest alias *(PR [#3446](https://github.com/tobymao/sqlglot/pull/3446) by [@georgesittas](https://github.com/georgesittas))*
- [`0927ae3`](https://github.com/tobymao/sqlglot/commit/0927ae3c448ebf068b89bfa5e46b8f135121b470) - **executor**: use timezone-aware object to represent datetime in UTC *(PR [#3447](https://github.com/tobymao/sqlglot/pull/3447) by [@georgesittas](https://github.com/georgesittas))*
- [`9338ebc`](https://github.com/tobymao/sqlglot/commit/9338ebc6dc9635f12639b562ee2af140cf708b6b) - tsql drop view no catalog *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`30f9d30`](https://github.com/tobymao/sqlglot/commit/30f9d30d8ab3727a43b1e6f363f28631cbfa7f92) - bump ruff to 0.4.3 *(PR [#3430](https://github.com/tobymao/sqlglot/pull/3430) by [@georgesittas](https://github.com/georgesittas))*
- [`91bed56`](https://github.com/tobymao/sqlglot/commit/91bed5607e442d416021a1f93e4a457fb47b6a1f) - test 3.12 *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.14.0] - 2024-05-07
### :boom: BREAKING CHANGES
- due to [`258ad3b`](https://github.com/tobymao/sqlglot/commit/258ad3bbf73f55d02ed78a93fa0f16d4630159e3) - parse column instead of identifier for SET assignment LHS *(PR [#3417](https://github.com/tobymao/sqlglot/pull/3417) by [@georgesittas](https://github.com/georgesittas))*:

  parse column instead of identifier for SET assignment LHS (#3417)


### :bug: Bug Fixes
- [`258ad3b`](https://github.com/tobymao/sqlglot/commit/258ad3bbf73f55d02ed78a93fa0f16d4630159e3) - parse column instead of identifier for SET assignment LHS *(PR [#3417](https://github.com/tobymao/sqlglot/pull/3417) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3415](https://github.com/tobymao/sqlglot/issues/3415) opened by [@tekumara](https://github.com/tekumara)*
- [`17c31da`](https://github.com/tobymao/sqlglot/commit/17c31da9e159dc1cdd91bd6df38c43606bdc48c9) - **lineage**: get rid of comments in Node names *(PR [#3418](https://github.com/tobymao/sqlglot/pull/3418) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3414](https://github.com/tobymao/sqlglot/issues/3414) opened by [@jaspev123](https://github.com/jaspev123)*
- [`ea197ea`](https://github.com/tobymao/sqlglot/commit/ea197eae2fcdbeba395b53cf4864fc2e44134c71) - **snowflake**: ensure OBJECT_CONSTRUCT is not generated inside of VALUES *(PR [#3419](https://github.com/tobymao/sqlglot/pull/3419) by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.7] - 2024-05-04
### :wrench: Chores
- [`4dbcd4f`](https://github.com/tobymao/sqlglot/commit/4dbcd4f7147204b7bafa32d14dfe615882562b6b) - refactor publish workflow for sqlglotrs releasing *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.6] - 2024-05-04
### :wrench: Chores
- [`aa4f90a`](https://github.com/tobymao/sqlglot/commit/aa4f90acde9c022fb7f984b30763c732977c1b4c) - refactor publish workflow for sqlglotrs releasing *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.5] - 2024-05-04
### :wrench: Chores
- [`0deffd8`](https://github.com/tobymao/sqlglot/commit/0deffd89a8c6d2da90c9a654c22b78dd4c7dd8f6) - refactor publish workflow for sqlglotrs releasing *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.4] - 2024-05-04
### :wrench: Chores
- [`5125732`](https://github.com/tobymao/sqlglot/commit/5125732f05408750aceefba99b48aeb4def89557) - refactor publish workflow for sqlglotrs releasing *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.3] - 2024-05-04
### :wrench: Chores
- [`0a36dd8`](https://github.com/tobymao/sqlglot/commit/0a36dd85cd7de544a509f7e4ccdddf0cb0c1f697) - fix should-deploy-rs bash condition *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.2] - 2024-05-04
### :bug: Bug Fixes
- [`fc979a0`](https://github.com/tobymao/sqlglot/commit/fc979a0055c0f402cda77448d9c7dfecf45a901f) - **snowflake**: make FILE_FORMAT option always be uppercase in COPY INTO *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`1b5b77d`](https://github.com/tobymao/sqlglot/commit/1b5b77d849260589a2f7d3593c4472e47cae0280) - improve unsupported error documentation *(PR [#3406](https://github.com/tobymao/sqlglot/pull/3406) by [@georgesittas](https://github.com/georgesittas))*
- [`fcb51af`](https://github.com/tobymao/sqlglot/commit/fcb51afc4631cfc5f494c9114d4aba667aa46087) - release sqlglotrs only when Cargo.toml is updated *(PR [#3408](https://github.com/tobymao/sqlglot/pull/3408) by [@georgesittas](https://github.com/georgesittas))*


## [v23.13.1] - 2024-05-04
### :bug: Bug Fixes
- [`2c2a788`](https://github.com/tobymao/sqlglot/commit/2c2a788bb3a5a46e7729a117a6e6b62d33beb020) - **snowflake**: COPY postfix *(PR [#3398](https://github.com/tobymao/sqlglot/pull/3398) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3388](https://github.com/tobymao/sqlglot/issues/3388) opened by [@dangoldin](https://github.com/dangoldin)*


## [v23.13.0] - 2024-05-03
### :boom: BREAKING CHANGES
- due to [`cc6259d`](https://github.com/tobymao/sqlglot/commit/cc6259de3d68831ded31bfb7fafe1ce654aa89dd) - Mark UDTF child scopes as ScopeType.SUBQUERY *(PR [#3390](https://github.com/tobymao/sqlglot/pull/3390) by [@VaggelisD](https://github.com/VaggelisD))*:

  Mark UDTF child scopes as ScopeType.SUBQUERY (#3390)

- due to [`33bae9b`](https://github.com/tobymao/sqlglot/commit/33bae9b527b27f02dfafff3f45534f85aa9e0d9d) - get rid of superfluous "parameters" arg in RegexpReplace *(PR [#3394](https://github.com/tobymao/sqlglot/pull/3394) by [@georgesittas](https://github.com/georgesittas))*:

  get rid of superfluous "parameters" arg in RegexpReplace (#3394)

- due to [`3768514`](https://github.com/tobymao/sqlglot/commit/3768514e3b2f256b69553e173b40f17180744ab0) - snowflake optional merge insert *(commit by [@tobymao](https://github.com/tobymao))*:

  snowflake optional merge insert

- due to [`d1b4f1f`](https://github.com/tobymao/sqlglot/commit/d1b4f1f256cd772bec366d6bf13d9589e1fdfc4b) - Introducing TIMESTAMP_NTZ token and data type *(PR [#3386](https://github.com/tobymao/sqlglot/pull/3386) by [@VaggelisD](https://github.com/VaggelisD))*:

  Introducing TIMESTAMP_NTZ token and data type (#3386)


### :sparkles: New Features
- [`d1b4f1f`](https://github.com/tobymao/sqlglot/commit/d1b4f1f256cd772bec366d6bf13d9589e1fdfc4b) - Introducing TIMESTAMP_NTZ token and data type *(PR [#3386](https://github.com/tobymao/sqlglot/pull/3386) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3379](https://github.com/tobymao/sqlglot/issues/3379) opened by [@aersam](https://github.com/aersam)*
- [`16691f9`](https://github.com/tobymao/sqlglot/commit/16691f962822a132e233d61c2b67ec0fc3da51eb) - **prql**: add support for AGGREGATE *(PR [#3395](https://github.com/tobymao/sqlglot/pull/3395) by [@fool1280](https://github.com/fool1280))*
- [`534fb80`](https://github.com/tobymao/sqlglot/commit/534fb80462370b5236061472496c35a16e9bab4a) - **postgres**: add support for anonymos index DDL syntax *(PR [#3403](https://github.com/tobymao/sqlglot/pull/3403) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`a2afcca`](https://github.com/tobymao/sqlglot/commit/a2afccafd300939eaa5a3b075820f3bf8e8dcaac) - **mysql**: don't cast into invalid numeric/text types *(PR [#3375](https://github.com/tobymao/sqlglot/pull/3375) by [@georgesittas](https://github.com/georgesittas))*
- [`60b5c3b`](https://github.com/tobymao/sqlglot/commit/60b5c3b1b5dfb4aa00754f4b2473ad054b8dd14a) - **spark**: transpile presto TRY, fix JSON casting issue *(PR [#3376](https://github.com/tobymao/sqlglot/pull/3376) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3374](https://github.com/tobymao/sqlglot/issues/3374) opened by [@cploonker](https://github.com/cploonker)*
- [`3e8de71`](https://github.com/tobymao/sqlglot/commit/3e8de7124b735a6ab52971a3e51702c4e7b74be5) - **postgres**: allow FOR clause without FROM in SUBSTRING closes [#3377](https://github.com/tobymao/sqlglot/pull/3377) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`b2a7e55`](https://github.com/tobymao/sqlglot/commit/b2a7e550b25fd95eb0abba63228c9e285be168e0) - **optimizer**: Remove XOR from connector simplifications *(PR [#3380](https://github.com/tobymao/sqlglot/pull/3380) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3372](https://github.com/tobymao/sqlglot/issues/3372) opened by [@colincointe](https://github.com/colincointe)*
- [`477754c`](https://github.com/tobymao/sqlglot/commit/477754c72c47b6dc9dd01463b8f6fae6686cb1ac) - **trino**: bring back TRIM parsing *(PR [#3385](https://github.com/tobymao/sqlglot/pull/3385) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3384](https://github.com/tobymao/sqlglot/issues/3384) opened by [@dmelchor-stripe](https://github.com/dmelchor-stripe)*
- [`cc6259d`](https://github.com/tobymao/sqlglot/commit/cc6259de3d68831ded31bfb7fafe1ce654aa89dd) - **optimizer**: Mark UDTF child scopes as ScopeType.SUBQUERY *(PR [#3390](https://github.com/tobymao/sqlglot/pull/3390) by [@VaggelisD](https://github.com/VaggelisD))*
- [`0d23b20`](https://github.com/tobymao/sqlglot/commit/0d23b20352a8931adf8224d322da324b18e8282d) - allow joins in FROM expression parser *(PR [#3389](https://github.com/tobymao/sqlglot/pull/3389) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3387](https://github.com/tobymao/sqlglot/issues/3387) opened by [@MikeWallis42](https://github.com/MikeWallis42)*
- [`e7021df`](https://github.com/tobymao/sqlglot/commit/e7021df397a1dc5e726d1e391ef6428a3190856d) - **duckdb**: Preserve DATE_SUB roundtrip *(PR [#3382](https://github.com/tobymao/sqlglot/pull/3382) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3373](https://github.com/tobymao/sqlglot/issues/3373) opened by [@zergar](https://github.com/zergar)*
- [`641b296`](https://github.com/tobymao/sqlglot/commit/641b296017591b65ffc223d28b37e51886789ca7) - **postgres**: tokenize INT8 as BIGINT *(PR [#3392](https://github.com/tobymao/sqlglot/pull/3392) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3391](https://github.com/tobymao/sqlglot/issues/3391) opened by [@fuzi1996](https://github.com/fuzi1996)*
- [`33bae9b`](https://github.com/tobymao/sqlglot/commit/33bae9b527b27f02dfafff3f45534f85aa9e0d9d) - get rid of superfluous "parameters" arg in RegexpReplace *(PR [#3394](https://github.com/tobymao/sqlglot/pull/3394) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3393](https://github.com/tobymao/sqlglot/issues/3393) opened by [@rzykov](https://github.com/rzykov)*
- [`3768514`](https://github.com/tobymao/sqlglot/commit/3768514e3b2f256b69553e173b40f17180744ab0) - snowflake optional merge insert *(commit by [@tobymao](https://github.com/tobymao))*
- [`f44cd24`](https://github.com/tobymao/sqlglot/commit/f44cd248a82f5519afd0edba5112a499b804fe8f) - make generated constraint parsing more lenient fixes [#3397](https://github.com/tobymao/sqlglot/pull/3397) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`00ff877`](https://github.com/tobymao/sqlglot/commit/00ff87719ab4d6e3a407334c8d811366d0c7ead5) - **tsql**: quote hash sign as well for quoted temporary tables *(PR [#3401](https://github.com/tobymao/sqlglot/pull/3401) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3399](https://github.com/tobymao/sqlglot/issues/3399) opened by [@gforsyth](https://github.com/gforsyth)*
- [`84b7026`](https://github.com/tobymao/sqlglot/commit/84b7026e2fbc4e73c3b4c0c86cb764b95541841e) - **trino**: support for data type 'tdigest' *(PR [#3402](https://github.com/tobymao/sqlglot/pull/3402) by [@suryaiyer95](https://github.com/suryaiyer95))*
- [`24e1115`](https://github.com/tobymao/sqlglot/commit/24e1115c957d42a5511c1c428516e3ce5426cd88) - **trino|presto**: adding cast support for "hyperloglog" column type *(PR [#3405](https://github.com/tobymao/sqlglot/pull/3405) by [@uncledata](https://github.com/uncledata))*


## [v23.12.2] - 2024-04-30
### :sparkles: New Features
- [`d2a6f16`](https://github.com/tobymao/sqlglot/commit/d2a6f16c35cbe355932d0e0eab2fc6ba096d8a97) - COPY TO/FROM statement *(PR [#3359](https://github.com/tobymao/sqlglot/pull/3359) by [@VaggelisD](https://github.com/VaggelisD))*
- [`f034ea0`](https://github.com/tobymao/sqlglot/commit/f034ea0fdd7429bf6694e07b4aff06c665c10951) - **mysql**: Transpile TimestampTrunc *(PR [#3367](https://github.com/tobymao/sqlglot/pull/3367) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3366](https://github.com/tobymao/sqlglot/issues/3366) opened by [@sivpr2000](https://github.com/sivpr2000)*

### :bug: Bug Fixes
- [`f697cb1`](https://github.com/tobymao/sqlglot/commit/f697cb16b6d744253febb2f83476853e63e06f88) - duckdb describe query closes [#3353](https://github.com/tobymao/sqlglot/pull/3353) *(commit by [@tobymao](https://github.com/tobymao))*
- [`6e0fc5d`](https://github.com/tobymao/sqlglot/commit/6e0fc5dd8e1921aac1e3f9834dd6a1c0e30b9e50) - export optimizer functions explicitly in init *(PR [#3358](https://github.com/tobymao/sqlglot/pull/3358) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3354](https://github.com/tobymao/sqlglot/issues/3354) opened by [@tekumara](https://github.com/tekumara)*
- [`23d45ee`](https://github.com/tobymao/sqlglot/commit/23d45eefb8b5f650d2e723499a12ac6801d5cd14) - **postgres**: don't generate CommentColumnConstraint *(PR [#3357](https://github.com/tobymao/sqlglot/pull/3357) by [@georgesittas](https://github.com/georgesittas))*
- [`e87685b`](https://github.com/tobymao/sqlglot/commit/e87685b6971d6ddb7d222993b38aa224c39c5154) - **lineage**: use source names of derived table sources for laterals *(PR [#3360](https://github.com/tobymao/sqlglot/pull/3360) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3356](https://github.com/tobymao/sqlglot/issues/3356) opened by [@eliaperantoni](https://github.com/eliaperantoni)*
- [`e82a30b`](https://github.com/tobymao/sqlglot/commit/e82a30b6563547daea0bb087e1b6b5bf3b0532d3) - **postgres**: don't generate SchemaCommentProperty *(PR [#3364](https://github.com/tobymao/sqlglot/pull/3364) by [@georgesittas](https://github.com/georgesittas))*
- [`47dc52c`](https://github.com/tobymao/sqlglot/commit/47dc52c99ea50b55d08f2b57885eebbd577b8b46) - **mysql**: convert epoch extraction into UNIX_TIMESTAMP call *(PR [#3369](https://github.com/tobymao/sqlglot/pull/3369) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3368](https://github.com/tobymao/sqlglot/issues/3368) opened by [@FaizelK](https://github.com/FaizelK)*
- [`b8f0979`](https://github.com/tobymao/sqlglot/commit/b8f0979537cf3ad9ef83f2c30d6cfb23cd4d2d1e) - **mysql**: generate GROUP_CONCAT for ArrayAgg *(PR [#3370](https://github.com/tobymao/sqlglot/pull/3370) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3368](https://github.com/tobymao/sqlglot/issues/3368) opened by [@FaizelK](https://github.com/FaizelK)*

### :recycle: Refactors
- [`b928f54`](https://github.com/tobymao/sqlglot/commit/b928f542a81d299311d01bd8f1eb762a13adf5c8) - don't mutate the AST when creating DDL scopes *(PR [#3371](https://github.com/tobymao/sqlglot/pull/3371) by [@georgesittas](https://github.com/georgesittas))*


## [v23.12.1] - 2024-04-25
### :wrench: Chores
- [`719d394`](https://github.com/tobymao/sqlglot/commit/719d3949b75bcdac0d19b86d7398c5d9c4b5bdc3) - add a test for quoted aliases *(commit by [@tobymao](https://github.com/tobymao))*
- [`6d7a9f4`](https://github.com/tobymao/sqlglot/commit/6d7a9f4ec0cd87efe19128dc9e55967172bf324e) - use unknown token types *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.12.0] - 2024-04-25
### :boom: BREAKING CHANGES
- due to [`c5ce47b`](https://github.com/tobymao/sqlglot/commit/c5ce47ba7863e0c536e076ea78ec27cb52324493) - Combine aggregate functions with orderby from WITHIN GROUP *(PR [#3352](https://github.com/tobymao/sqlglot/pull/3352) by [@VaggelisD](https://github.com/VaggelisD))*:

  Combine aggregate functions with orderby from WITHIN GROUP (#3352)


### :sparkles: New Features
- [`80793cc`](https://github.com/tobymao/sqlglot/commit/80793ccdb52b1975d93c64a20380047bc6cf4479) - parse (a,) as a tuple instead of a paren *(PR [#3341](https://github.com/tobymao/sqlglot/pull/3341) by [@georgesittas](https://github.com/georgesittas))*
- [`b3826f8`](https://github.com/tobymao/sqlglot/commit/b3826f873dc81adbfe4fbe35e83b71f4c37c3b16) - allow comments to be attached for identifiers used in definitions *(PR [#3340](https://github.com/tobymao/sqlglot/pull/3340) by [@georgesittas](https://github.com/georgesittas))*
- [`ce7d893`](https://github.com/tobymao/sqlglot/commit/ce7d893c7e0d627b94e9225a06b83b863bd61a40) - **clickhouse**: Parse window functions in ParameterizedAggFuncs *(PR [#3347](https://github.com/tobymao/sqlglot/pull/3347) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3344](https://github.com/tobymao/sqlglot/issues/3344) opened by [@alesk](https://github.com/alesk)*

### :bug: Bug Fixes
- [`0e54975`](https://github.com/tobymao/sqlglot/commit/0e54975bf27f8d765378f47872d372ba3817088e) - **tsql**: only use target table name when generating sp_rename *(PR [#3342](https://github.com/tobymao/sqlglot/pull/3342) by [@georgesittas](https://github.com/georgesittas))*
- [`52bdd0c`](https://github.com/tobymao/sqlglot/commit/52bdd0ce104606c520ad4edf8c781ccc502d5a0e) - **tsql**: Convert TIMESTAMP to ROWVERSION, transpile both to BINARY *(PR [#3348](https://github.com/tobymao/sqlglot/pull/3348) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3345](https://github.com/tobymao/sqlglot/issues/3345) opened by [@aersam](https://github.com/aersam)*
- [`c5ce47b`](https://github.com/tobymao/sqlglot/commit/c5ce47ba7863e0c536e076ea78ec27cb52324493) - **duckdb**: Combine aggregate functions with orderby from WITHIN GROUP *(PR [#3352](https://github.com/tobymao/sqlglot/pull/3352) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3350](https://github.com/tobymao/sqlglot/issues/3350) opened by [@btyuhas](https://github.com/btyuhas)*

### :wrench: Chores
- [`eae2f6b`](https://github.com/tobymao/sqlglot/commit/eae2f6be8f13eb44c404dc638ec50d08f203b094) - update sqlglot logo *(commit by [@tobymao](https://github.com/tobymao))*
- [`fb9a7ad`](https://github.com/tobymao/sqlglot/commit/fb9a7ad8f2af98a248e4576677b7b615b9d4c3e7) - copy png *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.11.2] - 2024-04-19
### :bug: Bug Fixes
- [`68595eb`](https://github.com/tobymao/sqlglot/commit/68595eba02ca9f3a01359566104b4315a313ec0a) - edge case *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.11.1] - 2024-04-19
### :bug: Bug Fixes
- [`9cf6f4e`](https://github.com/tobymao/sqlglot/commit/9cf6f4e49208d5a41bca1bd437d31b1ed894e6eb) - don't allow any_token on reserved keywords *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.11.0] - 2024-04-19
### :boom: BREAKING CHANGES
- due to [`290e408`](https://github.com/tobymao/sqlglot/commit/290e408ccf0d0eeec767d4b58bc1293878a3a3ae) - Preserve DPipe in simplify_concat *(PR [#3317](https://github.com/tobymao/sqlglot/pull/3317) by [@VaggelisD](https://github.com/VaggelisD))*:

  Preserve DPipe in simplify_concat (#3317)

- due to [`83cff79`](https://github.com/tobymao/sqlglot/commit/83cff79633225fe3d8606ec3a5a9e8c1081edd0c) - add comprehensive reserved keywords for presto and redshift *(PR [#3322](https://github.com/tobymao/sqlglot/pull/3322) by [@tobymao](https://github.com/tobymao))*:

  add comprehensive reserved keywords for presto and redshift (#3322)

- due to [`61f5b12`](https://github.com/tobymao/sqlglot/commit/61f5b1274cc1f3d68f0f16d4b3efcdc082f67257) - Introduce partition in exp.Table *(PR [#3327](https://github.com/tobymao/sqlglot/pull/3327) by [@VaggelisD](https://github.com/VaggelisD))*:

  Introduce partition in exp.Table (#3327)

- due to [`1832ff1`](https://github.com/tobymao/sqlglot/commit/1832ff130da06ec905835583f101c031dc4faf1d) - dynamic styling for inline arrays *(commit by [@tobymao](https://github.com/tobymao))*:

  dynamic styling for inline arrays

- due to [`5fb7f5b`](https://github.com/tobymao/sqlglot/commit/5fb7f5b21bc441af8d6fabaff7c3d542d96d3811) - dont double indent comments *(commit by [@tobymao](https://github.com/tobymao))*:

  dont double indent comments


### :sparkles: New Features
- [`4f1691a`](https://github.com/tobymao/sqlglot/commit/4f1691a221f3d7395774f8c131a656a3ec531534) - allow qualify to also annotate on the fly for unnest support *(PR [#3316](https://github.com/tobymao/sqlglot/pull/3316) by [@tobymao](https://github.com/tobymao))*
- [`83cff79`](https://github.com/tobymao/sqlglot/commit/83cff79633225fe3d8606ec3a5a9e8c1081edd0c) - add comprehensive reserved keywords for presto and redshift *(PR [#3322](https://github.com/tobymao/sqlglot/pull/3322) by [@tobymao](https://github.com/tobymao))*
- [`ef3311a`](https://github.com/tobymao/sqlglot/commit/ef3311a8ece67e6300e5ff121660dea8cfd80480) - **hive**: Add 'STORED AS' option in INSERT DIRECTORY *(PR [#3326](https://github.com/tobymao/sqlglot/pull/3326) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3320](https://github.com/tobymao/sqlglot/issues/3320) opened by [@bkyryliuk](https://github.com/bkyryliuk)*
- [`7f9cb2d`](https://github.com/tobymao/sqlglot/commit/7f9cb2d2fe2c09e94f9dbaafcc0a808428b5b21c) - **clickhouse**: Add support for DATE_FORMAT / formatDateTime *(PR [#3329](https://github.com/tobymao/sqlglot/pull/3329) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3324](https://github.com/tobymao/sqlglot/issues/3324) opened by [@PaienNate](https://github.com/PaienNate)*
- [`61f5b12`](https://github.com/tobymao/sqlglot/commit/61f5b1274cc1f3d68f0f16d4b3efcdc082f67257) - Introduce partition in exp.Table *(PR [#3327](https://github.com/tobymao/sqlglot/pull/3327) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3319](https://github.com/tobymao/sqlglot/issues/3319) opened by [@bkyryliuk](https://github.com/bkyryliuk)*
- [`31744b2`](https://github.com/tobymao/sqlglot/commit/31744b26ed97c12fd3cb1e3a0661695fac4c0736) - **prql**: handle NULL *(PR [#3331](https://github.com/tobymao/sqlglot/pull/3331) by [@fool1280](https://github.com/fool1280))*
- [`1105044`](https://github.com/tobymao/sqlglot/commit/1105044fa8c5af8269eeddfe8e160f0c52de913c) - **tsql**: add alter table rename *(commit by [@tobymao](https://github.com/tobymao))*
- [`1832ff1`](https://github.com/tobymao/sqlglot/commit/1832ff130da06ec905835583f101c031dc4faf1d) - dynamic styling for inline arrays *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`ef84f17`](https://github.com/tobymao/sqlglot/commit/ef84f177b7d76b7bf43d6ef38a89cfbe47f4e13b) - **optimizer**: don't simplify parentheses when parent is SubqueryPredicate *(PR [#3315](https://github.com/tobymao/sqlglot/pull/3315) by [@georgesittas](https://github.com/georgesittas))*
- [`290e408`](https://github.com/tobymao/sqlglot/commit/290e408ccf0d0eeec767d4b58bc1293878a3a3ae) - **optimizer**: Preserve DPipe in simplify_concat *(PR [#3317](https://github.com/tobymao/sqlglot/pull/3317) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#2439](https://github.com/TobikoData/sqlmesh/issues/2439) opened by [@ma1f](https://github.com/ma1f)*
- [`52b957a`](https://github.com/tobymao/sqlglot/commit/52b957a0691b09dc43628703b9b3633d7238df5b) - transform eliminate_qualify on generated columns *(PR [#3307](https://github.com/tobymao/sqlglot/pull/3307) by [@viplazylmht](https://github.com/viplazylmht))*
- [`eb8d7b8`](https://github.com/tobymao/sqlglot/commit/eb8d7b80c74850d791ac51a117ed5381b3431b3b) - remove e*s mapping because it's not equivalent to %f *(commit by [@tobymao](https://github.com/tobymao))*
- [`9de1494`](https://github.com/tobymao/sqlglot/commit/9de1494899bfc9ad13270a38054a8deab2fc926e) - allow bigquery udf with resered keyword closes [#3332](https://github.com/tobymao/sqlglot/pull/3332) *(PR [#3333](https://github.com/tobymao/sqlglot/pull/3333) by [@tobymao](https://github.com/tobymao))*
- [`e2b6213`](https://github.com/tobymao/sqlglot/commit/e2b62133add5a39e3a2df1d0c8e634fcab3487ff) - don't double comment unions *(commit by [@tobymao](https://github.com/tobymao))*
- [`5fb7f5b`](https://github.com/tobymao/sqlglot/commit/5fb7f5b21bc441af8d6fabaff7c3d542d96d3811) - dont double indent comments *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`81b28c2`](https://github.com/tobymao/sqlglot/commit/81b28c2a7882b642069afb80cee16991542f84e3) - fix tests with latest duckdb *(commit by [@tobymao](https://github.com/tobymao))*
- [`17f7eaf`](https://github.com/tobymao/sqlglot/commit/17f7eaff564790b1fe7faa414143accf362f550e) - add test *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.10.0] - 2024-04-12
### :bug: Bug Fixes
- [`506760d`](https://github.com/tobymao/sqlglot/commit/506760d2597779e287be4fffdeb1b375994320b1) - **redshift**: unqualify unnest columns *(PR [#3314](https://github.com/tobymao/sqlglot/pull/3314) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`0450521`](https://github.com/tobymao/sqlglot/commit/0450521a4470633be26ad5399247d5c9083e2afc) - get rid of 1st projection pad for leading comma formatting *(PR [#3308](https://github.com/tobymao/sqlglot/pull/3308) by [@georgesittas](https://github.com/georgesittas))*


## [v23.9.0] - 2024-04-12
### :boom: BREAKING CHANGES
- due to [`32cdc36`](https://github.com/tobymao/sqlglot/commit/32cdc3635b22e3e5d0cd5caf5a6ad171ca7c34fb) - allow unions to be limited directly and stop subquerying since… *(PR [#3301](https://github.com/tobymao/sqlglot/pull/3301) by [@tobymao](https://github.com/tobymao))*:

  allow unions to be limited directly and stop subquerying since… (#3301)

- due to [`3c97d34`](https://github.com/tobymao/sqlglot/commit/3c97d3437ea573fd3764eab05ed619353fced580) - parse right-hand side of <value> IN (<query>) as a Subquery *(PR [#3304](https://github.com/tobymao/sqlglot/pull/3304) by [@georgesittas](https://github.com/georgesittas))*:

  parse right-hand side of <value> IN (<query>) as a Subquery (#3304)

- due to [`75e0c69`](https://github.com/tobymao/sqlglot/commit/75e0c69e33922168fcadb4e457ae93815bf533e1) - cast less aggressively *(PR [#3302](https://github.com/tobymao/sqlglot/pull/3302) by [@georgesittas](https://github.com/georgesittas))*:

  cast less aggressively (#3302)


### :sparkles: New Features
- [`a721923`](https://github.com/tobymao/sqlglot/commit/a72192306c8fad6253ad9a03661edcfaa15757c7) - **prql**: Add support for SORT *(PR [#3297](https://github.com/tobymao/sqlglot/pull/3297) by [@fool1280](https://github.com/fool1280))*
- [`2ea438b`](https://github.com/tobymao/sqlglot/commit/2ea438b89f76a357390d657fe3f9e01d2a79e7e4) - is_negative helper method *(commit by [@tobymao](https://github.com/tobymao))*
- [`b28cd89`](https://github.com/tobymao/sqlglot/commit/b28cd89823a38f3a90c57344a44719364d66d723) - improve transpilation of datetime functions to Teradata *(PR [#3295](https://github.com/tobymao/sqlglot/pull/3295) by [@maureen-daum](https://github.com/maureen-daum))*
- [`32cdc36`](https://github.com/tobymao/sqlglot/commit/32cdc3635b22e3e5d0cd5caf5a6ad171ca7c34fb) - allow unions to be limited directly and stop subquerying since… *(PR [#3301](https://github.com/tobymao/sqlglot/pull/3301) by [@tobymao](https://github.com/tobymao))*
  - :arrow_lower_right: *addresses issue [#3300](https://github.com/tobymao/sqlglot/issues/3300) opened by [@williaster](https://github.com/williaster)*
- [`1bc51df`](https://github.com/tobymao/sqlglot/commit/1bc51dfa9d8fd5d7dbea42d3d55aa1db66776ce5) - **teradata**: handle transpile of quarter function *(PR [#3303](https://github.com/tobymao/sqlglot/pull/3303) by [@maureen-daum](https://github.com/maureen-daum))*
- [`4790414`](https://github.com/tobymao/sqlglot/commit/4790414b887b347cb94d810eeb3fe4713970984e) - **prql**: Handle DESC with sort *(PR [#3299](https://github.com/tobymao/sqlglot/pull/3299) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`3c97d34`](https://github.com/tobymao/sqlglot/commit/3c97d3437ea573fd3764eab05ed619353fced580) - parse right-hand side of <value> IN (<query>) as a Subquery *(PR [#3304](https://github.com/tobymao/sqlglot/pull/3304) by [@georgesittas](https://github.com/georgesittas))*
- [`75e0c69`](https://github.com/tobymao/sqlglot/commit/75e0c69e33922168fcadb4e457ae93815bf533e1) - cast less aggressively *(PR [#3302](https://github.com/tobymao/sqlglot/pull/3302) by [@georgesittas](https://github.com/georgesittas))*
- [`d3472c6`](https://github.com/tobymao/sqlglot/commit/d3472c664fdfb7c9cfa9a54c6b0491b605cf4913) - Add postgres transpilation for TIME_TO_UNIX *(PR [#3305](https://github.com/tobymao/sqlglot/pull/3305) by [@crericha](https://github.com/crericha))*
- [`2224881`](https://github.com/tobymao/sqlglot/commit/2224881ed378abe075ebcd3bfbc3eee901f89d71) - case when / if should ignore null types *(commit by [@tobymao](https://github.com/tobymao))*
- [`5b2feb7`](https://github.com/tobymao/sqlglot/commit/5b2feb760ecd4c8ee64f8c464518e7e874f9b9bb) - allow unnesting to bring struct fields into scope *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`b523bb6`](https://github.com/tobymao/sqlglot/commit/b523bb630b28866ebab581d43e99f0b2b821ec12) - cleanup teradata to simplify first *(commit by [@tobymao](https://github.com/tobymao))*
- [`6f73186`](https://github.com/tobymao/sqlglot/commit/6f73186681e8eb9f100a1fe4104c82cbae9d0f61) - refactor to use inline lambda *(commit by [@tobymao](https://github.com/tobymao))*
- [`6b21bba`](https://github.com/tobymao/sqlglot/commit/6b21bba378e411797a57d3de8bd06d3efb6afa8c) - make test runnable *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.8.2] - 2024-04-10
### :sparkles: New Features
- [`eabb708`](https://github.com/tobymao/sqlglot/commit/eabb708db9ce7255f947542d57c31a6c93103985) - **prql**: add filter, set operations  *(PR [#3291](https://github.com/tobymao/sqlglot/pull/3291) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`94c188d`](https://github.com/tobymao/sqlglot/commit/94c188d4920fd03e978253ed98711de259d6acb2) - **optimizer**: propagate recursive CTE source to children scopes early *(PR [#3294](https://github.com/tobymao/sqlglot/pull/3294) by [@georgesittas](https://github.com/georgesittas))*
- [`281db61`](https://github.com/tobymao/sqlglot/commit/281db61009ee01d10690dcc1f2039062b2a1a58c) - replace fully qualified columns with generated table aliases since they become invalid *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.8.1] - 2024-04-09
### :sparkles: New Features
- [`942856d`](https://github.com/tobymao/sqlglot/commit/942856d1bdae43b13114a15a66c84467a0f90e75) - **postgres**: add COMMENT ON MATERIALIZED VIEW *(PR [#3293](https://github.com/tobymao/sqlglot/pull/3293) by [@l-vincent-l](https://github.com/l-vincent-l))*

### :bug: Bug Fixes
- [`fd24b27`](https://github.com/tobymao/sqlglot/commit/fd24b2779fa962077e84d234b6821e67f3815551) - make exp.to_column more lenient *(PR [#3292](https://github.com/tobymao/sqlglot/pull/3292) by [@georgesittas](https://github.com/georgesittas))*


## [v23.8.0] - 2024-04-08
### :boom: BREAKING CHANGES
- due to [`6bba030`](https://github.com/tobymao/sqlglot/commit/6bba0308b590aed73e454c2c40d600c670e0ad7f) - transpile map retrieval to duckdb, transpile TRY_ELEMENT_AT *(PR [#3277](https://github.com/tobymao/sqlglot/pull/3277) by [@georgesittas](https://github.com/georgesittas))*:

  transpile map retrieval to duckdb, transpile TRY_ELEMENT_AT (#3277)

- due to [`02218fc`](https://github.com/tobymao/sqlglot/commit/02218fc4f75d22487976572f51bd131170a728e5) - allow to_column to properly parse quoted column paths, make types simpler *(PR [#3289](https://github.com/tobymao/sqlglot/pull/3289) by [@tobymao](https://github.com/tobymao))*:

  allow to_column to properly parse quoted column paths, make types simpler (#3289)


### :sparkles: New Features
- [`08222c2`](https://github.com/tobymao/sqlglot/commit/08222c2c626353be108347b95644660fe04dfcd1) - **clickhouse**: add support for MATERIALIZED, EPHEMERAL column constraints *(PR [#3275](https://github.com/tobymao/sqlglot/pull/3275) by [@pkit](https://github.com/pkit))*
- [`6bba030`](https://github.com/tobymao/sqlglot/commit/6bba0308b590aed73e454c2c40d600c670e0ad7f) - transpile map retrieval to duckdb, transpile TRY_ELEMENT_AT *(PR [#3277](https://github.com/tobymao/sqlglot/pull/3277) by [@georgesittas](https://github.com/georgesittas))*
- [`1726923`](https://github.com/tobymao/sqlglot/commit/17269232ea7f1f2ebf6daae7a49d55ccadc31798) - desc history databricks closes [#3280](https://github.com/tobymao/sqlglot/pull/3280) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0690cbc`](https://github.com/tobymao/sqlglot/commit/0690cbc14f023589f38bcceea443642c5a9cc586) - **snowflake**: FINAL/RUNNING keywords in MATCH_RECOGNIZE MEASURES *(PR [#3284](https://github.com/tobymao/sqlglot/pull/3284) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3282](https://github.com/tobymao/sqlglot/issues/3282) opened by [@galunto](https://github.com/galunto)*
- [`1311ba3`](https://github.com/tobymao/sqlglot/commit/1311ba3da3b5e05f148d602885fcc34cc73c3c6f) - **presto**: add support for DISTINCT / ALL after GROUP BY *(PR [#3290](https://github.com/tobymao/sqlglot/pull/3290) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3286](https://github.com/tobymao/sqlglot/issues/3286) opened by [@bkyryliuk](https://github.com/bkyryliuk)*

### :bug: Bug Fixes
- [`f65d812`](https://github.com/tobymao/sqlglot/commit/f65d8129b0ae887ff882cf5117f04f64b7e7db6f) - move EphemeralColumnConstraint generation to base generator *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`6d1c44d`](https://github.com/tobymao/sqlglot/commit/6d1c44d5b7ac9e3e929de84a761906ad42a07aee) - **optimizer**: unnest union subqueries *(PR [#3278](https://github.com/tobymao/sqlglot/pull/3278) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3276](https://github.com/tobymao/sqlglot/issues/3276) opened by [@khabiri](https://github.com/khabiri)*
- [`a37d231`](https://github.com/tobymao/sqlglot/commit/a37d231200af1dc99fc45fc40627671ee82f6d5e) - **presto**: allow qualify to be an alias closes [#3287](https://github.com/tobymao/sqlglot/pull/3287) *(commit by [@tobymao](https://github.com/tobymao))*
- [`02218fc`](https://github.com/tobymao/sqlglot/commit/02218fc4f75d22487976572f51bd131170a728e5) - allow to_column to properly parse quoted column paths, make types simpler *(PR [#3289](https://github.com/tobymao/sqlglot/pull/3289) by [@tobymao](https://github.com/tobymao))*
- [`fe0eb57`](https://github.com/tobymao/sqlglot/commit/fe0eb57feecce413e3e2992db73424c8cf585599) - pass quoted to the identifier *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`c793629`](https://github.com/tobymao/sqlglot/commit/c79362953a5bf12278f861b8b5d39e6847b22e3b) - another test case *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.7.0] - 2024-04-04
### :sparkles: New Features
- [`e33fb01`](https://github.com/tobymao/sqlglot/commit/e33fb012b47892fab03fab7de896495951f23174) - **prql**: Add support for TAKE  *(PR [#3258](https://github.com/tobymao/sqlglot/pull/3258) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`19302ab`](https://github.com/tobymao/sqlglot/commit/19302abe17a6828e1928075de45c1e2a4f3008ce) - **optimizer**: preserve the original type when creating a date literal *(PR [#3273](https://github.com/tobymao/sqlglot/pull/3273) by [@georgesittas](https://github.com/georgesittas))*


## [v23.6.4] - 2024-04-03
### :bug: Bug Fixes
- [`803fc9e`](https://github.com/tobymao/sqlglot/commit/803fc9e8f245e48e8b0e13760c5fa60cd596a464) - allow placeholders in units closes [#3265](https://github.com/tobymao/sqlglot/pull/3265) *(PR [#3267](https://github.com/tobymao/sqlglot/pull/3267) by [@tobymao](https://github.com/tobymao))*
- [`64ae85b`](https://github.com/tobymao/sqlglot/commit/64ae85ba1344b293ba01dfa300d100ff144cdd7b) - nested cte ordering closes [#3266](https://github.com/tobymao/sqlglot/pull/3266) *(commit by [@tobymao](https://github.com/tobymao))*
- [`09287d9`](https://github.com/tobymao/sqlglot/commit/09287d9b2a39d2476d1f72880f9d2dccfdb210ec) - amend interval unit parsing regression *(PR [#3269](https://github.com/tobymao/sqlglot/pull/3269) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3268](https://github.com/tobymao/sqlglot/issues/3268) opened by [@LilyFoote](https://github.com/LilyFoote)*
- [`bc26e84`](https://github.com/tobymao/sqlglot/commit/bc26e840d171dd03e6053f22ecd785d59cbd4f80) - **optimizer**: tweaks to date simplification *(PR [#3270](https://github.com/tobymao/sqlglot/pull/3270) by [@barakalon](https://github.com/barakalon))*


## [v23.6.0] - 2024-04-02
### :wrench: Chores
- [`4eec748`](https://github.com/tobymao/sqlglot/commit/4eec748d7fd0c73d9593cb3da2b9ebc1d2440436) - deploy sqlglot and then sqlglotrs *(PR [#3264](https://github.com/tobymao/sqlglot/pull/3264) by [@georgesittas](https://github.com/georgesittas))*


## [v23.4.0] - 2024-04-02
### :boom: BREAKING CHANGES
- due to [`e148fe1`](https://github.com/tobymao/sqlglot/commit/e148fe1ace1fe647369c14f2649f428307686a2f) - describe formatted closes [#3244](https://github.com/tobymao/sqlglot/pull/3244) *(commit by [@tobymao](https://github.com/tobymao))*:

  describe formatted closes #3244

- due to [`2c359e7`](https://github.com/tobymao/sqlglot/commit/2c359e790a58e4df9008282401a5578d3ce9d3a4) - properly transpile escape sequences *(PR [#3256](https://github.com/tobymao/sqlglot/pull/3256) by [@georgesittas](https://github.com/georgesittas))*:

  properly transpile escape sequences (#3256)

- due to [`9787567`](https://github.com/tobymao/sqlglot/commit/978756783b639e174f3f614f3e39382fef296640) - bump sqlglotrs to 0.2.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.2.0


### :sparkles: New Features
- [`8dba8e2`](https://github.com/tobymao/sqlglot/commit/8dba8e2508f04ccdf9b10eaa8a456478190a53a5) - **optimizer**: Support for small integer CAST elimination *(PR [#3234](https://github.com/tobymao/sqlglot/pull/3234) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3229](https://github.com/tobymao/sqlglot/issues/3229) opened by [@NickCrews](https://github.com/NickCrews)*
- [`e148fe1`](https://github.com/tobymao/sqlglot/commit/e148fe1ace1fe647369c14f2649f428307686a2f) - describe formatted closes [#3244](https://github.com/tobymao/sqlglot/pull/3244) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a48d7eb`](https://github.com/tobymao/sqlglot/commit/a48d7eb9f3d1f9b3d1ffb9b3ec99b1024b7c3da9) - allow non func hints closes [#3248](https://github.com/tobymao/sqlglot/pull/3248) *(commit by [@tobymao](https://github.com/tobymao))*
- [`d90ec95`](https://github.com/tobymao/sqlglot/commit/d90ec95001a9747d6066d1872c5a9402e2837f62) - add conversion of named tuples and classes to structs *(PR [#3245](https://github.com/tobymao/sqlglot/pull/3245) by [@tobymao](https://github.com/tobymao))*
- [`f88640b`](https://github.com/tobymao/sqlglot/commit/f88640b8df22e29ad2fa845b580cf78ad4fb2262) - **clickhouse**: CREATE TABLE computed columns, column compression, index *(PR [#3252](https://github.com/tobymao/sqlglot/pull/3252) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3243](https://github.com/tobymao/sqlglot/issues/3243) opened by [@lksv](https://github.com/lksv)*
- [`a64ec1b`](https://github.com/tobymao/sqlglot/commit/a64ec1bf60fda00e6dd7122a338c6dac80d005e4) - **snowflake**: MATCH_CONDITION in ASOF JOIN *(PR [#3255](https://github.com/tobymao/sqlglot/pull/3255) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3254](https://github.com/tobymao/sqlglot/issues/3254) opened by [@Bilbottom](https://github.com/Bilbottom)*

### :bug: Bug Fixes
- [`a630c50`](https://github.com/tobymao/sqlglot/commit/a630c50737bb9deb6f44e1afd374b113612a1d24) - allow interval spans closes [#3246](https://github.com/tobymao/sqlglot/pull/3246) *(commit by [@tobymao](https://github.com/tobymao))*
- [`28c5ee7`](https://github.com/tobymao/sqlglot/commit/28c5ee7243af9fb8aa5abf2d5d36d6fa4ef47681) - **mysql**: Duplicate parsing of ENGINE_ATTRIBUTE *(PR [#3253](https://github.com/tobymao/sqlglot/pull/3253) by [@VaggelisD](https://github.com/VaggelisD))*
- [`2c359e7`](https://github.com/tobymao/sqlglot/commit/2c359e790a58e4df9008282401a5578d3ce9d3a4) - properly transpile escape sequences *(PR [#3256](https://github.com/tobymao/sqlglot/pull/3256) by [@georgesittas](https://github.com/georgesittas))*
- [`6badfd1`](https://github.com/tobymao/sqlglot/commit/6badfd17b416380a4077f2ef48f1efcbed3c78d3) - Fix STRPOS for Presto & Trino *(PR [#3261](https://github.com/tobymao/sqlglot/pull/3261) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3259](https://github.com/tobymao/sqlglot/issues/3259) opened by [@amitgilad3](https://github.com/amitgilad3)*

### :wrench: Chores
- [`9787567`](https://github.com/tobymao/sqlglot/commit/978756783b639e174f3f614f3e39382fef296640) - bump sqlglotrs to 0.2.0 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.3.0] - 2024-03-29
### :boom: BREAKING CHANGES
- due to [`0919be5`](https://github.com/tobymao/sqlglot/commit/0919be5eea7aba175e173dbfc0b6547e5c9473a8) - StrToUnix Hive parsing, Presto generation fixes *(PR [#3225](https://github.com/tobymao/sqlglot/pull/3225) by [@georgesittas](https://github.com/georgesittas))*:

  StrToUnix Hive parsing, Presto generation fixes (#3225)

- due to [`163c85c`](https://github.com/tobymao/sqlglot/commit/163c85c8ed327150a6e5c79f1a4b52a8848d4408) - convert dt with isoformat sep space for better compat, trino doesnt accept T *(commit by [@tobymao](https://github.com/tobymao))*:

  convert dt with isoformat sep space for better compat, trino doesnt accept T


### :sparkles: New Features
- [`59f1d13`](https://github.com/tobymao/sqlglot/commit/59f1d13bc5e37ebe6636b05e0381facc9725f7b0) - **oracle**: Support for CONNECT BY [NOCYCLE] *(PR [#3238](https://github.com/tobymao/sqlglot/pull/3238) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3237](https://github.com/tobymao/sqlglot/issues/3237) opened by [@Hal-H2Apps](https://github.com/Hal-H2Apps)*
- [`12563ae`](https://github.com/tobymao/sqlglot/commit/12563ae0645487d5e63343224e1016cce4be447b) - mvp for transpling sqlite's STRFTIME *(PR [#3242](https://github.com/tobymao/sqlglot/pull/3242) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3240](https://github.com/tobymao/sqlglot/issues/3240) opened by [@markhalonen](https://github.com/markhalonen)*

### :bug: Bug Fixes
- [`0919be5`](https://github.com/tobymao/sqlglot/commit/0919be5eea7aba175e173dbfc0b6547e5c9473a8) - StrToUnix Hive parsing, Presto generation fixes *(PR [#3225](https://github.com/tobymao/sqlglot/pull/3225) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3221](https://github.com/tobymao/sqlglot/issues/3221) opened by [@luhea](https://github.com/luhea)*
- [`163c85c`](https://github.com/tobymao/sqlglot/commit/163c85c8ed327150a6e5c79f1a4b52a8848d4408) - convert dt with isoformat sep space for better compat, trino doesnt accept T *(commit by [@tobymao](https://github.com/tobymao))*
- [`555647d`](https://github.com/tobymao/sqlglot/commit/555647d5541c2e52b40d098ee42f6454518e8401) - make property value parsing more lenient *(PR [#3230](https://github.com/tobymao/sqlglot/pull/3230) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3228](https://github.com/tobymao/sqlglot/issues/3228) opened by [@hsheth2](https://github.com/hsheth2)*
- [`8325039`](https://github.com/tobymao/sqlglot/commit/83250398c7804863a6b3f339305600df39515ccc) - **duckdb**: wrap columns inside of INTERVAL expressions *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`fd5783f`](https://github.com/tobymao/sqlglot/commit/fd5783f34cb0cb7052477f25a9847a5efd61c04f) - don't evaluate Rand twice when ordering by it *(PR [#3233](https://github.com/tobymao/sqlglot/pull/3233) by [@georgesittas](https://github.com/georgesittas))*
- [`b097da5`](https://github.com/tobymao/sqlglot/commit/b097da5a624fa467830464427ec57bf3b303de3f) - index error when comment sql is none *(commit by [@tobymao](https://github.com/tobymao))*
- [`bf94ce3`](https://github.com/tobymao/sqlglot/commit/bf94ce317497ab92e9fe0562b3034f3482601072) - > 1 nested joins closes [#3231](https://github.com/tobymao/sqlglot/pull/3231) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2a3a5cd`](https://github.com/tobymao/sqlglot/commit/2a3a5cdcffe39d42153b3e960a580d084a27c0eb) - properly parse/generate duckdb MAP {..} syntax, annotate MAPs *(PR [#3241](https://github.com/tobymao/sqlglot/pull/3241) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`647611e`](https://github.com/tobymao/sqlglot/commit/647611e16bdb5ecfc2eec30111cc6689200836b7) - only set vars with necessary *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.1.0] - 2024-03-26
### :boom: BREAKING CHANGES
- due to [`71b82b4`](https://github.com/tobymao/sqlglot/commit/71b82b424b9c336072b011785a0e3e9650ae1380) - allow transformations that mutate the visited node's parent *(PR [#3182](https://github.com/tobymao/sqlglot/pull/3182) by [@georgesittas](https://github.com/georgesittas))*:

  allow transformations that mutate the visited node's parent (#3182)


### :sparkles: New Features
- [`c19878a`](https://github.com/tobymao/sqlglot/commit/c19878a329078ce6ebfbb4337316ff5e43b8c924) - transpile Snowflake's ADDTIME *(PR [#3180](https://github.com/tobymao/sqlglot/pull/3180) by [@georgesittas](https://github.com/georgesittas))*
- [`66e2e49`](https://github.com/tobymao/sqlglot/commit/66e2e497626a77540b9addd35f2edb287c7b62fe) - improve lineage perf *(commit by [@tobymao](https://github.com/tobymao))*
- [`ad23608`](https://github.com/tobymao/sqlglot/commit/ad23608f9f3724f0c35e5d517bba51f77a84f6cb) - **mysql**: Parse MODIFY COLUMN *(PR [#3189](https://github.com/tobymao/sqlglot/pull/3189) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3186](https://github.com/tobymao/sqlglot/issues/3186) opened by [@kosti-hokkanen-supermetrics](https://github.com/kosti-hokkanen-supermetrics)*
- [`a18444d`](https://github.com/tobymao/sqlglot/commit/a18444dbd7ccfc05b189dcb2005c85a1048cc8de) - add expressions for CORR, COVAR_SAMP, COVAR_POP *(PR [#3193](https://github.com/tobymao/sqlglot/pull/3193) by [@ttzhou](https://github.com/ttzhou))*
- [`3620b99`](https://github.com/tobymao/sqlglot/commit/3620b9974c28df7d4d189ebd5fdcb675f41a275d) - add support for converting `bytes` to sqlglot AST *(PR [#3198](https://github.com/tobymao/sqlglot/pull/3198) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3195](https://github.com/tobymao/sqlglot/issues/3195) opened by [@aersam](https://github.com/aersam)*
- [`648c819`](https://github.com/tobymao/sqlglot/commit/648c819071082f7a1f2f6587336ae765d4915034) - redshift starts with support *(PR [#3194](https://github.com/tobymao/sqlglot/pull/3194) by [@eakmanrq](https://github.com/eakmanrq))*
- [`c355a4a`](https://github.com/tobymao/sqlglot/commit/c355a4a821c7eaf76df510020d825a9f326068de) - **tsql**: add support for WITH <view_attribute> in view DDL *(PR [#3203](https://github.com/tobymao/sqlglot/pull/3203) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3202](https://github.com/tobymao/sqlglot/issues/3202) opened by [@mr-miles](https://github.com/mr-miles)*
- [`8622eb2`](https://github.com/tobymao/sqlglot/commit/8622eb21c89a0d7569e27b3c739592cb96946a3a) - **duckdb**: add support for heredoc string syntax *(PR [#3212](https://github.com/tobymao/sqlglot/pull/3212) by [@georgesittas](https://github.com/georgesittas))*
- [`b50dc5e`](https://github.com/tobymao/sqlglot/commit/b50dc5ecc7d29bce43229d050da8c4e37951853c) - Support for MySQL & Redshift UnixTotime *(PR [#3223](https://github.com/tobymao/sqlglot/pull/3223) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3214](https://github.com/tobymao/sqlglot/issues/3214) opened by [@exgalibas](https://github.com/exgalibas)*
- [`2f6a2f1`](https://github.com/tobymao/sqlglot/commit/2f6a2f13bbd40f3d5348b0ed1b8cf6736ef9d1c5) - **optimizer**: Support for UNION BY NAME *(PR [#3224](https://github.com/tobymao/sqlglot/pull/3224) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3222](https://github.com/tobymao/sqlglot/issues/3222) opened by [@yiyuanliu](https://github.com/yiyuanliu)*

### :bug: Bug Fixes
- [`71b82b4`](https://github.com/tobymao/sqlglot/commit/71b82b424b9c336072b011785a0e3e9650ae1380) - allow transformations that mutate the visited node's parent *(PR [#3182](https://github.com/tobymao/sqlglot/pull/3182) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3181](https://github.com/tobymao/sqlglot/issues/3181) opened by [@l-vincent-l](https://github.com/l-vincent-l)*
- [`6827edd`](https://github.com/tobymao/sqlglot/commit/6827edd108bbc6ecfcc0f03495f00c08022efb3b) - **postgres**: Fix ARROW/DARROW column operators *(PR [#3191](https://github.com/tobymao/sqlglot/pull/3191) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3185](https://github.com/tobymao/sqlglot/issues/3185) opened by [@ZipBrandon](https://github.com/ZipBrandon)*
- [`0dd9ba5`](https://github.com/tobymao/sqlglot/commit/0dd9ba5ef57d29b6406a5d2a7e381eb6e6f56221) - Fix backtracking through try/catch exceptions *(PR [#3190](https://github.com/tobymao/sqlglot/pull/3190) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3175](https://github.com/tobymao/sqlglot/issues/3175) opened by [@herry13](https://github.com/herry13)*
- [`5cdd874`](https://github.com/tobymao/sqlglot/commit/5cdd8749bacb101711a477798ff96bace44ccfb1) - **generator**: compute csv leading comma pad length correctly *(PR [#3201](https://github.com/tobymao/sqlglot/pull/3201) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3199](https://github.com/tobymao/sqlglot/issues/3199) opened by [@giovannipcarvalho](https://github.com/giovannipcarvalho)*
- [`73fc807`](https://github.com/tobymao/sqlglot/commit/73fc807a48bfadc5bbe5594b55ba45480e93be3c) - **tokenizer**: don't increment array cursor by 2 on CRLF *(PR [#3204](https://github.com/tobymao/sqlglot/pull/3204) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3200](https://github.com/tobymao/sqlglot/issues/3200) opened by [@mr-miles](https://github.com/mr-miles)*
- [`af1b026`](https://github.com/tobymao/sqlglot/commit/af1b02697303160050ee32b1c89ff80f14d9d0fa) - **snowflake**: convert VALUES with invalid expressions into UNION ALL *(PR [#3213](https://github.com/tobymao/sqlglot/pull/3213) by [@georgesittas](https://github.com/georgesittas))*
- [`ec4648f`](https://github.com/tobymao/sqlglot/commit/ec4648f7eb982bf48b5bf09271c1955b892867fa) - **optimizer**: don't merge ORDER BY into UNION *(PR [#3215](https://github.com/tobymao/sqlglot/pull/3215) by [@barakalon](https://github.com/barakalon))*
  - :arrow_lower_right: *fixes issue [#3211](https://github.com/tobymao/sqlglot/issues/3211) opened by [@rorynormaness](https://github.com/rorynormaness)*
- [`e4dd052`](https://github.com/tobymao/sqlglot/commit/e4dd0526031591179156a1eea45089213b23cdf7) - allow snowflake object_construct with string keys to transpile to sqlglot dialect *(commit by [@tobymao](https://github.com/tobymao))*
- [`9e39076`](https://github.com/tobymao/sqlglot/commit/9e39076b7f581dc68e10c558ff8f6c9809bfe841) - **tsql**: datestrtodate for tsql closes [#3216](https://github.com/tobymao/sqlglot/pull/3216) *(commit by [@tobymao](https://github.com/tobymao))*
- [`e7c9158`](https://github.com/tobymao/sqlglot/commit/e7c91584ac7fb35082ebd1d4873f13307ea848af) - bq datetime to timestamp *(PR [#3220](https://github.com/tobymao/sqlglot/pull/3220) by [@eakmanrq](https://github.com/eakmanrq))*
- [`e6b8d1f`](https://github.com/tobymao/sqlglot/commit/e6b8d1f0061d55bf434d1a838f858b9fa412e312) - **optimizer**: constrain UDTF scope boundary *(PR [#3226](https://github.com/tobymao/sqlglot/pull/3226) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3209](https://github.com/tobymao/sqlglot/issues/3209) opened by [@rorynormaness](https://github.com/rorynormaness)*

### :recycle: Refactors
- [`4cd0e17`](https://github.com/tobymao/sqlglot/commit/4cd0e1719a55a75dac1114736fbbe48a8aa8f294) - get rid of redundant condition in Expression.replace *(PR [#3192](https://github.com/tobymao/sqlglot/pull/3192) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`3879518`](https://github.com/tobymao/sqlglot/commit/3879518f951233fed3434c493a5786573ee814fd) - bump sqlglotrs to 0.1.3 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v23.0.5] - 2024-03-20
### :bug: Bug Fixes
- [`ed2c9e1`](https://github.com/tobymao/sqlglot/commit/ed2c9e126cc7e679c543adaa2827c1f5c47b96d7) - move varchar max conversion to base *(commit by [@tobymao](https://github.com/tobymao))*
- [`e3b6139`](https://github.com/tobymao/sqlglot/commit/e3b61392b1d050447f77fcf1b04efd6dcbfc311e) - move comment from window function to Window expression *(PR [#3178](https://github.com/tobymao/sqlglot/pull/3178) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2299](https://github.com/TobikoData/sqlmesh/issues/2299) opened by [@georgesittas](https://github.com/georgesittas)*
- [`a452276`](https://github.com/tobymao/sqlglot/commit/a452276da4daaa436a9ac95566bcbb2954d149e3) - **clickhouse**: Fixing FORMAT being parsed as implicit alias *(PR [#3179](https://github.com/tobymao/sqlglot/pull/3179) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3176](https://github.com/tobymao/sqlglot/issues/3176) opened by [@mlipiev](https://github.com/mlipiev)*


## [v23.0.4] - 2024-03-20
### :bug: Bug Fixes
- [`42cf703`](https://github.com/tobymao/sqlglot/commit/42cf70351e7811a077da29af42b28662ede203ac) - redshift varchar(max) catch lower case *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`20cd803`](https://github.com/tobymao/sqlglot/commit/20cd8038268b162af7bae63d54ed2f349502042a) - cleanup redundant check *(commit by [@tobymao](https://github.com/tobymao))*
- [`7e12342`](https://github.com/tobymao/sqlglot/commit/7e12342029d33ff139a3566243789f54e36f4525) - add superset to readme *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.0.3] - 2024-03-19
### :sparkles: New Features
- [`bc8bc7f`](https://github.com/tobymao/sqlglot/commit/bc8bc7f8c9a6a20a35bab8ea7b34cf6431616b50) - replace a nested child node with a list convenience *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`eaaeab0`](https://github.com/tobymao/sqlglot/commit/eaaeab088010f55ccc221a9a4968f0d4ff67d8b1) - **snowflake**: Allow non-literal expressions too in DATE functions *(PR [#3167](https://github.com/tobymao/sqlglot/pull/3167) by [@VaggelisD](https://github.com/VaggelisD))*


## [v23.0.2] - 2024-03-19
### :sparkles: New Features
- [`32cc2be`](https://github.com/tobymao/sqlglot/commit/32cc2be1b19ade551b42cc70a96f1675ac8773f4) - **postgres**: add support for materialized CTEs *(PR [#3171](https://github.com/tobymao/sqlglot/pull/3171) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3170](https://github.com/tobymao/sqlglot/issues/3170) opened by [@betodealmeida](https://github.com/betodealmeida)*

### :bug: Bug Fixes
- [`df4ce17`](https://github.com/tobymao/sqlglot/commit/df4ce17f24bbb16a64172e351f4e27ac74de668a) - can't expand group by for nulls and bools *(commit by [@tobymao](https://github.com/tobymao))*
- [`d859fc0`](https://github.com/tobymao/sqlglot/commit/d859fc0f6eeb0971dab5b22748d1e84425829444) - unnest annotation with generate_date_array *(PR [#3169](https://github.com/tobymao/sqlglot/pull/3169) by [@tobymao](https://github.com/tobymao))*


## [v23.0.1] - 2024-03-19
### :sparkles: New Features
- [`931774d`](https://github.com/tobymao/sqlglot/commit/931774dde50aa04efecd1ae9cdd6965655670d71) - iterative connector sql *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`5e18d49`](https://github.com/tobymao/sqlglot/commit/5e18d490be3990116bbacd1b09dd52542f51c151) - fill in missing implementation details for replace(None) *(PR [#3166](https://github.com/tobymao/sqlglot/pull/3166) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3165](https://github.com/tobymao/sqlglot/issues/3165) opened by [@streamnsight](https://github.com/streamnsight)*
- [`a0df28f`](https://github.com/tobymao/sqlglot/commit/a0df28f4092ca84d07111cead550b9d6772993ad) - can't simplify null parens *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`eb0a6c3`](https://github.com/tobymao/sqlglot/commit/eb0a6c31c92d16abe785087271b14f7611ff24bc) - actually pop the where statement *(commit by [@tobymao](https://github.com/tobymao))*
- [`f6778ef`](https://github.com/tobymao/sqlglot/commit/f6778ef039a646fb5641f0e91b28f6cbc2f52e78) - add recursion test *(commit by [@tobymao](https://github.com/tobymao))*


## [v23.0.0] - 2024-03-18
### :sparkles: New Features
- [`e838713`](https://github.com/tobymao/sqlglot/commit/e838713bdb3da8a5d04eed43b2015a9d3a71addd) - **mysql**: Support for multi arg GROUP_CONCAT *(PR [#3150](https://github.com/tobymao/sqlglot/pull/3150) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3142](https://github.com/tobymao/sqlglot/issues/3142) opened by [@optionals](https://github.com/optionals)*
- [`7e8f134`](https://github.com/tobymao/sqlglot/commit/7e8f134fb2d78940b27f81be7a347caee371601c) - **test**: Add standard alias to some TPC-DS query *(PR [#3151](https://github.com/tobymao/sqlglot/pull/3151) by [@fool1280](https://github.com/fool1280))*
- [`6d0e965`](https://github.com/tobymao/sqlglot/commit/6d0e9658733c672154ec69fd2a4140332954b466) - add skip limit token kwarg *(PR [#3149](https://github.com/tobymao/sqlglot/pull/3149) by [@z3z1ma](https://github.com/z3z1ma))*
- [`3ed5845`](https://github.com/tobymao/sqlglot/commit/3ed58458f9c89a1241a6fa6bb787e236289af58d) - include table alias in bigquery unnest *(PR [#3156](https://github.com/tobymao/sqlglot/pull/3156) by [@eakmanrq](https://github.com/eakmanrq))*
- [`706fac3`](https://github.com/tobymao/sqlglot/commit/706fac382fbde6c1c6af8acd277291a3f18f94ee) - add bigquery mod op *(PR [#3157](https://github.com/tobymao/sqlglot/pull/3157) by [@eakmanrq](https://github.com/eakmanrq))*
- [`6ffdc25`](https://github.com/tobymao/sqlglot/commit/6ffdc25c673db33c3e9ac5a2c6970c4331a3f978) - **clickhouse**: Support for INSERT INTO TABLE FUNCTION *(PR [#3162](https://github.com/tobymao/sqlglot/pull/3162) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3161](https://github.com/tobymao/sqlglot/issues/3161) opened by [@mlipiev](https://github.com/mlipiev)*
- [`021af42`](https://github.com/tobymao/sqlglot/commit/021af4206f4ff2ad4bd57d30cf1f2f78f24fc844) - **snowflake**: Adding support for DATE, TO_DATE, TRY_TO_DATE functions *(PR [#3160](https://github.com/tobymao/sqlglot/pull/3160) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3152](https://github.com/tobymao/sqlglot/issues/3152) opened by [@lei0zhou](https://github.com/lei0zhou)*

### :bug: Bug Fixes
- [`cfde552`](https://github.com/tobymao/sqlglot/commit/cfde552005e1682d6dd1b71850e163021bf4532f) - asof identifier closes [#3153](https://github.com/tobymao/sqlglot/pull/3153) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b1e6eef`](https://github.com/tobymao/sqlglot/commit/b1e6eefcd4dd60f541047a10ed35c1ac733a636c) - bigquery values transpilation with no column alias *(commit by [@tobymao](https://github.com/tobymao))*
- [`c0760b3`](https://github.com/tobymao/sqlglot/commit/c0760b3be11af701273e55c2c976d67d9a575cc4) - parse over any closes [#3155](https://github.com/tobymao/sqlglot/pull/3155) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`38b931e`](https://github.com/tobymao/sqlglot/commit/38b931ebed9255ce5d0d6185414b6f01ca02b0fd) - pin ruff *(commit by [@tobymao](https://github.com/tobymao))*
- [`66a6284`](https://github.com/tobymao/sqlglot/commit/66a62847342f15b8d412fb91814342951fe23247) - improve type hints of Query methods *(PR [#3148](https://github.com/tobymao/sqlglot/pull/3148) by [@georgesittas](https://github.com/georgesittas))*


## [v22.5.0] - 2024-03-14
### :boom: BREAKING CHANGES
- due to [`2b4952e`](https://github.com/tobymao/sqlglot/commit/2b4952eb151b3f20739803e7bf443b56da457b1f) - desugar LOG2 and LOG10 by converting them into LOG *(PR [#3139](https://github.com/tobymao/sqlglot/pull/3139) by [@georgesittas](https://github.com/georgesittas))*:

  desugar LOG2 and LOG10 by converting them into LOG (#3139)


### :sparkles: New Features
- [`c01ff44`](https://github.com/tobymao/sqlglot/commit/c01ff44b036526807624ba2d1f4b247081e8c56f) - **snowflake**: Add TO_TIMESTAMP test and update env.py *(PR [#3130](https://github.com/tobymao/sqlglot/pull/3130) by [@fool1280](https://github.com/fool1280))*
- [`8526c8e`](https://github.com/tobymao/sqlglot/commit/8526c8e30376c0826ab31a0a342656d5ebced662) - **tsql**: transpile LIMIT with OFFSET properly *(PR [#3145](https://github.com/tobymao/sqlglot/pull/3145) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3144](https://github.com/tobymao/sqlglot/issues/3144) opened by [@iMayK](https://github.com/iMayK)*

### :bug: Bug Fixes
- [`a9db8ff`](https://github.com/tobymao/sqlglot/commit/a9db8ff6ac528da8c3a7a66f0b80a3f0d1a0ed7e) - don't mutate parent nested classes if undefined in a dialect *(PR [#3134](https://github.com/tobymao/sqlglot/pull/3134) by [@georgesittas](https://github.com/georgesittas))*
- [`d6bac3e`](https://github.com/tobymao/sqlglot/commit/d6bac3e54c6445c52daa04015b1b2e4a6933e682) - **duckdb**: Slice + Array bug *(PR [#3137](https://github.com/tobymao/sqlglot/pull/3137) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3136](https://github.com/tobymao/sqlglot/issues/3136) opened by [@cpcloud](https://github.com/cpcloud)*
- [`230a845`](https://github.com/tobymao/sqlglot/commit/230a845d82576b24ef8a3bbcc83677ed637e8247) - optimizer bugs *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`2b4952e`](https://github.com/tobymao/sqlglot/commit/2b4952eb151b3f20739803e7bf443b56da457b1f) - desugar LOG2 and LOG10 by converting them into LOG *(PR [#3139](https://github.com/tobymao/sqlglot/pull/3139) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3138](https://github.com/tobymao/sqlglot/issues/3138) opened by [@baruchoxman](https://github.com/baruchoxman)*

### :wrench: Chores
- [`ebbf5a1`](https://github.com/tobymao/sqlglot/commit/ebbf5a14da12b442bff84d93f8542d4322e0811d) - copy sqlglot.svg in docs/ to also display logo in website *(PR [#3147](https://github.com/tobymao/sqlglot/pull/3147) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3146](https://github.com/tobymao/sqlglot/issues/3146) opened by [@lostmygithubaccount](https://github.com/lostmygithubaccount)*


## [v22.4.0] - 2024-03-12
### :boom: BREAKING CHANGES
- due to [`b1c8cac`](https://github.com/tobymao/sqlglot/commit/b1c8cace6ed3e58657726fa5617a6df63d91f737) - traverse union scopes iteratively *(PR [#3112](https://github.com/tobymao/sqlglot/pull/3112) by [@georgesittas](https://github.com/georgesittas))*:

  traverse union scopes iteratively (#3112)


### :sparkles: New Features
- [`88033da`](https://github.com/tobymao/sqlglot/commit/88033dad05550cde05dcb86cce61a621c071382c) - **test**: add more passing tpcds tests *(PR [#3110](https://github.com/tobymao/sqlglot/pull/3110) by [@fool1280](https://github.com/fool1280))*
- [`804af34`](https://github.com/tobymao/sqlglot/commit/804af347a7cefac251b78fdcb8ff35b63c249d82) - **duckdb**: add support for positional joins *(PR [#3111](https://github.com/tobymao/sqlglot/pull/3111) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3109](https://github.com/tobymao/sqlglot/issues/3109) opened by [@dylanscott](https://github.com/dylanscott)*
- [`c4e7bbf`](https://github.com/tobymao/sqlglot/commit/c4e7bbfd3d88f3efb1fea806f85091dbe32379cf) - improve transpilation of TO_NUMBER *(commit by [@codeDing18](https://github.com/codeDing18))*
- [`80d484c`](https://github.com/tobymao/sqlglot/commit/80d484c428329fb53c905fff9f86ea0ee7bcef3b) - **postgres**: generate StrToDate *(PR [#3124](https://github.com/tobymao/sqlglot/pull/3124) by [@georgesittas](https://github.com/georgesittas))*
- [`09708f5`](https://github.com/tobymao/sqlglot/commit/09708f571bb7b62e96bbfba363b00714243d1a17) - Adding EXCLUDE constraint support *(PR [#3116](https://github.com/tobymao/sqlglot/pull/3116) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3097](https://github.com/tobymao/sqlglot/issues/3097) opened by [@dezhin](https://github.com/dezhin)*
- [`9b25a8e`](https://github.com/tobymao/sqlglot/commit/9b25a8e3788c4cc7a299c703fe5b4086fe86015d) - Adding BACKUP property *(PR [#3127](https://github.com/tobymao/sqlglot/pull/3127) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3125](https://github.com/tobymao/sqlglot/issues/3125) opened by [@hsheth2](https://github.com/hsheth2)*
- [`0ea849b`](https://github.com/tobymao/sqlglot/commit/0ea849b35bd3dd980c4f851d3ea7b5bc628e6fb5) - Adding NAME data type in Postgres/Redshift *(PR [#3128](https://github.com/tobymao/sqlglot/pull/3128) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3123](https://github.com/tobymao/sqlglot/issues/3123) opened by [@hsheth2](https://github.com/hsheth2)*

### :bug: Bug Fixes
- [`c333017`](https://github.com/tobymao/sqlglot/commit/c333017fe49c0645cdaa3a75d0a7cc6a5b46dddc) - correctly generate ArrayJoin in various dialects *(PR [#3120](https://github.com/tobymao/sqlglot/pull/3120) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3119](https://github.com/tobymao/sqlglot/issues/3119) opened by [@cpcloud](https://github.com/cpcloud)*
- [`12d72a6`](https://github.com/tobymao/sqlglot/commit/12d72a6ff6534919979f77a5f045aa9d947d9a09) - make the lineage sources dict type covariant *(PR [#3122](https://github.com/tobymao/sqlglot/pull/3122) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3121](https://github.com/tobymao/sqlglot/issues/3121) opened by [@rexledesma](https://github.com/rexledesma)*
- [`b1c8cac`](https://github.com/tobymao/sqlglot/commit/b1c8cace6ed3e58657726fa5617a6df63d91f737) - traverse union scopes iteratively *(PR [#3112](https://github.com/tobymao/sqlglot/pull/3112) by [@georgesittas](https://github.com/georgesittas))*
- [`94b5a2f`](https://github.com/tobymao/sqlglot/commit/94b5a2fcba3c41d38734f045b7f1d5d4735e4828) - **athena**: Fix CREATE TABLE properties, STRING data type *(PR [#3129](https://github.com/tobymao/sqlglot/pull/3129) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#3126](https://github.com/tobymao/sqlglot/issues/3126) opened by [@matthias-Q](https://github.com/matthias-Q)*

### :recycle: Refactors
- [`0ce9ef1`](https://github.com/tobymao/sqlglot/commit/0ce9ef12d9c030b145d7a7a7432bfc188d6c179a) - improve parsing of storage provider setting in index params *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v22.3.1] - 2024-03-09
### :sparkles: New Features
- [`80b2320`](https://github.com/tobymao/sqlglot/commit/80b23201f9668a5845002c1c21b0a394003847f9) - no recursion dfs *(PR [#3105](https://github.com/tobymao/sqlglot/pull/3105) by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`fa84e2c`](https://github.com/tobymao/sqlglot/commit/fa84e2c2d9ae349033039ec649decc371561e421) - copy all arg keys, including those set to None *(PR [#3108](https://github.com/tobymao/sqlglot/pull/3108) by [@georgesittas](https://github.com/georgesittas))*


## [v22.3.0] - 2024-03-08
### :sparkles: New Features
- [`46c9c2c`](https://github.com/tobymao/sqlglot/commit/46c9c2c35ea5132995cb07a99b94d18d959e6172) - **snowflake**: parse CREATE SEQUENCE *(PR [#3072](https://github.com/tobymao/sqlglot/pull/3072) by [@tekumara](https://github.com/tekumara))*
  - :arrow_lower_right: *addresses issue [#2954](https://github.com/tobymao/sqlglot/issues/2954) opened by [@tharwan](https://github.com/tharwan)*
- [`9f1e1ad`](https://github.com/tobymao/sqlglot/commit/9f1e1ad4350fb412319511825ca3da9b9af14084) - add Athena dialect *(PR [#3089](https://github.com/tobymao/sqlglot/pull/3089) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3087](https://github.com/tobymao/sqlglot/issues/3087) opened by [@sbrandtb](https://github.com/sbrandtb)*
- [`efee388`](https://github.com/tobymao/sqlglot/commit/efee38858c4501ccace4b3eb3f066cb352f3ac60) - no more recursion for union generation *(PR [#3101](https://github.com/tobymao/sqlglot/pull/3101) by [@tobymao](https://github.com/tobymao))*
- [`ddab9df`](https://github.com/tobymao/sqlglot/commit/ddab9dff663985d9473ce4b2dbe4fe266ae1bdf7) - **duckdb**: add support for exp.ArrayJoin *(PR [#3102](https://github.com/tobymao/sqlglot/pull/3102) by [@seruman](https://github.com/seruman))*
- [`8d5be0c`](https://github.com/tobymao/sqlglot/commit/8d5be0cf54e77000b220cfcca0edfdeb1759b70b) - **duckdb**: make ARRAY_TO_STRING transpilable to other dialects *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a38db01`](https://github.com/tobymao/sqlglot/commit/a38db014cce8ada9554c205c879ae0c0dfda1b14) - Generalizing CREATE SEQUENCE *(PR [#3090](https://github.com/tobymao/sqlglot/pull/3090) by [@VaggelisD](https://github.com/VaggelisD))*
- [`18fd079`](https://github.com/tobymao/sqlglot/commit/18fd0794302a1ecaa91be9dfbc7feddd0b8a3b05) - no recursion copy *(PR [#3103](https://github.com/tobymao/sqlglot/pull/3103) by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`b101013`](https://github.com/tobymao/sqlglot/commit/b101013336d0aef6dc99b5ebef85afc12591e212) - subquery edge cases *(PR [#3076](https://github.com/tobymao/sqlglot/pull/3076) by [@tobymao](https://github.com/tobymao))*
- [`8c4400b`](https://github.com/tobymao/sqlglot/commit/8c4400ba194661d1e1ee4aa4ea2649b2356a5f02) - **bigquery**: more table qualification edge cases closes [#3083](https://github.com/tobymao/sqlglot/pull/3083) *(commit by [@tobymao](https://github.com/tobymao))*
- [`d898f55`](https://github.com/tobymao/sqlglot/commit/d898f559fac44789da08689e835619f978c05a3e) - **bigquery**: even more edge cases *(commit by [@tobymao](https://github.com/tobymao))*
- [`4fb74ff`](https://github.com/tobymao/sqlglot/commit/4fb74ff61effd9e5fa8593cdf1c9229d5106ab7e) - dataframe optimize user input *(PR [#3092](https://github.com/tobymao/sqlglot/pull/3092) by [@eakmanrq](https://github.com/eakmanrq))*
  - :arrow_lower_right: *fixes issue [#3091](https://github.com/tobymao/sqlglot/issues/3091) opened by [@alexdemeo](https://github.com/alexdemeo)*

### :recycle: Refactors
- [`cea7508`](https://github.com/tobymao/sqlglot/commit/cea7508c5f2b5838e889486d28df47ad9b263345) - **lineage**: simplify `Node.walk()` *(PR [#3098](https://github.com/tobymao/sqlglot/pull/3098) by [@rexledesma](https://github.com/rexledesma))*
- [`ebe5a46`](https://github.com/tobymao/sqlglot/commit/ebe5a462ed50711d6ded18b454c5294e487e323f) - **executor**: simplify column type inference *(PR [#3104](https://github.com/tobymao/sqlglot/pull/3104) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`6c67a2b`](https://github.com/tobymao/sqlglot/commit/6c67a2b0dbe2a66ea4ce2e008101f4cf41b1c517) - reduce size of tpcds *(commit by [@tobymao](https://github.com/tobymao))*
- [`21e4fca`](https://github.com/tobymao/sqlglot/commit/21e4fca2b744a22981d8ff1696986061d3344d40) - update dialect count in README to include Athena *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v22.2.1] - 2024-03-04
### :sparkles: New Features
- [`19e07f3`](https://github.com/tobymao/sqlglot/commit/19e07f39688f53fafac25655616883363f20b1cf) - initial commit prql *(commit by [@tobymao](https://github.com/tobymao))*
- [`13b64fd`](https://github.com/tobymao/sqlglot/commit/13b64fdcdde35e8fe022f76f4f2a5d55d53b982f) - more prql *(commit by [@tobymao](https://github.com/tobymao))*
- [`3d263aa`](https://github.com/tobymao/sqlglot/commit/3d263aafbfb8d45bc678914e1eb925592c30eaf8) - **oracle**: Support for INSERT hint *(PR [#3077](https://github.com/tobymao/sqlglot/pull/3077) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *addresses issue [#3074](https://github.com/tobymao/sqlglot/issues/3074) opened by [@sunrutcon](https://github.com/sunrutcon)*

### :bug: Bug Fixes
- [`c51b64f`](https://github.com/tobymao/sqlglot/commit/c51b64fa6a437698fd8b347d98ffaf9fb543d2d5) - json extract precedence closes [#3068](https://github.com/tobymao/sqlglot/pull/3068) *(commit by [@tobymao](https://github.com/tobymao))*
- [`223a475`](https://github.com/tobymao/sqlglot/commit/223a4751f88809710872fa7d757d22d9eeeb4f40) - **planner**: don't overwrite JOIN step name *(PR [#3071](https://github.com/tobymao/sqlglot/pull/3071) by [@georgesittas](https://github.com/georgesittas))*
- [`2770ddc`](https://github.com/tobymao/sqlglot/commit/2770ddcc34148f85caeabf2b6f4f799b3e825a6c) - drop CLUSTER/DISTRIBUTED/SORT BY modifiers when unsupported *(PR [#3069](https://github.com/tobymao/sqlglot/pull/3069) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3066](https://github.com/tobymao/sqlglot/issues/3066) opened by [@cploonker](https://github.com/cploonker)*
- [`4173ea2`](https://github.com/tobymao/sqlglot/commit/4173ea29bbd8944896c259fe45209de69fcbdc46) - handle lineage of subqueries *(PR [#3075](https://github.com/tobymao/sqlglot/pull/3075) by [@tobymao](https://github.com/tobymao))*
- [`0ebce40`](https://github.com/tobymao/sqlglot/commit/0ebce40f5e524c61ece022bbf8640556e880a4bf) - **redshift**: don't transform multi-arg DISTINCT clause *(PR [#3079](https://github.com/tobymao/sqlglot/pull/3079) by [@georgesittas](https://github.com/georgesittas))*


## [v22.2.0] - 2024-03-01
### :boom: BREAKING CHANGES
- due to [`08bafbd`](https://github.com/tobymao/sqlglot/commit/08bafbd597b6a81e222832fac9f068f1290e41fa) - handle unnesting groups closes [#3056](https://github.com/tobymao/sqlglot/pull/3056) *(PR [#3058](https://github.com/tobymao/sqlglot/pull/3058) by [@tobymao](https://github.com/tobymao))*:

  handle unnesting groups closes #3056 (#3058)

- due to [`4029fab`](https://github.com/tobymao/sqlglot/commit/4029fab81e9abcedd6321baaf5baf9aa192f643d) - expand alias refs of double aggs if it is a window func *(PR [#3059](https://github.com/tobymao/sqlglot/pull/3059) by [@tobymao](https://github.com/tobymao))*:

  expand alias refs of double aggs if it is a window func (#3059)


### :sparkles: New Features
- [`8662e31`](https://github.com/tobymao/sqlglot/commit/8662e31bd115eb668c4b74377ed2985937e81510) - **postgres**: improve transpilation of JSON array unnesting *(PR [#3063](https://github.com/tobymao/sqlglot/pull/3063) by [@georgesittas](https://github.com/georgesittas))*
- [`c9bde44`](https://github.com/tobymao/sqlglot/commit/c9bde44bac5f7026388ec6357a6c1e00ee760edc) - Making parse_number & parse_string more lenient *(PR [#3064](https://github.com/tobymao/sqlglot/pull/3064) by [@VaggelisD](https://github.com/VaggelisD))*

### :bug: Bug Fixes
- [`08bafbd`](https://github.com/tobymao/sqlglot/commit/08bafbd597b6a81e222832fac9f068f1290e41fa) - handle unnesting groups closes [#3056](https://github.com/tobymao/sqlglot/pull/3056) *(PR [#3058](https://github.com/tobymao/sqlglot/pull/3058) by [@tobymao](https://github.com/tobymao))*
- [`4029fab`](https://github.com/tobymao/sqlglot/commit/4029fab81e9abcedd6321baaf5baf9aa192f643d) - expand alias refs of double aggs if it is a window func *(PR [#3059](https://github.com/tobymao/sqlglot/pull/3059) by [@tobymao](https://github.com/tobymao))*
- [`4e6e82c`](https://github.com/tobymao/sqlglot/commit/4e6e82c9d4d9ef33635446e19b4b44f3ae27160c) - **snowflake**: allow any identifier after : closes [#3061](https://github.com/tobymao/sqlglot/pull/3061) *(PR [#3062](https://github.com/tobymao/sqlglot/pull/3062) by [@georgesittas](https://github.com/georgesittas))*
- [`e2becea`](https://github.com/tobymao/sqlglot/commit/e2becead1e6be12ddf8bde703d2c403220506784) - is distinct from parsing *(commit by [@tobymao](https://github.com/tobymao))*
- [`c8a753b`](https://github.com/tobymao/sqlglot/commit/c8a753b488d99172db9df10616e8bd3431452ff8) - Ignore Identifier nodes in the diffing algorithm *(PR [#3065](https://github.com/tobymao/sqlglot/pull/3065) by [@izeigerman](https://github.com/izeigerman))*


## [v22.1.1] - 2024-02-29
### :sparkles: New Features
- [`1e25ec9`](https://github.com/tobymao/sqlglot/commit/1e25ec984510a1ffee76956b0dcb15bcd84f5d44) - **test**: handle NULL value in TPC-DS  *(PR [#3052](https://github.com/tobymao/sqlglot/pull/3052) by [@fool1280](https://github.com/fool1280))*
- [`ad21b6b`](https://github.com/tobymao/sqlglot/commit/ad21b6b47716d394ca6b8fb3b82d58b887d5adb3) - **test**: add more passing tpc-ds test *(PR [#3053](https://github.com/tobymao/sqlglot/pull/3053) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`08249af`](https://github.com/tobymao/sqlglot/commit/08249af50351a24277e1f3f1574629eb5c68d3a5) - Hive UnixToTime regression, README stale results *(PR [#3055](https://github.com/tobymao/sqlglot/pull/3055) by [@VaggelisD](https://github.com/VaggelisD))*
- [`39b3813`](https://github.com/tobymao/sqlglot/commit/39b381341fe697ae54f5d3a438b4035447fe552a) - **redshift**: don't pop recursive cte table columns *(commit by [@tobymao](https://github.com/tobymao))*
- [`6a9501f`](https://github.com/tobymao/sqlglot/commit/6a9501f7407be3682ce3b9cc73b7340ad9a0c2e8) - ensure UDF identifier quotes are preserved *(PR [#3057](https://github.com/tobymao/sqlglot/pull/3057) by [@georgesittas](https://github.com/georgesittas))*


## [v22.1.0] - 2024-02-29
### :sparkles: New Features
- [`6393979`](https://github.com/tobymao/sqlglot/commit/63939796b39c69b25adfc6f224ccd4761f23cb66) - **oracle**: connect_by_root closes [#3050](https://github.com/tobymao/sqlglot/pull/3050) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`bd0a40d`](https://github.com/tobymao/sqlglot/commit/bd0a40dde2ab2ad168ada0d5bae0c99fba9d762f) - normalize column for lineage and raise if cannot find closes [#3049](https://github.com/tobymao/sqlglot/pull/3049) *(commit by [@tobymao](https://github.com/tobymao))*


## [v22.0.2] - 2024-02-28
### :sparkles: New Features
- [`51f8d58`](https://github.com/tobymao/sqlglot/commit/51f8d5897b18e6f7c0bc66881a3e36c8842ff2ff) - **tsql**: add support for OPTION clause, select only *(PR [#3025](https://github.com/tobymao/sqlglot/pull/3025) by [@nadav-botanica](https://github.com/nadav-botanica))*
- [`c9eef99`](https://github.com/tobymao/sqlglot/commit/c9eef99b8fe3367c22a8186fb397ad550ac11386) - Support for TRUNCATE TABLE/DATABASE DDL *(PR [#3026](https://github.com/tobymao/sqlglot/pull/3026) by [@VaggelisD](https://github.com/VaggelisD))*
- [`703b878`](https://github.com/tobymao/sqlglot/commit/703b87816c3e5f7b50407d2f2a14f3a9cba4e3f8) - **mysql**: add LOCK property, allow properties after ALTER TABLE *(PR [#3027](https://github.com/tobymao/sqlglot/pull/3027) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3020](https://github.com/tobymao/sqlglot/issues/3020) opened by [@samotarnik](https://github.com/samotarnik)*

### :bug: Bug Fixes
- [`bc4acb9`](https://github.com/tobymao/sqlglot/commit/bc4acb9582a80a6c3d4b491b48a68f110e399e3a) - allow trailing comma in ORDER BY list *(PR [#3031](https://github.com/tobymao/sqlglot/pull/3031) by [@georgesittas](https://github.com/georgesittas))*
- [`4105639`](https://github.com/tobymao/sqlglot/commit/4105639ddbbc504d4bd4607511ac35e8ca30c774) - **bigquery**: unquoted project-0.x closes [#3029](https://github.com/tobymao/sqlglot/pull/3029) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f1f2aec`](https://github.com/tobymao/sqlglot/commit/f1f2aecb09c6c0d9a965d87669368945abd112cc) - bigquery edgecase *(commit by [@tobymao](https://github.com/tobymao))*
- [`5c01c01`](https://github.com/tobymao/sqlglot/commit/5c01c010348271e8cfddea3ed0ac51293c3819b3) - handle falsey values for replace_placeholders kwargs *(PR [#3036](https://github.com/tobymao/sqlglot/pull/3036) by [@sarchila](https://github.com/sarchila))*
- [`ccfbb22`](https://github.com/tobymao/sqlglot/commit/ccfbb2238131bda8fc7a3ad8a9c50a0f009dac52) - **clickhouse**: make CTE expression parser more flexible fixes [#3038](https://github.com/tobymao/sqlglot/pull/3038) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`30e0bb1`](https://github.com/tobymao/sqlglot/commit/30e0bb13162e75c53b031bbb69c66093f8ad4a96) - another edge case *(commit by [@tobymao](https://github.com/tobymao))*
- [`0d93852`](https://github.com/tobymao/sqlglot/commit/0d938524a618b4bd7c057623a2c8755ca3afec6d) - **oracle**: handle GLOBAL/PRIVATE keyword in temp table DDL *(PR [#3045](https://github.com/tobymao/sqlglot/pull/3045) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3037](https://github.com/tobymao/sqlglot/issues/3037) opened by [@gforsyth](https://github.com/gforsyth)*
- [`e89d38d`](https://github.com/tobymao/sqlglot/commit/e89d38ddd5f699f2ac09baf77238ad5fab00acb8) - **duckdb**: recognize ENUM as a type *(PR [#3044](https://github.com/tobymao/sqlglot/pull/3044) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#3043](https://github.com/tobymao/sqlglot/issues/3043) opened by [@joouha](https://github.com/joouha)*
- [`4db7781`](https://github.com/tobymao/sqlglot/commit/4db77816a44652b3edc8aae5aab24242854f9a14) - avoid raising a KeyError in the lineage module, log a warning *(PR [#3048](https://github.com/tobymao/sqlglot/pull/3048) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`5337980`](https://github.com/tobymao/sqlglot/commit/53379805454f0e6f325581b839d2fcb37c10de1b) - simplify parsing of keyword sequences as Vars *(PR [#3034](https://github.com/tobymao/sqlglot/pull/3034) by [@georgesittas](https://github.com/georgesittas))*
- [`bc35c59`](https://github.com/tobymao/sqlglot/commit/bc35c59004cb3fb9849f0ee8e5f06b356396c0b0) - use _parse_var_from_options for USE statement parser *(PR [#3035](https://github.com/tobymao/sqlglot/pull/3035) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`c0d355a`](https://github.com/tobymao/sqlglot/commit/c0d355a27d86539dfd95a87fea7e1bd75c4fabe4) - bump sqlglotrs to 0.1.2 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v22.0.1] - 2024-02-26
### :bug: Bug Fixes
- [`e2fc6e8`](https://github.com/tobymao/sqlglot/commit/e2fc6e88dc7ae52d956dd84721de197c6c698d90) - **optimizer**: fix parent mutation of new_projections in column qualifier *(PR [#3030](https://github.com/tobymao/sqlglot/pull/3030) by [@georgesittas](https://github.com/georgesittas))*


## [v22.0.0] - 2024-02-26
### :boom: BREAKING CHANGES
- due to [`2507aa2`](https://github.com/tobymao/sqlglot/commit/2507aa2dbad3304304558565f266a7f94acd9e98) - consolidate Subqueryable and Unionable into Query expression *(PR [#2992](https://github.com/tobymao/sqlglot/pull/2992) by [@georgesittas](https://github.com/georgesittas))*:

  consolidate Subqueryable and Unionable into Query expression (#2992)

- due to [`d5eb2b1`](https://github.com/tobymao/sqlglot/commit/d5eb2b1e0907026e6e981a8f453f747cb16f44d6) - make implicit unnest syntax explicit by using UNNEST calls *(PR [#3005](https://github.com/tobymao/sqlglot/pull/3005) by [@georgesittas](https://github.com/georgesittas))*:

  make implicit unnest syntax explicit by using UNNEST calls (#3005)

- due to [`238f9aa`](https://github.com/tobymao/sqlglot/commit/238f9aa7c32037d0c280cfe6ece77eed9c311cc5) - refactor structs to always be aliases *(PR [#3017](https://github.com/tobymao/sqlglot/pull/3017) by [@tobymao](https://github.com/tobymao))*:

  refactor structs to always be aliases (#3017)

- due to [`06bcfcd`](https://github.com/tobymao/sqlglot/commit/06bcfcdf69f850693d941675bbcfce1aa80482f6) - select expressions not statements closes [#3022](https://github.com/tobymao/sqlglot/pull/3022), statements can be parsed without into *(commit by [@tobymao](https://github.com/tobymao))*:

  select expressions not statements closes #3022, statements can be parsed without into

- due to [`1612e62`](https://github.com/tobymao/sqlglot/commit/1612e622bd3514d9ca366837f47452969e5267d8) - Add reference to lineage node *(PR [#3018](https://github.com/tobymao/sqlglot/pull/3018) by [@vchan](https://github.com/vchan))*:

  Add reference to lineage node (#3018)


### :sparkles: New Features
- [`e50609b`](https://github.com/tobymao/sqlglot/commit/e50609b119c65407f4f7fe27f06510187dc750a0) - Supporting RANGE <-> GENERATE_SERIES between DuckDB & SQLite *(PR [#3010](https://github.com/tobymao/sqlglot/pull/3010) by [@VaggelisD](https://github.com/VaggelisD))*
- [`1709ec2`](https://github.com/tobymao/sqlglot/commit/1709ec2519edc4b1a91f435d76f1b962355be326) - bigquery e6s format *(commit by [@tobymao](https://github.com/tobymao))*
- [`17e34e7`](https://github.com/tobymao/sqlglot/commit/17e34e79d22e3c8211f1bf42047d4ed3557628b6) - add unnest type annotations *(PR [#3019](https://github.com/tobymao/sqlglot/pull/3019) by [@tobymao](https://github.com/tobymao))*
- [`efdbc12`](https://github.com/tobymao/sqlglot/commit/efdbc127a06b1c6204327caa0d6b0cb01590da13) - clickhouse prewhere closes [#3024](https://github.com/tobymao/sqlglot/pull/3024) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1612e62`](https://github.com/tobymao/sqlglot/commit/1612e622bd3514d9ca366837f47452969e5267d8) - Add reference to lineage node *(PR [#3018](https://github.com/tobymao/sqlglot/pull/3018) by [@vchan](https://github.com/vchan))*
- [`5c3bd10`](https://github.com/tobymao/sqlglot/commit/5c3bd1074960874b4557b13df6d30782fe7b0757) - **test**: add more passing tests of tpc-ds *(PR [#3016](https://github.com/tobymao/sqlglot/pull/3016) by [@fool1280](https://github.com/fool1280))*

### :bug: Bug Fixes
- [`7f547e6`](https://github.com/tobymao/sqlglot/commit/7f547e641f7a0ecaa804d5bea14bd24abce1d346) - it's actually seconds + fraction *(commit by [@tobymao](https://github.com/tobymao))*
- [`238f9aa`](https://github.com/tobymao/sqlglot/commit/238f9aa7c32037d0c280cfe6ece77eed9c311cc5) - refactor structs to always be aliases *(PR [#3017](https://github.com/tobymao/sqlglot/pull/3017) by [@tobymao](https://github.com/tobymao))*
  - :arrow_lower_right: *fixes issue [#3015](https://github.com/tobymao/sqlglot/issues/3015) opened by [@wizardxz](https://github.com/wizardxz)*
- [`06bcfcd`](https://github.com/tobymao/sqlglot/commit/06bcfcdf69f850693d941675bbcfce1aa80482f6) - select expressions not statements closes [#3022](https://github.com/tobymao/sqlglot/pull/3022), statements can be parsed without into *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`2507aa2`](https://github.com/tobymao/sqlglot/commit/2507aa2dbad3304304558565f266a7f94acd9e98) - consolidate Subqueryable and Unionable into Query expression *(PR [#2992](https://github.com/tobymao/sqlglot/pull/2992) by [@georgesittas](https://github.com/georgesittas))*
- [`d5eb2b1`](https://github.com/tobymao/sqlglot/commit/d5eb2b1e0907026e6e981a8f453f747cb16f44d6) - make implicit unnest syntax explicit by using UNNEST calls *(PR [#3005](https://github.com/tobymao/sqlglot/pull/3005) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2996](https://github.com/tobymao/sqlglot/issues/2996) opened by [@wizardxz](https://github.com/wizardxz)*
- [`8943179`](https://github.com/tobymao/sqlglot/commit/8943179dfadba4ed36740322e1e5d3611032b51e) - move limit method to Query, get rid of Subquery.subquery override *(PR [#3013](https://github.com/tobymao/sqlglot/pull/3013) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`9595240`](https://github.com/tobymao/sqlglot/commit/9595240a1c0f0e5ace9f67f31564e5d5edb9a9d2) - make prewhere clickhouse only *(commit by [@tobymao](https://github.com/tobymao))*


## [v21.2.1] - 2024-02-22
### :sparkles: New Features
- [`2a88e40`](https://github.com/tobymao/sqlglot/commit/2a88e40da89fa083bbd8fd0174082fa8e677780a) - **bigquery**: support ELSE and ELSEIF procedural statements *(PR [#3011](https://github.com/tobymao/sqlglot/pull/3011) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#3009](https://github.com/tobymao/sqlglot/issues/3009) opened by [@razvan-am](https://github.com/razvan-am)*
- [`d2e15ed`](https://github.com/tobymao/sqlglot/commit/d2e15ed9b2ab2699f7105f73170b9d780293d432) - improve transpilation of Doris' MONTHS_ADD *(PR [#3012](https://github.com/tobymao/sqlglot/pull/3012) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`c23ac05`](https://github.com/tobymao/sqlglot/commit/c23ac05379e2aa5cb5681e26e2c0b8137300baa3) - bigquery group by order by rewriting with indices *(commit by [@tobymao](https://github.com/tobymao))*


## [v21.2.0] - 2024-02-22
### :boom: BREAKING CHANGES
- due to [`2940417`](https://github.com/tobymao/sqlglot/commit/2940417116761f821c913bf093759243db33c343) - simplify ADD CONSTRAINT handling *(PR [#2990](https://github.com/tobymao/sqlglot/pull/2990) by [@georgesittas](https://github.com/georgesittas))*:

  simplify ADD CONSTRAINT handling (#2990)


### :sparkles: New Features
- [`7c48079`](https://github.com/tobymao/sqlglot/commit/7c4807918de53d18fbfe0295b2644f0ad46003a8) - support parameters in BigQuery / DuckDB *(PR [#2991](https://github.com/tobymao/sqlglot/pull/2991) by [@r1b](https://github.com/r1b))*
- [`b7c2744`](https://github.com/tobymao/sqlglot/commit/b7c2744eba3df631b575e8ab35f29f46419f83ba) - **tests**: update test_executor with tpc-ds  *(PR [#2983](https://github.com/tobymao/sqlglot/pull/2983) by [@fool1280](https://github.com/fool1280))*
- [`c433cad`](https://github.com/tobymao/sqlglot/commit/c433cad7df383e97308ceb946d7f1dc171a5d60b) - allow more leniant bigquery wildcard parsing *(PR [#2998](https://github.com/tobymao/sqlglot/pull/2998) by [@tobymao](https://github.com/tobymao))*
- [`8607247`](https://github.com/tobymao/sqlglot/commit/860724732b70b5557221998a45c3c950b39d664a) - support LEFT JOIN UNNEST in duckdb *(PR [#2999](https://github.com/tobymao/sqlglot/pull/2999) by [@r1b](https://github.com/r1b))*
- [`64e38ed`](https://github.com/tobymao/sqlglot/commit/64e38edb32f9a66a9503e75424d0545da3dbe5df) - add support for more Snowflake SHOW commands *(PR [#3002](https://github.com/tobymao/sqlglot/pull/3002) by [@DanCardin](https://github.com/DanCardin))*

### :bug: Bug Fixes
- [`bc18f56`](https://github.com/tobymao/sqlglot/commit/bc18f56a39e0034e2b285efd7a882a417c517a99) - **optimizer**: don't coerce nested arg types in annotate_by_args *(PR [#2997](https://github.com/tobymao/sqlglot/pull/2997) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2152](https://github.com/TobikoData/sqlmesh/issues/2152) opened by [@plaflamme](https://github.com/plaflamme)*
- [`ccd8cc0`](https://github.com/tobymao/sqlglot/commit/ccd8cc01429d21653198edce079679e17dbb22f6) - doris to_char closes [#3001](https://github.com/tobymao/sqlglot/pull/3001) *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`2940417`](https://github.com/tobymao/sqlglot/commit/2940417116761f821c913bf093759243db33c343) - simplify ADD CONSTRAINT handling *(PR [#2990](https://github.com/tobymao/sqlglot/pull/2990) by [@georgesittas](https://github.com/georgesittas))*
- [`d2711f7`](https://github.com/tobymao/sqlglot/commit/d2711f717aac4a7b624225d31c7fa827f8287476) - clean up duplicative placeholder_sql implementations *(PR [#2993](https://github.com/tobymao/sqlglot/pull/2993) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`53efb58`](https://github.com/tobymao/sqlglot/commit/53efb587a642a171bdb4fb6ad4c33a83c4391908) - cleanup tests *(commit by [@tobymao](https://github.com/tobymao))*


## [v21.1.2] - 2024-02-19
### :sparkles: New Features
- [`b8cbf66`](https://github.com/tobymao/sqlglot/commit/b8cbf66471158371a27d9145b3b553b7a1384c9d) - **bigquery**: parse procedural EXCEPTION WHEN statement into a Command closes [#2981](https://github.com/tobymao/sqlglot/pull/2981) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`906ceee`](https://github.com/tobymao/sqlglot/commit/906ceee0946c547f83177916c3f8d6aeb23023a8) - **duckdb**: implement generation logic for exp.ArrayAny *(PR [#2984](https://github.com/tobymao/sqlglot/pull/2984) by [@georgesittas](https://github.com/georgesittas))*
- [`92455e4`](https://github.com/tobymao/sqlglot/commit/92455e4d4e2c8d5a874a5050d9a38f943479cdca) - **snowflake**: create storage integration  *(PR [#2985](https://github.com/tobymao/sqlglot/pull/2985) by [@tekumara](https://github.com/tekumara))*
- [`bedf6e9`](https://github.com/tobymao/sqlglot/commit/bedf6e9dabf9da25e1fff2f3c8ae22fbf7face0b) - improve transpilation support for ArrayAny *(PR [#2986](https://github.com/tobymao/sqlglot/pull/2986) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2707](https://github.com/tobymao/sqlglot/issues/2707) opened by [@HuashiSCNU0303](https://github.com/HuashiSCNU0303)*

### :bug: Bug Fixes
- [`cc67ab2`](https://github.com/tobymao/sqlglot/commit/cc67ab2513c71a6b9574f8c3cf4c8ba2927d798f) - **tsql**: map StrPosition back to CHARINDEX fixes [#2968](https://github.com/tobymao/sqlglot/pull/2968) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`46f15ce`](https://github.com/tobymao/sqlglot/commit/46f15cef87de3159bc1d422b2620278e9e27ec16) - **postgres**: ensure json extraction can roundtrip unaltered *(PR [#2974](https://github.com/tobymao/sqlglot/pull/2974) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2971](https://github.com/tobymao/sqlglot/issues/2971) opened by [@l-vincent-l](https://github.com/l-vincent-l)*
- [`7ee4fe7`](https://github.com/tobymao/sqlglot/commit/7ee4fe73b29234f2837a212b6c872efd7f5c30ea) - expand using with star except *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`5a34f3d`](https://github.com/tobymao/sqlglot/commit/5a34f3d5f652ac209fd122aa25e46d99d8e5cba6) - clean up tech debt in dialect implementations *(PR [#2977](https://github.com/tobymao/sqlglot/pull/2977) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`ae92789`](https://github.com/tobymao/sqlglot/commit/ae92789cdac4c4f0bb3d5f542bd9fe93aee4ea70) - rephrase some sentences in the FAQ section *(PR [#2980](https://github.com/tobymao/sqlglot/pull/2980) by [@georgesittas](https://github.com/georgesittas))*
- [`22ed4d0`](https://github.com/tobymao/sqlglot/commit/22ed4d0a976dbba15962670873422e86874680b0) - cleanup kv defs from brackets *(PR [#2987](https://github.com/tobymao/sqlglot/pull/2987) by [@tobymao](https://github.com/tobymao))*


## [v21.1.1] - 2024-02-14
### :sparkles: New Features
- [`1d0b3d3`](https://github.com/tobymao/sqlglot/commit/1d0b3d3a22ba5a8128505d636a2ff71d0ea03d03) - add support for multi-part interval addition syntax *(PR [#2970](https://github.com/tobymao/sqlglot/pull/2970) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2969](https://github.com/tobymao/sqlglot/issues/2969) opened by [@aersam](https://github.com/aersam)*

### :bug: Bug Fixes
- [`1c67f03`](https://github.com/tobymao/sqlglot/commit/1c67f030cd9df530e26c620079b2298b1db97d50) - **parser**: enable parsing of values into Identifier for some dialects *(PR [#2962](https://github.com/tobymao/sqlglot/pull/2962) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2957](https://github.com/tobymao/sqlglot/issues/2957) opened by [@hsheth2](https://github.com/hsheth2)*
- [`d8b0d4f`](https://github.com/tobymao/sqlglot/commit/d8b0d4fcc82662004056a68b05ca20f30996661f) - don't treat VALUES as a keyword in BigQuery, Redshift *(PR [#2965](https://github.com/tobymao/sqlglot/pull/2965) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2963](https://github.com/tobymao/sqlglot/issues/2963) opened by [@sean-rose](https://github.com/sean-rose)*
- [`5b7fd10`](https://github.com/tobymao/sqlglot/commit/5b7fd107f279c2f83c9d66d4353032c6d830202c) - **optimizer**: more optimizations for qualifying wide tables *(PR [#2972](https://github.com/tobymao/sqlglot/pull/2972) by [@barakalon](https://github.com/barakalon))*
- [`6cb985a`](https://github.com/tobymao/sqlglot/commit/6cb985ae1346c1a912ed6f81be30310ee1c91dfa) - pass dialect in to_table call inside replace_tables *(PR [#2973](https://github.com/tobymao/sqlglot/pull/2973) by [@georgesittas](https://github.com/georgesittas))*


## [v21.1.0] - 2024-02-12
### :sparkles: New Features
- [`e71d489`](https://github.com/tobymao/sqlglot/commit/e71d4899e6744812fdefc2704c66bbd6043b5bc9) - add array and tuple helpers *(commit by [@tobymao](https://github.com/tobymao))*
- [`876e075`](https://github.com/tobymao/sqlglot/commit/876e07580bb2de06b587fc8ad40eb67604ae8507) - **postgres**: root operator closes [#2940](https://github.com/tobymao/sqlglot/pull/2940) *(commit by [@tobymao](https://github.com/tobymao))*
- [`e731276`](https://github.com/tobymao/sqlglot/commit/e731276dd5490a7d294430e0887eebf19e16d28f) - **snowflake**: add support for SHOW USERS *(PR [#2948](https://github.com/tobymao/sqlglot/pull/2948) by [@DanCardin](https://github.com/DanCardin))*
- [`b9d4468`](https://github.com/tobymao/sqlglot/commit/b9d44688c2b785212db635f121b686df02e2dec9) - **tableau**: identifier and quotes closes [#2950](https://github.com/tobymao/sqlglot/pull/2950) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f8d9dbf`](https://github.com/tobymao/sqlglot/commit/f8d9dbf6744f95bf4b7517e8bcc35dd3a6f70c5d) - **sqlite**: add support for IIF *(PR [#2951](https://github.com/tobymao/sqlglot/pull/2951) by [@georgesittas](https://github.com/georgesittas))*
- [`b755551`](https://github.com/tobymao/sqlglot/commit/b7555516c6bf038dc39c4bba2b243839ceb6e3b5) - **clickhouse**: add basic support for system statement *(PR [#2953](https://github.com/tobymao/sqlglot/pull/2953) by [@GaliFFun](https://github.com/GaliFFun))*

### :bug: Bug Fixes
- [`844018b`](https://github.com/tobymao/sqlglot/commit/844018b8d3a3398d746fdc04c966c7e19d311998) - explode_outer to unnest closes [#2941](https://github.com/tobymao/sqlglot/pull/2941) *(commit by [@tobymao](https://github.com/tobymao))*
- [`159da45`](https://github.com/tobymao/sqlglot/commit/159da4523d6eb3ca6853d631bb98dc8f13c7b0fb) - posexplode_outer to unnest *(PR [#2942](https://github.com/tobymao/sqlglot/pull/2942) by [@chelsea-lin](https://github.com/chelsea-lin))*
- [`76d6634`](https://github.com/tobymao/sqlglot/commit/76d66340e566bd9fa8c783f5d311101eb2e80480) - **spark**: CREATE TABLE ... PARTITIONED BY fixes *(PR [#2937](https://github.com/tobymao/sqlglot/pull/2937) by [@barakalon](https://github.com/barakalon))*
- [`d07ddf9`](https://github.com/tobymao/sqlglot/commit/d07ddf9b460c1b6f672fda4f34dc9231419e6c9d) - **optimizer**: remove redundant casts *(PR [#2945](https://github.com/tobymao/sqlglot/pull/2945) by [@barakalon](https://github.com/barakalon))*
- [`b70a394`](https://github.com/tobymao/sqlglot/commit/b70a394222bf209298026fd100f6b9498acf9fff) - if doesn't support different types *(commit by [@tobymao](https://github.com/tobymao))*
- [`6a988e0`](https://github.com/tobymao/sqlglot/commit/6a988e0160022d33623cd036bf84bb0b222c9062) - **bigquery**: fix annotation of timestamp(x) *(PR [#2946](https://github.com/tobymao/sqlglot/pull/2946) by [@georgesittas](https://github.com/georgesittas))*
- [`78e6d0d`](https://github.com/tobymao/sqlglot/commit/78e6d0de83efbff1d3b61c8550db56c1819f7c22) - **optimizer**: qualify_columns optimizations for wide tables *(PR [#2955](https://github.com/tobymao/sqlglot/pull/2955) by [@barakalon](https://github.com/barakalon))*
- [`c20cc70`](https://github.com/tobymao/sqlglot/commit/c20cc70dfc7f6395af157521c7e99074d697beb4) - **redshift**: don't assume Table is an unnested Column if Join has a predicate *(PR [#2956](https://github.com/tobymao/sqlglot/pull/2956) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2952](https://github.com/tobymao/sqlglot/issues/2952) opened by [@vidit-wisdom](https://github.com/vidit-wisdom)*

### :wrench: Chores
- [`c4524ce`](https://github.com/tobymao/sqlglot/commit/c4524ce1e6a85e16db7ea0289116d0160732dc51) - fix unit test *(commit by [@tobymao](https://github.com/tobymao))*


## [v21.0.2] - 2024-02-08
### :sparkles: New Features
- [`1842c96`](https://github.com/tobymao/sqlglot/commit/1842c96611cadb0227dd3ce8f42457679ab0e08b) - **clickhouse**: add support for LIMIT BY clause *(PR [#2926](https://github.com/tobymao/sqlglot/pull/2926) by [@georgesittas](https://github.com/georgesittas))*
- [`9241858`](https://github.com/tobymao/sqlglot/commit/9241858e559f089b166d9b794e3ebb395624d84a) - add typing for explode closes [#2927](https://github.com/tobymao/sqlglot/pull/2927) *(commit by [@tobymao](https://github.com/tobymao))*
- [`85073d1`](https://github.com/tobymao/sqlglot/commit/85073d1538de8ceef3e5c622a901efd9e6bd38e3) - transpile multi-arg DISTINCT expression *(PR [#2936](https://github.com/tobymao/sqlglot/pull/2936) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2930](https://github.com/tobymao/sqlglot/issues/2930) opened by [@Jake-00](https://github.com/Jake-00)*

### :bug: Bug Fixes
- [`b827626`](https://github.com/tobymao/sqlglot/commit/b8276262bdca57e358284fadfdd468d2bc957e84) - remove find method from Schema *(PR [#2934](https://github.com/tobymao/sqlglot/pull/2934) by [@georgesittas](https://github.com/georgesittas))*
- [`08cd117`](https://github.com/tobymao/sqlglot/commit/08cd117322302f08c95889ebf8699f4171c1d504) - **postgres**: fallback to parameter parser if heredoc is untokenizable *(PR [#2935](https://github.com/tobymao/sqlglot/pull/2935) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2931](https://github.com/tobymao/sqlglot/issues/2931) opened by [@eric-zhu](https://github.com/eric-zhu)*

### :wrench: Chores
- [`e4b5edb`](https://github.com/tobymao/sqlglot/commit/e4b5edbef42944b44d11c35aea31411ce3d79826) - bump sqlglotrs to 0.1.1 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v21.0.1] - 2024-02-07
### :sparkles: New Features
- [`3a20eac`](https://github.com/tobymao/sqlglot/commit/3a20eaccbf5d5a80bd24b95c837cca8103dfe70a) - **clickhouse**: add support for JSONExtractString, clean up some helpers *(PR [#2925](https://github.com/tobymao/sqlglot/pull/2925) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2051](https://github.com/tobymao/sqlglot/issues/2051) opened by [@BTheunissen](https://github.com/BTheunissen)*


## [v21.0.0] - 2024-02-07
### :boom: BREAKING CHANGES
- due to [`b4e8868`](https://github.com/tobymao/sqlglot/commit/b4e886877ecfbafdd64c515c765c3c54764bd987) - improve transpilation of JSON paths across dialects *(PR [#2883](https://github.com/tobymao/sqlglot/pull/2883) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilation of JSON paths across dialects (#2883)

- due to [`aa388ea`](https://github.com/tobymao/sqlglot/commit/aa388ea64404a26550dbb0734f4d3e35111f9e2c) - ignore nulls closes [#2896](https://github.com/tobymao/sqlglot/pull/2896) *(PR [#2898](https://github.com/tobymao/sqlglot/pull/2898) by [@tobymao](https://github.com/tobymao))*:

  ignore nulls closes #2896 (#2898)

- due to [`617a8c0`](https://github.com/tobymao/sqlglot/commit/617a8c0dfc5e9f2716f7827381af0db2e135059e) - timestamp diff for mysql and databricks *(commit by [@tobymao](https://github.com/tobymao))*:

  timestamp diff for mysql and databricks

- due to [`b00b393`](https://github.com/tobymao/sqlglot/commit/b00b393d853ae05a3fce4ef78d7673edbcabf67d) - use raise instead of assert for assert_is *(commit by [@tobymao](https://github.com/tobymao))*:

  use raise instead of assert for assert_is

- due to [`326aa31`](https://github.com/tobymao/sqlglot/commit/326aa31e32e511f4e40d3a5a7b1d599b5e2c1307) - deprecate case where transforms can be plain strs *(PR [#2919](https://github.com/tobymao/sqlglot/pull/2919) by [@georgesittas](https://github.com/georgesittas))*:

  deprecate case where transforms can be plain strs (#2919)


### :sparkles: New Features
- [`fb450f0`](https://github.com/tobymao/sqlglot/commit/fb450f0263ecd6b7c9d0f49d84441327d50b9d83) - add tsql right left auto casting closes [#2899](https://github.com/tobymao/sqlglot/pull/2899) *(commit by [@tobymao](https://github.com/tobymao))*
- [`617a8c0`](https://github.com/tobymao/sqlglot/commit/617a8c0dfc5e9f2716f7827381af0db2e135059e) - timestamp diff for mysql and databricks *(commit by [@tobymao](https://github.com/tobymao))*
- [`3fa92ca`](https://github.com/tobymao/sqlglot/commit/3fa92cac285cbb2bd9d8b5724dadb77be7e12731) - **redshift**: parse GETDATE *(PR [#2904](https://github.com/tobymao/sqlglot/pull/2904) by [@erickpeirson](https://github.com/erickpeirson))*
- [`d262139`](https://github.com/tobymao/sqlglot/commit/d26213998b27fa9b6a66b6d21ab5a3a15f65635e) - **snowflake**: implement parsing logic for SHOW TABLES *(PR [#2913](https://github.com/tobymao/sqlglot/pull/2913) by [@tekumara](https://github.com/tekumara))*
- [`838e780`](https://github.com/tobymao/sqlglot/commit/838e7800c32ad16074efef6a188ebd89083a9717) - improve transpilation of CREATE TABLE LIKE statement *(PR [#2923](https://github.com/tobymao/sqlglot/pull/2923) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2922](https://github.com/tobymao/sqlglot/issues/2922) opened by [@tharwan](https://github.com/tharwan)*
- [`cbbad1f`](https://github.com/tobymao/sqlglot/commit/cbbad1fc40b6b2ca837ddb0f798b1802ad4063da) - improve transpilation of JSON path wildcards *(PR [#2924](https://github.com/tobymao/sqlglot/pull/2924) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`aa388ea`](https://github.com/tobymao/sqlglot/commit/aa388ea64404a26550dbb0734f4d3e35111f9e2c) - ignore nulls closes [#2896](https://github.com/tobymao/sqlglot/pull/2896) *(PR [#2898](https://github.com/tobymao/sqlglot/pull/2898) by [@tobymao](https://github.com/tobymao))*
- [`b00b393`](https://github.com/tobymao/sqlglot/commit/b00b393d853ae05a3fce4ef78d7673edbcabf67d) - use raise instead of assert for assert_is *(commit by [@tobymao](https://github.com/tobymao))*
- [`ab97246`](https://github.com/tobymao/sqlglot/commit/ab972462b1c545b4a60bb88cb40cdb98cb64e360) - array overlaps closes [#2903](https://github.com/tobymao/sqlglot/pull/2903) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f3bdcb0`](https://github.com/tobymao/sqlglot/commit/f3bdcb087bb993289c4a1a5d2de54155ae2d2681) - **duckdb**: fix JSON pointer path parsing, reduce warning noise *(PR [#2911](https://github.com/tobymao/sqlglot/pull/2911) by [@georgesittas](https://github.com/georgesittas))*
- [`072264f`](https://github.com/tobymao/sqlglot/commit/072264f8af25737050f7becd27af5a9331bde896) - **mysql**: SHOW SCHEMAS *(PR [#2916](https://github.com/tobymao/sqlglot/pull/2916) by [@barakalon](https://github.com/barakalon))*
- [`15fdff2`](https://github.com/tobymao/sqlglot/commit/15fdff2df3363ab8d3595e7eeb8baee65e525733) - **optimizer**: don't remove NOT parenthesis *(PR [#2917](https://github.com/tobymao/sqlglot/pull/2917) by [@barakalon](https://github.com/barakalon))*
- [`d20d826`](https://github.com/tobymao/sqlglot/commit/d20d826e9cc4a9b0d636a9b56b5547cd906a5903) - have table exclude this if schema target *(PR [#2921](https://github.com/tobymao/sqlglot/pull/2921) by [@eakmanrq](https://github.com/eakmanrq))*

### :recycle: Refactors
- [`b4e8868`](https://github.com/tobymao/sqlglot/commit/b4e886877ecfbafdd64c515c765c3c54764bd987) - improve transpilation of JSON paths across dialects *(PR [#2883](https://github.com/tobymao/sqlglot/pull/2883) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2839](https://github.com/tobymao/sqlglot/issues/2839) opened by [@FlaviovLeal](https://github.com/FlaviovLeal)*
- [`9481f94`](https://github.com/tobymao/sqlglot/commit/9481f946b068e43d99c9aaae6e1c59abf384eeac) - several JSON path improvements *(PR [#2914](https://github.com/tobymao/sqlglot/pull/2914) by [@georgesittas](https://github.com/georgesittas))*
- [`326aa31`](https://github.com/tobymao/sqlglot/commit/326aa31e32e511f4e40d3a5a7b1d599b5e2c1307) - deprecate case where transforms can be plain strs *(PR [#2919](https://github.com/tobymao/sqlglot/pull/2919) by [@georgesittas](https://github.com/georgesittas))*
- [`15582f4`](https://github.com/tobymao/sqlglot/commit/15582f40bd18da3fa7adbe454b401ef8d31a131e) - move JSON path generation logic in Generator *(PR [#2920](https://github.com/tobymao/sqlglot/pull/2920) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`afb4f9b`](https://github.com/tobymao/sqlglot/commit/afb4f9bfe074200e60b5a870267fe21aa04a87c5) - switch to ruff *(commit by [@tobymao](https://github.com/tobymao))*
- [`f9fdf7b`](https://github.com/tobymao/sqlglot/commit/f9fdf7b3bb25aa7e830b70600728bb35ee1e4ff7) - switch to ruff *(PR [#2912](https://github.com/tobymao/sqlglot/pull/2912) by [@tobymao](https://github.com/tobymao))*
- [`71c33fa`](https://github.com/tobymao/sqlglot/commit/71c33fa13b9c416ae50acb10a9b08dcfcfd35f92) - pandas warning *(commit by [@tobymao](https://github.com/tobymao))*


## [v20.11.0] - 2024-01-29
### :boom: BREAKING CHANGES
- due to [`eb8b40a`](https://github.com/tobymao/sqlglot/commit/eb8b40aade54eec8b34a808dda95420dcf7a7e13) - deprecate NULL, TRUE, FALSE constant expressions *(PR [#2884](https://github.com/tobymao/sqlglot/pull/2884) by [@georgesittas](https://github.com/georgesittas))*:

  deprecate NULL, TRUE, FALSE constant expressions (#2884)


### :sparkles: New Features
- [`3a8ed85`](https://github.com/tobymao/sqlglot/commit/3a8ed8573d5562110b312586ae6fca22038e5d05) - add alter table alter comment closes [#2889](https://github.com/tobymao/sqlglot/pull/2889) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`dc2d7d7`](https://github.com/tobymao/sqlglot/commit/dc2d7d7dd4253fe6b247d534bd92327f186e9aa8) - **tsql**: len text transpilation closes [#2885](https://github.com/tobymao/sqlglot/pull/2885) *(commit by [@tobymao](https://github.com/tobymao))*
- [`ad50f47`](https://github.com/tobymao/sqlglot/commit/ad50f479c47d5b4990f1b41272c69079a453cf21) - type imports *(PR [#2886](https://github.com/tobymao/sqlglot/pull/2886) by [@tobymao](https://github.com/tobymao))*
- [`e4fb7f6`](https://github.com/tobymao/sqlglot/commit/e4fb7f6e1b8ab15ceb5acc6a93256c849c738740) - union should return union *(commit by [@tobymao](https://github.com/tobymao))*
- [`8f795ea`](https://github.com/tobymao/sqlglot/commit/8f795ea00164b69acba093c3684ab54b62138e8e) - don't expand star except/replace refs *(commit by [@tobymao](https://github.com/tobymao))*
- [`218121c`](https://github.com/tobymao/sqlglot/commit/218121c274656a1b252143a7d0fc2d73407115ca) - alter table cluster by closes [#2887](https://github.com/tobymao/sqlglot/pull/2887) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5cec283`](https://github.com/tobymao/sqlglot/commit/5cec2839f8ed8477821bf766025f4b5de0621fe2) - bigquery script if statement closes [#2888](https://github.com/tobymao/sqlglot/pull/2888) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5fc7791`](https://github.com/tobymao/sqlglot/commit/5fc7791a4d19d704c0d4fafe8924cf8f76fcb867) - all view column options without types closes [#2891](https://github.com/tobymao/sqlglot/pull/2891) *(commit by [@tobymao](https://github.com/tobymao))*
- [`102304e`](https://github.com/tobymao/sqlglot/commit/102304e28f2ed7126840789837ed797a75bae44e) - **postgres**: generate CurrentUser without parentheses closes [#2893](https://github.com/tobymao/sqlglot/pull/2893) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`43902db`](https://github.com/tobymao/sqlglot/commit/43902db25706a2434fe7e9ba39addd1c31c2aa64) - error level ignore comments closes [#2895](https://github.com/tobymao/sqlglot/pull/2895) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`eb8b40a`](https://github.com/tobymao/sqlglot/commit/eb8b40aade54eec8b34a808dda95420dcf7a7e13) - deprecate NULL, TRUE, FALSE constant expressions *(PR [#2884](https://github.com/tobymao/sqlglot/pull/2884) by [@georgesittas](https://github.com/georgesittas))*
- [`29cddd5`](https://github.com/tobymao/sqlglot/commit/29cddd5c3f5401033197d47e7544cedd91b8046c) - change warning message *(commit by [@tobymao](https://github.com/tobymao))*
- [`9eac93e`](https://github.com/tobymao/sqlglot/commit/9eac93e0acd5ae8b034045759fc48937586cbc2e) - upgrade black *(commit by [@tobymao](https://github.com/tobymao))*
- [`4f3fac7`](https://github.com/tobymao/sqlglot/commit/4f3fac7815e0d8206c80f1f255336ab630503d4d) - cleanup command parsing and warnings *(commit by [@tobymao](https://github.com/tobymao))*


## [v20.10.0] - 2024-01-24
### :boom: BREAKING CHANGES
- due to [`1f5fc39`](https://github.com/tobymao/sqlglot/commit/1f5fc39c10b92b94bd94afa5fd038fdb9afeb4b4) - jsonpath parsing *(PR [#2867](https://github.com/tobymao/sqlglot/pull/2867) by [@tobymao](https://github.com/tobymao))*:

  jsonpath parsing (#2867)


### :sparkles: New Features
- [`89b439e`](https://github.com/tobymao/sqlglot/commit/89b439e2f93b6c3bedb4e58fe4b5014d42dd5080) - **postgres**: support the INCLUDE clause in INDEX creation *(PR [#2857](https://github.com/tobymao/sqlglot/pull/2857) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2855](undefined) opened by [@dezhin](https://github.com/dezhin)*
- [`90ffff8`](https://github.com/tobymao/sqlglot/commit/90ffff83266b5714b1371a576d9484dfbe4be155) - **clickhouse**: AggregateFunction data type *(PR [#2832](https://github.com/tobymao/sqlglot/pull/2832) by [@pkit](https://github.com/pkit))*
- [`326d3ae`](https://github.com/tobymao/sqlglot/commit/326d3ae7113cca4a67cca3ab3335f7b8dde91f71) - improve transpilation of Spark's TO_UTC_TIMESTAMP *(PR [#2861](https://github.com/tobymao/sqlglot/pull/2861) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2846](undefined) opened by [@hdemers](https://github.com/hdemers)*
- [`6d03587`](https://github.com/tobymao/sqlglot/commit/6d035871feee2b206d73e37b5f9b8c99fc40708f) - **snowflake**: SHOW SCHEMAS/OBJECTS *(PR [#2845](https://github.com/tobymao/sqlglot/pull/2845) by [@tekumara](https://github.com/tekumara))*
  - :arrow_lower_right: *addresses issue [#2784](undefined) opened by [@tekumara](https://github.com/tekumara)*
- [`d5a08b8`](https://github.com/tobymao/sqlglot/commit/d5a08b8f3a1cdd87e9560c0fe4f11b0f1586978b) - **optimizer**: improve struct type annotation support for EQ-delimited kv pairs *(PR [#2863](https://github.com/tobymao/sqlglot/pull/2863) by [@fool1280](https://github.com/fool1280))*
- [`1f5fc39`](https://github.com/tobymao/sqlglot/commit/1f5fc39c10b92b94bd94afa5fd038fdb9afeb4b4) - jsonpath parsing *(PR [#2867](https://github.com/tobymao/sqlglot/pull/2867) by [@tobymao](https://github.com/tobymao))*
- [`7fd9045`](https://github.com/tobymao/sqlglot/commit/7fd9045488beb88b2726ae906b8769b7963d1b37) - add support for rename column *(PR [#2866](https://github.com/tobymao/sqlglot/pull/2866) by [@gableh](https://github.com/gableh))*
- [`89b781b`](https://github.com/tobymao/sqlglot/commit/89b781b991ce264cd7f8c44fa67860eb9a587b07) - **postgres**: add support for the INHERITS property closes [#2871](https://github.com/tobymao/sqlglot/pull/2871) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`970c202`](https://github.com/tobymao/sqlglot/commit/970c2022a27d4fc355fedb7af830367a5dd96009) - **postgres**: add support for the SET property *(PR [#2873](https://github.com/tobymao/sqlglot/pull/2873) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2872](undefined) opened by [@edongashi](https://github.com/edongashi)*
- [`6845c37`](https://github.com/tobymao/sqlglot/commit/6845c37e749448972a231926236c08affb71a64f) - make the CREATE parser more lenient *(PR [#2875](https://github.com/tobymao/sqlglot/pull/2875) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`bf03a45`](https://github.com/tobymao/sqlglot/commit/bf03a45d8df9abd63b8102e431c13ca0eb0b0fb0) - **snowflake**: extend _parse_range to gracefully handle colon operator *(PR [#2856](https://github.com/tobymao/sqlglot/pull/2856) by [@georgesittas](https://github.com/georgesittas))*
- [`ad14f4e`](https://github.com/tobymao/sqlglot/commit/ad14f4ed1ed3870ae0d7370643c67c235fc89b4b) - qualify alter table table refs in optimizer qualify *(PR [#2862](https://github.com/tobymao/sqlglot/pull/2862) by [@z3z1ma](https://github.com/z3z1ma))*
- [`8599903`](https://github.com/tobymao/sqlglot/commit/859990356210f3091b1b66647c1a674fdb0f2ad9) - **optimizer**: compute external columns for union sopes correctly *(PR [#2864](https://github.com/tobymao/sqlglot/pull/2864) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2860](undefined) opened by [@derekpaulsen](https://github.com/derekpaulsen)*
- [`3e065f9`](https://github.com/tobymao/sqlglot/commit/3e065f9503607e6c620fc187e2bdc2d45f7fa1dd) - **optimizer**: don't copy projection in qualify_outputs when attaching alias *(PR [#2868](https://github.com/tobymao/sqlglot/pull/2868) by [@georgesittas](https://github.com/georgesittas))*
- [`a642758`](https://github.com/tobymao/sqlglot/commit/a6427585e16fa4b5adc9e01cc22baeb09b2f69bb) - avoid dag cycle with unnesting subqueries closes [#2876](https://github.com/tobymao/sqlglot/pull/2876) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0648453`](https://github.com/tobymao/sqlglot/commit/06484532f0e222004e844501a192b8d4aec654c7) - set div type on multiplication closes [#2878](https://github.com/tobymao/sqlglot/pull/2878) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b7fb800`](https://github.com/tobymao/sqlglot/commit/b7fb8006b825ec36d62135877b4e32fccad8d044) - **oracle**: generate with time zone for timestamptz fixes [#2879](https://github.com/tobymao/sqlglot/pull/2879) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`8770e3b`](https://github.com/tobymao/sqlglot/commit/8770e3b7855110a82cb3bc05f3cb6c36a88cfdb2) - **optimizer**: don't qualify CTEs for DDL/DML statements *(PR [#2880](https://github.com/tobymao/sqlglot/pull/2880) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2877](undefined) opened by [@hsheth2](https://github.com/hsheth2)*

### :wrench: Chores
- [`d55cfba`](https://github.com/tobymao/sqlglot/commit/d55cfba7226a61cc8419b3a83eaa0a2ead23be10) - move postgres tests *(commit by [@tobymao](https://github.com/tobymao))*
- [`0e43c58`](https://github.com/tobymao/sqlglot/commit/0e43c58a38bb6af63f653062e79626b85f605b63) - **parser**: warn when parsing (>1 tokens) SQL into exp.Command *(PR [#2874](https://github.com/tobymao/sqlglot/pull/2874) by [@georgesittas](https://github.com/georgesittas))*


## [v20.9.0] - 2024-01-18
### :boom: BREAKING CHANGES
- due to [`1be93e4`](https://github.com/tobymao/sqlglot/commit/1be93e45d8347e5fa8a4e39dad625c6dd66ea461) - properly support all unix time scales *(commit by [@tobymao](https://github.com/tobymao))*:

  properly support all unix time scales


### :sparkles: New Features
- [`816976f`](https://github.com/tobymao/sqlglot/commit/816976f52865fb8ade580c727a890a90378c8e50) - extend submodule annotate_types to handle STRUCT *(PR [#2783](https://github.com/tobymao/sqlglot/pull/2783) by [@fool1280](https://github.com/fool1280))*
- [`7bce2f6`](https://github.com/tobymao/sqlglot/commit/7bce2f6abe79dfd8064c625294d94364042207c5) - **oracle**: add support for ORDER SIBLINGS BY clause *(PR [#2821](https://github.com/tobymao/sqlglot/pull/2821) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2819](undefined) opened by [@Shweta-BI-Lead](https://github.com/Shweta-BI-Lead)*
- [`ce8d254`](https://github.com/tobymao/sqlglot/commit/ce8d254305f56724982eed8e099ab1abeb8750a1) - **snowflake**: parse RM/REMOVE as commands *(PR [#2825](https://github.com/tobymao/sqlglot/pull/2825) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2824](undefined) opened by [@sfc-gh-jlambert](https://github.com/sfc-gh-jlambert)*
- [`1902778`](https://github.com/tobymao/sqlglot/commit/19027786facf8ff730af49c1693149e244502cb0) - add support for multi-unit intervals *(PR [#2822](https://github.com/tobymao/sqlglot/pull/2822) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2793](undefined) opened by [@nikmalviya](https://github.com/nikmalviya)*
- [`a537898`](https://github.com/tobymao/sqlglot/commit/a53789840b56be747fa5d670a9d5ea120aee371a) - add support for DESCRIBE EXTENDED *(PR [#2828](https://github.com/tobymao/sqlglot/pull/2828) by [@georgesittas](https://github.com/georgesittas))*
- [`6e50759`](https://github.com/tobymao/sqlglot/commit/6e50759fb19c8d00825f626fb8c1ab6792fabd56) - **clickhouse**: support Date32 type *(PR [#2830](https://github.com/tobymao/sqlglot/pull/2830) by [@pkit](https://github.com/pkit))*
- [`9560e8f`](https://github.com/tobymao/sqlglot/commit/9560e8fa93d6ac7f4f015bd55091d2fe75e85508) - add support for Heredocs in Databricks Python UDFs *(PR [#2801](https://github.com/tobymao/sqlglot/pull/2801) by [@viethungle-vt1401](https://github.com/viethungle-vt1401))*
- [`52ed590`](https://github.com/tobymao/sqlglot/commit/52ed590b0fa75ef8a9f6e4cb2fb48b4fff65996f) - transpile SELECT .. INTO to dialects that do not support it *(PR [#2820](https://github.com/tobymao/sqlglot/pull/2820) by [@giorgosnikolaou](https://github.com/giorgosnikolaou))*
- [`ea536c4`](https://github.com/tobymao/sqlglot/commit/ea536c4bd7bae0b2916d4bdf9a0ae6a7c5106135) - remove target alias in trino merge *(PR [#2852](https://github.com/tobymao/sqlglot/pull/2852) by [@eakmanrq](https://github.com/eakmanrq))*

### :bug: Bug Fixes
- [`6ddbefc`](https://github.com/tobymao/sqlglot/commit/6ddbefcb08ef933454ff8501ac4a3ea4cba2fe60) - **snowflake**: apply range parser after colon, if any *(PR [#2800](https://github.com/tobymao/sqlglot/pull/2800) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2798](undefined) opened by [@mbarugelCA](https://github.com/mbarugelCA)*
- [`0c4f44e`](https://github.com/tobymao/sqlglot/commit/0c4f44e8027b28613c72285313493c4683c65275) - **oracle**: regexp_replace replacement is optional closes [#2803](https://github.com/tobymao/sqlglot/pull/2803) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5072d5a`](https://github.com/tobymao/sqlglot/commit/5072d5af9a9f629e857071a66228317afd89b1a6) - **oracle**: improve parsing of JSON_OBJECT[AGG] functions *(PR [#2807](https://github.com/tobymao/sqlglot/pull/2807) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2806](undefined) opened by [@Shweta-BI-Lead](https://github.com/Shweta-BI-Lead)*
- [`ea39f10`](https://github.com/tobymao/sqlglot/commit/ea39f10150916f2624cb6efcefb6752154c2f88c) - **optimizer**: pushdown predicates more conservatively to avoid DAG cycles *(PR [#2808](https://github.com/tobymao/sqlglot/pull/2808) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2797](undefined) opened by [@Konntroll](https://github.com/Konntroll)*
- [`ea58003`](https://github.com/tobymao/sqlglot/commit/ea58003caeed17861085825c19a1a5823e065691) - **snowflake**: insert overwrite into closes [#2815](https://github.com/tobymao/sqlglot/pull/2815) *(commit by [@tobymao](https://github.com/tobymao))*
- [`d5fa5be`](https://github.com/tobymao/sqlglot/commit/d5fa5be010a2656ead5524a0d756da6e25ab31dc) - **optimizer**: handle table alias columns for (UN)PIVOTs *(PR [#2816](https://github.com/tobymao/sqlglot/pull/2816) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2791](undefined) opened by [@billstark](https://github.com/billstark)*
- [`24177f3`](https://github.com/tobymao/sqlglot/commit/24177f39b448490618d3b04f8b8ad75ec2069fd3) - unnest star expansion closes [#2811](https://github.com/tobymao/sqlglot/pull/2811) *(PR [#2818](https://github.com/tobymao/sqlglot/pull/2818) by [@tobymao](https://github.com/tobymao))*
- [`3dba6c1`](https://github.com/tobymao/sqlglot/commit/3dba6c194e2fff90f93f0370255a94a1b2a2365a) - add parentheses to no_safe_divide args *(PR [#2826](https://github.com/tobymao/sqlglot/pull/2826) by [@j1ah0ng](https://github.com/j1ah0ng))*
- [`ed68d6b`](https://github.com/tobymao/sqlglot/commit/ed68d6be51e8054eaf8b7bf64048f20c999c6cd2) - **clickhouse**: add ipv4/6 data type parser *(PR [#2829](https://github.com/tobymao/sqlglot/pull/2829) by [@pkit](https://github.com/pkit))*
- [`70b280f`](https://github.com/tobymao/sqlglot/commit/70b280f24df0f4a0d4cc8262d72ce46412f76be3) - allow insert columns to have spaces *(commit by [@tobymao](https://github.com/tobymao))*
- [`57917b8`](https://github.com/tobymao/sqlglot/commit/57917b88ddd9d7d85bc76fc8cb46ffcbf228453d) - **oracle**: TO_TIMESTAMP not parsed as StrToTime *(PR [#2833](https://github.com/tobymao/sqlglot/pull/2833) by [@pkit](https://github.com/pkit))*
  - :arrow_lower_right: *fixes issue [#2831](undefined) opened by [@Rhiyo](https://github.com/Rhiyo)*
- [`9960e11`](https://github.com/tobymao/sqlglot/commit/9960e114818640f26aaa6d911ad3e7ee53df1842) - **optimizer**: annotate struct value without alias correctly *(PR [#2812](https://github.com/tobymao/sqlglot/pull/2812) by [@fool1280](https://github.com/fool1280))*
- [`a6d396b`](https://github.com/tobymao/sqlglot/commit/a6d396b79cfa199a66b04af9ed62bcd7cd619096) - **doris**: add transformation of aggregation function and last_day function *(PR [#2835](https://github.com/tobymao/sqlglot/pull/2835) by [@echo-hhj](https://github.com/echo-hhj))*
- [`8cc252b`](https://github.com/tobymao/sqlglot/commit/8cc252b61b418004bbd6380a8447cb383cf51282) - interval without unit alias closes [#2838](https://github.com/tobymao/sqlglot/pull/2838) *(commit by [@tobymao](https://github.com/tobymao))*
- [`607817f`](https://github.com/tobymao/sqlglot/commit/607817f7e43edefe0a077bfeb81a77dd78e170e5) - schema with period name closes [#2842](https://github.com/tobymao/sqlglot/pull/2842) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1be93e4`](https://github.com/tobymao/sqlglot/commit/1be93e45d8347e5fa8a4e39dad625c6dd66ea461) - properly support all unix time scales *(commit by [@tobymao](https://github.com/tobymao))*
- [`a657fc0`](https://github.com/tobymao/sqlglot/commit/a657fc0ea21aff7452f292fecfcb4bc08ca2e4e9) - **clickhouse,doris**: fix the transformation of ArraySum *(PR [#2843](https://github.com/tobymao/sqlglot/pull/2843) by [@echo-hhj](https://github.com/echo-hhj))*
- [`c92888c`](https://github.com/tobymao/sqlglot/commit/c92888c6b49d2ba60ce789281535679fd93cd235) - **parser**: fix order of query modifier parsing for nested subqueries *(PR [#2851](https://github.com/tobymao/sqlglot/pull/2851) by [@georgesittas](https://github.com/georgesittas))*
- [`7949a4f`](https://github.com/tobymao/sqlglot/commit/7949a4f295fc0a9f0becca9b6460f8517ec733f1) - **clickhouse**: ensure arraySum generation is preserved, add tests *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`17a6ac6`](https://github.com/tobymao/sqlglot/commit/17a6ac6f5fb96a42668842b093a823662b5850b8) - move comment in expr as alias next to the alias *(PR [#2853](https://github.com/tobymao/sqlglot/pull/2853) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`9e5ae50`](https://github.com/tobymao/sqlglot/commit/9e5ae50e02879cfb4915584df90f8dcfadbca321) - use flag instead of regex *(commit by [@tobymao](https://github.com/tobymao))*
- [`5c13a1e`](https://github.com/tobymao/sqlglot/commit/5c13a1e8e2ede284d12920734cea8ff82ebaf054) - simplify merge without target transformation *(PR [#2854](https://github.com/tobymao/sqlglot/pull/2854) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`5996a69`](https://github.com/tobymao/sqlglot/commit/5996a6949979dcfceee133f943a010ec4820e808) - **presto**: get rid of assert in ELEMENT_AT parser *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`f9a02ec`](https://github.com/tobymao/sqlglot/commit/f9a02ecc44f4d0139aae5edda922cc95d4a3e892) - fix table, column instantiation in schema normalization tests *(PR [#2817](https://github.com/tobymao/sqlglot/pull/2817) by [@georgesittas](https://github.com/georgesittas))*


## [v20.8.0] - 2024-01-08
### :boom: BREAKING CHANGES
- due to [`68e1214`](https://github.com/tobymao/sqlglot/commit/68e121462b2c3dc388f3ae0d1d392ee8afc63133) - column field typing *(commit by [@tobymao](https://github.com/tobymao))*:

  column field typing


### :sparkles: New Features
- [`2d822f3`](https://github.com/tobymao/sqlglot/commit/2d822f3972bf0f77baaadb135a5e19c1bc0c4040) - improve support for Doris' TO_DATE, Oracle's SYSDATE *(PR [#2775](https://github.com/tobymao/sqlglot/pull/2775) by [@georgesittas](https://github.com/georgesittas))*
- [`7187215`](https://github.com/tobymao/sqlglot/commit/71872159114b85324d08191b854ab8462a298742) - desc builder *(commit by [@tobymao](https://github.com/tobymao))*
- [`ba62639`](https://github.com/tobymao/sqlglot/commit/ba62639aa6d81a062c867ebe20af64446b931b7d) - add support for CREATE FUNCTION (SQL) characteristics for MySQL and Databricks *(PR [#2777](https://github.com/tobymao/sqlglot/pull/2777) by [@viethungle-vt1401](https://github.com/viethungle-vt1401))*
  - :arrow_lower_right: *addresses issue [#1980](undefined) opened by [@xinglin-zhao](https://github.com/xinglin-zhao)*
- [`963e2dc`](https://github.com/tobymao/sqlglot/commit/963e2dc9a4b699938d0477bc379e9d2da01818af) - **snowflake**: add support for SHOW COLUMNS  *(PR [#2778](https://github.com/tobymao/sqlglot/pull/2778) by [@andrew-sha](https://github.com/andrew-sha))*
- [`46c9733`](https://github.com/tobymao/sqlglot/commit/46c973309850d4e32b1a0f0594d7b143eb14d059) - **tsql**: round func closes [#2790](https://github.com/tobymao/sqlglot/pull/2790) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2dfb7e8`](https://github.com/tobymao/sqlglot/commit/2dfb7e806c52715d9e83d2201ed63974ff238ad3) - **optimizer**: add support for the UNPIVOT operator *(PR [#2771](https://github.com/tobymao/sqlglot/pull/2771) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`68e1214`](https://github.com/tobymao/sqlglot/commit/68e121462b2c3dc388f3ae0d1d392ee8afc63133) - column field typing *(commit by [@tobymao](https://github.com/tobymao))*
- [`3f31706`](https://github.com/tobymao/sqlglot/commit/3f31706b913e53d13e45fe94b41ee115cc7bd5c5) - tsql exec command [#2772](https://github.com/tobymao/sqlglot/pull/2772) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f6cbadb`](https://github.com/tobymao/sqlglot/commit/f6cbadb7a293035720460f869dd1a6d48a707d04) - **snowflake**: add a couple of special fn types *(PR [#2774](https://github.com/tobymao/sqlglot/pull/2774) by [@georgesittas](https://github.com/georgesittas))*
- [`0634f73`](https://github.com/tobymao/sqlglot/commit/0634f738d3a935cde3e7df1671c65e666c7a52b4) - **optimizer**: replace star with outer column list *(PR [#2776](https://github.com/tobymao/sqlglot/pull/2776) by [@georgesittas](https://github.com/georgesittas))*
- [`d31ae0d`](https://github.com/tobymao/sqlglot/commit/d31ae0decb46678851744356c7b113f8c1c3e8c9) - allow string aliases closes [#2788](https://github.com/tobymao/sqlglot/pull/2788) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8f8f00e`](https://github.com/tobymao/sqlglot/commit/8f8f00ec66beb6dc3d90898ead29828eee8f5e32) - don't transform null ordering with positional orders closes [#2779](https://github.com/tobymao/sqlglot/pull/2779) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f85ce3b`](https://github.com/tobymao/sqlglot/commit/f85ce3b354366b2e206e6d2815f34a8e345d10ba) - **tsql**: gracefully handle complex formats in FORMAT *(PR [#2794](https://github.com/tobymao/sqlglot/pull/2794) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2787](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`a2499f5`](https://github.com/tobymao/sqlglot/commit/a2499f591eeb7538db86abd8cc9341c8d91e325d) - **tsql**: generate correct TRIM syntax closes [#2786](https://github.com/tobymao/sqlglot/pull/2786) *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`59ecd2f`](https://github.com/tobymao/sqlglot/commit/59ecd2f17cac61b1ed7d206437d2fab4497e58fa) - **clickhouse**: allow transpilation of countIf, fix 2 arg variant parsing *(PR [#2795](https://github.com/tobymao/sqlglot/pull/2795) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2792](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`3f7748b`](https://github.com/tobymao/sqlglot/commit/3f7748b08d3f6616d8ea7eac6ded4980b6507ddf) - don't transpile nulls last in window specs *(commit by [@tobymao](https://github.com/tobymao))*


## [v20.7.1] - 2024-01-04
### :bug: Bug Fixes
- [`9d94b9c`](https://github.com/tobymao/sqlglot/commit/9d94b9c77f5fbd22ad147789f23e971ec6bb3c72) - don't normalize schema if normalize is set to false *(PR [#2767](https://github.com/tobymao/sqlglot/pull/2767) by [@tobymao](https://github.com/tobymao))*


## [v20.6.0] - 2024-01-04
### :boom: BREAKING CHANGES
- due to [`4648c6a`](https://github.com/tobymao/sqlglot/commit/4648c6acbeab8d5155be3f8e53d11d8f00c33a2e) - set sample clause keyword(s) as class constant to enable transpilation *(PR [#2750](https://github.com/tobymao/sqlglot/pull/2750) by [@georgesittas](https://github.com/georgesittas))*:

  set sample clause keyword(s) as class constant to enable transpilation (#2750)

- due to [`0b6bdc4`](https://github.com/tobymao/sqlglot/commit/0b6bdc4513aa417dae0a00565b56e78b544306e7) - improve transpilation of JSON value extraction *(PR [#2744](https://github.com/tobymao/sqlglot/pull/2744) by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilation of JSON value extraction (#2744)

- due to [`862b305`](https://github.com/tobymao/sqlglot/commit/862b305db47c0cea4a66b61211a3ed088e935ea5) - improve table sampling transpilation *(PR [#2761](https://github.com/tobymao/sqlglot/pull/2761) by [@georgesittas](https://github.com/georgesittas))*:

  improve table sampling transpilation (#2761)


### :sparkles: New Features
- [`202f035`](https://github.com/tobymao/sqlglot/commit/202f035e88e78c15b34c9cc85c4396eab8da1d29) - clickhouse: add aggregate parsing *(PR [#2734](https://github.com/tobymao/sqlglot/pull/2734) by [@pkit](https://github.com/pkit))*
- [`e772e26`](https://github.com/tobymao/sqlglot/commit/e772e262c551f6aa5bb726579253a55db5686da3) - guess the correct dialect in case we get an unknown one *(PR [#2753](https://github.com/tobymao/sqlglot/pull/2753) by [@georgesittas](https://github.com/georgesittas))*
- [`7a07862`](https://github.com/tobymao/sqlglot/commit/7a07862dba744a969c98b2c1069531423673461d) - implement to_s method in Expression for verbose repr mode *(PR [#2756](https://github.com/tobymao/sqlglot/pull/2756) by [@georgesittas](https://github.com/georgesittas))*
- [`4072184`](https://github.com/tobymao/sqlglot/commit/4072184cc498a509bceba2a3dfe12f43794273df) - improve transpilation of TIME/TIMESTAMP_FROM_PARTS *(PR [#2755](https://github.com/tobymao/sqlglot/pull/2755) by [@georgesittas](https://github.com/georgesittas))*
- [`3bd811d`](https://github.com/tobymao/sqlglot/commit/3bd811d67e7001046812f2a8590bd07e23b81c88) - **optimizer**: allow star expansion to be turned off *(PR [#2762](https://github.com/tobymao/sqlglot/pull/2762) by [@georgesittas](https://github.com/georgesittas))*
- [`c246285`](https://github.com/tobymao/sqlglot/commit/c24628531bbd587473e0a43ded7ba8b5e4f35cd8) - bigquery unix_date closes [#2758](https://github.com/tobymao/sqlglot/pull/2758) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a2abbc7`](https://github.com/tobymao/sqlglot/commit/a2abbc773fb330e669c81abc115a81e1055a060f) - improve transpilation of LAST_DAY *(PR [#2766](https://github.com/tobymao/sqlglot/pull/2766) by [@georgesittas](https://github.com/georgesittas))*
- [`7e7ac65`](https://github.com/tobymao/sqlglot/commit/7e7ac65bb67ef5a45ef487859a9cff4f2d0fc07a) - **snowflake**: add support for OBJECT_CONSTRUCT_KEEP_NULL *(PR [#2769](https://github.com/tobymao/sqlglot/pull/2769) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2768](undefined) opened by [@tekumara](https://github.com/tekumara)*

### :bug: Bug Fixes
- [`ed972f9`](https://github.com/tobymao/sqlglot/commit/ed972f9f50fcdc612b7770739a249a918c4d4011) - alter table rename should not qualify with db in postgres *(PR [#2736](https://github.com/tobymao/sqlglot/pull/2736) by [@z3z1ma](https://github.com/z3z1ma))*
- [`1ebfb36`](https://github.com/tobymao/sqlglot/commit/1ebfb3688975e420a70bac10c49ad127446c4c65) - else interval *(commit by [@tobymao](https://github.com/tobymao))*
- [`a43174f`](https://github.com/tobymao/sqlglot/commit/a43174f8ebdb4a51ad12d7dc9c332372a5a0bd84) - generate `CROSS JOIN` instead of comma in `explode_to_unnest` transformation *(PR [#2739](https://github.com/tobymao/sqlglot/pull/2739) by [@cpcloud](https://github.com/cpcloud))*
  - :arrow_lower_right: *fixes issue [#2735](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`e543c55`](https://github.com/tobymao/sqlglot/commit/e543c558a3efea960028bf4c9864cad48e616e82) - interval is null *(commit by [@tobymao](https://github.com/tobymao))*
- [`fb3188f`](https://github.com/tobymao/sqlglot/commit/fb3188f43bfdef2fb315b8b1280aaa207bd7888a) - lineage closes [#2742](https://github.com/tobymao/sqlglot/pull/2742) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b608b2d`](https://github.com/tobymao/sqlglot/commit/b608b2d944934d9da4a2cb373bdf69322f25041a) - **duckdb**: percentile_cont closes [#2741](https://github.com/tobymao/sqlglot/pull/2741) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0b6bdc4`](https://github.com/tobymao/sqlglot/commit/0b6bdc4513aa417dae0a00565b56e78b544306e7) - improve transpilation of JSON value extraction *(PR [#2744](https://github.com/tobymao/sqlglot/pull/2744) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2738](undefined) opened by [@tekumara](https://github.com/tekumara)*
- [`33d6e5f`](https://github.com/tobymao/sqlglot/commit/33d6e5f2636451ffcdd914c100a446246d8031df) - **bigquery**: enable transpilation of single-argument TIME func *(PR [#2752](https://github.com/tobymao/sqlglot/pull/2752) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2749](undefined) opened by [@jherrmannNetfonds](https://github.com/jherrmannNetfonds)*
- [`72f8cfa`](https://github.com/tobymao/sqlglot/commit/72f8cfa2c156efb586bc91d20efd8d0cf9c18735) - **snowflake**: parse two argument version of TIMESTAMP_FROM_PARTS *(PR [#2754](https://github.com/tobymao/sqlglot/pull/2754) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2751](undefined) opened by [@notgerry](https://github.com/notgerry)*
- [`2a94f2b`](https://github.com/tobymao/sqlglot/commit/2a94f2ba0d93ee1454a2d19cc8577e211fd5cbe0) - **bigquery**: fix parsing of COUNTIF *(PR [#2765](https://github.com/tobymao/sqlglot/pull/2765) by [@giovannipcarvalho](https://github.com/giovannipcarvalho))*
  - :arrow_lower_right: *fixes issue [#2764](undefined) opened by [@giovannipcarvalho](https://github.com/giovannipcarvalho)*
- [`862b305`](https://github.com/tobymao/sqlglot/commit/862b305db47c0cea4a66b61211a3ed088e935ea5) - improve table sampling transpilation *(PR [#2761](https://github.com/tobymao/sqlglot/pull/2761) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2757](undefined) opened by [@tekumara](https://github.com/tekumara)*

### :recycle: Refactors
- [`4648c6a`](https://github.com/tobymao/sqlglot/commit/4648c6acbeab8d5155be3f8e53d11d8f00c33a2e) - set sample clause keyword(s) as class constant to enable transpilation *(PR [#2750](https://github.com/tobymao/sqlglot/pull/2750) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2747](undefined) opened by [@tekumara](https://github.com/tekumara)*


## [v20.5.0] - 2023-12-29
### :boom: BREAKING CHANGES
- due to [`87498c3`](https://github.com/tobymao/sqlglot/commit/87498c37496978d9b2d42029fecb0d34bc88eef7) - redshift unnesting to column *(commit by [@tobymao](https://github.com/tobymao))*:

  redshift unnesting to column

- due to [`a4e899a`](https://github.com/tobymao/sqlglot/commit/a4e899aaba317028480a440faeddb026bb464890) - normalize units to all be upper case *(PR [#2714](https://github.com/tobymao/sqlglot/pull/2714) by [@tobymao](https://github.com/tobymao))*:

  normalize units to all be upper case (#2714)

- due to [`92b4ec1`](https://github.com/tobymao/sqlglot/commit/92b4ec1356db2c3de29411807e1f393658ab39c7) - Change Expression.__repr__ format *(PR [#2723](https://github.com/tobymao/sqlglot/pull/2723) by [@barakalon](https://github.com/barakalon))*:

  Change Expression.__repr__ format (#2723)


### :sparkles: New Features
- [`92b4ec1`](https://github.com/tobymao/sqlglot/commit/92b4ec1356db2c3de29411807e1f393658ab39c7) - Change Expression.__repr__ format *(PR [#2723](https://github.com/tobymao/sqlglot/pull/2723) by [@barakalon](https://github.com/barakalon))*

### :bug: Bug Fixes
- [`db9de94`](https://github.com/tobymao/sqlglot/commit/db9de94004b8cb1ee7a10d9dd63d679adff12bc8) - Grant 'contents: write' permissions to the 'deploy' job *(commit by [@izeigerman](https://github.com/izeigerman))*
- [`1abd731`](https://github.com/tobymao/sqlglot/commit/1abd731143beb148d4759f6db721a0c429f3ec1f) - allow final in clickhouse ctes *(commit by [@tobymao](https://github.com/tobymao))*
- [`87498c3`](https://github.com/tobymao/sqlglot/commit/87498c37496978d9b2d42029fecb0d34bc88eef7) - redshift unnesting to column *(commit by [@tobymao](https://github.com/tobymao))*
- [`55735f5`](https://github.com/tobymao/sqlglot/commit/55735f5ec546dcf1b663058a72fbea47c280b487) - **redshift**: auto refresh closes [#2713](https://github.com/tobymao/sqlglot/pull/2713) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b415037`](https://github.com/tobymao/sqlglot/commit/b4150373a22ecabbbe9332c90983c3be94a44137) - **hive**: parse values alias closes [#2717](https://github.com/tobymao/sqlglot/pull/2717) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8979e51`](https://github.com/tobymao/sqlglot/commit/8979e51004962baac7cbc7d7809a22061eb374bd) - **duckdb**: udf no types closes [#2718](https://github.com/tobymao/sqlglot/pull/2718) *(commit by [@tobymao](https://github.com/tobymao))*
- [`2888452`](https://github.com/tobymao/sqlglot/commit/28884523e21bb5b89424700ac3c5b08ae4eecd24) - **snowflake**: timezone_from_parts nanos closes [#2720](https://github.com/tobymao/sqlglot/pull/2720) *(commit by [@tobymao](https://github.com/tobymao))*
- [`4fd6c90`](https://github.com/tobymao/sqlglot/commit/4fd6c90b0a97c8f6c029e3369749d49885182dc2) - predicate pushdown closes [#2721](https://github.com/tobymao/sqlglot/pull/2721) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0ae6f6c`](https://github.com/tobymao/sqlglot/commit/0ae6f6cb728eed0b7b7cfbd52e445a32ba62069f) - **snowflake, duckdb**: generate TimestampDiff correctly *(PR [#2728](https://github.com/tobymao/sqlglot/pull/2728) by [@georgesittas](https://github.com/georgesittas))*
- [`a8582b1`](https://github.com/tobymao/sqlglot/commit/a8582b164928263c5cf4f8415d89831d93c0e684) - include empty lists in the hide_missing repr check *(PR [#2729](https://github.com/tobymao/sqlglot/pull/2729) by [@georgesittas](https://github.com/georgesittas))*
- [`33df497`](https://github.com/tobymao/sqlglot/commit/33df497a038ea43d2ff12da66494185e0577fe7c) - **bigquery**: improve transpilation of structs / brackets *(PR [#2730](https://github.com/tobymao/sqlglot/pull/2730) by [@georgesittas](https://github.com/georgesittas))*
- [`af50c79`](https://github.com/tobymao/sqlglot/commit/af50c79b8e982c005bdace43e202bf5ec282b259) - **duckdb**: named and positional parameters closes [#2732](https://github.com/tobymao/sqlglot/pull/2732) *(commit by [@tobymao](https://github.com/tobymao))*
- [`ea43cec`](https://github.com/tobymao/sqlglot/commit/ea43cecb86cabcb9b09b93a7321c1fa379f7f42a) - **duckdb**: position and named paramaters closes [#2732](https://github.com/tobymao/sqlglot/pull/2732) *(commit by [@tobymao](https://github.com/tobymao))*
- [`c50b948`](https://github.com/tobymao/sqlglot/commit/c50b94823ec4a7a402eccca2ec4d56c7687751e0) - **bigquery**: add FUNCTIONS entry for FORMAT_DATE to facilitate transpilation *(PR [#2733](https://github.com/tobymao/sqlglot/pull/2733) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`a4e899a`](https://github.com/tobymao/sqlglot/commit/a4e899aaba317028480a440faeddb026bb464890) - normalize units to all be upper case *(PR [#2714](https://github.com/tobymao/sqlglot/pull/2714) by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`c628af5`](https://github.com/tobymao/sqlglot/commit/c628af5c653d808f924bc8784989c10253b6c98b) - Update installation steps in README to include sqlglotrs *(PR [#2725](https://github.com/tobymao/sqlglot/pull/2725) by [@izeigerman](https://github.com/izeigerman))*


## [v20.2.0] - 2023-12-14
### :bug: Bug Fixes
- [`1a484b3`](https://github.com/tobymao/sqlglot/commit/1a484b3db939dff5aeeddb5145ce9a24ea3163bd) - **presto**: coerce DATEADD expression to BIGINT *(PR [#2648](https://github.com/tobymao/sqlglot/pull/2648) by [@barakalon](https://github.com/barakalon))*
- [`3cdb81a`](https://github.com/tobymao/sqlglot/commit/3cdb81ac82be73b4b72ebac016bbe892d6337cad) - **duckdb**: transpile bigquery structs w/ aliases correctly *(PR [#2650](https://github.com/tobymao/sqlglot/pull/2650) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2649](undefined) opened by [@nakalamvokis](https://github.com/nakalamvokis)*
- [`49dc0da`](https://github.com/tobymao/sqlglot/commit/49dc0daf0a51f9e4920e27712ff91da90e01a5ce) - **tokenizer**: advance self._start by a character when we encounter CRLF *(PR [#2658](https://github.com/tobymao/sqlglot/pull/2658) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2656](undefined) opened by [@charsmith](https://github.com/charsmith)*
- [`8ee5492`](https://github.com/tobymao/sqlglot/commit/8ee54923ba04e6ec69136b7810d5c87866692a79) - **duckdb**: support the 6 arg. variant of make_timestamp *(PR [#2659](https://github.com/tobymao/sqlglot/pull/2659) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2655](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`9cf71ff`](https://github.com/tobymao/sqlglot/commit/9cf71ffe96288ed347ecf6724d1ef8b92cb77938) - Get rid of the custom text encoding in the tokenizer *(PR [#2660](https://github.com/tobymao/sqlglot/pull/2660) by [@izeigerman](https://github.com/izeigerman))*
- [`462c970`](https://github.com/tobymao/sqlglot/commit/462c970e26c174dbb03a9f50764315bde568e6c4) - make union parsing non-recursive, allowing to parse infinite large unions and lhs binding *(PR [#2662](https://github.com/tobymao/sqlglot/pull/2662) by [@tobymao](https://github.com/tobymao))*
- [`8860521`](https://github.com/tobymao/sqlglot/commit/8860521bb48260fb0ab94f6e831b523b35c40b62) - **optimizer**: handle case where top-level query is a Subquery in scope *(PR [#2661](https://github.com/tobymao/sqlglot/pull/2661) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2654](undefined) opened by [@J000Z](https://github.com/J000Z)*
- [`4617ac3`](https://github.com/tobymao/sqlglot/commit/4617ac39fcd520bd25cb4a03013a354bfc2fc769) - **executor**: respect LIMIT in set operations *(PR [#2665](https://github.com/tobymao/sqlglot/pull/2665) by [@georgesittas](https://github.com/georgesittas))*
- [`a8d4b05`](https://github.com/tobymao/sqlglot/commit/a8d4b0516e9282acdf4791060c4d539e2303cc12) - postgres exec as command closes [#2666](https://github.com/tobymao/sqlglot/pull/2666) *(commit by [@tobymao](https://github.com/tobymao))*
- [`bf6d3e4`](https://github.com/tobymao/sqlglot/commit/bf6d3e4bbf25c47a77a75229143a8bb19d4ec6f5) - **optimizer**: eliminate eliminate_unions *(PR [#2663](https://github.com/tobymao/sqlglot/pull/2663) by [@barakalon](https://github.com/barakalon))*
- [`2ae0deb`](https://github.com/tobymao/sqlglot/commit/2ae0debec0b945b0ece250d8e1e29b072b05602a) - **snowflake**: refactor location paths *(PR [#2668](https://github.com/tobymao/sqlglot/pull/2668) by [@georgesittas](https://github.com/georgesittas))*
- [`9177c6a`](https://github.com/tobymao/sqlglot/commit/9177c6a2b3cfcab3901df7227da333ca965626f6) - **snowflake**: time travel syntax can be used in queries as well *(PR [#2674](https://github.com/tobymao/sqlglot/pull/2674) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2670](undefined) opened by [@sfc-gh-jlambert](https://github.com/sfc-gh-jlambert)*
- [`7188431`](https://github.com/tobymao/sqlglot/commit/7188431dab000fceac55f6852b76a7da4394df65) - ignore unnesting non subqueries closes [#2676](https://github.com/tobymao/sqlglot/pull/2676) *(commit by [@tobymao](https://github.com/tobymao))*
- [`9e7112b`](https://github.com/tobymao/sqlglot/commit/9e7112b67f90cc269982c5b7e29ea296d1b19d9f) - create .venv when publishing so that maturin doesn't crash  *(PR [#2675](https://github.com/tobymao/sqlglot/pull/2675) by [@georgesittas](https://github.com/georgesittas))*

### :recycle: Refactors
- [`2383003`](https://github.com/tobymao/sqlglot/commit/238300381b53c232a7ad0fd9e5b8b2ceaf563f08) - add is_connected helper to see if tokens have no white space *(commit by [@tobymao](https://github.com/tobymao))*


## [v20.1.0] - 2023-12-07
### :boom: BREAKING CHANGES
- due to [`96f9b0e`](https://github.com/tobymao/sqlglot/commit/96f9b0e096d5b4505f92c70d8a3733b141205c1b) - make generation of CONCAT less verbose *(PR [#2639](https://github.com/tobymao/sqlglot/pull/2639) by [@georgesittas](https://github.com/georgesittas))*:

  make generation of CONCAT less verbose (#2639)


### :sparkles: New Features
- [`7a505f0`](https://github.com/tobymao/sqlglot/commit/7a505f04f4e0ed72b6f8f3ea6560d613dbc40726) - **clickhouse**: add support for arrayJoin *(PR [#2640](https://github.com/tobymao/sqlglot/pull/2640) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2638](undefined) opened by [@pkit](https://github.com/pkit)*
- [`2062553`](https://github.com/tobymao/sqlglot/commit/20625539c62bba4b9ece401086182f4ae0d1be4d) - add comments to replace table *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`4f07aaa`](https://github.com/tobymao/sqlglot/commit/4f07aaaefa92b7bc2ca0b1c56b442e257c67dd8b) - from_utc double cast *(commit by [@tobymao](https://github.com/tobymao))*
- [`96f9b0e`](https://github.com/tobymao/sqlglot/commit/96f9b0e096d5b4505f92c70d8a3733b141205c1b) - make generation of CONCAT less verbose *(PR [#2639](https://github.com/tobymao/sqlglot/pull/2639) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#91](undefined) opened by [@vegarsti](https://github.com/vegarsti)*
  - :arrow_lower_right: *fixes issue [#1815](undefined) opened by [@jonaswvd](https://github.com/jonaswvd)*
- [`1a231f7`](https://github.com/tobymao/sqlglot/commit/1a231f7d83da43b66517359f0c13883f829dd2d5) - avoid recursive set op calls *(commit by [@tobymao](https://github.com/tobymao))*


## [v20.0.0] - 2023-12-07
### :boom: BREAKING CHANGES
- due to [`be89da3`](https://github.com/tobymao/sqlglot/commit/be89da3747fa95d98e0d5a28d19d98f5822a8979) - introduce Dialect settings, make MySQL case-sensitive by default *(PR [#2627](https://github.com/tobymao/sqlglot/pull/2627) by [@georgesittas](https://github.com/georgesittas))*:

  introduce Dialect settings, make MySQL case-sensitive by default (#2627)

- due to [`4d68e39`](https://github.com/tobymao/sqlglot/commit/4d68e39a3aadc9b07d3f96c51fc46fd407486d86) - remove redundant todate closes [#2636](https://github.com/tobymao/sqlglot/pull/2636) *(commit by [@tobymao](https://github.com/tobymao))*:

  remove redundant todate closes #2636

- due to [`1e387f6`](https://github.com/tobymao/sqlglot/commit/1e387f63853efbcc4da7ce8d822fe8975ffe52f3) - parse functions with positional args in exp.func *(PR [#2622](https://github.com/tobymao/sqlglot/pull/2622) by [@georgesittas](https://github.com/georgesittas))*:

  parse functions with positional args in exp.func (#2622)

- due to [`ee2e7f0`](https://github.com/tobymao/sqlglot/commit/ee2e7f099b46d2c8548ab34784d19b77fe838ce7) - snowflake column transform constraints closes [#2634](https://github.com/tobymao/sqlglot/pull/2634) *(commit by [@tobymao](https://github.com/tobymao))*:

  snowflake column transform constraints closes #2634

- due to [`656d54c`](https://github.com/tobymao/sqlglot/commit/656d54c808d65a2ec1443719e3f35dc651ed98ab) - make lineage html more reusable *(commit by [@tobymao](https://github.com/tobymao))*:

  make lineage html more reusable


### :sparkles: New Features
- [`be89da3`](https://github.com/tobymao/sqlglot/commit/be89da3747fa95d98e0d5a28d19d98f5822a8979) - introduce Dialect settings, make MySQL case-sensitive by default *(PR [#2627](https://github.com/tobymao/sqlglot/pull/2627) by [@georgesittas](https://github.com/georgesittas))*
- [`ee2e7f0`](https://github.com/tobymao/sqlglot/commit/ee2e7f099b46d2c8548ab34784d19b77fe838ce7) - snowflake column transform constraints closes [#2634](https://github.com/tobymao/sqlglot/pull/2634) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`b0c5375`](https://github.com/tobymao/sqlglot/commit/b0c5375be98ac7e74870ea69d58c211cf9c73dea) - **tsql**: add dw, hour to the DATEPART-only formats *(PR [#2632](https://github.com/tobymao/sqlglot/pull/2632) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2630](undefined) opened by [@abadugu13](https://github.com/abadugu13)*
- [`4d68e39`](https://github.com/tobymao/sqlglot/commit/4d68e39a3aadc9b07d3f96c51fc46fd407486d86) - remove redundant todate closes [#2636](https://github.com/tobymao/sqlglot/pull/2636) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1e387f6`](https://github.com/tobymao/sqlglot/commit/1e387f63853efbcc4da7ce8d822fe8975ffe52f3) - parse functions with positional args in exp.func *(PR [#2622](https://github.com/tobymao/sqlglot/pull/2622) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2621](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
  - :arrow_lower_right: *fixes issue [#2631](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`78697b4`](https://github.com/tobymao/sqlglot/commit/78697b48d9cf310b9098aa48c5180acf279d9945) - **optimizer**: simplify Sub/Div more conservatively, they're not associative *(PR [#2635](https://github.com/tobymao/sqlglot/pull/2635) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2633](undefined) opened by [@jamespan](https://github.com/jamespan)*

### :recycle: Refactors
- [`656d54c`](https://github.com/tobymao/sqlglot/commit/656d54c808d65a2ec1443719e3f35dc651ed98ab) - make lineage html more reusable *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`12a00e9`](https://github.com/tobymao/sqlglot/commit/12a00e985dacf508f8268b2bb209156f748e197c) - make normalization strategy str enum *(commit by [@tobymao](https://github.com/tobymao))*


## [v19.9.0] - 2023-12-05
### :boom: BREAKING CHANGES
- due to [`6e71c34`](https://github.com/tobymao/sqlglot/commit/6e71c348eab63125e412382381acbcbd79efac43) - remove safe versions and use a flag instead *(PR [#2629](https://github.com/tobymao/sqlglot/pull/2629) by [@tobymao](https://github.com/tobymao))*:

  remove safe versions and use a flag instead (#2629)


### :bug: Bug Fixes
- [`4755293`](https://github.com/tobymao/sqlglot/commit/4755293dba1a82df7d6a520056ecb81c6a5117ea) - attach function comments to the AST *(PR [#2628](https://github.com/tobymao/sqlglot/pull/2628) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2626](undefined) opened by [@aersam](https://github.com/aersam)*
- [`160f06d`](https://github.com/tobymao/sqlglot/commit/160f06dc902cc9c666093c47a811d7bd18c91a30) - **snowflake**: allow rename/replace identifier *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`6e71c34`](https://github.com/tobymao/sqlglot/commit/6e71c348eab63125e412382381acbcbd79efac43) - remove safe versions and use a flag instead *(PR [#2629](https://github.com/tobymao/sqlglot/pull/2629) by [@tobymao](https://github.com/tobymao))*


## [v19.8.3] - 2023-12-04
### :sparkles: New Features
- [`05b41e8`](https://github.com/tobymao/sqlglot/commit/05b41e81639eabe6f2c4116fb7c14ea99da8b655) - add identify to table_name *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`6351007`](https://github.com/tobymao/sqlglot/commit/6351007e36bf7af9dfdb0b797bed4834fa394de1) - tokenize CRLF sequence correctly *(PR [#2623](https://github.com/tobymao/sqlglot/pull/2623) by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`f9a43a1`](https://github.com/tobymao/sqlglot/commit/f9a43a176fcd6ab952bfd8bca21473d541423156) - don't patch loggers at method level to silence warnings *(PR [#2620](https://github.com/tobymao/sqlglot/pull/2620) by [@georgesittas](https://github.com/georgesittas))*


## [v19.8.2] - 2023-12-01
### :bug: Bug Fixes
- [`5657a60`](https://github.com/tobymao/sqlglot/commit/5657a60169680c45feb67fe7a1da16b4c9ee22b6) - **tsql, teradata**: Distinct goes before top *(PR [#2618](https://github.com/tobymao/sqlglot/pull/2618) by [@treysp](https://github.com/treysp))*
- [`c0e751a`](https://github.com/tobymao/sqlglot/commit/c0e751a71cd69746b7acb5f1950f1b925c826373) - **duckdb**: arrays are 1-indexed *(PR [#2619](https://github.com/tobymao/sqlglot/pull/2619) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2617](undefined) opened by [@j1ah0ng](https://github.com/j1ah0ng)*


## [v19.8.1] - 2023-12-01
### :bug: Bug Fixes
- [`441f624`](https://github.com/tobymao/sqlglot/commit/441f624c5cafb49e3d73d4435b7cf4bf3e891150) - cannot have union with limit *(commit by [@tobymao](https://github.com/tobymao))*


## [v19.8.0] - 2023-12-01
### :boom: BREAKING CHANGES
- due to [`b5f690b`](https://github.com/tobymao/sqlglot/commit/b5f690bc36e2278ec9d9299041497485f73198a8) - add timestamp functions to BQ and DuckDB closes [#2611](https://github.com/tobymao/sqlglot/pull/2611) *(PR [#2612](https://github.com/tobymao/sqlglot/pull/2612) by [@j1ah0ng](https://github.com/j1ah0ng))*:

  add timestamp functions to BQ and DuckDB closes #2611 (#2612)

- due to [`019e0e5`](https://github.com/tobymao/sqlglot/commit/019e0e5ba4b5df1ef3b34510c2fa07f8623af364) - qualify columns added in explode to unnest transformation *(PR [#2615](https://github.com/tobymao/sqlglot/pull/2615) by [@georgesittas](https://github.com/georgesittas))*:

  qualify columns added in explode to unnest transformation (#2615)


### :sparkles: New Features
- [`5af7ac3`](https://github.com/tobymao/sqlglot/commit/5af7ac359efc7d2575eb2cdfd0fe34b9518805c2) - helper method for dot parts *(commit by [@tobymao](https://github.com/tobymao))*
- [`da0a4b1`](https://github.com/tobymao/sqlglot/commit/da0a4b1cc3d093012e4a92a9bb6f70c7db11749c) - **postgres**: add support for operators with schema path *(PR [#2610](https://github.com/tobymao/sqlglot/pull/2610) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2609](undefined) opened by [@ninja96826](https://github.com/ninja96826)*

### :bug: Bug Fixes
- [`568ddd1`](https://github.com/tobymao/sqlglot/commit/568ddd12d98142146073e27e206426a66b73eb42) - treat parameters as primary expressions *(PR [#2605](https://github.com/tobymao/sqlglot/pull/2605) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2604](undefined) opened by [@bruderooo](https://github.com/bruderooo)*
- [`526d760`](https://github.com/tobymao/sqlglot/commit/526d7602018268f8dc6e8111b26de4df8601d0cf) - revert 568ddd12, parse placeholder in sample instead *(PR [#2606](https://github.com/tobymao/sqlglot/pull/2606) by [@georgesittas](https://github.com/georgesittas))*
- [`bb75218`](https://github.com/tobymao/sqlglot/commit/bb7521820d6d22a669bfb8cbbc0ee8a37d32c361) - always expand sentinel line break in pretty mode *(PR [#2608](https://github.com/tobymao/sqlglot/pull/2608) by [@georgesittas](https://github.com/georgesittas))*
- [`9106702`](https://github.com/tobymao/sqlglot/commit/9106702decda3957ef0f88f4a4d88b5a14a55a8c) - properly normalize and parse schema for replace_tables and expand *(commit by [@tobymao](https://github.com/tobymao))*
- [`d47879f`](https://github.com/tobymao/sqlglot/commit/d47879f049914ec94df8b09cd5a60e8ad64b2f59) - **snowflake**: unnest sql doesn't need subquery *(commit by [@tobymao](https://github.com/tobymao))*
- [`b5f690b`](https://github.com/tobymao/sqlglot/commit/b5f690bc36e2278ec9d9299041497485f73198a8) - add timestamp functions to BQ and DuckDB closes [#2611](https://github.com/tobymao/sqlglot/pull/2611) *(PR [#2612](https://github.com/tobymao/sqlglot/pull/2612) by [@j1ah0ng](https://github.com/j1ah0ng))*
- [`5aa134d`](https://github.com/tobymao/sqlglot/commit/5aa134d8489cea96d8fa891aaa26e68983ee7537) - preserve alias quotes in explode_to_unnest *(PR [#2613](https://github.com/tobymao/sqlglot/pull/2613) by [@georgesittas](https://github.com/georgesittas))*
- [`5509e31`](https://github.com/tobymao/sqlglot/commit/5509e31cde2e008e6afbe352d4700acb1d6b9c25) - generate UnixToTime correctly (spark, bq, presto, snowflake, duckdb) *(PR [#2614](https://github.com/tobymao/sqlglot/pull/2614) by [@georgesittas](https://github.com/georgesittas))*
- [`019e0e5`](https://github.com/tobymao/sqlglot/commit/019e0e5ba4b5df1ef3b34510c2fa07f8623af364) - qualify columns added in explode to unnest transformation *(PR [#2615](https://github.com/tobymao/sqlglot/pull/2615) by [@georgesittas](https://github.com/georgesittas))*
- [`ad9fe11`](https://github.com/tobymao/sqlglot/commit/ad9fe1156c55d6aa278a43bc979da216c2a1a7d9) - **snowflake**: snowflake array_contains closes [#2616](https://github.com/tobymao/sqlglot/pull/2616) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`4ec01d3`](https://github.com/tobymao/sqlglot/commit/4ec01d398535738a55c15202a5a88bae3f9a86dc) - cleanup types *(commit by [@tobymao](https://github.com/tobymao))*


## [v19.7.0] - 2023-11-28
### :boom: BREAKING CHANGES
- due to [`8cd7d1c`](https://github.com/tobymao/sqlglot/commit/8cd7d1c0bb56aff6bcd08a3ae4e71f68022307b8) - use more canonical cast instead of to_date *(commit by [@tobymao](https://github.com/tobymao))*:

  use more canonical cast instead of to_date

- due to [`c413b7f`](https://github.com/tobymao/sqlglot/commit/c413b7fa56c69da3297eb575de7600316b491f18) - expand positional args in order by as aliases *(PR [#2599](https://github.com/tobymao/sqlglot/pull/2599) by [@tobymao](https://github.com/tobymao))*:

  expand positional args in order by as aliases (#2599)

- due to [`13817f1`](https://github.com/tobymao/sqlglot/commit/13817f187a79bcc559f7e6939729fce8fecbd812) - avoid unnecessary copying in normalization *(PR [#2602](https://github.com/tobymao/sqlglot/pull/2602) by [@tobymao](https://github.com/tobymao))*:

  avoid unnecessary copying in normalization (#2602)


### :sparkles: New Features
- [`739c3c7`](https://github.com/tobymao/sqlglot/commit/739c3c7e4ab1c5986323f2ff6e0ad24bfab8fa0a) - insert returning builder closes [#2579](https://github.com/tobymao/sqlglot/pull/2579) *(commit by [@tobymao](https://github.com/tobymao))*
- [`6e3c7c1`](https://github.com/tobymao/sqlglot/commit/6e3c7c197e8ca2dd48af389e12951ddd22313430) - **tsql**: default database .. closes [#2594](https://github.com/tobymao/sqlglot/pull/2594) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`8cd7d1c`](https://github.com/tobymao/sqlglot/commit/8cd7d1c0bb56aff6bcd08a3ae4e71f68022307b8) - use more canonical cast instead of to_date *(commit by [@tobymao](https://github.com/tobymao))*
- [`08d60b6`](https://github.com/tobymao/sqlglot/commit/08d60b6ef414515920666b56ee8fadb386df0022) - **tsql**: add special chars in single var tokens *(PR [#2582](https://github.com/tobymao/sqlglot/pull/2582) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2581](undefined) opened by [@Hal-H2Apps](https://github.com/Hal-H2Apps)*
- [`bf29a9b`](https://github.com/tobymao/sqlglot/commit/bf29a9b04152672d261649f392316cdaa39ef1e2) - handle ending spaces after keywords closes [#2585](https://github.com/tobymao/sqlglot/pull/2585) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f53f656`](https://github.com/tobymao/sqlglot/commit/f53f6565330cf594c07977b4f1d169220ae8fe19) - **optimizer**: respect EXCEPT when expanding star for PIVOTs *(PR [#2589](https://github.com/tobymao/sqlglot/pull/2589) by [@georgesittas](https://github.com/georgesittas))*
- [`426075f`](https://github.com/tobymao/sqlglot/commit/426075fe17b60b419f38a8ef5735977b953b92af) - **duckdb**: unqualify columns under Pivot *(PR [#2590](https://github.com/tobymao/sqlglot/pull/2590) by [@georgesittas](https://github.com/georgesittas))*
- [`4774431`](https://github.com/tobymao/sqlglot/commit/4774431e7f9cf8146d0e4721d8c49957696c7727) - **tsql**: generate DATEPART when the format is quarter *(PR [#2591](https://github.com/tobymao/sqlglot/pull/2591) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2586](undefined) opened by [@abadugu13](https://github.com/abadugu13)*
- [`dc783a8`](https://github.com/tobymao/sqlglot/commit/dc783a8b723ca000e6ff2343675f4f0030716037) - **bigquery**: generate FORMAT_DATE for TimeToStr *(PR [#2596](https://github.com/tobymao/sqlglot/pull/2596) by [@georgesittas](https://github.com/georgesittas))*
- [`2ecfd34`](https://github.com/tobymao/sqlglot/commit/2ecfd34628853711dc30e86b3888a88a2d0869a0) - time format chunk misses from mapping, but its constituent parts are not *(PR [#2598](https://github.com/tobymao/sqlglot/pull/2598) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2597](undefined) opened by [@j-bennet](https://github.com/j-bennet)*
- [`c413b7f`](https://github.com/tobymao/sqlglot/commit/c413b7fa56c69da3297eb575de7600316b491f18) - expand positional args in order by as aliases *(PR [#2599](https://github.com/tobymao/sqlglot/pull/2599) by [@tobymao](https://github.com/tobymao))*
- [`c66e413`](https://github.com/tobymao/sqlglot/commit/c66e413c916c15d8e485d42055a9214db936c56c) - **oracle**: to_char nlsparam closes [#2601](https://github.com/tobymao/sqlglot/pull/2601) *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`13817f1`](https://github.com/tobymao/sqlglot/commit/13817f187a79bcc559f7e6939729fce8fecbd812) - avoid unnecessary copying in normalization *(PR [#2602](https://github.com/tobymao/sqlglot/pull/2602) by [@tobymao](https://github.com/tobymao))*


## [v19.6.0] - 2023-11-20
### :bug: Bug Fixes
- [`7647227`](https://github.com/tobymao/sqlglot/commit/76472275018a069abc7079bc52cac94fb8d2d348) - **oracle**: parse DROP CONSTRAINT into DROP instead of Command *(PR [#2573](https://github.com/tobymao/sqlglot/pull/2573) by [@HassanShafiq123](https://github.com/HassanShafiq123))*
  - :arrow_lower_right: *fixes issue [#2572](undefined) opened by [@HassanShafiq123](https://github.com/HassanShafiq123)*
- [`f5899a1`](https://github.com/tobymao/sqlglot/commit/f5899a1a0da096e012b7abd0627a372e4202a612) - **bigquery**: bigquery only allows literals in LIMIT *(PR [#2574](https://github.com/tobymao/sqlglot/pull/2574) by [@treysp](https://github.com/treysp))*
- [`757c433`](https://github.com/tobymao/sqlglot/commit/757c433944d6afd942b74edc84afb07b140e1a1c) - treat := as PropertyEQ in base sqlglot classes *(PR [#2576](https://github.com/tobymao/sqlglot/pull/2576) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2575](undefined) opened by [@j-bennet](https://github.com/j-bennet)*

### :wrench: Chores
- [`1e16001`](https://github.com/tobymao/sqlglot/commit/1e160012aecfb754294e2d6e207610741bdd264a) - cleanup tests *(commit by [@tobymao](https://github.com/tobymao))*


## [v19.5.1] - 2023-11-16
### :bug: Bug Fixes
- [`932b610`](https://github.com/tobymao/sqlglot/commit/932b610bae7996b488a0406db76f308ddbfd5e44) - interval simplification with non literal *(commit by [@tobymao](https://github.com/tobymao))*


## [v19.5.0] - 2023-11-16
### :boom: BREAKING CHANGES
- due to [`96d514c`](https://github.com/tobymao/sqlglot/commit/96d514c52e9f467d0fac196cee3c7ba3441ace5b) - get rid of SetAgg to use ArrayUniqueAgg for consistency *(PR [#2566](https://github.com/tobymao/sqlglot/pull/2566) by [@georgesittas](https://github.com/georgesittas))*:

  get rid of SetAgg to use ArrayUniqueAgg for consistency (#2566)


### :sparkles: New Features
- [`3766686`](https://github.com/tobymao/sqlglot/commit/3766686999cb7f80cdca46150d7e74f954d84cbd) - **executor**: add support for array_unique_agg *(PR [#2564](https://github.com/tobymao/sqlglot/pull/2564) by [@wezham](https://github.com/wezham))*
- [`53b3677`](https://github.com/tobymao/sqlglot/commit/53b3677c919811424f3d0ed5f21aa9c0e76452f5) - **executor**: add support for null replacement value in ARRAY_JOIN *(PR [#2569](https://github.com/tobymao/sqlglot/pull/2569) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`ac79a59`](https://github.com/tobymao/sqlglot/commit/ac79a59a0b2a6143689427a4c93a6548b19ef5f8) - tokenizer should not reuse list in initializer *(commit by [@tobymao](https://github.com/tobymao))*
- [`2029896`](https://github.com/tobymao/sqlglot/commit/2029896d672b021d9fb471162c1933bfdd2c10d1) - **snowflake**: Snowflake only supports literals in LIMIT *(PR [#2568](https://github.com/tobymao/sqlglot/pull/2568) by [@treysp](https://github.com/treysp))*

### :recycle: Refactors
- [`96d514c`](https://github.com/tobymao/sqlglot/commit/96d514c52e9f467d0fac196cee3c7ba3441ace5b) - get rid of SetAgg to use ArrayUniqueAgg for consistency *(PR [#2566](https://github.com/tobymao/sqlglot/pull/2566) by [@georgesittas](https://github.com/georgesittas))*
- [`c4da9fc`](https://github.com/tobymao/sqlglot/commit/c4da9fc9b657a90791d4189d551d5963742f6188) - tokenizer performance improvements *(commit by [@tobymao](https://github.com/tobymao))*


## [v19.4.0] - 2023-11-14
### :boom: BREAKING CHANGES
- due to [`5034d92`](https://github.com/tobymao/sqlglot/commit/5034d92d63aaf4d6861a7c6bf2c041c60fb8043c) - transpile NULLS FIRST/LAST to dialects that dont support it *(PR [#2554](https://github.com/tobymao/sqlglot/pull/2554) by [@georgesittas](https://github.com/georgesittas))*:

  transpile NULLS FIRST/LAST to dialects that dont support it (#2554)


### :sparkles: New Features
- [`5034d92`](https://github.com/tobymao/sqlglot/commit/5034d92d63aaf4d6861a7c6bf2c041c60fb8043c) - transpile NULLS FIRST/LAST to dialects that dont support it *(PR [#2554](https://github.com/tobymao/sqlglot/pull/2554) by [@georgesittas](https://github.com/georgesittas))*
- [`a6bdff9`](https://github.com/tobymao/sqlglot/commit/a6bdff945860f758222df4b0b6051542522fd697) - **snowflake**: improve TRY_CAST -> CAST transpilation *(PR [#2561](https://github.com/tobymao/sqlglot/pull/2561) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`9d345e5`](https://github.com/tobymao/sqlglot/commit/9d345e586354cf40e973e0980535f9a0e5698f3e) - **spark**: string(n) -> varchar(n) closes [#2552](https://github.com/tobymao/sqlglot/pull/2552) *(commit by [@tobymao](https://github.com/tobymao))*
- [`e5e6d92`](https://github.com/tobymao/sqlglot/commit/e5e6d9242de733c95da5a179a2a5acf824f47476) - **optimizer**: dont cast right side of IS *(PR [#2559](https://github.com/tobymao/sqlglot/pull/2559) by [@barakalon](https://github.com/barakalon))*
- [`18793b0`](https://github.com/tobymao/sqlglot/commit/18793b05be1ffbe9e42bfd1e274479d122067880) - **snowflake**: generate SHA1 for exp.SHA *(PR [#2557](https://github.com/tobymao/sqlglot/pull/2557) by [@georgesittas](https://github.com/georgesittas))*
- [`8cfb39e`](https://github.com/tobymao/sqlglot/commit/8cfb39eeccfec3a246d1e28c1922d7f605ec53f5) - **clickhouse**: scalar ctes second try *(commit by [@tobymao](https://github.com/tobymao))*
- [`0f9912f`](https://github.com/tobymao/sqlglot/commit/0f9912f03fb5ae99bfa9abfeb44a6e975d88600a) - **snowflake**: only generate TRY_CAST if cast value is of text type *(PR [#2560](https://github.com/tobymao/sqlglot/pull/2560) by [@georgesittas](https://github.com/georgesittas))*


## [v19.3.1] - 2023-11-10
### :bug: Bug Fixes
- [`1d557a7`](https://github.com/tobymao/sqlglot/commit/1d557a76496fd73552782a3f48d70253060faf77) - **tsql**: only call subquery method in CTAS if it's not one already *(PR [#2553](https://github.com/tobymao/sqlglot/pull/2553) by [@georgesittas](https://github.com/georgesittas))*


## [v19.3.0] - 2023-11-10
### :sparkles: New Features
- [`1136f86`](https://github.com/tobymao/sqlglot/commit/1136f866ff491f265e7a4fd75d8e1efe3c300b33) - add exp.IsInf expression to represent ISINF, IS_INF *(PR [#2548](https://github.com/tobymao/sqlglot/pull/2548) by [@j1ah0ng](https://github.com/j1ah0ng))*

### :bug: Bug Fixes
- [`d92f2be`](https://github.com/tobymao/sqlglot/commit/d92f2beeeddf1f8e752e412e89212865e13e2128) - don't bubble up CTEs for the CREATE DDL statement *(PR [#2550](https://github.com/tobymao/sqlglot/pull/2550) by [@georgesittas](https://github.com/georgesittas))*
- [`39ef0e1`](https://github.com/tobymao/sqlglot/commit/39ef0e131670c835a38619d3a6eb4be7a0680881) - **tsql**: preserve column projection quotes for newly added Alias nodes *(PR [#2551](https://github.com/tobymao/sqlglot/pull/2551) by [@georgesittas](https://github.com/georgesittas))*


## [v19.2.0] - 2023-11-10
### :boom: BREAKING CHANGES
- due to [`9f42b6b`](https://github.com/tobymao/sqlglot/commit/9f42b6b0b49a7ef6ce7147347fbb136b51d2e8a1) - disallow nested CTEs for Spark and Databricks *(PR [#2544](https://github.com/tobymao/sqlglot/pull/2544) by [@georgesittas](https://github.com/georgesittas))*:

  disallow nested CTEs for Spark and Databricks (#2544)


### :sparkles: New Features
- [`91483b0`](https://github.com/tobymao/sqlglot/commit/91483b0daef2aec4a076b6d4214983d14bf9f105) - add / move fixed-width integer tokens to base class *(PR [#2540](https://github.com/tobymao/sqlglot/pull/2540) by [@j1ah0ng](https://github.com/j1ah0ng))*
- [`45334eb`](https://github.com/tobymao/sqlglot/commit/45334eb226199b8e48ab2751567f65e8675f4ff5) - **bigquery**: array contains to exist unnest closes [#2547](https://github.com/tobymao/sqlglot/pull/2547) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`9f42b6b`](https://github.com/tobymao/sqlglot/commit/9f42b6b0b49a7ef6ce7147347fbb136b51d2e8a1) - disallow nested CTEs for Spark and Databricks *(PR [#2544](https://github.com/tobymao/sqlglot/pull/2544) by [@georgesittas](https://github.com/georgesittas))*


## [v19.1.3] - 2023-11-09
### :sparkles: New Features
- [`d9d64e0`](https://github.com/tobymao/sqlglot/commit/d9d64e0d82c4c89dca132c4d16c0024fefd7aeda) - **postgres**: parse CREATE CONSTRAINT TRIGGER as Command *(PR [#2541](https://github.com/tobymao/sqlglot/pull/2541) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2538](undefined) opened by [@sweigert](https://github.com/sweigert)*
- [`ea41ddc`](https://github.com/tobymao/sqlglot/commit/ea41ddc69ff05aaf9c54bd0b5a1bea8ffbbf1c5b) - **optimizer**: simplify DATE_ADD on literals *(PR [#2537](https://github.com/tobymao/sqlglot/pull/2537) by [@barakalon](https://github.com/barakalon))*
- [`73746ed`](https://github.com/tobymao/sqlglot/commit/73746edf5c34851a5ab27569cf16885fa121b1db) - more robust boolean conversions for tsql *(PR [#2543](https://github.com/tobymao/sqlglot/pull/2543) by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`9922232`](https://github.com/tobymao/sqlglot/commit/9922232e4630fea627c882e432c220a18201c0e4) - presto -> spark to_json closes [#2536](https://github.com/tobymao/sqlglot/pull/2536) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1c6d348`](https://github.com/tobymao/sqlglot/commit/1c6d3484ee97c65462e86ef93463a72f5392ae47) - **parser**: take TokenType.RAW_STRING into account in _parse_string *(PR [#2542](https://github.com/tobymao/sqlglot/pull/2542) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2539](undefined) opened by [@braunreyes](https://github.com/braunreyes)*
- [`b5a477f`](https://github.com/tobymao/sqlglot/commit/b5a477f93128ddd816f7f59ca923c07719ae7805) - tsql top paren term parsing *(commit by [@tobymao](https://github.com/tobymao))*


## [v19.1.2] - 2023-11-09
### :sparkles: New Features
- [`7de4922`](https://github.com/tobymao/sqlglot/commit/7de4922e3d111c66d1f6bf25efb1640e41a09383) - **hive**: add fine-grained parsing for REFRESH *(PR [#2531](https://github.com/tobymao/sqlglot/pull/2531) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2530](undefined) opened by [@juliands-stripe](https://github.com/juliands-stripe)*

### :bug: Bug Fixes
- [`8abf1d7`](https://github.com/tobymao/sqlglot/commit/8abf1d77589af1d7cae7c47ee82ca14833b9966a) - **snowflake**: avoid advancing beyond array limit when parsing staged files *(PR [#2529](https://github.com/tobymao/sqlglot/pull/2529) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2528](undefined) opened by [@nsenno-dbr](https://github.com/nsenno-dbr)*
- [`151f14b`](https://github.com/tobymao/sqlglot/commit/151f14ba66a5f3f37d8159450df5813f82fa4427) - transpile Snowflake structs correctly *(PR [#2534](https://github.com/tobymao/sqlglot/pull/2534) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2533](undefined) opened by [@nsenno-dbr](https://github.com/nsenno-dbr)*
- [`5c750f3`](https://github.com/tobymao/sqlglot/commit/5c750f30896c665b7ea80db0c671af723b39cc93) - **tsql**: convert boolean columns into explicit conditions *(PR [#2535](https://github.com/tobymao/sqlglot/pull/2535) by [@georgesittas](https://github.com/georgesittas))*


## [v19.1.1] - 2023-11-08
### :bug: Bug Fixes
- [`ff69304`](https://github.com/tobymao/sqlglot/commit/ff693048669170b090ad7ab2e6fd76f06b057c2f) - snowflake->spark sample transpilation closes [#2526](https://github.com/tobymao/sqlglot/pull/2526) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a43132b`](https://github.com/tobymao/sqlglot/commit/a43132bfaa4c18e88ad1a0156df020e78f3e0dfe) - Alter column set type statement for MySQL *(PR [#2527](https://github.com/tobymao/sqlglot/pull/2527) by [@izeigerman](https://github.com/izeigerman))*


## [v19.1.0] - 2023-11-08
### :boom: BREAKING CHANGES
- due to [`c6db124`](https://github.com/tobymao/sqlglot/commit/c6db1240b8481af0003a4bed4225f8e46578f182) - transpile division *(PR [#2513](https://github.com/tobymao/sqlglot/pull/2513) by [@barakalon](https://github.com/barakalon))*:

  transpile division (#2513)

- due to [`3469e75`](https://github.com/tobymao/sqlglot/commit/3469e75f6fc0c34d68c3a6f1a1b51d8cce3439ff) - typed div and safe div semantics *(PR [#2516](https://github.com/tobymao/sqlglot/pull/2516) by [@barakalon](https://github.com/barakalon))*:

  typed div and safe div semantics (#2516)


### :sparkles: New Features
- [`f95947f`](https://github.com/tobymao/sqlglot/commit/f95947f72029a1c19433fa90e4e188af5de28faf) - **bigquery**: add support for FOR .. IN statement *(PR [#2507](https://github.com/tobymao/sqlglot/pull/2507) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#2506](undefined) opened by [@scholtzan](https://github.com/scholtzan)*
- [`01d446b`](https://github.com/tobymao/sqlglot/commit/01d446b7cd9fb73de44feaf5d6665806e3e4a16b) - **optimizer**: annotate type of ABS *(PR [#2524](https://github.com/tobymao/sqlglot/pull/2524) by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`c7302cf`](https://github.com/tobymao/sqlglot/commit/c7302cf54f71002878dce0f57c0c4a3b5aeafb41) - struct conversion for non correlated queries *(commit by [@tobymao](https://github.com/tobymao))*
- [`1aa727c`](https://github.com/tobymao/sqlglot/commit/1aa727c7578edc3f0be9f32fe5171ce9be185180) - subquery column lineage closes [#2510](https://github.com/tobymao/sqlglot/pull/2510) *(commit by [@tobymao](https://github.com/tobymao))*
- [`c6db124`](https://github.com/tobymao/sqlglot/commit/c6db1240b8481af0003a4bed4225f8e46578f182) - transpile division *(PR [#2513](https://github.com/tobymao/sqlglot/pull/2513) by [@barakalon](https://github.com/barakalon))*
- [`3787389`](https://github.com/tobymao/sqlglot/commit/3787389d4b369b910580931992d0133667d94969) - lineage with subquery and cte closes [#2515](https://github.com/tobymao/sqlglot/pull/2515) *(commit by [@tobymao](https://github.com/tobymao))*
- [`3469e75`](https://github.com/tobymao/sqlglot/commit/3469e75f6fc0c34d68c3a6f1a1b51d8cce3439ff) - typed div and safe div semantics *(PR [#2516](https://github.com/tobymao/sqlglot/pull/2516) by [@barakalon](https://github.com/barakalon))*
- [`52066ea`](https://github.com/tobymao/sqlglot/commit/52066ea899d1150cb2eaee71e606921f5f18bd09) - **tsql**: parse DROP CONSTRAINT into Drop instead of Command *(PR [#2521](https://github.com/tobymao/sqlglot/pull/2521) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2519](undefined) opened by [@HassanShafiq123](https://github.com/HassanShafiq123)*
- [`190f028`](https://github.com/tobymao/sqlglot/commit/190f028752bdcb8919ba79cef52dc96d11997975) - **oracle**: parse TO_CHAR using format_time_lambda *(PR [#2523](https://github.com/tobymao/sqlglot/pull/2523) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#2517](undefined) opened by [@CBQu](https://github.com/CBQu)*
- [`e2e11ae`](https://github.com/tobymao/sqlglot/commit/e2e11ae2d8a8cfa5205b727cf748e01944c54d70) - **optimizer**: more support for date literals in simplify *(PR [#2525](https://github.com/tobymao/sqlglot/pull/2525) by [@barakalon](https://github.com/barakalon))*

### :wrench: Chores
- [`2065210`](https://github.com/tobymao/sqlglot/commit/206521070bd9f0156e1102f7bc93c452535a464b) - expressions type *(commit by [@tobymao](https://github.com/tobymao))*
- [`7ff5f25`](https://github.com/tobymao/sqlglot/commit/7ff5f254e755bfef02c694ca3920d10bc6e174cd) - use tuples instead of sets for inline collection instantiations *(PR [#2520](https://github.com/tobymao/sqlglot/pull/2520) by [@georgesittas](https://github.com/georgesittas))*


## [v19.0.3] - 2023-11-02
### :bug: Bug Fixes
- [`cdaf832`](https://github.com/tobymao/sqlglot/commit/cdaf832a392549f2910d97f1659c2dce0b0a6cf4) - **planner**: missing step of subquery *(PR [#2503](https://github.com/tobymao/sqlglot/pull/2503) by [@Thearas](https://github.com/Thearas))*

### :wrench: Chores
- [`c51d9ae`](https://github.com/tobymao/sqlglot/commit/c51d9ae7c45a66d32b4264712cc7d652964411fa) - fix type hint for normalize_identifiers *(PR [#2505](https://github.com/tobymao/sqlglot/pull/2505) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v19.0.2] - 2023-11-01
### :sparkles: New Features
- [`3d60f0e`](https://github.com/tobymao/sqlglot/commit/3d60f0edd570ba51660110e3752d57a783b172ed) - **optimizer**: more date function coercion *(PR [#2493](https://github.com/tobymao/sqlglot/pull/2493) by [@barakalon](https://github.com/barakalon))*
- [`01c1abb`](https://github.com/tobymao/sqlglot/commit/01c1abb8fb1ff3d4e96c7aae203e3a137960b5a3) - **tsql**: add period constraint, system versioning property, generate as row *(PR [#2484](https://github.com/tobymao/sqlglot/pull/2484) by [@Rik-de-Kort](https://github.com/Rik-de-Kort))*
- [`b18235b`](https://github.com/tobymao/sqlglot/commit/b18235bb8a31a189cb634548db350282935a2991) - use parse_identifiers in qualify tables *(PR [#2502](https://github.com/tobymao/sqlglot/pull/2502) by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`471591c`](https://github.com/tobymao/sqlglot/commit/471591ce3e499d695541defdcfdc27cc07f01907) - **teradata**: support `TRYCAST` *(PR [#2496](https://github.com/tobymao/sqlglot/pull/2496) by [@hsheth2](https://github.com/hsheth2))*
- [`5cdbdf6`](https://github.com/tobymao/sqlglot/commit/5cdbdf6c102c37d91ee335cbe123a8aa3596e310) - **teradata**: support `**` for exponent *(PR [#2495](https://github.com/tobymao/sqlglot/pull/2495) by [@hsheth2](https://github.com/hsheth2))*
- [`70227fc`](https://github.com/tobymao/sqlglot/commit/70227fc8506d765ff7ac25606623a33ec0408284) - **postgres**: allow opclass types to be namespaced *(PR [#2499](https://github.com/tobymao/sqlglot/pull/2499) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2498](undefined) opened by [@Nitrino](https://github.com/Nitrino)*
- [`b434717`](https://github.com/tobymao/sqlglot/commit/b4347170891fac6c1fb29084032fe4dc01844711) - **tsql**: improve support for SYSTEM_VERSIONING *(PR [#2501](https://github.com/tobymao/sqlglot/pull/2501) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`8691e1a`](https://github.com/tobymao/sqlglot/commit/8691e1acaaaa2906da163e210593244f55b56093) - **oracle**: alter table add multiple columns closes [#2500](https://github.com/tobymao/sqlglot/pull/2500) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`808b0bb`](https://github.com/tobymao/sqlglot/commit/808b0bbc4781bd671f52169259434f7ad656e004) - skip branch if no exponent parsing *(commit by [@tobymao](https://github.com/tobymao))*


## [v19.0.1] - 2023-10-31
### :sparkles: New Features
- [`12596fd`](https://github.com/tobymao/sqlglot/commit/12596fdb83504b0e3e5f06132041771680c2560b) - support teradata as format no type closes [#2485](https://github.com/tobymao/sqlglot/pull/2485) *(PR [#2486](https://github.com/tobymao/sqlglot/pull/2486) by [@tobymao](https://github.com/tobymao))*
- [`f25b61c`](https://github.com/tobymao/sqlglot/commit/f25b61c084435f77e66dd980e9e4aec42f33b99d) - **redshift**: add support for DATE_DIFF *(PR [#2491](https://github.com/tobymao/sqlglot/pull/2491) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`83ecc5a`](https://github.com/tobymao/sqlglot/commit/83ecc5afb9be5ca3f7c6db3ca9f91e18aec93b88) - get rid of UNKNOWN type mapping in base Generator class *(PR [#2487](https://github.com/tobymao/sqlglot/pull/2487) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`22990ef`](https://github.com/tobymao/sqlglot/commit/22990efde00043ea1ede769aacc83f314ea920ae) - qualify catalog only if db is present *(PR [#2489](https://github.com/tobymao/sqlglot/pull/2489) by [@eakmanrq](https://github.com/eakmanrq))*
- [`8e20328`](https://github.com/tobymao/sqlglot/commit/8e203288adcbb45b23bfbb57416333ef6f91e370) - **schema**: use to_identifier as fallback when normalizing names *(PR [#2492](https://github.com/tobymao/sqlglot/pull/2492) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`f6c34b0`](https://github.com/tobymao/sqlglot/commit/f6c34b08d17b5c5c6aadeac362a4c85b4688b987) - **teradata**: add UPD and DEL abbreviations *(PR [#2494](https://github.com/tobymao/sqlglot/pull/2494) by [@hsheth2](https://github.com/hsheth2))*


## [v19.0.0] - 2023-10-30
### :boom: BREAKING CHANGES
- due to [`7a6da28`](https://github.com/tobymao/sqlglot/commit/7a6da28907ef519b954bc813fe180c306fe5006b) - generator now always copies, making transforms much simpler *(PR [#2477](https://github.com/tobymao/sqlglot/pull/2477) by [@tobymao](https://github.com/tobymao))*:

  generator now always copies, making transforms much simpler (#2477)


### :sparkles: New Features
- [`542ea6c`](https://github.com/tobymao/sqlglot/commit/542ea6c196679ecb28f16f28c8fe1d62bf9f82e1) - **mysql**: convert full outer join to union with left/right outer joins *(PR [#2461](https://github.com/tobymao/sqlglot/pull/2461) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2458](undefined) opened by [@treysp](https://github.com/treysp)*
- [`b3990bc`](https://github.com/tobymao/sqlglot/commit/b3990bc74131ff537303eb24608d40f7456a9ee9) - **postgres**: support WITH ORDINALITY in table functions  *(PR [#2465](https://github.com/tobymao/sqlglot/pull/2465) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1619](undefined) opened by [@deschman](https://github.com/deschman)*
- [`c3852db`](https://github.com/tobymao/sqlglot/commit/c3852db1f873b2ed4096d582aa334723512a545d) - **postgres**: add support for the PARTITION OF property in CREATE *(PR [#2476](https://github.com/tobymao/sqlglot/pull/2476) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2469](undefined) opened by [@judahrand](https://github.com/judahrand)*

### :bug: Bug Fixes
- [`4f9cb22`](https://github.com/tobymao/sqlglot/commit/4f9cb2297119fd61dc4ab88b76c4db5c46e4ece6) - facilitate transpilation of Redshift's LISTAGG *(PR [#2460](https://github.com/tobymao/sqlglot/pull/2460) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`81ab80a`](https://github.com/tobymao/sqlglot/commit/81ab80a745531fa345e7ea45a356334c68a908a0) - **mysql**: str_to_date for datetime *(PR [#2473](https://github.com/tobymao/sqlglot/pull/2473) by [@barakalon](https://github.com/barakalon))*
- [`a1252d8`](https://github.com/tobymao/sqlglot/commit/a1252d8ba7d2394bbb14ccd42d835da8cd4eb740) - **teradata**: add eq, minus abbreviations fixes [#2474](https://github.com/tobymao/sqlglot/pull/2474) *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`e6f31d6`](https://github.com/tobymao/sqlglot/commit/e6f31d65825d6e3324b4564d202e610d3068e263) - **snowflake**: don't add time format in TO_TIMESTAMP if not supplied *(PR [#2475](https://github.com/tobymao/sqlglot/pull/2475) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`2307910`](https://github.com/tobymao/sqlglot/commit/23079105af1bdcbd849d813b402ee1a3b55fdacd) - **optimizer**: make normalize_identifiers identifier conversion more lenient *(PR [#2478](https://github.com/tobymao/sqlglot/pull/2478) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`ed5c559`](https://github.com/tobymao/sqlglot/commit/ed5c5593a6316da81695b1bfaad90b465acb99ba) - **snowflake**: avoid crash on OBJECT_CONSTRUCT without arguments *(PR [#2482](https://github.com/tobymao/sqlglot/pull/2482) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2479](undefined) opened by [@wedotech-ashley](https://github.com/wedotech-ashley)*
- [`df0a698`](https://github.com/tobymao/sqlglot/commit/df0a69842daf3ec957effba206b79e27a97c91b4) - **tsql**: add 'dddd' to '%A' time mapping *(PR [#2483](https://github.com/tobymao/sqlglot/pull/2483) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2481](undefined) opened by [@SudarshanVS](https://github.com/SudarshanVS)*

### :recycle: Refactors
- [`7a6da28`](https://github.com/tobymao/sqlglot/commit/7a6da28907ef519b954bc813fe180c306fe5006b) - generator now always copies, making transforms much simpler *(PR [#2477](https://github.com/tobymao/sqlglot/pull/2477) by [@tobymao](https://github.com/tobymao))*


## [v18.17.0] - 2023-10-25
### :boom: BREAKING CHANGES
- due to [`c8e87b6`](https://github.com/tobymao/sqlglot/commit/c8e87b6cab18924b6fa307252f19e83eef4f2f03) - unnabreviate units, e.g. ms to millisecond *(PR [#2451](https://github.com/tobymao/sqlglot/pull/2451) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  unnabreviate units, e.g. ms to millisecond (#2451)


### :sparkles: New Features
- [`7ded253`](https://github.com/tobymao/sqlglot/commit/7ded2536f348a0470af044b56fa0fd6cfb09bb0d) - add support for {fn ...} function syntax *(PR [#2447](https://github.com/tobymao/sqlglot/pull/2447) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`ee18756`](https://github.com/tobymao/sqlglot/commit/ee18756e8779f0267025e63789bddf99cdf631e0) - **snowflake**: register APPROX_TOP_K as AggFunc *(PR [#2450](https://github.com/tobymao/sqlglot/pull/2450) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2449](undefined) opened by [@yogevyuval](https://github.com/yogevyuval)*
- [`57b744a`](https://github.com/tobymao/sqlglot/commit/57b744a431614818cd84f65a8d6105655cd462a4) - **optimizer**: infer bracket type *(PR [#2441](https://github.com/tobymao/sqlglot/pull/2441) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`64b18e6`](https://github.com/tobymao/sqlglot/commit/64b18e6555865e447723e8e4619e9eba399e7cee) - add ArgMax, ArgMin expressions, fix their transpilation *(PR [#2454](https://github.com/tobymao/sqlglot/pull/2454) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2453](undefined) opened by [@erikcw](https://github.com/erikcw)*
- [`959fa92`](https://github.com/tobymao/sqlglot/commit/959fa9212ee698029eb6e1957d4f2d22590fccb0) - **lineage**: terminal table node for select star *(PR [#2456](https://github.com/tobymao/sqlglot/pull/2456) by [@edpaget](https://github.com/edpaget))*

### :bug: Bug Fixes
- [`e11ecaf`](https://github.com/tobymao/sqlglot/commit/e11ecaf2e0c5225df7ea5ca6e5622be1e94b2625) - **clickhouse**: treat CURRENT_DATE as a function *(PR [#2439](https://github.com/tobymao/sqlglot/pull/2439) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2438](undefined) opened by [@samuelcolvin](https://github.com/samuelcolvin)*
- [`fdb1668`](https://github.com/tobymao/sqlglot/commit/fdb166801144b721677d23c195e5bd3d35ee8841) - improve bracket parsing error, set Slice type to Unknown *(PR [#2440](https://github.com/tobymao/sqlglot/pull/2440) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2437](undefined) opened by [@samuelcolvin](https://github.com/samuelcolvin)*
- [`546eb54`](https://github.com/tobymao/sqlglot/commit/546eb545f2f6a37408c41d013089f2f29e922eb3) - **redshift**: don't rewrite JSON_PARSE to PARSE_JSON *(commit by [@purcell](https://github.com/purcell))*
- [`00e3515`](https://github.com/tobymao/sqlglot/commit/00e3515ff3157105d84f21f56cfb64bae3267640) - **parser**: treat 'use' as a valid identifier token *(PR [#2446](https://github.com/tobymao/sqlglot/pull/2446) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2444](undefined) opened by [@CBQu](https://github.com/CBQu)*
- [`c8e87b6`](https://github.com/tobymao/sqlglot/commit/c8e87b6cab18924b6fa307252f19e83eef4f2f03) - unnabreviate units, e.g. ms to millisecond *(PR [#2451](https://github.com/tobymao/sqlglot/pull/2451) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2448](undefined) opened by [@samuelcolvin](https://github.com/samuelcolvin)*
- [`11bfc75`](https://github.com/tobymao/sqlglot/commit/11bfc7518bb39938661c55d2de54ca0cbd042c6a) - **lineage**: handle unions with SELECT STAR *(PR [#2452](https://github.com/tobymao/sqlglot/pull/2452) by [@edpaget](https://github.com/edpaget))*
- [`b856477`](https://github.com/tobymao/sqlglot/commit/b856477200faf552441a8d27adb5cff4b83dda65) - add table alias in unnest with columns only *(PR [#2457](https://github.com/tobymao/sqlglot/pull/2457) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2455](undefined) opened by [@bgedik](https://github.com/bgedik)*

### :wrench: Chores
- [`927f5aa`](https://github.com/tobymao/sqlglot/commit/927f5aaf20155fe5c7e0a1c466eae19c59913045) - add docstrings in rest of transforms *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.16.1] - 2023-10-21
### :sparkles: New Features
- [`16fb83f`](https://github.com/tobymao/sqlglot/commit/16fb83ff3f9a2bd77ce3edfa6f4932916a033d4c) - **bigquery**: default collate closes [#2434](https://github.com/tobymao/sqlglot/pull/2434) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`87efe41`](https://github.com/tobymao/sqlglot/commit/87efe41839a4f8e14e9b5dc0810156d06ae053a7) - **duckdb**: regexp_replace modifiers closes [#2436](https://github.com/tobymao/sqlglot/pull/2436) *(commit by [@tobymao](https://github.com/tobymao))*
- [`be38964`](https://github.com/tobymao/sqlglot/commit/be3896441016f0356ca3d3c02f74deed6a63879e) - infinite loop due to uppercase expansion *(commit by [@tobymao](https://github.com/tobymao))*


## [v18.16.0] - 2023-10-21
### :bug: Bug Fixes
- [`5b388bc`](https://github.com/tobymao/sqlglot/commit/5b388bc72aa8e9e01561b077ddbaeb1935894403) - attach comments to Union expressions *(PR [#2432](https://github.com/tobymao/sqlglot/pull/2432) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2430](undefined) opened by [@SudarshanVS](https://github.com/SudarshanVS)*
- [`e6721d1`](https://github.com/tobymao/sqlglot/commit/e6721d17d111b952c717e8a21b176e08a68226a7) - subquery lineage closes [#2431](https://github.com/tobymao/sqlglot/pull/2431) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b4f76eb`](https://github.com/tobymao/sqlglot/commit/b4f76eb6ff7a4bdb377c3aac8cbde886c3894416) - **clickhouse**: neq bug closes [#2435](https://github.com/tobymao/sqlglot/pull/2435) *(commit by [@tobymao](https://github.com/tobymao))*


## [v18.15.1] - 2023-10-19
### :sparkles: New Features
- [`469226b`](https://github.com/tobymao/sqlglot/commit/469226b31443038ff2c28ce1a03c8c96df598f44) - **snowflake**: support for ALTER TABLE SWAP WITH *(PR [#2420](https://github.com/tobymao/sqlglot/pull/2420) by [@teraamp](https://github.com/teraamp))*
- [`13a5df2`](https://github.com/tobymao/sqlglot/commit/13a5df2d20fa57efd4699ce93d98789666131173) - **lineage**: lineage from UDTFs that use columns *(PR [#2424](https://github.com/tobymao/sqlglot/pull/2424) by [@edpaget](https://github.com/edpaget))*
- [`8d7e4e9`](https://github.com/tobymao/sqlglot/commit/8d7e4e9ec0ef639376d1b4db55f6578e1eb431d1) - show version in cli *(commit by [@tobymao](https://github.com/tobymao))*
- [`88ddaa7`](https://github.com/tobymao/sqlglot/commit/88ddaa7f49327b37acdb6cd87b4189b872b964c1) - make data type build more flexible *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`e1186a5`](https://github.com/tobymao/sqlglot/commit/e1186a5c4c75e876aedb47185beef50396089650) - **redshift**: generate the IDENTITY constraint correctly *(PR [#2418](https://github.com/tobymao/sqlglot/pull/2418) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`a00b5b5`](https://github.com/tobymao/sqlglot/commit/a00b5b56d5c2d197bb1d1d56bfeedb9c7ab40ddf) - smarter detection of timestamp() fallback to cast *(commit by [@tobymao](https://github.com/tobymao))*
- [`c5028d4`](https://github.com/tobymao/sqlglot/commit/c5028d4a651f80aa2599cd69bfe486f8b7ce5d0f) - **teradata**: teradata only supports top closes [#2419](https://github.com/tobymao/sqlglot/pull/2419) *(commit by [@tobymao](https://github.com/tobymao))*
- [`3173683`](https://github.com/tobymao/sqlglot/commit/31736832153872b0dcab067cea8b76970369e58a) - **snowflake**: map DAYOF[MONTH|YEAR] back to itself fixes [#2422](https://github.com/tobymao/sqlglot/pull/2422) *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`e612089`](https://github.com/tobymao/sqlglot/commit/e6120897fb46c4bbc90184d177d45df264e49768) - ts_or_ds reparse closes [#2428](https://github.com/tobymao/sqlglot/pull/2428) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`fda3576`](https://github.com/tobymao/sqlglot/commit/fda35766ee93e2e01ff1ee53ae804d6fe5109ce3) - fix type hint of swaptable_sql *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.15.0] - 2023-10-17
### :bug: Bug Fixes
- [`d75d760`](https://github.com/tobymao/sqlglot/commit/d75d760778973d88325e0b6d167243385140d698) - bigquery parse_json wide_number_mode *(commit by [@tobymao](https://github.com/tobymao))*


## [v18.14.0] - 2023-10-17
### :bug: Bug Fixes
- [`4163d5f`](https://github.com/tobymao/sqlglot/commit/4163d5f8ad1d926830a4273d1e8f3ec4a059b13b) - make `RegexpILike` a subclass of `Binary` as well as `Func` *(PR [#2404](https://github.com/tobymao/sqlglot/pull/2404) by [@samuelcolvin](https://github.com/samuelcolvin))*
- [`de0cd98`](https://github.com/tobymao/sqlglot/commit/de0cd98ff682868ecb2299425f52ea45a9b88f27) - clickhouse any -> has closes [#2408](https://github.com/tobymao/sqlglot/pull/2408) *(commit by [@tobymao](https://github.com/tobymao))*
- [`463b6d6`](https://github.com/tobymao/sqlglot/commit/463b6d63ecdb6a0cf9b44716494aaad7f3efb26b) - **redshift**: add a missing retreat in the group by parser *(PR [#2412](https://github.com/tobymao/sqlglot/pull/2412) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2411](undefined) opened by [@mrmammadov](https://github.com/mrmammadov)*
- [`f8109be`](https://github.com/tobymao/sqlglot/commit/f8109be1b0b620c48db30b82519a5d17b992cce7) - **bigquery**: revert commit 09e2eeb, only reduce NULL for BQ at sqlgen time *(PR [#2414](https://github.com/tobymao/sqlglot/pull/2414) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`f970f7c`](https://github.com/tobymao/sqlglot/commit/f970f7cd424747c4e18cd902cc99f55383e79dbc) - **clickhouse**: map RegexpILike to match *(PR [#2407](https://github.com/tobymao/sqlglot/pull/2407) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2406](undefined) opened by [@samuelcolvin](https://github.com/samuelcolvin)*
- [`1c8883f`](https://github.com/tobymao/sqlglot/commit/1c8883fa66fdc95c8166a7fba4d443dca6e8fd86) - clickhouse any transpilation closes [#2408](https://github.com/tobymao/sqlglot/pull/2408) *(commit by [@tobymao](https://github.com/tobymao))*
- [`586de82`](https://github.com/tobymao/sqlglot/commit/586de8263b577e791355db39e9e3a0526fd7a1fc) - casting *(commit by [@tobymao](https://github.com/tobymao))*
- [`d9958f9`](https://github.com/tobymao/sqlglot/commit/d9958f9438eecf8137361ab76f02acda63e59790) - presto/trino limit can only contain literals *(commit by [@tobymao](https://github.com/tobymao))*
- [`7744648`](https://github.com/tobymao/sqlglot/commit/77446481dd59c213368fe554e8011e9a7f5c0f71) - trino view column and partition column *(PR [#2416](https://github.com/tobymao/sqlglot/pull/2416) by [@eakmanrq](https://github.com/eakmanrq))*


## [v18.13.0] - 2023-10-13
### :sparkles: New Features
- [`e2e960a`](https://github.com/tobymao/sqlglot/commit/e2e960acfdd02cbb22689f1f0f5f00d50e4a537e) - **duckdb**: add parsing support for timestamp types with unit suffixes *(PR [#2400](https://github.com/tobymao/sqlglot/pull/2400) by [@cpcloud](https://github.com/cpcloud))*
- [`f024ac5`](https://github.com/tobymao/sqlglot/commit/f024ac5c7321bfdc605dc8bae72d96f1349bd485) - redshift approximate count distinct *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`d7021d1`](https://github.com/tobymao/sqlglot/commit/d7021d1609b44b9a9fec5d09b5cb862162a3280b) - **optimizer**: don't propagate equality constraints from IF/CASE outwards *(PR [#2396](https://github.com/tobymao/sqlglot/pull/2396) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`ed45fad`](https://github.com/tobymao/sqlglot/commit/ed45fadedb90e3e58e67e47f19091ff147a63ff6) - **teradata**: FOR in LOCKING ROW FOR ACCESS is optional *(PR [#2402](https://github.com/tobymao/sqlglot/pull/2402) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2401](undefined) opened by [@hsheth2](https://github.com/hsheth2)*

### :recycle: Refactors
- [`d83f242`](https://github.com/tobymao/sqlglot/commit/d83f242420e4feaa950244322806678ad868cedf) - split out when matched *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`85a16b1`](https://github.com/tobymao/sqlglot/commit/85a16b16ab7ead731ab255fdb8efb13558d0b232) - remove generic schema type *(PR [#2399](https://github.com/tobymao/sqlglot/pull/2399) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.12.0] - 2023-10-10
### :boom: BREAKING CHANGES
- due to [`28308da`](https://github.com/tobymao/sqlglot/commit/28308dae5ab7a0aff3ca2afeff88fa4554babba3) - support spark trycast, treat databricks as strict_cast closes [#2389](https://github.com/tobymao/sqlglot/pull/2389) *(commit by [@tobymao](https://github.com/tobymao))*:

  support spark trycast, treat databricks as strict_cast closes #2389

- due to [`c7c3869`](https://github.com/tobymao/sqlglot/commit/c7c3869b01e984a243c071660f27a2c6c4863892) - add explode outer and change hiearchy are explosions closes [#2393](https://github.com/tobymao/sqlglot/pull/2393) *(commit by [@tobymao](https://github.com/tobymao))*:

  add explode outer and change hiearchy are explosions closes #2393


### :sparkles: New Features
- [`f4f8366`](https://github.com/tobymao/sqlglot/commit/f4f8366f6f761fefd72cd2a1ee4c462a0e18ec42) - **schema**: add method to check if column exists *(PR [#2381](https://github.com/tobymao/sqlglot/pull/2381) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`da2c6f1`](https://github.com/tobymao/sqlglot/commit/da2c6f167be2de8b61ab9b0fb60c2bc2b6b24408) - **optimizer**: simplify CONCAT_WS *(PR [#2383](https://github.com/tobymao/sqlglot/pull/2383) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`cca58dd`](https://github.com/tobymao/sqlglot/commit/cca58dd2e7a45d1150b37f8e76baa3571fed8135) - **optimizer**: propagate constants *(PR [#2386](https://github.com/tobymao/sqlglot/pull/2386) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`28308da`](https://github.com/tobymao/sqlglot/commit/28308dae5ab7a0aff3ca2afeff88fa4554babba3) - support spark trycast, treat databricks as strict_cast closes [#2389](https://github.com/tobymao/sqlglot/pull/2389) *(commit by [@tobymao](https://github.com/tobymao))*
- [`c7c3869`](https://github.com/tobymao/sqlglot/commit/c7c3869b01e984a243c071660f27a2c6c4863892) - add explode outer and change hiearchy are explosions closes [#2393](https://github.com/tobymao/sqlglot/pull/2393) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`0fb1652`](https://github.com/tobymao/sqlglot/commit/0fb1652784845f083b87e952644a8a9790d28d8e) - replace executor None values with np.NaN to silence Pandas>2.1 warnings *(PR [#2384](https://github.com/tobymao/sqlglot/pull/2384) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`a849794`](https://github.com/tobymao/sqlglot/commit/a8497944424a524fdc1c9fdb9e10aa2f3558bdd5) - **mysql**: move parsing logic for JSON_TABLE to base parser *(PR [#2387](https://github.com/tobymao/sqlglot/pull/2387) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`e08c1c0`](https://github.com/tobymao/sqlglot/commit/e08c1c0e8a5a2fbc46872d163eac52071b49a1cf) - correctly handle agg subexpressions with no selections *(PR [#2390](https://github.com/tobymao/sqlglot/pull/2390) by [@ginter](https://github.com/ginter))*
- [`8afa7a1`](https://github.com/tobymao/sqlglot/commit/8afa7a12ca35400133a49571b49d001bdb526188) - **schema**: don't specialize type variable in MappingSchema *(PR [#2394](https://github.com/tobymao/sqlglot/pull/2394) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.11.6] - 2023-10-06
### :bug: Bug Fixes
- [`ef12aa7`](https://github.com/tobymao/sqlglot/commit/ef12aa7b4c24b431dbedcf917f61d18a89dc3a0f) - normalize_identifiers parses identifier strings *(commit by [@tobymao](https://github.com/tobymao))*


## [v18.11.5] - 2023-10-06
### :sparkles: New Features
- [`ae27e46`](https://github.com/tobymao/sqlglot/commit/ae27e46cf60bbbcb456997afe942a6e8ab9d03c1) - **spark**: from_utc_timestamp -> at time zone *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`d9bfaa7`](https://github.com/tobymao/sqlglot/commit/d9bfaa7a5d3a43c5fa1d34dd3b33d1847e0cbb13) - preserve identifiers in postgres merge *(commit by [@tobymao](https://github.com/tobymao))*


## [v18.11.4] - 2023-10-05
### :sparkles: New Features
- [`3266e51`](https://github.com/tobymao/sqlglot/commit/3266e51fa3f2fcb311bc2fe8b212e423d4253082) - **bigquery**: improve support for CREATE MODEL DDL statement *(PR [#2380](https://github.com/tobymao/sqlglot/pull/2380) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`dd8334d`](https://github.com/tobymao/sqlglot/commit/dd8334d35d757b7309246ceb4e00f077eb19c9d6) - **parser**: don't consume identifier in unnamed constraint parser *(PR [#2377](https://github.com/tobymao/sqlglot/pull/2377) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2376](undefined) opened by [@Nitrino](https://github.com/Nitrino)*


## [v18.11.3] - 2023-10-04
### :sparkles: New Features
- [`347ac51`](https://github.com/tobymao/sqlglot/commit/347ac51da6a553a7904739f0f3ad6b4bb4db01c6) - **redshift**: add support for Redshift's super array index iteration *(PR [#2373](https://github.com/tobymao/sqlglot/pull/2373) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2372](undefined) opened by [@erdrix](https://github.com/erdrix)*
- [`160d841`](https://github.com/tobymao/sqlglot/commit/160d8415d297f998a800c518ce2e85ec41deedae) - **bigquery**: add support for ML.PREDICT function *(PR [#2375](https://github.com/tobymao/sqlglot/pull/2375) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`6a65a09`](https://github.com/tobymao/sqlglot/commit/6a65a096e69f2e0cee933d0221d0f6a93aeec159) - **postgres**: translate variance_pop to var_pop and variance to var_samp *(PR [#2371](https://github.com/tobymao/sqlglot/pull/2371) by [@cpcloud](https://github.com/cpcloud))*


## [v18.11.2] - 2023-10-03
### :bug: Bug Fixes
- [`513fe2c`](https://github.com/tobymao/sqlglot/commit/513fe2c5cf06db2a797cdc0422a49a95103c403e) - **parser**: support END keyword when parsing create procedure DDLs *(PR [#2369](https://github.com/tobymao/sqlglot/pull/2369) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`1ba5f98`](https://github.com/tobymao/sqlglot/commit/1ba5f98b400483e53d7b1c56a7d5a599f4926234) - distinct from parsing *(commit by [@tobymao](https://github.com/tobymao))*


## [v18.11.1] - 2023-10-03
### :bug: Bug Fixes
- [`f777155`](https://github.com/tobymao/sqlglot/commit/f777155eb6249a51290d38eaa1dfa1f867a38602) - unescape escape sequences on read, re-escape them on generation *(PR [#2367](https://github.com/tobymao/sqlglot/pull/2367) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2325](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`0d1e674`](https://github.com/tobymao/sqlglot/commit/0d1e674015a6ff3eca90a6b6263119ad01a55db6) - **optimizer**: handle edge case in DATE_TRUNC simplification *(PR [#2368](https://github.com/tobymao/sqlglot/pull/2368) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.11.0] - 2023-10-03
### :sparkles: New Features
- [`e4da5d7`](https://github.com/tobymao/sqlglot/commit/e4da5d732d8b3add5c73a0aee66838a806a8e506) - **clickhouse**: add support for SAMPLE BY property in CREATE DDL *(PR [#2355](https://github.com/tobymao/sqlglot/pull/2355) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2352](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`8dc2a9c`](https://github.com/tobymao/sqlglot/commit/8dc2a9ccbec63b9a2dec577547e301046a02a4d8) - add the ability to set meta in sql comments *(PR [#2351](https://github.com/tobymao/sqlglot/pull/2351) by [@tobymao](https://github.com/tobymao))*
- [`d2047ec`](https://github.com/tobymao/sqlglot/commit/d2047ec67d486e42506db7d0f1a642a5e6c40c11) - **snowflake**: add support for staged file file_format clause *(PR [#2359](https://github.com/tobymao/sqlglot/pull/2359) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`55e2d15`](https://github.com/tobymao/sqlglot/commit/55e2d15519830a91a420c5ad3315b75ddd2087ce) - switch identifier normalization off using comments *(PR [#2361](https://github.com/tobymao/sqlglot/pull/2361) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`1df9333`](https://github.com/tobymao/sqlglot/commit/1df93336d49a12f4ac00b0da45c05c07735ffa11) - **parser**: exclude set operators from unnest offset alias token set *(PR [#2349](https://github.com/tobymao/sqlglot/pull/2349) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2348](undefined) opened by [@sean-rose](https://github.com/sean-rose)*
- [`a794bfe`](https://github.com/tobymao/sqlglot/commit/a794bfef771ced62dd3720a9d3bd49e9e99bd020) - **hive**: don't generate WithDataProperty *(PR [#2350](https://github.com/tobymao/sqlglot/pull/2350) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5fb7174`](https://github.com/tobymao/sqlglot/commit/5fb71743d9274b7e0e825a761be3672c6299e453) - fix perf issues with nested left joins *(commit by [@tobymao](https://github.com/tobymao))*
- [`ac4e572`](https://github.com/tobymao/sqlglot/commit/ac4e572be99a636ea013db3c33bf01bc80732d57) - handle strings in Table.parts, use dialect for parsing in table_name *(PR [#2353](https://github.com/tobymao/sqlglot/pull/2353) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`e8273e2`](https://github.com/tobymao/sqlglot/commit/e8273e26b2300e7782c33c9858b0217af3570e4c) - **hive**: don't generate BYTE when transpiling Oracle's VARCHAR(5 BYTE) *(PR [#2358](https://github.com/tobymao/sqlglot/pull/2358) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2356](undefined) opened by [@CaryMoore-DB](https://github.com/CaryMoore-DB)*
- [`2bc30a5`](https://github.com/tobymao/sqlglot/commit/2bc30a5de6c642a49e8ad66dbc5c6f6c82cb77d9) - **mysql**: DATE_ADD for datetimes *(PR [#2360](https://github.com/tobymao/sqlglot/pull/2360) by [@barakalon](https://github.com/barakalon))*
- [`a270c15`](https://github.com/tobymao/sqlglot/commit/a270c15480e2db592b0a2f13c8fb3edb8587cdaa) - **redshift**: treat single quote as an escape character *(PR [#2365](https://github.com/tobymao/sqlglot/pull/2365) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2364](undefined) opened by [@erdrix](https://github.com/erdrix)*
- [`0e93890`](https://github.com/tobymao/sqlglot/commit/0e93890670596b5cf97d66eabb84bd0be4f0bb13) - **optimizer**: don't merge CTEs with EXPLODE projections into outer scopes *(PR [#2366](https://github.com/tobymao/sqlglot/pull/2366) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.10.1] - 2023-09-29
### :sparkles: New Features
- [`4665016`](https://github.com/tobymao/sqlglot/commit/466501635d131a91812684441fdfbbfede53a242) - **postgres**: struct_extract *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`17e39d0`](https://github.com/tobymao/sqlglot/commit/17e39d04916e3e406307ed8922cb2597b4a6998a) - **snowflake**: fix staged table path parsing *(PR [#2346](https://github.com/tobymao/sqlglot/pull/2346) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.10.0] - 2023-09-29
### :sparkles: New Features
- [`090724d`](https://github.com/tobymao/sqlglot/commit/090724da9ed7a9c1c0b8024b68340a15b0b64ff0) - add `eliminate_qualify` to clickhouse, mysql, oracle, postgres, and tsql dialects *(PR [#2339](https://github.com/tobymao/sqlglot/pull/2339) by [@cpcloud](https://github.com/cpcloud))*
- [`e2c8366`](https://github.com/tobymao/sqlglot/commit/e2c83665f81db2af934f1ae831748578bcef8216) - **executor**: add support for TRIM, fix TRIM CSV-style parsing order *(PR [#2342](https://github.com/tobymao/sqlglot/pull/2342) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2341](undefined) opened by [@skyserenaa](https://github.com/skyserenaa)*

### :bug: Bug Fixes
- [`fcc2d8f`](https://github.com/tobymao/sqlglot/commit/fcc2d8f4d3077c9f8fd59fbd906cbdf8985bac8c) - **mysql,optimizer**: TO_DAYS transpilation and more date casting *(PR [#2334](https://github.com/tobymao/sqlglot/pull/2334) by [@barakalon](https://github.com/barakalon))*


## [v18.9.0] - 2023-09-28
### :boom: BREAKING CHANGES
- due to [`f0e5eb6`](https://github.com/tobymao/sqlglot/commit/f0e5eb6a904d8ee4420c6a9acf489db9b7fa108f) - revert escape sequence changes introduced in [#2230](https://github.com/tobymao/sqlglot/pull/2230) *(PR [#2336](https://github.com/tobymao/sqlglot/pull/2336) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  revert escape sequence changes introduced in #2230 (#2336)


### :sparkles: New Features
- [`f80501c`](https://github.com/tobymao/sqlglot/commit/f80501ce1d262587856201e2ef2c625dbd446959) - **presto**: group_concat -> array_join closes [#2331](https://github.com/tobymao/sqlglot/pull/2331) *(commit by [@tobymao](https://github.com/tobymao))*
- [`8af4054`](https://github.com/tobymao/sqlglot/commit/8af4054d4e96b62309b1543d51548728bdba520f) - **snowflake**: add support for staged file table syntax *(PR [#2333](https://github.com/tobymao/sqlglot/pull/2333) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2330](undefined) opened by [@ShayYaari](https://github.com/ShayYaari)*
- [`bcd342a`](https://github.com/tobymao/sqlglot/commit/bcd342a739f5202737be147b98ec7852373d2a86) - **mysql**: add unsigned decimal type *(PR [#2340](https://github.com/tobymao/sqlglot/pull/2340) by [@Nitrino](https://github.com/Nitrino))*

### :bug: Bug Fixes
- [`58c7849`](https://github.com/tobymao/sqlglot/commit/58c7849042178ed6b46e39edf9a98fd101ac2bf3) - **clickhouse**: don't generate parentheses, match R_PAREN conditionally *(PR [#2332](https://github.com/tobymao/sqlglot/pull/2332) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`f0e5eb6`](https://github.com/tobymao/sqlglot/commit/f0e5eb6a904d8ee4420c6a9acf489db9b7fa108f) - revert escape sequence changes introduced in [#2230](https://github.com/tobymao/sqlglot/pull/2230) *(PR [#2336](https://github.com/tobymao/sqlglot/pull/2336) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5ea5438`](https://github.com/tobymao/sqlglot/commit/5ea54385b6cbeafbfdf89ff414b6c2638445af61) - **snowflake**: allow window to be used as a table alias *(PR [#2337](https://github.com/tobymao/sqlglot/pull/2337) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2335](undefined) opened by [@arunbalasubramani](https://github.com/arunbalasubramani)*
- [`79c208a`](https://github.com/tobymao/sqlglot/commit/79c208a253020fdc153c85e19869ca32101f2367) - **snowflake, bigquery**: parse COLLATE as a func instead of a binary operator *(PR [#2343](https://github.com/tobymao/sqlglot/pull/2343) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.8.0] - 2023-09-26
### :boom: BREAKING CHANGES
- due to [`66d7385`](https://github.com/tobymao/sqlglot/commit/66d738514feaddf3a92095dcdef62c0d43c291f5) - store expressions in Offset.expression when using builder fixes [#2312](https://github.com/tobymao/sqlglot/pull/2312) *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  store expressions in Offset.expression when using builder fixes #2312

- due to [`cdcc564`](https://github.com/tobymao/sqlglot/commit/cdcc564130a01188295948d5562f71d78dbffa12) - make ObjectIdentifier, IntervalSpan and PseudoType DataTypes *(PR [#2315](https://github.com/tobymao/sqlglot/pull/2315) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  make ObjectIdentifier, IntervalSpan and PseudoType DataTypes (#2315)

- due to [`ebdfc59`](https://github.com/tobymao/sqlglot/commit/ebdfc592026db133f4a9828ff88c096a7d6b0f54) - add support for heredoc strings (Postgres, ClickHouse) *(PR [#2328](https://github.com/tobymao/sqlglot/pull/2328) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  add support for heredoc strings (Postgres, ClickHouse) (#2328)


### :sparkles: New Features
- [`c50e74c`](https://github.com/tobymao/sqlglot/commit/c50e74cf78a64d7b461054d4dc35a2e40195277b) - support for percentiles in duckdb, snowflake *(PR [#2302](https://github.com/tobymao/sqlglot/pull/2302) by [@longxiaofei](https://github.com/longxiaofei))*
- [`8ed0a81`](https://github.com/tobymao/sqlglot/commit/8ed0a810ef4ec36ca648ad55b7a873ce04b8da30) - **bigquery**: add support for CREATE TABLE .. COPY DDL syntax *(PR [#2305](https://github.com/tobymao/sqlglot/pull/2305) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2304](undefined) opened by [@razvan-am](https://github.com/razvan-am)*
- [`3cb3131`](https://github.com/tobymao/sqlglot/commit/3cb3131fc9210c46afba2ece8c43fad852106de5) - **clickhouse**: add isnan and startswith renamings *(PR [#2310](https://github.com/tobymao/sqlglot/pull/2310) by [@cpcloud](https://github.com/cpcloud))*
- [`f473e88`](https://github.com/tobymao/sqlglot/commit/f473e88523d2c0a66719e3b8a3e8fcfb6cc5ed3c) - **postgres**: add support for operator classes in CREATE INDEX DDL *(PR [#2317](https://github.com/tobymao/sqlglot/pull/2317) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2278](undefined) opened by [@Nitrino](https://github.com/Nitrino)*
- [`64a7b93`](https://github.com/tobymao/sqlglot/commit/64a7b93a62595201294b0b30a1c791d9ffc6f856) - **optimizer**: canonicalize date arithmetic funcs *(PR [#2320](https://github.com/tobymao/sqlglot/pull/2320) by [@barakalon](https://github.com/barakalon))*
- [`f3d928b`](https://github.com/tobymao/sqlglot/commit/f3d928bd48f7268a43f0803845c734d6e1fedd34) - **optimizer**: ensure boolean predicates on CASE statement *(PR [#2321](https://github.com/tobymao/sqlglot/pull/2321) by [@barakalon](https://github.com/barakalon))*
- [`ebdfc59`](https://github.com/tobymao/sqlglot/commit/ebdfc592026db133f4a9828ff88c096a7d6b0f54) - add support for heredoc strings (Postgres, ClickHouse) *(PR [#2328](https://github.com/tobymao/sqlglot/pull/2328) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2316](undefined) opened by [@pkit](https://github.com/pkit)*

### :bug: Bug Fixes
- [`c51ecb1`](https://github.com/tobymao/sqlglot/commit/c51ecb168a068079f331c0a4c67b5ca61f6674f1) - **clickhouse**: fix incorrect array param generation for clickhouse quantiles *(PR [#2311](https://github.com/tobymao/sqlglot/pull/2311) by [@cpcloud](https://github.com/cpcloud))*
- [`66d7385`](https://github.com/tobymao/sqlglot/commit/66d738514feaddf3a92095dcdef62c0d43c291f5) - store expressions in Offset.expression when using builder fixes [#2312](https://github.com/tobymao/sqlglot/pull/2312) *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`cdcc564`](https://github.com/tobymao/sqlglot/commit/cdcc564130a01188295948d5562f71d78dbffa12) - make ObjectIdentifier, IntervalSpan and PseudoType DataTypes *(PR [#2315](https://github.com/tobymao/sqlglot/pull/2315) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`aa2c4c3`](https://github.com/tobymao/sqlglot/commit/aa2c4c3dbb07116e595690a56e7bc2d123557545) - **optimizer**: a couple simplify_date_trunc enhancements *(PR [#2319](https://github.com/tobymao/sqlglot/pull/2319) by [@barakalon](https://github.com/barakalon))*
- [`180cd8e`](https://github.com/tobymao/sqlglot/commit/180cd8e21713f01080a6b32c55739f5220f56526) - **clickhouse**: support SAMPLE clause fixes [#2323](https://github.com/tobymao/sqlglot/pull/2323) *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`8242a2c`](https://github.com/tobymao/sqlglot/commit/8242a2c65466ab7245a4a9573403787a262d0bbc) - **mysql**: transpile CHAR *(PR [#2329](https://github.com/tobymao/sqlglot/pull/2329) by [@barakalon](https://github.com/barakalon))*

### :wrench: Chores
- [`363e102`](https://github.com/tobymao/sqlglot/commit/363e10244e8afadca18ffbb303cd6c1765fe1a46) - fix type *(commit by [@tobymao](https://github.com/tobymao))*


## [v18.7.0] - 2023-09-22
### :sparkles: New Features
- [`f1b6546`](https://github.com/tobymao/sqlglot/commit/f1b6546b463b2a6aa075bf31f54d3aa8a59b748f) - add iter for expressions *(commit by [@tobymao](https://github.com/tobymao))*
- [`13877fe`](https://github.com/tobymao/sqlglot/commit/13877fe117c2811c2cd4d277c585f0cd4715f18a) - **optimizer**: replace date funcs *(PR [#2299](https://github.com/tobymao/sqlglot/pull/2299) by [@barakalon](https://github.com/barakalon))*

### :bug: Bug Fixes
- [`fc793c4`](https://github.com/tobymao/sqlglot/commit/fc793c4ff321808fd359ba3886e236ac7476de0d) - **postgres**: generate ARRAY[] correctly *(PR [#2287](https://github.com/tobymao/sqlglot/pull/2287) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1457](undefined) opened by [@cmvarmour](https://github.com/cmvarmour)*
- [`06e0869`](https://github.com/tobymao/sqlglot/commit/06e0869e7aa5714d77e6ec763da38d6a422965fa) - unnest complex closes [#2284](https://github.com/tobymao/sqlglot/pull/2284) *(commit by [@tobymao](https://github.com/tobymao))*
- [`5aa7e2a`](https://github.com/tobymao/sqlglot/commit/5aa7e2a8e07cb41ee34c92309d6828e52a4ff64b) - **bigquery**: preserve log argument order when parsing and generation dialects match *(PR [#2293](https://github.com/tobymao/sqlglot/pull/2293) by [@cpcloud](https://github.com/cpcloud))*
  - :arrow_lower_right: *fixes issue [#2292](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`44f732d`](https://github.com/tobymao/sqlglot/commit/44f732d56eafb93880790955862bac454edf800f) - **parser**: make kwarg parsing more robust *(PR [#2295](https://github.com/tobymao/sqlglot/pull/2295) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2288](undefined) opened by [@crash-g](https://github.com/crash-g)*
- [`8fe91e2`](https://github.com/tobymao/sqlglot/commit/8fe91e25ff3a8a1c7619b6e50b5d3dcbd6f6521b) - **redshift**: generate correct SQL VALUES clause alias *(PR [#2298](https://github.com/tobymao/sqlglot/pull/2298) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`6429042`](https://github.com/tobymao/sqlglot/commit/642904248523c04f17ee67d7d075237f48a72159) - **bigquery**: anticipate OPTION property after JS UDF definition *(PR [#2297](https://github.com/tobymao/sqlglot/pull/2297) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2290](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`29550c1`](https://github.com/tobymao/sqlglot/commit/29550c131ce2d0209a11cd045a23683b6218c40f) - safedpipe is always varchar *(commit by [@tobymao](https://github.com/tobymao))*


## [v18.6.0] - 2023-09-21
### :boom: BREAKING CHANGES
- due to [`8100311`](https://github.com/tobymao/sqlglot/commit/8100311e68d79914dbcd1da0fd56cd963eb8c189) - explode to unnest with multiple explosions *(PR [#2235](https://github.com/tobymao/sqlglot/pull/2235) by [@tobymao](https://github.com/tobymao))*:

  explode to unnest with multiple explosions (#2235)

- due to [`ff19f4c`](https://github.com/tobymao/sqlglot/commit/ff19f4c4e1d735710bb945ced6d6c15af186c058) - don't parse SEMI, ANTI as table aliases, fix join side issue *(PR [#2247](https://github.com/tobymao/sqlglot/pull/2247) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  don't parse SEMI, ANTI as table aliases, fix join side issue (#2247)

- due to [`8ebbfe2`](https://github.com/tobymao/sqlglot/commit/8ebbfe214dbdff967bd0923b215dcd73ddca6bb1) - store expressions in Limit.expression when using builder *(PR [#2249](https://github.com/tobymao/sqlglot/pull/2249) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  store expressions in Limit.expression when using builder (#2249)

- due to [`4badd91`](https://github.com/tobymao/sqlglot/commit/4badd91549e172890653fdd54f96a7e878bf23c9) - preserve ascending order of sorting when present *(PR [#2256](https://github.com/tobymao/sqlglot/pull/2256) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  preserve ascending order of sorting when present (#2256)

- due to [`e90312a`](https://github.com/tobymao/sqlglot/commit/e90312a81f929dfd6af1fb6799cd77e0bf58ea47) - improve transpilation of T-SQL's SET assignment command *(PR [#2275](https://github.com/tobymao/sqlglot/pull/2275) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  improve transpilation of T-SQL's SET assignment command (#2275)


### :sparkles: New Features
- [`f63a06b`](https://github.com/tobymao/sqlglot/commit/f63a06b5b743db58ea62769e3ba605e44ab7f60a) - eliminate semi/anti joins transformation *(PR [#2242](https://github.com/tobymao/sqlglot/pull/2242) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2240](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`d1cfa01`](https://github.com/tobymao/sqlglot/commit/d1cfa01e999e494faea948c8959471deae6e4a2a) - **optimizer**: simplify date_trunc *(PR [#2271](https://github.com/tobymao/sqlglot/pull/2271) by [@barakalon](https://github.com/barakalon))*
- [`e90312a`](https://github.com/tobymao/sqlglot/commit/e90312a81f929dfd6af1fb6799cd77e0bf58ea47) - improve transpilation of T-SQL's SET assignment command *(PR [#2275](https://github.com/tobymao/sqlglot/pull/2275) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`69a2c67`](https://github.com/tobymao/sqlglot/commit/69a2c67f7713586a831cf1db236b555abe54b12f) - **optimizer**: simplify_equality *(PR [#2281](https://github.com/tobymao/sqlglot/pull/2281) by [@barakalon](https://github.com/barakalon))*
- [`ef062d1`](https://github.com/tobymao/sqlglot/commit/ef062d10c2f42d17ede45927bebc643c90f3eb79) - kill *(PR [#2285](https://github.com/tobymao/sqlglot/pull/2285) by [@barakalon](https://github.com/barakalon))*

### :bug: Bug Fixes
- [`66aadfc`](https://github.com/tobymao/sqlglot/commit/66aadfc60b9bffe914b89043b293a3e7357d5dde) - unescape escape sequences *(PR [#2230](https://github.com/tobymao/sqlglot/pull/2230) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2225](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`cd30eb7`](https://github.com/tobymao/sqlglot/commit/cd30eb765a4b733c1b12148609eb3bafa4983eab) - **tsql**: include catalog and db in create if not exists *(PR [#2231](https://github.com/tobymao/sqlglot/pull/2231) by [@treysp](https://github.com/treysp))*
- [`c2238a5`](https://github.com/tobymao/sqlglot/commit/c2238a5d551b8054f183a310b41cf1908bad1323) - **snowflake**: transpile SELECT UNNEST(x) to TABLE(FLATTEN(..)) *(PR [#2232](https://github.com/tobymao/sqlglot/pull/2232) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2228](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`8509d52`](https://github.com/tobymao/sqlglot/commit/8509d52a763169c4ff9529b58ed0ed04499984b5) - don't unnest subqueries when the parent select doesn't have a from since the subquery can't be joined *(PR [#2233](https://github.com/tobymao/sqlglot/pull/2233) by [@ginter](https://github.com/ginter))*
- [`8100311`](https://github.com/tobymao/sqlglot/commit/8100311e68d79914dbcd1da0fd56cd963eb8c189) - explode to unnest with multiple explosions *(PR [#2235](https://github.com/tobymao/sqlglot/pull/2235) by [@tobymao](https://github.com/tobymao))*
  - :arrow_lower_right: *fixes issue [#2227](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`1992ab9`](https://github.com/tobymao/sqlglot/commit/1992ab98347a1b3e9fdfbbe2657e6c7a29467685) - transpile bool xor closes [#2238](https://github.com/tobymao/sqlglot/pull/2238) *(commit by [@tobymao](https://github.com/tobymao))*
- [`12de208`](https://github.com/tobymao/sqlglot/commit/12de208ff99671df7f2cd18b8d9952fb3072336d) - **snowflake**: use IF instead of FILTER(WHERE cond) for conditional aggregation *(PR [#2241](https://github.com/tobymao/sqlglot/pull/2241) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2239](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`94d56be`](https://github.com/tobymao/sqlglot/commit/94d56bee6532a5713c89a147eb428837c1efe024) - **bigquery**: regex with raw strings compile closes [#2236](https://github.com/tobymao/sqlglot/pull/2236) *(commit by [@tobymao](https://github.com/tobymao))*
- [`ff19f4c`](https://github.com/tobymao/sqlglot/commit/ff19f4c4e1d735710bb945ced6d6c15af186c058) - don't parse SEMI, ANTI as table aliases, fix join side issue *(PR [#2247](https://github.com/tobymao/sqlglot/pull/2247) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2244](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`8ebbfe2`](https://github.com/tobymao/sqlglot/commit/8ebbfe214dbdff967bd0923b215dcd73ddca6bb1) - store expressions in Limit.expression when using builder *(PR [#2249](https://github.com/tobymao/sqlglot/pull/2249) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2248](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`829415c`](https://github.com/tobymao/sqlglot/commit/829415c3a9295cf281aad255c614b3b344cab8eb) - **snowflake**: rename GenerateSeries, include offset in unnest_sql *(PR [#2243](https://github.com/tobymao/sqlglot/pull/2243) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`ba013d6`](https://github.com/tobymao/sqlglot/commit/ba013d6ad795bf7455b523d765cf573e670aa2e6) - **presto**: treat struct with key-value definition as unsupported *(PR [#2245](https://github.com/tobymao/sqlglot/pull/2245) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2229](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`ab7effe`](https://github.com/tobymao/sqlglot/commit/ab7effe6186a756991cbd3f95e1a87b088978a39) - **parser**: check for column operators after having parsed brackets *(PR [#2251](https://github.com/tobymao/sqlglot/pull/2251) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2250](undefined) opened by [@wezham](https://github.com/wezham)*
- [`3b654f2`](https://github.com/tobymao/sqlglot/commit/3b654f24f91f47b424e14f48a45286b514dec30c) - **dataframe**: ensure Column.alias preserves quotes *(PR [#2254](https://github.com/tobymao/sqlglot/pull/2254) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`f76ebb2`](https://github.com/tobymao/sqlglot/commit/f76ebb257bd1938326a5c6c845fb93388179b423) - **tsql**: generate SELECT INTO from CTAS *(PR [#2237](https://github.com/tobymao/sqlglot/pull/2237) by [@treysp](https://github.com/treysp))*
- [`4badd91`](https://github.com/tobymao/sqlglot/commit/4badd91549e172890653fdd54f96a7e878bf23c9) - preserve ascending order of sorting when present *(PR [#2256](https://github.com/tobymao/sqlglot/pull/2256) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2253](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`86538ba`](https://github.com/tobymao/sqlglot/commit/86538bae5304c6c78254e36f6226734d42ab190b) - **bigquery**: reduce the scope where UNKNOWN is treated as NULL *(PR [#2260](https://github.com/tobymao/sqlglot/pull/2260) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2258](undefined) opened by [@middagj](https://github.com/middagj)*
- [`12c83b6`](https://github.com/tobymao/sqlglot/commit/12c83b69828af32ade4cb546042b0ea1b267d154) - **hive**: get rid of any ASC in a ClusteredColumnConstraint *(PR [#2261](https://github.com/tobymao/sqlglot/pull/2261) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`35927a2`](https://github.com/tobymao/sqlglot/commit/35927a2c8df0a9644556e47d3173b3629e8f0c08) - **tsql**: correctly escape single quotes in EXEC *(PR [#2263](https://github.com/tobymao/sqlglot/pull/2263) by [@treysp](https://github.com/treysp))*
- [`61f85a4`](https://github.com/tobymao/sqlglot/commit/61f85a4862240fdcdd4faf4847c14a755151091d) - **postgres**: parse RESTRICT constraint action *(PR [#2267](https://github.com/tobymao/sqlglot/pull/2267) by [@Nitrino](https://github.com/Nitrino))*
- [`d5b229a`](https://github.com/tobymao/sqlglot/commit/d5b229a65030c5b75ba30e795f63f8615a473be6) - **snowflake**: implement correct semantics of EXCEPT, RENAME *(PR [#2268](https://github.com/tobymao/sqlglot/pull/2268) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2265](undefined) opened by [@diogo-fernan](https://github.com/diogo-fernan)*
- [`ccff88c`](https://github.com/tobymao/sqlglot/commit/ccff88c5eea32d9a940dcfe79a721d331bd8c697) - **postgres**: add support for WHERE clause in CREATE INDEX *(PR [#2269](https://github.com/tobymao/sqlglot/pull/2269) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2266](undefined) opened by [@Nitrino](https://github.com/Nitrino)*
- [`ed8714f`](https://github.com/tobymao/sqlglot/commit/ed8714f1e2f2ab1052a9cbf3fe34b4ab3e937ee5) - **bigquery**: allow overlaps to be used as an identifier *(PR [#2273](https://github.com/tobymao/sqlglot/pull/2273) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2272](undefined) opened by [@turntable-justin](https://github.com/turntable-justin)*
- [`76b7077`](https://github.com/tobymao/sqlglot/commit/76b707727f374c8d94395674fcf0b50acdb2e5cf) - ensure Expression is not an iterable to avoid inf. loops *(PR [#2280](https://github.com/tobymao/sqlglot/pull/2280) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`6475b84`](https://github.com/tobymao/sqlglot/commit/6475b84d084783912ea00c8d634f3c5bc456c4c4) - remove copy *(PR [#2282](https://github.com/tobymao/sqlglot/pull/2282) by [@barakalon](https://github.com/barakalon))*
- [`310d691`](https://github.com/tobymao/sqlglot/commit/310d691d836637c48d4a7fb281a5eddab5926c2c) - improve performance of VALUES -> UNION ALL transpilation *(PR [#2283](https://github.com/tobymao/sqlglot/pull/2283) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`0e996f5`](https://github.com/tobymao/sqlglot/commit/0e996f55591be016eb21ae8838be7d6543d698eb) - cleanup *(commit by [@tobymao](https://github.com/tobymao))*
- [`bca7570`](https://github.com/tobymao/sqlglot/commit/bca757031e801d94c90a3c7c45b4a0b9e644bba0) - cleanup pop *(commit by [@tobymao](https://github.com/tobymao))*


## [v18.5.1] - 2023-09-15
### :sparkles: New Features
- [`0378325`](https://github.com/tobymao/sqlglot/commit/03783258d5229f338568cd838d8a454e698274c5) - improve support for percentiles in duckdb, postgres *(PR [#2219](https://github.com/tobymao/sqlglot/pull/2219) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`b59ef0f`](https://github.com/tobymao/sqlglot/commit/b59ef0f6abbe90310b56a4cea72f0850c22e1086) - add support for scoped user-defined types *(PR [#2226](https://github.com/tobymao/sqlglot/pull/2226) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2217](undefined) opened by [@sashindeitidata](https://github.com/sashindeitidata)*

### :bug: Bug Fixes
- [`b3c97de`](https://github.com/tobymao/sqlglot/commit/b3c97decb98ca237e3ccac87e053e2a25419522c) - **mysql**: timestamp add/sub closes [#2214](https://github.com/tobymao/sqlglot/pull/2214) *(commit by [@tobymao](https://github.com/tobymao))*
- [`4634220`](https://github.com/tobymao/sqlglot/commit/46342204037afb91f3c865b367d70fe4f8116584) - use parse primary in the sample parser to handle nums like .25 *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`fd1ed25`](https://github.com/tobymao/sqlglot/commit/fd1ed25210340bce4292a1ff698b26198ff8bf57) - **oracle**: add support for locking reads fixes [#2216](https://github.com/tobymao/sqlglot/pull/2216) *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5ec8e1f`](https://github.com/tobymao/sqlglot/commit/5ec8e1f58b3e5fa62682529d0b0bf5a507659878) - normalize before qualifying tables *(commit by [@tobymao](https://github.com/tobymao))*
- [`da398f4`](https://github.com/tobymao/sqlglot/commit/da398f49b276eb0598d85e8cec5f17aca2a41361) - parse and generate JSON <literal> correctly *(PR [#2220](https://github.com/tobymao/sqlglot/pull/2220) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`6bc8e13`](https://github.com/tobymao/sqlglot/commit/6bc8e132b2f067577702c26299eafae5fa45f2f9) - **mysql**: transpile ISNULL to IS NULL *(PR [#2221](https://github.com/tobymao/sqlglot/pull/2221) by [@barakalon](https://github.com/barakalon))*
- [`2fa4043`](https://github.com/tobymao/sqlglot/commit/2fa40435d0d87ed02ee9556575909ea28e550868) - **mysql**: transpile MONTHNAME *(PR [#2222](https://github.com/tobymao/sqlglot/pull/2222) by [@barakalon](https://github.com/barakalon))*
- [`857e380`](https://github.com/tobymao/sqlglot/commit/857e38075945ee0057fdfb4140d9636c0400c587) - **mysql**: TIMESTAMP -> CAST *(PR [#2223](https://github.com/tobymao/sqlglot/pull/2223) by [@barakalon](https://github.com/barakalon))*


## [v18.5.0] - 2023-09-13
### :sparkles: New Features
- [`72e939e`](https://github.com/tobymao/sqlglot/commit/72e939e901eb0b2adde6f66ebe31bb8c498f70c6) - **parser**: allow functions in FETCH clause *(PR [#2207](https://github.com/tobymao/sqlglot/pull/2207) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2204](undefined) opened by [@sashindeitidata](https://github.com/sashindeitidata)*
- [`d944e17`](https://github.com/tobymao/sqlglot/commit/d944e1747b8729b9b0cbec5664711eff12c53cea) - **mysql**: add support for [UN]LOCK TABLES as a Command *(PR [#2212](https://github.com/tobymao/sqlglot/pull/2212) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2210](undefined) opened by [@Nitrino](https://github.com/Nitrino)*

### :bug: Bug Fixes
- [`416b341`](https://github.com/tobymao/sqlglot/commit/416b341c45cd0a766a9919cc5a11b5f90dc3b3f3) - use SUPPORTS_USER_DEFINED_TYPES to set udt in schema _to_data_type *(PR [#2203](https://github.com/tobymao/sqlglot/pull/2203) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`14c1b3b`](https://github.com/tobymao/sqlglot/commit/14c1b3b60c3d7473d488b78293055be2db428add) - **mysql**: add support for index type in the UNIQUE KEY constraint *(PR [#2211](https://github.com/tobymao/sqlglot/pull/2211) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2209](undefined) opened by [@Nitrino](https://github.com/Nitrino)*
- [`5c7210a`](https://github.com/tobymao/sqlglot/commit/5c7210ab113c94da0c77a05166a2e8d452764c84) - **oracle**: allow CONNECT BY / START WITH to be interchanged *(PR [#2208](https://github.com/tobymao/sqlglot/pull/2208) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2205](undefined) opened by [@sashindeitidata](https://github.com/sashindeitidata)*


## [v18.4.1] - 2023-09-12
### :boom: BREAKING CHANGES
- due to [`f85b535`](https://github.com/tobymao/sqlglot/commit/f85b535f2b74279d63cc60c456ecdc73096389f5) - parse schema UDTs into DataTypes instead of identifiers *(PR [#2201](https://github.com/tobymao/sqlglot/pull/2201) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  parse schema UDTs into DataTypes instead of identifiers (#2201)


### :bug: Bug Fixes
- [`a228656`](https://github.com/tobymao/sqlglot/commit/a2286563303d98570cb73104795821970382ed3a) - **tokenizer**: treat quote as escape only if its followed by itself *(PR [#2199](https://github.com/tobymao/sqlglot/pull/2199) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2198](undefined) opened by [@czrobert](https://github.com/czrobert)*
- [`fa09688`](https://github.com/tobymao/sqlglot/commit/fa09688a422d322a60bd539f537bd9e6bf49017a) - **mysql**: for update regression due to list comprehensions closes [#2200](https://github.com/tobymao/sqlglot/pull/2200) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f85b535`](https://github.com/tobymao/sqlglot/commit/f85b535f2b74279d63cc60c456ecdc73096389f5) - parse schema UDTs into DataTypes instead of identifiers *(PR [#2201](https://github.com/tobymao/sqlglot/pull/2201) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.4.0] - 2023-09-12
### :sparkles: New Features
- [`5e2042a`](https://github.com/tobymao/sqlglot/commit/5e2042aaa0e4be08d02c369a660d3b37ce78b567) - add TINYTEXT and TINYBLOB types *(PR [#2182](https://github.com/tobymao/sqlglot/pull/2182) by [@Nitrino](https://github.com/Nitrino))*
- [`0c536bd`](https://github.com/tobymao/sqlglot/commit/0c536bd3ca0fc0bf0d9ba649281530faf53304dd) - **oracle**: add support for JSON_ARRAYAGG *(PR [#2189](https://github.com/tobymao/sqlglot/pull/2189) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`f4e3e09`](https://github.com/tobymao/sqlglot/commit/f4e3e095c5eebc347f5d95e41fd68252af9b13bc) - **oracle**: add support for JSON_TABLE *(PR [#2191](https://github.com/tobymao/sqlglot/pull/2191) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2187](undefined) opened by [@sashindeitidata](https://github.com/sashindeitidata)*
- [`11d95ff`](https://github.com/tobymao/sqlglot/commit/11d95ff3ece4691aa4d766c60c6765cd8a68589a) - add redshift concat_ws support *(PR [#2194](https://github.com/tobymao/sqlglot/pull/2194) by [@eakmanrq](https://github.com/eakmanrq))*

### :bug: Bug Fixes
- [`c7433bf`](https://github.com/tobymao/sqlglot/commit/c7433bfe5086eb66895b43514eb4edfa56eb1228) - join using with star *(commit by [@tobymao](https://github.com/tobymao))*
- [`451439c`](https://github.com/tobymao/sqlglot/commit/451439c84a8feda05d51c47180c9f69cc92f22d6) - **clickhouse**: add missing type mappings for string types *(PR [#2183](https://github.com/tobymao/sqlglot/pull/2183) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5ba5165`](https://github.com/tobymao/sqlglot/commit/5ba51657bc810139a28603b1bb542d44173bdc55) - **duckdb**: rename VariancePop -> var_pop in DuckDB *(PR [#2184](https://github.com/tobymao/sqlglot/pull/2184) by [@gforsyth](https://github.com/gforsyth))*
- [`d192515`](https://github.com/tobymao/sqlglot/commit/d19251566424ba07efe46b3be4ac6bbe327e7821) - **optimizer**: merge subqueries should use alias from outer scope *(PR [#2185](https://github.com/tobymao/sqlglot/pull/2185) by [@barakalon](https://github.com/barakalon))*
- [`12db377`](https://github.com/tobymao/sqlglot/commit/12db377ea8b07b1ff418dc988ef1ea4c20288206) - **mysql**: multi table update closes [#2193](https://github.com/tobymao/sqlglot/pull/2193) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b9f5ede`](https://github.com/tobymao/sqlglot/commit/b9f5edee02aed346ebaea767274cc08e3960419b) - **oracle**: make parentheses in JSON_TABLE's COLUMNS clause optional *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`8c51275`](https://github.com/tobymao/sqlglot/commit/8c512750044efa059adc3afee32517684dabfc12) - **mysql**: parse column prefix in index / pk defn. correctly *(PR [#2197](https://github.com/tobymao/sqlglot/pull/2197) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2195](undefined) opened by [@Nitrino](https://github.com/Nitrino)*

### :recycle: Refactors
- [`a81dd14`](https://github.com/tobymao/sqlglot/commit/a81dd14a6de1a50438eae64c2dd20e4841c29572) - override Bracket.output_name only when there's one bracket expression *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`7ae5a94`](https://github.com/tobymao/sqlglot/commit/7ae5a9463cd68371f6ed45b9e00582eb44cead3b) - fix mutation bug in Column.to_dot, simplify Dot.build *(PR [#2196](https://github.com/tobymao/sqlglot/pull/2196) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`981ad23`](https://github.com/tobymao/sqlglot/commit/981ad23cd1bf2b95e121bb9a7f3b677d4a053be4) - **duckdb**: fix var_pop tests *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.3.0] - 2023-09-07
### :boom: BREAKING CHANGES
- due to [`3fc2eb5`](https://github.com/tobymao/sqlglot/commit/3fc2eb581528504db4523c3e0a537000e026a4cc) - improve support for interval spans like HOUR TO SECOND *(PR [#2167](https://github.com/tobymao/sqlglot/pull/2167) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  improve support for interval spans like HOUR TO SECOND (#2167)

- due to [`93b7ba2`](https://github.com/tobymao/sqlglot/commit/93b7ba20640a880ceeb63660b796ab94579bb73a) - MySQL Timestamp Data Types *(PR [#2173](https://github.com/tobymao/sqlglot/pull/2173) by [@eakmanrq](https://github.com/eakmanrq))*:

  MySQL Timestamp Data Types (#2173)


### :sparkles: New Features
- [`5dd0fda`](https://github.com/tobymao/sqlglot/commit/5dd0fdaaf9ec8bc5f9f0a2cd01395222eacf28a0) - **spark**: add support for raw strings *(PR [#2165](https://github.com/tobymao/sqlglot/pull/2165) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2162](undefined) opened by [@aersam](https://github.com/aersam)*
- [`d9f8910`](https://github.com/tobymao/sqlglot/commit/d9f89109e9795685392adb43bc2e87fbd346f263) - **teradata**: add support for the SAMPLE clause *(PR [#2169](https://github.com/tobymao/sqlglot/pull/2169) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`63ac621`](https://github.com/tobymao/sqlglot/commit/63ac621f7507d35ccdc32784ec0631437ddf0c1b) - **mysql**: improve support for unsigned int types *(PR [#2172](https://github.com/tobymao/sqlglot/pull/2172) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2166](undefined) opened by [@Nitrino](https://github.com/Nitrino)*
- [`cd301cc`](https://github.com/tobymao/sqlglot/commit/cd301cc9aa7a910fc6f7f0b9cc2dbba9a7d9ea24) - **postgres**: add support for ALTER TABLE ONLY ... *(PR [#2179](https://github.com/tobymao/sqlglot/pull/2179) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2178](undefined) opened by [@Nitrino](https://github.com/Nitrino)*

### :bug: Bug Fixes
- [`3fc2eb5`](https://github.com/tobymao/sqlglot/commit/3fc2eb581528504db4523c3e0a537000e026a4cc) - improve support for interval spans like HOUR TO SECOND *(PR [#2167](https://github.com/tobymao/sqlglot/pull/2167) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2163](undefined) opened by [@aersam](https://github.com/aersam)*
- [`93b7ba2`](https://github.com/tobymao/sqlglot/commit/93b7ba20640a880ceeb63660b796ab94579bb73a) - MySQL Timestamp Data Types *(PR [#2173](https://github.com/tobymao/sqlglot/pull/2173) by [@eakmanrq](https://github.com/eakmanrq))*
- [`6d761f9`](https://github.com/tobymao/sqlglot/commit/6d761f9934fcf57a06fb4645e43ce91dca6adc96) - filter_sql use strip closes [#2180](https://github.com/tobymao/sqlglot/pull/2180) *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`5fbe303`](https://github.com/tobymao/sqlglot/commit/5fbe303504f19a1c949d0acf777c2bf2d3ecc1b6) - add minimum python version required to setup.py *(PR [#2170](https://github.com/tobymao/sqlglot/pull/2170) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2168](undefined) opened by [@jlardieri5](https://github.com/jlardieri5)*


## [v18.2.0] - 2023-09-05
### :sparkles: New Features
- [`5df9b5f`](https://github.com/tobymao/sqlglot/commit/5df9b5f658d24267e4f6b00bd89eb0b2f4dc5bfc) - **snowflake**: desc table type closes [#2145](https://github.com/tobymao/sqlglot/pull/2145) *(commit by [@tobymao](https://github.com/tobymao))*
- [`a48051c`](https://github.com/tobymao/sqlglot/commit/a48051cd2d4c1c141f6c98bdc31cccb7700f5b4c) - **teradata**: improve support for DATABASE statement *(PR [#2160](https://github.com/tobymao/sqlglot/pull/2160) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2159](undefined) opened by [@Ashay4u](https://github.com/Ashay4u)*

### :bug: Bug Fixes
- [`d4d7a5c`](https://github.com/tobymao/sqlglot/commit/d4d7a5c57179ecc7c89afdd273e349e23b4f9849) - **bigquery**: allow numbers in table name *(commit by [@tobymao](https://github.com/tobymao))*
- [`7b589ae`](https://github.com/tobymao/sqlglot/commit/7b589ae2ce4071f004b0cd3eedab5fca4deb79bb) - **snowflake**: better support for LISTAGG *(PR [#2147](https://github.com/tobymao/sqlglot/pull/2147) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2146](undefined) opened by [@alonc-sygnia](https://github.com/alonc-sygnia)*
- [`9c4a9cd`](https://github.com/tobymao/sqlglot/commit/9c4a9cdb6f76e57018a2342fc1309f85bf643ab4) - **hive**: parse <number> <date_part> as an interval instead of an alias *(PR [#2151](https://github.com/tobymao/sqlglot/pull/2151) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2123](undefined) opened by [@liujiwen-up](https://github.com/liujiwen-up)*
- [`585d0bf`](https://github.com/tobymao/sqlglot/commit/585d0bfcbd40125492640480c890af328d8bc51f) - tokenizer with ambious keywords *(commit by [@tobymao](https://github.com/tobymao))*
- [`f0bddde`](https://github.com/tobymao/sqlglot/commit/f0bddde63d47a620592261e1a1810aabcd8ec800) - **oracle**: remove COALESCE -> NVL mapping fixes [#2158](https://github.com/tobymao/sqlglot/pull/2158) *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`dcacef1`](https://github.com/tobymao/sqlglot/commit/dcacef1234ac75b708cc8416b349d3a6a47f59b6) - **parser**: use _parse_bitwise for values of JSON_OBJECT *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`db1303c`](https://github.com/tobymao/sqlglot/commit/db1303ca64ae25d0e75f9cd232c56d633ccdd851) - **parser**: solve interval parsing bug *(PR [#2157](https://github.com/tobymao/sqlglot/pull/2157) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2154](undefined) opened by [@liujiwen-up](https://github.com/liujiwen-up)*
- [`36ac469`](https://github.com/tobymao/sqlglot/commit/36ac469de9a9292bfcb89cd564af88e786ce584b) - remove inconsistent quotes from eliminate distinct on transform *(commit by [@tobymao](https://github.com/tobymao))*

### :wrench: Chores
- [`f934575`](https://github.com/tobymao/sqlglot/commit/f93457578944ff20f7ceef93ac52255ed6b10eca) - improve type hints for dialects *(PR [#2152](https://github.com/tobymao/sqlglot/pull/2152) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.1.0] - 2023-09-01
### :boom: BREAKING CHANGES
- due to [`f3fee3a`](https://github.com/tobymao/sqlglot/commit/f3fee3a928a6fec81e2e39ba97f86ed7f8015f15) - iceberg version/timestamp snapshots, bigquery, refactor tsql closes [#2128](https://github.com/tobymao/sqlglot/pull/2128) *(commit by [@tobymao](https://github.com/tobymao))*:

  iceberg version/timestamp snapshots, bigquery, refactor tsql closes #2128


### :sparkles: New Features
- [`f3fee3a`](https://github.com/tobymao/sqlglot/commit/f3fee3a928a6fec81e2e39ba97f86ed7f8015f15) - iceberg version/timestamp snapshots, bigquery, refactor tsql closes [#2128](https://github.com/tobymao/sqlglot/pull/2128) *(commit by [@tobymao](https://github.com/tobymao))*
- [`30e3e2f`](https://github.com/tobymao/sqlglot/commit/30e3e2fe7c77d122bac02fd69ddf1b344d414eb3) - add support for transpiling some datetime functions from bq to databricks *(PR [#2142](https://github.com/tobymao/sqlglot/pull/2142) by [@fireis](https://github.com/fireis))*

### :bug: Bug Fixes
- [`632ad59`](https://github.com/tobymao/sqlglot/commit/632ad599cd6e3d6c4e6f6b8bf75a3b4018c6946c) - **mysql**: binary x parsing closes [#2130](https://github.com/tobymao/sqlglot/pull/2130) *(commit by [@tobymao](https://github.com/tobymao))*
- [`999a950`](https://github.com/tobymao/sqlglot/commit/999a950e7ff6709b5785fc97da058d1227de5eb5) - **optimizer**: dont simplify parens with multiple predicates closes [#2131](https://github.com/tobymao/sqlglot/pull/2131) *(commit by [@tobymao](https://github.com/tobymao))*
- [`bf7af1f`](https://github.com/tobymao/sqlglot/commit/bf7af1fe71bf1b2fc28161d0e77fe407775f30c8) - **parser**: support order in update statement *(PR [#2134](https://github.com/tobymao/sqlglot/pull/2134) by [@brosoul](https://github.com/brosoul))*
- [`7a27931`](https://github.com/tobymao/sqlglot/commit/7a279317608346610e9351f204c15500dd6e63bd) - **bigquery**: parse JSON_OBJECT properly for key-value pairs *(PR [#2136](https://github.com/tobymao/sqlglot/pull/2136) by [@middagj](https://github.com/middagj))*
- [`2ad559d`](https://github.com/tobymao/sqlglot/commit/2ad559d3997d87a0fb899164cdf938de3a6e4f0c) - **mysql**: generate JSON_OBJECT properly *(PR [#2139](https://github.com/tobymao/sqlglot/pull/2139) by [@middagj](https://github.com/middagj))*
- [`bd96d0c`](https://github.com/tobymao/sqlglot/commit/bd96d0c1a1928f506516caf11a093c0dd2180ce2) - **tsql**: support adding multiple columns with ALTER TABLE *(PR [#2140](https://github.com/tobymao/sqlglot/pull/2140) by [@treysp](https://github.com/treysp))*
- [`32d8e54`](https://github.com/tobymao/sqlglot/commit/32d8e5423a7d7e1b56805fd0020b4aac3ce15d84) - row number in transform requires order by *(commit by [@tobymao](https://github.com/tobymao))*
- [`39bce6d`](https://github.com/tobymao/sqlglot/commit/39bce6d4e607e620418713940b8729c07e1751f5) - **mysql**: allow unquoted identifiers that start with a number *(PR [#2141](https://github.com/tobymao/sqlglot/pull/2141) by [@middagj](https://github.com/middagj))*


## [v18.0.1] - 2023-08-30
### :sparkles: New Features
- [`c3d013b`](https://github.com/tobymao/sqlglot/commit/c3d013b0d6404c8eb9eb9acc6be4664d0fddcfa6) - **databricks**: shallow clone *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`be56bc0`](https://github.com/tobymao/sqlglot/commit/be56bc094ef8fed5b87af6d7ab6a8e15e95e2b15) - optimizer join needs on condition *(commit by [@tobymao](https://github.com/tobymao))*
- [`cc0a6e2`](https://github.com/tobymao/sqlglot/commit/cc0a6e2f362af447db990825357f00d4f512e805) - **tsql**: generate IDENTITY instead of AUTO_INCREMENT *(PR [#2127](https://github.com/tobymao/sqlglot/pull/2127) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2126](undefined) opened by [@rozhnev](https://github.com/rozhnev)*
- [`d1ccb03`](https://github.com/tobymao/sqlglot/commit/d1ccb038266aa49fe175ecf8ecaf699f959e589f) - **bigquery**: unnest with structs closes [#2125](https://github.com/tobymao/sqlglot/pull/2125) *(commit by [@tobymao](https://github.com/tobymao))*
- [`6a0110a`](https://github.com/tobymao/sqlglot/commit/6a0110aafa0a861b51a610fa89c421e951b35f9a) - create the struct key properly in struct_extract_sql *(PR [#2129](https://github.com/tobymao/sqlglot/pull/2129) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v18.0.0] - 2023-08-29
### :bug: Bug Fixes
- [`c9d679b`](https://github.com/tobymao/sqlglot/commit/c9d679b6a3e665004f5df824bf3d4828cc2d4ab9) - parse INTERVAL ... YEAR *(PR [#2122](https://github.com/tobymao/sqlglot/pull/2122) by [@barakalon](https://github.com/barakalon))*
- [`d5bae81`](https://github.com/tobymao/sqlglot/commit/d5bae81e53220f382d7e3bc15fb0afe63b1a502e) - table with empty identifiers *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.16.2] - 2023-08-28
### :sparkles: New Features
- [`5b14f52`](https://github.com/tobymao/sqlglot/commit/5b14f521ad37eb33fbfad7fdd1537b47390c3e65) - **duckdb**: by_name modifiers [#2118](https://github.com/tobymao/sqlglot/pull/2118) *(commit by [@tobymao](https://github.com/tobymao))*
- [`cf2789f`](https://github.com/tobymao/sqlglot/commit/cf2789f35301339060feb426e8d630e2c398d083) - **duckdb**: allow selects in from leading syntax [#2118](https://github.com/tobymao/sqlglot/pull/2118) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`167d298`](https://github.com/tobymao/sqlglot/commit/167d2985d33448f04af9a4a4153deccecef2f610) - **presto**: transpile DATE_SUB *(PR [#2120](https://github.com/tobymao/sqlglot/pull/2120) by [@barakalon](https://github.com/barakalon))*


## [v17.16.1] - 2023-08-26
### :sparkles: New Features
- [`6e0cfbe`](https://github.com/tobymao/sqlglot/commit/6e0cfbee31507ed6ef8321bdb21dfd7191d976ed) - **tsql**: partition schema closes [#2115](https://github.com/tobymao/sqlglot/pull/2115) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`fe37fc7`](https://github.com/tobymao/sqlglot/commit/fe37fc7ba274e73a66ee1bdf41ce3ceced2f3010) - **tsql**: create if not exist index *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.16.0] - 2023-08-26
### :sparkles: New Features
- [`ca5c999`](https://github.com/tobymao/sqlglot/commit/ca5c9998258518fa939edef25fc7140070d25263) - start with connect by closes [#2112](https://github.com/tobymao/sqlglot/pull/2112) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`0316f7f`](https://github.com/tobymao/sqlglot/commit/0316f7f73ef8c7e065dd3e373c0eea98cb0c3156) - **presto**: datetime -> timestamp *(PR [#2113](https://github.com/tobymao/sqlglot/pull/2113) by [@barakalon](https://github.com/barakalon))*


## [v17.15.1] - 2023-08-24
### :sparkles: New Features
- [`dc5836c`](https://github.com/tobymao/sqlglot/commit/dc5836c3d82ec85c9f2bb98a6f0065ef66b80f43) - **tsql**: table constraints closes [#2106](https://github.com/tobymao/sqlglot/pull/2106) *(commit by [@tobymao](https://github.com/tobymao))*
- [`bda94df`](https://github.com/tobymao/sqlglot/commit/bda94dfebc169c1a8bb07330d3d0c8997fabcf17) - **tsql**: not for replication closes [#2107](https://github.com/tobymao/sqlglot/pull/2107) *(commit by [@tobymao](https://github.com/tobymao))*
- [`c99bf73`](https://github.com/tobymao/sqlglot/commit/c99bf7332bf878b81a6a95d0738dde03a7b990a1) - mysql mediumint and year types closes [#2109](https://github.com/tobymao/sqlglot/pull/2109) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`fb8a0b6`](https://github.com/tobymao/sqlglot/commit/fb8a0b62433ff4178f4c963b17d7e98d62c8dfb2) - regxpilike for snowflake *(commit by [@tobymao](https://github.com/tobymao))*
- [`f62f35c`](https://github.com/tobymao/sqlglot/commit/f62f35cf7f6c8cef567b80ee9c7c46282f736875) - clickhouse dateadd/datediff closes [#2108](https://github.com/tobymao/sqlglot/pull/2108) *(commit by [@tobymao](https://github.com/tobymao))*
- [`b0d82ea`](https://github.com/tobymao/sqlglot/commit/b0d82eaf9c18cd44dabe39428675bb4eb1a17f42) - property with ON keyword closes [#2111](https://github.com/tobymao/sqlglot/pull/2111) *(commit by [@tobymao](https://github.com/tobymao))*
- [`12bc916`](https://github.com/tobymao/sqlglot/commit/12bc91662cb7daf8ab9afd687cc7bdb981212ddd) - **tsql**: single quotes in if not exists *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.15.0] - 2023-08-22
### :boom: BREAKING CHANGES
- due to [`1da653f`](https://github.com/tobymao/sqlglot/commit/1da653f64f91556e5a32b5a513f5404886da0c37) - Have Spark put CTE at front of insert *(PR [#2086](https://github.com/tobymao/sqlglot/pull/2086) by [@eakmanrq](https://github.com/eakmanrq))*:

  Have Spark put CTE at front of insert (#2086)

- due to [`edb9a96`](https://github.com/tobymao/sqlglot/commit/edb9a9659a9989b6fd3a3626e33244abd81f4e52) - rename DataTypeSize -> DataTypeParam *(PR [#2097](https://github.com/tobymao/sqlglot/pull/2097) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  rename DataTypeSize -> DataTypeParam (#2097)

- due to [`28a0e20`](https://github.com/tobymao/sqlglot/commit/28a0e20fe108bd7b2e79b47d9081f8f47751643b) - add support for casting to user defined types *(PR [#2096](https://github.com/tobymao/sqlglot/pull/2096) by [@GeorgeSittas](https://github.com/GeorgeSittas))*:

  add support for casting to user defined types (#2096)

- due to [`075849f`](https://github.com/tobymao/sqlglot/commit/075849fccbf5377b189e971a8cdb30cc0bf0f6e9) - allow types to be identifiers closes [#2102](https://github.com/tobymao/sqlglot/pull/2102) *(commit by [@tobymao](https://github.com/tobymao))*:

  allow types to be identifiers closes #2102


### :sparkles: New Features
- [`33220b9`](https://github.com/tobymao/sqlglot/commit/33220b9cca8bb9f77aab2ff1fb47329cb8c9318f) - add more funcs to scope module *(commit by [@tobymao](https://github.com/tobymao))*
- [`2922bb0`](https://github.com/tobymao/sqlglot/commit/2922bb006359d0ffb1647265da3142b1da7367d2) - postgres full text search @@ closes [#2066](https://github.com/tobymao/sqlglot/pull/2066) *(commit by [@tobymao](https://github.com/tobymao))*
- [`c5dc9ac`](https://github.com/tobymao/sqlglot/commit/c5dc9acdfeb715de1c219eb228fe2dda1b9af497) - **oracle**: add support for old-style SELECT UNIQUE .. syntax *(PR [#2076](https://github.com/tobymao/sqlglot/pull/2076) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2074](undefined) opened by [@sashindeitidata](https://github.com/sashindeitidata)*
- [`6426c1f`](https://github.com/tobymao/sqlglot/commit/6426c1f8319ec2e7b3ad586a9946db6f1653d815) - **tsql**: transpile CREATE SCHEMA IF NOT EXISTS to dynamic SQL *(PR [#2083](https://github.com/tobymao/sqlglot/pull/2083) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2065](undefined) opened by [@deschman](https://github.com/deschman)*
- [`19300a8`](https://github.com/tobymao/sqlglot/commit/19300a80860d39bd6ff68bb77c2c012f2a2320ed) - **oracle**: add support for $, # symbols *(PR [#2095](https://github.com/tobymao/sqlglot/pull/2095) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2090](undefined) opened by [@sashindeitidata](https://github.com/sashindeitidata)*
- [`0b9a575`](https://github.com/tobymao/sqlglot/commit/0b9a575359e9b2e8b4412298262701abb6196a18) - **tsql**: update statistics cmd closes [#2101](https://github.com/tobymao/sqlglot/pull/2101) *(commit by [@tobymao](https://github.com/tobymao))*
- [`28a0e20`](https://github.com/tobymao/sqlglot/commit/28a0e20fe108bd7b2e79b47d9081f8f47751643b) - add support for casting to user defined types *(PR [#2096](https://github.com/tobymao/sqlglot/pull/2096) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2091](undefined) opened by [@sashindeitidata](https://github.com/sashindeitidata)*
- [`2843817`](https://github.com/tobymao/sqlglot/commit/2843817abf1dca97ffaa042741d6548180e93584) - add snowflake/tsql insert/stuff *(commit by [@tobymao](https://github.com/tobymao))*
- [`1fa5056`](https://github.com/tobymao/sqlglot/commit/1fa50566f9cab99e7c100cd9f95dca7e4f38e994) - tsql computed column exp closes [#2104](https://github.com/tobymao/sqlglot/pull/2104) *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`3c01cbf`](https://github.com/tobymao/sqlglot/commit/3c01cbfe73fea0e4e7a5858c471cad0939bfcfdc) - **presto**: allow REGEXP_REPLACE with 2 arguments *(PR [#2073](https://github.com/tobymao/sqlglot/pull/2073) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2072](undefined) opened by [@dmelchor-stripe](https://github.com/dmelchor-stripe)*
- [`3c493e9`](https://github.com/tobymao/sqlglot/commit/3c493e9ce7d273bfe662ee132d0048fda840a24b) - **optimizer**: solve an infinite loop problem in simplify *(PR [#2071](https://github.com/tobymao/sqlglot/pull/2071) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2068](undefined) opened by [@powerAmore](https://github.com/powerAmore)*
- [`c1febf2`](https://github.com/tobymao/sqlglot/commit/c1febf225413bdf7d3da0bf384fdcf31b2a6ac2a) - **duckdb**: improve struct kwarg parsing *(PR [#2082](https://github.com/tobymao/sqlglot/pull/2082) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2080](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`1da653f`](https://github.com/tobymao/sqlglot/commit/1da653f64f91556e5a32b5a513f5404886da0c37) - Have Spark put CTE at front of insert *(PR [#2086](https://github.com/tobymao/sqlglot/pull/2086) by [@eakmanrq](https://github.com/eakmanrq))*
- [`1aafe6e`](https://github.com/tobymao/sqlglot/commit/1aafe6e3e606603a5188696c0bca036ccdc1673a) - allow placeholders to be any ID_VAR token *(PR [#2093](https://github.com/tobymao/sqlglot/pull/2093) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2089](undefined) opened by [@sashindeitidata](https://github.com/sashindeitidata)*
- [`6fa1581`](https://github.com/tobymao/sqlglot/commit/6fa1581ae0bdf7e0073a7b9a355e72241e5e6959) - interval::int cast closes [#2098](https://github.com/tobymao/sqlglot/pull/2098) *(commit by [@tobymao](https://github.com/tobymao))*
- [`075849f`](https://github.com/tobymao/sqlglot/commit/075849fccbf5377b189e971a8cdb30cc0bf0f6e9) - allow types to be identifiers closes [#2102](https://github.com/tobymao/sqlglot/pull/2102) *(commit by [@tobymao](https://github.com/tobymao))*
- [`e474aa0`](https://github.com/tobymao/sqlglot/commit/e474aa05ba4d87d33ea36b852fcba4dae12b1253) - array<unknown> *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`edb9a96`](https://github.com/tobymao/sqlglot/commit/edb9a9659a9989b6fd3a3626e33244abd81f4e52) - rename DataTypeSize -> DataTypeParam *(PR [#2097](https://github.com/tobymao/sqlglot/pull/2097) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2087](undefined) opened by [@kszucs](https://github.com/kszucs)*

### :wrench: Chores
- [`afe0286`](https://github.com/tobymao/sqlglot/commit/afe02861748d2addcba3550b4e0ca066ca52bd2b) - remove unnecessary class constants *(PR [#2094](https://github.com/tobymao/sqlglot/pull/2094) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`a20794a`](https://github.com/tobymao/sqlglot/commit/a20794ab986b3b6401b016132cf2c5e36d50f4a9) - cleanup types and add sort by alias for hive *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.14.2] - 2023-08-15
### :sparkles: New Features
- [`cd2628a`](https://github.com/tobymao/sqlglot/commit/cd2628ac6c6ee19fee33ee8dda663c8593cc3398) - convert ANY_VALUE to MAX for some dialects *(PR [#2058](https://github.com/tobymao/sqlglot/pull/2058) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`d92a5b7`](https://github.com/tobymao/sqlglot/commit/d92a5b73895ed3d843f44e4b24b68bf283376ee6) - **optimizer**: improve type annotation for nested types *(PR [#2061](https://github.com/tobymao/sqlglot/pull/2061) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2060](undefined) opened by [@eric-zhu](https://github.com/eric-zhu)*

### :bug: Bug Fixes
- [`21b061f`](https://github.com/tobymao/sqlglot/commit/21b061f186e554ff557a43aa820061ab33750266) - escape sequence warnings closes [#2059](https://github.com/tobymao/sqlglot/pull/2059) *(commit by [@tobymao](https://github.com/tobymao))*
- [`56a3d89`](https://github.com/tobymao/sqlglot/commit/56a3d89efb636e6d0de2a6c2f54d3bca68c0894c) - **spark**: handle MAP(..) without arguments correctly *(PR [#2063](https://github.com/tobymao/sqlglot/pull/2063) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2062](undefined) opened by [@dmelchor-stripe](https://github.com/dmelchor-stripe)*
- [`7787342`](https://github.com/tobymao/sqlglot/commit/7787342681111410bf72c3561e6049e4a20449ea) - pushdown predicate to HAVING *(PR [#2064](https://github.com/tobymao/sqlglot/pull/2064) by [@barakalon](https://github.com/barakalon))*


## [v17.14.1] - 2023-08-15
### :bug: Bug Fixes
- [`0126d01`](https://github.com/tobymao/sqlglot/commit/0126d01458273724321aa16e4a4f5ce32d3741b2) - parenthesize coalesce simplification *(PR [#2057](https://github.com/tobymao/sqlglot/pull/2057) by [@barakalon](https://github.com/barakalon))*


## [v17.14.0] - 2023-08-14
### :boom: BREAKING CHANGES
- due to [`2e73a4f`](https://github.com/tobymao/sqlglot/commit/2e73a4f455f32a4d4bab45e3c7860aeb32ceaa03) - dict conversion had incorrect ast *(commit by [@tobymao](https://github.com/tobymao))*:

  dict conversion had incorrect ast


### :bug: Bug Fixes
- [`2e73a4f`](https://github.com/tobymao/sqlglot/commit/2e73a4f455f32a4d4bab45e3c7860aeb32ceaa03) - dict conversion had incorrect ast *(commit by [@tobymao](https://github.com/tobymao))*
- [`8affeff`](https://github.com/tobymao/sqlglot/commit/8affeff26022894d719132d78ccb40c22e3122a9) - coalesce simplify window func *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.13.0] - 2023-08-14
### :sparkles: New Features
- [`c817e19`](https://github.com/tobymao/sqlglot/commit/c817e1942b87bbd81e67f5142e0473e7dc31dc52) - improve support for NVL2 function *(PR [#2042](https://github.com/tobymao/sqlglot/pull/2042) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`689956b`](https://github.com/tobymao/sqlglot/commit/689956bcd2bfd6612f67b73be672fe7c6789be01) - add distribute by alias for hive window functions closes [#2043](https://github.com/tobymao/sqlglot/pull/2043) *(commit by [@tobymao](https://github.com/tobymao))*
- [`0746b6f`](https://github.com/tobymao/sqlglot/commit/0746b6f96d9b8fad0d8fbea3e23170e8d56eb3ee) - **tsql**: if object_id is not null support closes [#2044](https://github.com/tobymao/sqlglot/pull/2044) *(commit by [@tobymao](https://github.com/tobymao))*
- [`c37abfd`](https://github.com/tobymao/sqlglot/commit/c37abfd9d08a6e7a02fb047c478b9f3b41d9d45a) - any_value hive/spark/presto closes [#2053](https://github.com/tobymao/sqlglot/pull/2053) *(commit by [@tobymao](https://github.com/tobymao))*
- [`f8cb87d`](https://github.com/tobymao/sqlglot/commit/f8cb87d63ffb7f9e5961e2d11dbb6ebbe5bad63a) - **presto, spark**: improve support for STR_TO_MAP, SPLIT_TO_MAP *(PR [#2054](https://github.com/tobymao/sqlglot/pull/2054) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2050](undefined) opened by [@edwinlei](https://github.com/edwinlei)*

### :bug: Bug Fixes
- [`a35cfe0`](https://github.com/tobymao/sqlglot/commit/a35cfe0ad78a287efa324f4a24dabab9559af471) - **tsql**: add support for 'culture' argument in FORMAT *(PR [#2047](https://github.com/tobymao/sqlglot/pull/2047) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2046](undefined) opened by [@aersam](https://github.com/aersam)*
- [`cad6de8`](https://github.com/tobymao/sqlglot/commit/cad6de866c3236b5f6253cfc183f2b0d7cde8b87) - **tsql**: improve handling of table hints in MERGE statement *(PR [#2049](https://github.com/tobymao/sqlglot/pull/2049) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2048](undefined) opened by [@dmoore247](https://github.com/dmoore247)*
- [`96d4d8b`](https://github.com/tobymao/sqlglot/commit/96d4d8b5274377d65ed34a32c4dff564f4440fe1) - **parser**: ensure identifiers aren't treated as NO_PAREN_FUNCTIONS *(PR [#2056](https://github.com/tobymao/sqlglot/pull/2056) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.12.0] - 2023-08-11
### :sparkles: New Features
- [`95ec5b6`](https://github.com/tobymao/sqlglot/commit/95ec5b6fa14d80974491a932aec1b2ee5e924c36) - **hive**: improve transpilation of Bigquery's TIMESTAMP_ADD *(PR [#2012](https://github.com/tobymao/sqlglot/pull/2012) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`921d7a5`](https://github.com/tobymao/sqlglot/commit/921d7a5f1dc5c6830578c47f3a279d6b3b88be0c) - add apache doris dialect *(PR [#2006](https://github.com/tobymao/sqlglot/pull/2006) by [@liujiwen-up](https://github.com/liujiwen-up))*
- [`6e2705e`](https://github.com/tobymao/sqlglot/commit/6e2705ef99c04a9eb4948fb6c860d67327ed9b83) - simplify COALESCE *(PR [#2019](https://github.com/tobymao/sqlglot/pull/2019) by [@barakalon](https://github.com/barakalon))*
- [`915b1e2`](https://github.com/tobymao/sqlglot/commit/915b1e2ed6e11c6a370ec72ea09913201c36f9cf) - **databricks**: add support for UNPIVOT nulls option *(PR [#2021](https://github.com/tobymao/sqlglot/pull/2021) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2020](undefined) opened by [@aersam](https://github.com/aersam)*
- [`ea7891d`](https://github.com/tobymao/sqlglot/commit/ea7891d4d4596db9a4ecb88b26a0dadcf1757ab4) - **duckdb**: improve support for INT128, HUGEINT, NUMERIC *(PR [#2023](https://github.com/tobymao/sqlglot/pull/2023) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2022](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`190255d`](https://github.com/tobymao/sqlglot/commit/190255da78240686b52f7727c86b5710268af607) - **clickhouse**: add support for FixedString(N) type *(PR [#2036](https://github.com/tobymao/sqlglot/pull/2036) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2031](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`ccb1b14`](https://github.com/tobymao/sqlglot/commit/ccb1b14381fa1c3544c8a0c63e6a63b149d79350) - **clickhouse**: LowCardinality closes [#2033](https://github.com/tobymao/sqlglot/pull/2033) *(commit by [@tobymao](https://github.com/tobymao))*
- [`55a8ead`](https://github.com/tobymao/sqlglot/commit/55a8ead5acd0ddf0e92f8e15521c44be07c3ad5f) - **clickhouse**: add support for Enum types *(PR [#2038](https://github.com/tobymao/sqlglot/pull/2038) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2032](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`593153c`](https://github.com/tobymao/sqlglot/commit/593153c80fd10391d499a162b19495c1d8aae22c) - **presto,oracle**: add support for INTERVAL span types *(PR [#2035](https://github.com/tobymao/sqlglot/pull/2035) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2027](undefined) opened by [@cpcloud](https://github.com/cpcloud)*
- [`36f308e`](https://github.com/tobymao/sqlglot/commit/36f308e80253d4c6b13232f21a5fd221d0757fb5) - **clickhouse**: add support for Nested type *(PR [#2039](https://github.com/tobymao/sqlglot/pull/2039) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#2030](undefined) opened by [@cpcloud](https://github.com/cpcloud)*

### :bug: Bug Fixes
- [`bc46c3d`](https://github.com/tobymao/sqlglot/commit/bc46c3df03e05286d987a49570a1df2bf98c828c) - **executor**: add table normalization, fix python type mapping *(PR [#2015](https://github.com/tobymao/sqlglot/pull/2015) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`baab165`](https://github.com/tobymao/sqlglot/commit/baab165f04714d443bb5faced5a1a139f8eaf81c) - improve comment handling for several expressions *(PR [#2017](https://github.com/tobymao/sqlglot/pull/2017) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`325b26e`](https://github.com/tobymao/sqlglot/commit/325b26e7f9567f30531918b302ab13b8662a5341) - duckdb ISNAN *(PR [#2024](https://github.com/tobymao/sqlglot/pull/2024) by [@barakalon](https://github.com/barakalon))*
- [`ad75c6f`](https://github.com/tobymao/sqlglot/commit/ad75c6f7a67156bb12d14f8f344c04cc75138a63) - only some joins can be simplified to CROSS *(PR [#2025](https://github.com/tobymao/sqlglot/pull/2025) by [@barakalon](https://github.com/barakalon))*
- [`32eb129`](https://github.com/tobymao/sqlglot/commit/32eb129959207961566e1aedbcac500c2c0534c4) - **postgres**: improve parsing of array types closes [#2034](https://github.com/tobymao/sqlglot/pull/2034) *(PR [#2040](https://github.com/tobymao/sqlglot/pull/2040) by [@tobymao](https://github.com/tobymao))*
- [`4591092`](https://github.com/tobymao/sqlglot/commit/4591092d0d564977f33a2004fa37da40209f1559) - parse time[(p)] with time zone correctly *(PR [#2041](https://github.com/tobymao/sqlglot/pull/2041) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2028](undefined) opened by [@cpcloud](https://github.com/cpcloud)*

### :recycle: Refactors
- [`c3fd695`](https://github.com/tobymao/sqlglot/commit/c3fd695d1abd3e1fb83419afc6a4cb0d74613a4d) - **doris**: cleanup implementation of Doris dialect *(PR [#2018](https://github.com/tobymao/sqlglot/pull/2018) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`d6a5a28`](https://github.com/tobymao/sqlglot/commit/d6a5a28c71cac196c027294fabefcd63a09f073e) - minimize copies *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.10.2] - 2023-08-09
### :sparkles: New Features
- [`12a66dd`](https://github.com/tobymao/sqlglot/commit/12a66dd6879df0e9b242036ae5058ddbd01a39bb) - parse alias in tuples *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`7ccfd35`](https://github.com/tobymao/sqlglot/commit/7ccfd352acda6061fd712bb7d778c8e853a10a22) - **optimizer**: don't parse str argument in normalize_identifiers *(PR [#2010](https://github.com/tobymao/sqlglot/pull/2010) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.10.1] - 2023-08-08
### :bug: Bug Fixes
- [`f6fe54a`](https://github.com/tobymao/sqlglot/commit/f6fe54a535ae1d985c9f298afa1fc77f14a9943e) - **optimizer**: wrap expanded alias expressions *(PR [#2004](https://github.com/tobymao/sqlglot/pull/2004) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2003](undefined) opened by [@z3z1ma](https://github.com/z3z1ma)*
- [`900bec3`](https://github.com/tobymao/sqlglot/commit/900bec32f2d04d3ab5f12ab93679f1a14d9ac548) - if_sql mutation *(commit by [@tobymao](https://github.com/tobymao))*
- [`c73790d`](https://github.com/tobymao/sqlglot/commit/c73790d0249cf0f21f855ed4921c9a8727e2d5ff) - **optimizer**: ensure TableAlias column names shadow source columns *(PR [#2002](https://github.com/tobymao/sqlglot/pull/2002) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`caf2adc`](https://github.com/tobymao/sqlglot/commit/caf2adc2153a650c4a4062f35d004d2cb94d6d71) - **snowflake**: tokenize $$ as raw string delimiters *(PR [#2007](https://github.com/tobymao/sqlglot/pull/2007) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#2005](undefined) opened by [@wezham](https://github.com/wezham)*
- [`289493b`](https://github.com/tobymao/sqlglot/commit/289493bf213f71cc4c9efef1349bbed372a0d4b3) - remove several mutations in Generator methods *(PR [#2009](https://github.com/tobymao/sqlglot/pull/2009) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.10.0] - 2023-08-07
### :sparkles: New Features
- [`12c2e9b`](https://github.com/tobymao/sqlglot/commit/12c2e9be97580642c94571f726a0d26a16f40a63) - add terse coalesce operator *(PR [#1991](https://github.com/tobymao/sqlglot/pull/1991) by [@z3z1ma](https://github.com/z3z1ma))*

### :bug: Bug Fixes
- [`92f3288`](https://github.com/tobymao/sqlglot/commit/92f328814e5822038c53f295fe5f2d36bec00b32) - **snowflake**: generate WEEKOFYEAR instead of WEEK_OF_YEAR *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`c9dd971`](https://github.com/tobymao/sqlglot/commit/c9dd9716ab5fc0a7b5f9a821fc13da7985cb8933) - **parser, duckdb**: decode/encode in duckdb don't take charset *(PR [#1993](https://github.com/tobymao/sqlglot/pull/1993) by [@charsmith](https://github.com/charsmith))*
- [`e00d857`](https://github.com/tobymao/sqlglot/commit/e00d8578eb3ad11efc12f7bd793ffebacc407a8d) - **redshift**: parse dateadd alias date_add *(PR [#1995](https://github.com/tobymao/sqlglot/pull/1995) by [@gafeol](https://github.com/gafeol))*
- [`d219a65`](https://github.com/tobymao/sqlglot/commit/d219a65a75ce0183bcaa366a9eb878282a525eb2) - snowflake startswith closes [#1998](https://github.com/tobymao/sqlglot/pull/1998) *(commit by [@tobymao](https://github.com/tobymao))*


## [v17.9.1] - 2023-08-03
### :sparkles: New Features
- [`57df0b7`](https://github.com/tobymao/sqlglot/commit/57df0b77f8b44dc1ef08d47818b4364db242520d) - **optimizer**: allow normalize_identifiers to accept strings *(PR [#1992](https://github.com/tobymao/sqlglot/pull/1992) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`652d1c9`](https://github.com/tobymao/sqlglot/commit/652d1c9a87db6dda3a82a01b3305e240b7e3ffbb) - **optimizer**: wrap scalar subquery replacement in a MAX call *(PR [#1988](https://github.com/tobymao/sqlglot/pull/1988) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1987](undefined) opened by [@laurentiupiciu](https://github.com/laurentiupiciu)*
- [`1865959`](https://github.com/tobymao/sqlglot/commit/18659596058d503845597b9e82d46899aaf46d08) - ensure eliminate_qualify won't introduce duplicate projections *(PR [#1990](https://github.com/tobymao/sqlglot/pull/1990) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.9.0] - 2023-08-01
### :sparkles: New Features
- [`ea7c7da`](https://github.com/tobymao/sqlglot/commit/ea7c7da1aaf0ea157f73efce7622e326b0d0f419) - **teradata**: parse [COLLECT|HELP] STATISTICS as Commands *(PR [#1979](https://github.com/tobymao/sqlglot/pull/1979) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1978](undefined) opened by [@MarkBell920](https://github.com/MarkBell920)*

### :bug: Bug Fixes
- [`4af91a0`](https://github.com/tobymao/sqlglot/commit/4af91a05c2c149011526d294a8e0cc843351fded) - **parser**: parse placeholder as fallback for boolean, null, star *(PR [#1976](https://github.com/tobymao/sqlglot/pull/1976) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1975](undefined) opened by [@SoftwareGuy2020](https://github.com/SoftwareGuy2020)*
- [`be7d4e6`](https://github.com/tobymao/sqlglot/commit/be7d4e6f7da143c4eac6f9ad389d088c582b2d03) - control whether quotes are generated for extract's date part *(PR [#1981](https://github.com/tobymao/sqlglot/pull/1981) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`5436f53`](https://github.com/tobymao/sqlglot/commit/5436f53cd5037ab56cb414a178ef1edeb7827885) - Make date_add with incorrect expression more clear *(commit by [@tobymao](https://github.com/tobymao))*

### :recycle: Refactors
- [`8a44cc2`](https://github.com/tobymao/sqlglot/commit/8a44cc22119aa46af20f442645fe496217042722) - **optimizer**: improve handling of DDL optimization *(PR [#1972](https://github.com/tobymao/sqlglot/pull/1972) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`9e77c7b`](https://github.com/tobymao/sqlglot/commit/9e77c7bc72a645603ba9b780323bbd356e9dede1) - **optimizer**: factor out pseudocolumns in qualify columns *(PR [#1984](https://github.com/tobymao/sqlglot/pull/1984) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`78b0eed`](https://github.com/tobymao/sqlglot/commit/78b0eed515491c7ef53cccea8f5265e9abe89bc0) - update docstring with description of Expression.meta *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.8.5] - 2023-07-28
### :sparkles: New Features
- [`9d67283`](https://github.com/tobymao/sqlglot/commit/9d67283b3185baa43f2591a75c01493455948d40) - **optimizer**: add support for resolving CTEs in CREATE statements *(PR [#1949](https://github.com/tobymao/sqlglot/pull/1949) by [@gtoonstra](https://github.com/gtoonstra))*

### :bug: Bug Fixes
- [`2874ae5`](https://github.com/tobymao/sqlglot/commit/2874ae5175c3c522b75bfd7764fdbcd928cc94b4) - **tsql**: improve UDF parsing *(PR [#1973](https://github.com/tobymao/sqlglot/pull/1973) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *fixes issue [#1966](undefined) opened by [@bayees](https://github.com/bayees)*
- [`89c8635`](https://github.com/tobymao/sqlglot/commit/89c8635a73ae3f415d9ed8f13da1ab23400446cc) - **parser,bigquery**: make separator optional in STRING_AGG parser *(PR [#1974](https://github.com/tobymao/sqlglot/pull/1974) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.8.4] - 2023-07-28
### :sparkles: New Features
- [`7440e4a`](https://github.com/tobymao/sqlglot/commit/7440e4adbc5b62f53db54c6e50b0e5a69f5bffc2) - **tsql**: improve support for the DATEDIFF function *(PR [#1967](https://github.com/tobymao/sqlglot/pull/1967) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :bug: Bug Fixes
- [`2d5d714`](https://github.com/tobymao/sqlglot/commit/2d5d714b33c3b7ac2bcafd4b9484d7b8c3e25032) - **tsql**: revert float-to-datetime coercions *(PR [#1970](https://github.com/tobymao/sqlglot/pull/1970) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
- [`35f55e9`](https://github.com/tobymao/sqlglot/commit/35f55e9434bdb87449abd85683ee93f39d0c740f) - **mysql**: simplify LIMIT, OFFSET when their expression is complex *(PR [#1971](https://github.com/tobymao/sqlglot/pull/1971) by [@GeorgeSittas](https://github.com/GeorgeSittas))*

### :wrench: Chores
- [`5695667`](https://github.com/tobymao/sqlglot/commit/5695667e85adb62821e3ea2210855244982c451c) - add dialect parameter to parse for parity with parse_one *(PR [#1969](https://github.com/tobymao/sqlglot/pull/1969) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.8.3] - 2023-07-27
### :bug: Bug Fixes
- [`75b418c`](https://github.com/tobymao/sqlglot/commit/75b418c1a7dd2efc616639285438a67ebc2c6b08) - **mysql**: generate DATETIME instead of TIMESTAMP for TimeStrToTime *(commit by [@GeorgeSittas](https://github.com/GeorgeSittas))*


## [v17.8.2] - 2023-07-27
### :sparkles: New Features
- [`5e641c2`](https://github.com/tobymao/sqlglot/commit/5e641c2aa898aa55dd920fc7cff11bef90408379) - **presto**: add IPADDRESS/IPPREFIX data types *(PR [#1965](https://github.com/tobymao/sqlglot/pull/1965) by [@roykoand](https://github.com/roykoand))*
- [`d2685dd`](https://github.com/tobymao/sqlglot/commit/d2685dd4b52123c70e9b4f62e95b948a5b4b6f5f) - **mysql**: improve support for DDL index column constraints *(PR [#1961](https://github.com/tobymao/sqlglot/pull/1961) by [@GeorgeSittas](https://github.com/GeorgeSittas))*
  - :arrow_lower_right: *addresses issue [#1959](undefined) opened by [@ninja96826](https://github.com/ninja96826)*

### :bug: Bug Fixes
- [`7082b61`](https://github.com/tobymao/sqlglot/commit/7082b617e07c8e84e9a3e2a840a4f559d3bd2991) - trino->spark starts_with closes [#1963](https://github.com/tobymao/sqlglot/pull/1963) *(commit by [@tobymao](https://github.com/tobymao))*
- [`9787329`](https://github.com/tobymao/sqlglot/commit/97873293ccfd522867c4ae074a05ce97a44adef9) - trino->spark is_nan closes [#1964](https://github.com/tobymao/sqlglot/pull/1964) *(commit by [@tobymao](https://github.com/tobymao))*
- [`1ebe49f`](https://github.com/tobymao/sqlglot/commit/1ebe49fe475c86bf64ba39e9b67a11be9b2f65cd) - **mysql**: generate TimeStrToTime as a cast to TIMESTAMP *(PR [#1968](https://github.com/tobymao/sqlglot/pull/1968) by [@GeorgeSittas](https://github.com/GeorgeSittas))*


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
[v17.8.2]: https://github.com/tobymao/sqlglot/compare/v17.8.1...v17.8.2
[v17.8.3]: https://github.com/tobymao/sqlglot/compare/v17.8.2...v17.8.3
[v17.8.4]: https://github.com/tobymao/sqlglot/compare/v17.8.3...v17.8.4
[v17.8.5]: https://github.com/tobymao/sqlglot/compare/v17.8.4...v17.8.5
[v17.9.0]: https://github.com/tobymao/sqlglot/compare/v17.8.6...v17.9.0
[v17.9.1]: https://github.com/tobymao/sqlglot/compare/v17.9.0...v17.9.1
[v17.10.0]: https://github.com/tobymao/sqlglot/compare/v17.9.1...v17.10.0
[v17.10.1]: https://github.com/tobymao/sqlglot/compare/v17.10.0...v17.10.1
[v17.10.2]: https://github.com/tobymao/sqlglot/compare/v17.10.1...v17.10.2
[v17.12.0]: https://github.com/tobymao/sqlglot/compare/v17.11.0...v17.12.0
[v17.13.0]: https://github.com/tobymao/sqlglot/compare/v17.12.0...v17.13.0
[v17.14.0]: https://github.com/tobymao/sqlglot/compare/v17.13.0...v17.14.0
[v17.14.1]: https://github.com/tobymao/sqlglot/compare/v17.14.0...v17.14.1
[v17.14.2]: https://github.com/tobymao/sqlglot/compare/v17.14.1...v17.14.2
[v17.15.0]: https://github.com/tobymao/sqlglot/compare/v17.14.2...v17.15.0
[v17.15.1]: https://github.com/tobymao/sqlglot/compare/v17.15.0...v17.15.1
[v17.16.0]: https://github.com/tobymao/sqlglot/compare/v17.15.1...v17.16.0
[v17.16.1]: https://github.com/tobymao/sqlglot/compare/v17.16.0...v17.16.1
[v17.16.2]: https://github.com/tobymao/sqlglot/compare/v17.16.1...v17.16.2
[v18.0.0]: https://github.com/tobymao/sqlglot/compare/v17.16.2...v18.0.0
[v18.0.1]: https://github.com/tobymao/sqlglot/compare/v18.0.0...v18.0.1
[v18.1.0]: https://github.com/tobymao/sqlglot/compare/v18.0.1...v18.1.0
[v18.2.0]: https://github.com/tobymao/sqlglot/compare/v18.1.0...v18.2.0
[v18.3.0]: https://github.com/tobymao/sqlglot/compare/v18.2.0...v18.3.0
[v18.4.0]: https://github.com/tobymao/sqlglot/compare/v18.3.0...v18.4.0
[v18.4.1]: https://github.com/tobymao/sqlglot/compare/v18.4.0...v18.4.1
[v18.5.0]: https://github.com/tobymao/sqlglot/compare/v18.4.1...v18.5.0
[v18.5.1]: https://github.com/tobymao/sqlglot/compare/v18.5.0...v18.5.1
[v18.6.0]: https://github.com/tobymao/sqlglot/compare/v18.5.1...v18.6.0
[v18.7.0]: https://github.com/tobymao/sqlglot/compare/v18.6.0...v18.7.0
[v18.8.0]: https://github.com/tobymao/sqlglot/compare/v18.7.0...v18.8.0
[v18.9.0]: https://github.com/tobymao/sqlglot/compare/v18.8.0...v18.9.0
[v18.10.0]: https://github.com/tobymao/sqlglot/compare/v18.9.0...v18.10.0
[v18.10.1]: https://github.com/tobymao/sqlglot/compare/v18.10.0...v18.10.1
[v18.11.0]: https://github.com/tobymao/sqlglot/compare/v18.10.1...v18.11.0
[v18.11.1]: https://github.com/tobymao/sqlglot/compare/v18.11.0...v18.11.1
[v18.11.2]: https://github.com/tobymao/sqlglot/compare/v18.11.1...v18.11.2
[v18.11.3]: https://github.com/tobymao/sqlglot/compare/v18.11.2...v18.11.3
[v18.11.4]: https://github.com/tobymao/sqlglot/compare/v18.11.3...v18.11.4
[v18.11.5]: https://github.com/tobymao/sqlglot/compare/v18.11.4...v18.11.5
[v18.11.6]: https://github.com/tobymao/sqlglot/compare/v18.11.5...v18.11.6
[v18.12.0]: https://github.com/tobymao/sqlglot/compare/v18.11.6...v18.12.0
[v18.13.0]: https://github.com/tobymao/sqlglot/compare/v18.12.0...v18.13.0
[v18.14.0]: https://github.com/tobymao/sqlglot/compare/v18.13.0...v18.14.0
[v18.15.0]: https://github.com/tobymao/sqlglot/compare/v18.14.0...v18.15.0
[v18.15.1]: https://github.com/tobymao/sqlglot/compare/v18.15.0...v18.15.1
[v18.16.0]: https://github.com/tobymao/sqlglot/compare/v18.15.1...v18.16.0
[v18.16.1]: https://github.com/tobymao/sqlglot/compare/v18.16.0...v18.16.1
[v18.17.0]: https://github.com/tobymao/sqlglot/compare/v18.16.1...v18.17.0
[v19.0.0]: https://github.com/tobymao/sqlglot/compare/v18.17.0...v19.0.0
[v19.0.1]: https://github.com/tobymao/sqlglot/compare/v19.0.0...v19.0.1
[v19.0.2]: https://github.com/tobymao/sqlglot/compare/v19.0.1...v19.0.2
[v19.0.3]: https://github.com/tobymao/sqlglot/compare/v19.0.2...v19.0.3
[v19.1.0]: https://github.com/tobymao/sqlglot/compare/v19.0.3...v19.1.0
[v19.1.1]: https://github.com/tobymao/sqlglot/compare/v19.1.0...v19.1.1
[v19.1.2]: https://github.com/tobymao/sqlglot/compare/v19.1.1...v19.1.2
[v19.1.3]: https://github.com/tobymao/sqlglot/compare/v19.1.2...v19.1.3
[v19.2.0]: https://github.com/tobymao/sqlglot/compare/v19.1.3...v19.2.0
[v19.3.0]: https://github.com/tobymao/sqlglot/compare/v19.2.0...v19.3.0
[v19.3.1]: https://github.com/tobymao/sqlglot/compare/v19.3.0...v19.3.1
[v19.4.0]: https://github.com/tobymao/sqlglot/compare/v19.3.1...v19.4.0
[v19.5.0]: https://github.com/tobymao/sqlglot/compare/v19.4.0...v19.5.0
[v19.5.1]: https://github.com/tobymao/sqlglot/compare/v19.5.0...v19.5.1
[v19.6.0]: https://github.com/tobymao/sqlglot/compare/v19.5.1...v19.6.0
[v19.7.0]: https://github.com/tobymao/sqlglot/compare/v19.6.0...v19.7.0
[v19.8.0]: https://github.com/tobymao/sqlglot/compare/v19.7.0...v19.8.0
[v19.8.1]: https://github.com/tobymao/sqlglot/compare/v19.8.0...v19.8.1
[v19.8.2]: https://github.com/tobymao/sqlglot/compare/v19.8.1...v19.8.2
[v19.8.3]: https://github.com/tobymao/sqlglot/compare/v19.8.2...v19.8.3
[v19.9.0]: https://github.com/tobymao/sqlglot/compare/v19.8.3...v19.9.0
[v20.0.0]: https://github.com/tobymao/sqlglot/compare/v19.9.0...v20.0.0
[v20.1.0]: https://github.com/tobymao/sqlglot/compare/v20.0.0...v20.1.0
[v20.2.0]: https://github.com/tobymao/sqlglot/compare/v20.1.0...v20.2.0
[v20.5.0]: https://github.com/tobymao/sqlglot/compare/v20.4.0...v20.5.0
[v20.6.0]: https://github.com/tobymao/sqlglot/compare/v20.5.0...v20.6.0
[v20.7.1]: https://github.com/tobymao/sqlglot/compare/v20.6.0...v20.7.1
[v20.8.0]: https://github.com/tobymao/sqlglot/compare/v20.7.1...v20.8.0
[v20.9.0]: https://github.com/tobymao/sqlglot/compare/v20.8.0...v20.9.0
[v20.10.0]: https://github.com/tobymao/sqlglot/compare/v20.9.0...v20.10.0
[v20.11.0]: https://github.com/tobymao/sqlglot/compare/v20.10.0...v20.11.0
[v21.0.0]: https://github.com/tobymao/sqlglot/compare/v20.11.0...v21.0.0
[v21.0.1]: https://github.com/tobymao/sqlglot/compare/v21.0.0...v21.0.1
[v21.0.2]: https://github.com/tobymao/sqlglot/compare/v21.0.1...v21.0.2
[v21.1.0]: https://github.com/tobymao/sqlglot/compare/v21.0.2...v21.1.0
[v21.1.1]: https://github.com/tobymao/sqlglot/compare/v21.1.0...v21.1.1
[v21.1.2]: https://github.com/tobymao/sqlglot/compare/v21.1.1...v21.1.2
[v21.2.0]: https://github.com/tobymao/sqlglot/compare/v21.1.2...v21.2.0
[v21.2.1]: https://github.com/tobymao/sqlglot/compare/v21.2.0...v21.2.1
[v22.0.0]: https://github.com/tobymao/sqlglot/compare/v21.2.1...v22.0.0
[v22.0.1]: https://github.com/tobymao/sqlglot/compare/v22.0.0...v22.0.1
[v22.0.2]: https://github.com/tobymao/sqlglot/compare/v22.0.1...v22.0.2
[v22.1.0]: https://github.com/tobymao/sqlglot/compare/v22.0.2...v22.1.0
[v22.1.1]: https://github.com/tobymao/sqlglot/compare/v22.1.0...v22.1.1
[v22.2.0]: https://github.com/tobymao/sqlglot/compare/v22.1.1...v22.2.0
[v22.2.1]: https://github.com/tobymao/sqlglot/compare/v22.2.0...v22.2.1
[v22.3.0]: https://github.com/tobymao/sqlglot/compare/v22.2.1...v22.3.0
[v22.3.1]: https://github.com/tobymao/sqlglot/compare/v22.3.0...v22.3.1
[v22.4.0]: https://github.com/tobymao/sqlglot/compare/v22.3.1...v22.4.0
[v22.5.0]: https://github.com/tobymao/sqlglot/compare/v22.4.0...v22.5.0
[v23.0.0]: https://github.com/tobymao/sqlglot/compare/v22.5.0...v23.0.0
[v23.0.1]: https://github.com/tobymao/sqlglot/compare/v23.0.0...v23.0.1
[v23.0.2]: https://github.com/tobymao/sqlglot/compare/v23.0.1...v23.0.2
[v23.0.3]: https://github.com/tobymao/sqlglot/compare/v23.0.2...v23.0.3
[v23.0.4]: https://github.com/tobymao/sqlglot/compare/v23.0.3...v23.0.4
[v23.0.5]: https://github.com/tobymao/sqlglot/compare/v23.0.4...v23.0.5
[v23.1.0]: https://github.com/tobymao/sqlglot/compare/v23.0.5...v23.1.0
[v23.3.0]: https://github.com/tobymao/sqlglot/compare/v23.2.0...v23.3.0
[v23.4.0]: https://github.com/tobymao/sqlglot/compare/v23.3.0...v23.4.0
[v23.6.0]: https://github.com/tobymao/sqlglot/compare/v23.5.0...v23.6.0
[v23.6.4]: https://github.com/tobymao/sqlglot/compare/v23.6.3...v23.6.4
[v23.7.0]: https://github.com/tobymao/sqlglot/compare/v23.6.4...v23.7.0
[v23.8.0]: https://github.com/tobymao/sqlglot/compare/v23.7.0...v23.8.0
[v23.8.1]: https://github.com/tobymao/sqlglot/compare/v23.8.0...v23.8.1
[v23.8.2]: https://github.com/tobymao/sqlglot/compare/v23.8.1...v23.8.2
[v23.9.0]: https://github.com/tobymao/sqlglot/compare/v23.8.2...v23.9.0
[v23.10.0]: https://github.com/tobymao/sqlglot/compare/v23.9.0...v23.10.0
[v23.11.0]: https://github.com/tobymao/sqlglot/compare/v23.10.0...v23.11.0
[v23.11.1]: https://github.com/tobymao/sqlglot/compare/v23.11.0...v23.11.1
[v23.11.2]: https://github.com/tobymao/sqlglot/compare/v23.11.1...v23.11.2
[v23.12.0]: https://github.com/tobymao/sqlglot/compare/v23.11.2...v23.12.0
[v23.12.1]: https://github.com/tobymao/sqlglot/compare/v23.12.0...v23.12.1
[v23.12.2]: https://github.com/tobymao/sqlglot/compare/v23.12.1...v23.12.2
[v23.13.0]: https://github.com/tobymao/sqlglot/compare/v23.12.2...v23.13.0
[v23.13.1]: https://github.com/tobymao/sqlglot/compare/v23.13.0...v23.13.1
[v23.13.2]: https://github.com/tobymao/sqlglot/compare/v23.13.1...v23.13.2
[v23.13.3]: https://github.com/tobymao/sqlglot/compare/v23.13.2...v23.13.3
[v23.13.4]: https://github.com/tobymao/sqlglot/compare/v23.13.3...v23.13.4
[v23.13.5]: https://github.com/tobymao/sqlglot/compare/v23.13.4...v23.13.5
[v23.13.6]: https://github.com/tobymao/sqlglot/compare/v23.13.5...v23.13.6
[v23.13.7]: https://github.com/tobymao/sqlglot/compare/v23.13.6...v23.13.7
[v23.14.0]: https://github.com/tobymao/sqlglot/compare/v23.13.7...v23.14.0
[v23.15.0]: https://github.com/tobymao/sqlglot/compare/v23.14.0...v23.15.0
[v23.15.1]: https://github.com/tobymao/sqlglot/compare/v23.15.0...v23.15.1
[v23.15.2]: https://github.com/tobymao/sqlglot/compare/v23.15.1...v23.15.2
[v23.15.3]: https://github.com/tobymao/sqlglot/compare/v23.15.2...v23.15.3
[v23.15.6]: https://github.com/tobymao/sqlglot/compare/v23.15.5...v23.15.6
[v23.15.7]: https://github.com/tobymao/sqlglot/compare/v23.15.6...v23.15.7
[v23.15.8]: https://github.com/tobymao/sqlglot/compare/v23.15.7...v23.15.8
[v23.15.9]: https://github.com/tobymao/sqlglot/compare/v23.15.8...v23.15.9
[v23.15.10]: https://github.com/tobymao/sqlglot/compare/v23.15.9...v23.15.10
[v23.16.0]: https://github.com/tobymao/sqlglot/compare/v23.15.10...v23.16.0
[v23.17.0]: https://github.com/tobymao/sqlglot/compare/v23.16.0...v23.17.0
[v24.0.0]: https://github.com/tobymao/sqlglot/compare/v23.17.0...v24.0.0
[v24.0.1]: https://github.com/tobymao/sqlglot/compare/v24.0.0...v24.0.1
[v24.0.2]: https://github.com/tobymao/sqlglot/compare/v24.0.1...v24.0.2
[v24.0.3]: https://github.com/tobymao/sqlglot/compare/v24.0.2...v24.0.3
[v24.1.0]: https://github.com/tobymao/sqlglot/compare/v24.0.3...v24.1.0
[v24.1.1]: https://github.com/tobymao/sqlglot/compare/v24.1.0...v24.1.1
[v24.1.2]: https://github.com/tobymao/sqlglot/compare/v24.1.1...v24.1.2
[v25.0.0]: https://github.com/tobymao/sqlglot/compare/v24.1.2...v25.0.0
[v25.0.2]: https://github.com/tobymao/sqlglot/compare/v25.0.1...v25.0.2
[v25.0.3]: https://github.com/tobymao/sqlglot/compare/v25.0.2...v25.0.3
[v25.1.0]: https://github.com/tobymao/sqlglot/compare/v25.0.3...v25.1.0
[v25.2.0]: https://github.com/tobymao/sqlglot/compare/v25.1.0...v25.2.0
[v25.3.0]: https://github.com/tobymao/sqlglot/compare/v25.2.0...v25.3.0
[v25.3.1]: https://github.com/tobymao/sqlglot/compare/v25.3.0...v25.3.1
[v25.3.2]: https://github.com/tobymao/sqlglot/compare/v25.3.1...v25.3.2
[v25.3.3]: https://github.com/tobymao/sqlglot/compare/v25.3.2...v25.3.3
[v25.4.0]: https://github.com/tobymao/sqlglot/compare/v25.3.3...v25.4.0
[v25.4.1]: https://github.com/tobymao/sqlglot/compare/v25.4.0...v25.4.1
[v25.5.0]: https://github.com/tobymao/sqlglot/compare/v25.4.1...v25.5.0
[v25.5.1]: https://github.com/tobymao/sqlglot/compare/v25.5.0...v25.5.1
[v25.6.0]: https://github.com/tobymao/sqlglot/compare/v25.5.1...v25.6.0
[v25.6.1]: https://github.com/tobymao/sqlglot/compare/v25.6.0...v25.6.1
[v25.7.0]: https://github.com/tobymao/sqlglot/compare/v25.6.1...v25.7.0
[v25.7.1]: https://github.com/tobymao/sqlglot/compare/v25.7.0...v25.7.1
[v25.8.0]: https://github.com/tobymao/sqlglot/compare/v25.7.1...v25.8.0
[v25.8.1]: https://github.com/tobymao/sqlglot/compare/v25.8.0...v25.8.1
[v25.9.0]: https://github.com/tobymao/sqlglot/compare/v25.8.1...v25.9.0
[v25.10.0]: https://github.com/tobymao/sqlglot/compare/v25.9.0...v25.10.0
[v25.11.0]: https://github.com/tobymao/sqlglot/compare/v25.10.0...v25.11.0
[v25.11.1]: https://github.com/tobymao/sqlglot/compare/v25.11.0...v25.11.1
[v25.11.2]: https://github.com/tobymao/sqlglot/compare/v25.11.1...v25.11.2
[v25.11.3]: https://github.com/tobymao/sqlglot/compare/v25.11.2...v25.11.3
[v25.12.0]: https://github.com/tobymao/sqlglot/compare/v25.11.3...v25.12.0
[v25.13.0]: https://github.com/tobymao/sqlglot/compare/v25.12.0...v25.13.0
[v25.14.0]: https://github.com/tobymao/sqlglot/compare/v25.13.0...v25.14.0
[v25.15.0]: https://github.com/tobymao/sqlglot/compare/v25.14.0...v25.15.0
[v25.16.0]: https://github.com/tobymao/sqlglot/compare/v25.15.0...v25.16.0
[v25.16.1]: https://github.com/tobymao/sqlglot/compare/v25.16.0...v25.16.1
[v25.17.0]: https://github.com/tobymao/sqlglot/compare/v25.16.1...v25.17.0
[v25.18.0]: https://github.com/tobymao/sqlglot/compare/v25.17.0...v25.18.0
[v25.19.0]: https://github.com/tobymao/sqlglot/compare/v25.18.0...v25.19.0
[v25.20.0]: https://github.com/tobymao/sqlglot/compare/v25.19.0...v25.20.0
[v25.20.1]: https://github.com/tobymao/sqlglot/compare/v25.20.0...v25.20.1
[v25.21.0]: https://github.com/tobymao/sqlglot/compare/v25.20.1...v25.21.0
[v25.21.1]: https://github.com/tobymao/sqlglot/compare/v25.21.0...v25.21.1
[v25.21.2]: https://github.com/tobymao/sqlglot/compare/v25.21.1...v25.21.2
[v25.21.3]: https://github.com/tobymao/sqlglot/compare/v25.21.2...v25.21.3
[v25.22.0]: https://github.com/tobymao/sqlglot/compare/v25.21.3...v25.22.0
[v25.23.0]: https://github.com/tobymao/sqlglot/compare/v25.20.2...v25.23.0
[v25.23.1]: https://github.com/tobymao/sqlglot/compare/v25.23.0...v25.23.1
[v25.23.2]: https://github.com/tobymao/sqlglot/compare/v25.23.1...v25.23.2
[v25.24.0]: https://github.com/tobymao/sqlglot/compare/v25.23.2...v25.24.0
[v25.24.1]: https://github.com/tobymao/sqlglot/compare/v25.24.0...v25.24.1
[v25.24.2]: https://github.com/tobymao/sqlglot/compare/v25.24.1...v25.24.2
[v25.24.3]: https://github.com/tobymao/sqlglot/compare/v25.24.2...v25.24.3
[v25.24.4]: https://github.com/tobymao/sqlglot/compare/v25.24.3...v25.24.4
[v25.24.5]: https://github.com/tobymao/sqlglot/compare/v25.24.4...v25.24.5
[v25.25.0]: https://github.com/tobymao/sqlglot/compare/v25.24.5...v25.25.0
[v25.25.1]: https://github.com/tobymao/sqlglot/compare/v25.25.0...v25.25.1
[v25.26.0]: https://github.com/tobymao/sqlglot/compare/v25.25.1...v25.26.0
[v25.27.0]: https://github.com/tobymao/sqlglot/compare/v25.26.0...v25.27.0
[v25.28.0]: https://github.com/tobymao/sqlglot/compare/v25.27.0...v25.28.0
[v25.29.0]: https://github.com/tobymao/sqlglot/compare/v25.28.0...v25.29.0
[v25.30.0]: https://github.com/tobymao/sqlglot/compare/v25.29.0...v25.30.0
[v25.31.0]: https://github.com/tobymao/sqlglot/compare/v25.30.0...v25.31.0
[v25.31.1]: https://github.com/tobymao/sqlglot/compare/v25.31.0...v25.31.1
[v25.31.2]: https://github.com/tobymao/sqlglot/compare/v25.31.1...v25.31.2
[v25.31.3]: https://github.com/tobymao/sqlglot/compare/v25.31.2...v25.31.3
[v25.31.4]: https://github.com/tobymao/sqlglot/compare/v25.31.3...v25.31.4
[v25.32.0]: https://github.com/tobymao/sqlglot/compare/v25.31.4...v25.32.0
[v25.32.1]: https://github.com/tobymao/sqlglot/compare/v25.32.0...v25.32.1
[v25.33.0]: https://github.com/tobymao/sqlglot/compare/v25.32.1...v25.33.0
[v25.34.0]: https://github.com/tobymao/sqlglot/compare/v25.33.0...v25.34.0
[v25.34.1]: https://github.com/tobymao/sqlglot/compare/v25.34.0...v25.34.1
[v26.0.0]: https://github.com/tobymao/sqlglot/compare/v25.34.1...v26.0.0
[v26.0.1]: https://github.com/tobymao/sqlglot/compare/v26.0.0...v26.0.1
[v26.1.0]: https://github.com/tobymao/sqlglot/compare/v26.0.1...v26.1.0
[v26.1.1]: https://github.com/tobymao/sqlglot/compare/v26.1.0...v26.1.1
[v26.1.2]: https://github.com/tobymao/sqlglot/compare/v26.1.1...v26.1.2
[v26.1.3]: https://github.com/tobymao/sqlglot/compare/v26.1.2...v26.1.3
