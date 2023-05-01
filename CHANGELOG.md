Changelog
=========

## [v28.6.0] - 2026-01-13
### :boom: BREAKING CHANGES
- due to [`06ce65a`](https://github.com/tobymao/sqlglot/commit/06ce65ab4235d180d30c59a74756bdd8026fddc7) - support transpilation of bitwise agg functions *(PR [#6580](https://github.com/tobymao/sqlglot/pull/6580) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*:

  support transpilation of bitwise agg functions (#6580)

- due to [`1497ee6`](https://github.com/tobymao/sqlglot/commit/1497ee6197dd1bd15926ab40bb09f03f72a4da34) - corrected handling of ToChar for Postgres *(commit by [@dhawkins1234](https://github.com/dhawkins1234))*:

  corrected handling of ToChar for Postgres

- due to [`c52705d`](https://github.com/tobymao/sqlglot/commit/c52705dec16390e0651d8a68c3500d8fa11dff12) - annotate EXISTS, ALL, ANY as BOOLEAN *(PR [#6590](https://github.com/tobymao/sqlglot/pull/6590) by [@doripo](https://github.com/doripo))*:

  annotate EXISTS, ALL, ANY as BOOLEAN (#6590)

- due to [`8ef9d7b`](https://github.com/tobymao/sqlglot/commit/8ef9d7b2183589217df10004798fd751d4d618d0) - support transpilation of GREATEST, GREATEST_IGNORE_NULLS, LEAST, LEAST_IGNORE_NULLS from snowflake to duckdb *(PR [#6579](https://github.com/tobymao/sqlglot/pull/6579) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*:

  support transpilation of GREATEST, GREATEST_IGNORE_NULLS, LEAST, LEAST_IGNORE_NULLS from snowflake to duckdb (#6579)

- due to [`b93291b`](https://github.com/tobymao/sqlglot/commit/b93291bee7ada32b4d686db919d8d3d683d95425) - support transpilation of TRY_TO_BOOLEAN from Snowflake to DuckDB *(PR [#6594](https://github.com/tobymao/sqlglot/pull/6594) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*:

  support transpilation of TRY_TO_BOOLEAN from Snowflake to DuckDB (#6594)

- due to [`00e596d`](https://github.com/tobymao/sqlglot/commit/00e596d1a7a6b806cd1c632afd6b09f4f5270086) - support transpilation of BOOLXOR_AGG, BOOLAND_AGG, and BOOLOR_AGG from Snowflake to DuckDB *(PR [#6592](https://github.com/tobymao/sqlglot/pull/6592) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  support transpilation of BOOLXOR_AGG, BOOLAND_AGG, and BOOLOR_AGG from Snowflake to DuckDB (#6592)

- due to [`0ba33ad`](https://github.com/tobymao/sqlglot/commit/0ba33ad69abcb718f761090cdd867e45d9481b80) - Add transpilation support for DAYNAME and MONTHNAME functions. *(PR [#6603](https://github.com/tobymao/sqlglot/pull/6603) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Add transpilation support for DAYNAME and MONTHNAME functions. (#6603)

- due to [`62c0ef0`](https://github.com/tobymao/sqlglot/commit/62c0ef0ad4579a55d65498eddd4e649ae464b559) - Fix BQ's exp.Date transpilation *(PR [#6595](https://github.com/tobymao/sqlglot/pull/6595) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix BQ's exp.Date transpilation (#6595)

- due to [`77ff9d0`](https://github.com/tobymao/sqlglot/commit/77ff9d0c5cec8fdba7e64fe02c0db0a63a64f1e0) - annotate types for MAP_* functions *(PR [#6605](https://github.com/tobymao/sqlglot/pull/6605) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  annotate types for MAP_* functions (#6605)

- due to [`e7c1574`](https://github.com/tobymao/sqlglot/commit/e7c1574f89314a304933a2b3b391528d530ea596) - Add transpilation support for DATE_DIFF function *(PR [#6609](https://github.com/tobymao/sqlglot/pull/6609) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*:

  Add transpilation support for DATE_DIFF function (#6609)

- due to [`d8f0bbd`](https://github.com/tobymao/sqlglot/commit/d8f0bbdbd208acc0eccb7e9be331d2661169ad97) - conditionally consume +/- in scientific literal notation *(PR [#6610](https://github.com/tobymao/sqlglot/pull/6610) by [@georgesittas](https://github.com/georgesittas))*:

  conditionally consume +/- in scientific literal notation (#6610)

- due to [`13014e0`](https://github.com/tobymao/sqlglot/commit/13014e0f01d10f0a078ef8aed4569d3aa8bd741b) - move postgres range parsers to global level *(PR [#6591](https://github.com/tobymao/sqlglot/pull/6591) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  move postgres range parsers to global level (#6591)

- due to [`d26ff44`](https://github.com/tobymao/sqlglot/commit/d26ff4444fba6697f25efa3dea5ab751fe560f6b) - support adjacent ranges operator *(PR [#6611](https://github.com/tobymao/sqlglot/pull/6611) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  support adjacent ranges operator (#6611)

- due to [`1f3436b`](https://github.com/tobymao/sqlglot/commit/1f3436bfe7ddbbf212d61616153f150d32824450) - bq robust literal/non-literal type annotation *(PR [#6600](https://github.com/tobymao/sqlglot/pull/6600) by [@geooo109](https://github.com/geooo109))*:

  bq robust literal/non-literal type annotation (#6600)

- due to [`8f38887`](https://github.com/tobymao/sqlglot/commit/8f3888746a1f36446544483e920e1287ebf76b4f) - annotate snowflake array construct *(commit by [@georgesittas](https://github.com/georgesittas))*:

  annotate snowflake array construct

- due to [`870dba4`](https://github.com/tobymao/sqlglot/commit/870dba41cc88dc9e4802f3695f3f715e0c35e0ed) - support transpilation of LAST_DAY from Snowflake to Duckdb *(PR [#6614](https://github.com/tobymao/sqlglot/pull/6614) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*:

  support transpilation of LAST_DAY from Snowflake to Duckdb (#6614)

- due to [`870d600`](https://github.com/tobymao/sqlglot/commit/870d600a93108b1a1d68936244564548dec5f683) - support: postgres point *(PR [#6615](https://github.com/tobymao/sqlglot/pull/6615) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  support: postgres point (#6615)

- due to [`302fda0`](https://github.com/tobymao/sqlglot/commit/302fda0151094bc074b98847390cd554414258bb) - support and, or *(PR [#6625](https://github.com/tobymao/sqlglot/pull/6625) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  support and, or (#6625)

- due to [`fc5800d`](https://github.com/tobymao/sqlglot/commit/fc5800d1359f9b0933622cb355fdb7a34f2f486a) - support Snowflake to DuckDB transpilation of ZIPF  *(PR [#6618](https://github.com/tobymao/sqlglot/pull/6618) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  support Snowflake to DuckDB transpilation of ZIPF  (#6618)

- due to [`dea22ca`](https://github.com/tobymao/sqlglot/commit/dea22ca07076c1ef6a08f06f9fdc6070ca0fecb8) - support `RECORD` type *(PR [#6635](https://github.com/tobymao/sqlglot/pull/6635) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  support `RECORD` type (#6635)

- due to [`77783da`](https://github.com/tobymao/sqlglot/commit/77783da1329ad5d681a7d37928e4dbedd68ab365) - support: var-args in `xor` *(PR [#6634](https://github.com/tobymao/sqlglot/pull/6634) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  support: var-args in `xor` (#6634)

- due to [`b75a3e3`](https://github.com/tobymao/sqlglot/commit/b75a3e3b5d948f55df5b0096d116edf3b898e5e1) - handle BINARY keyword *(PR [#6636](https://github.com/tobymao/sqlglot/pull/6636) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  handle BINARY keyword (#6636)

- due to [`e7b5d6f`](https://github.com/tobymao/sqlglot/commit/e7b5d6f1b8031fbf25e1fd7c3b42309c4aae8810) - support transpilation of TRY_TO_BINARY from Snowflake to DuckDB *(PR [#6629](https://github.com/tobymao/sqlglot/pull/6629) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*:

  support transpilation of TRY_TO_BINARY from Snowflake to DuckDB (#6629)

- due to [`2bf9405`](https://github.com/tobymao/sqlglot/commit/2bf9405adf9c9c72c77f7aa8ab792779a3b9c5f3) - USING keyword in chr *(PR [#6637](https://github.com/tobymao/sqlglot/pull/6637) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  USING keyword in chr (#6637)

- due to [`235fc14`](https://github.com/tobymao/sqlglot/commit/235fc14f40c422e6339da0a6b17252ab9eb18ec2) - support charset *(PR [#6633](https://github.com/tobymao/sqlglot/pull/6633) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  support charset (#6633)

- due to [`d46969d`](https://github.com/tobymao/sqlglot/commit/d46969db50ff52b8657d5b33f0a106b69dbd1e2a) - annotate snowflake ARRAY_APPEND and ARRAY_PREPEND *(PR [#6645](https://github.com/tobymao/sqlglot/pull/6645) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*:

  annotate snowflake ARRAY_APPEND and ARRAY_PREPEND (#6645)

- due to [`9c54329`](https://github.com/tobymao/sqlglot/commit/9c543291a0e28ad044523747d19262243fed1f5d) - Type annotation for Snowflake ENCRYPT and DECRYPT functions *(PR [#6643](https://github.com/tobymao/sqlglot/pull/6643) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  Type annotation for Snowflake ENCRYPT and DECRYPT functions (#6643)

- due to [`7870bd0`](https://github.com/tobymao/sqlglot/commit/7870bd0dc7503d0a1863d7623be7fc12bb9412e4) - type annotation for COVAR_POP and COVAR_SAMP *(PR [#6656](https://github.com/tobymao/sqlglot/pull/6656) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  type annotation for COVAR_POP and COVAR_SAMP (#6656)

- due to [`2fa29b1`](https://github.com/tobymao/sqlglot/commit/2fa29b1e0feb6d8f9290c105e5fa0f349a011e22) - Type annotation for ARRAY_REMOVE function *(PR [#6653](https://github.com/tobymao/sqlglot/pull/6653) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*:

  Type annotation for ARRAY_REMOVE function (#6653)

- due to [`3d59af5`](https://github.com/tobymao/sqlglot/commit/3d59af557caf5c0fd109ae687b8408264543f9ea) - Added UNIFORM transpilation for Snowflake to DuckDB *(PR [#6640](https://github.com/tobymao/sqlglot/pull/6640) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  Added UNIFORM transpilation for Snowflake to DuckDB (#6640)

- due to [`5552f12`](https://github.com/tobymao/sqlglot/commit/5552f121f9c9a8103fc3ccf58fd5eed076fe0575) - annotation support for LOCALTIME function *(PR [#6651](https://github.com/tobymao/sqlglot/pull/6651) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  annotation support for LOCALTIME function (#6651)

- due to [`a669651`](https://github.com/tobymao/sqlglot/commit/a6696518efefabaf815d7c61f1ae923c39ce6107) - Type annotation for Snowflake LOCALTIMESTAMP *(PR [#6652](https://github.com/tobymao/sqlglot/pull/6652) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  Type annotation for Snowflake LOCALTIMESTAMP (#6652)

- due to [`40ccce4`](https://github.com/tobymao/sqlglot/commit/40ccce492746cc969467c234b9c7d345454da683) - Transpile Snowflake NORMAL to DuckDB using Box-Muller transform *(PR [#6654](https://github.com/tobymao/sqlglot/pull/6654) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  Transpile Snowflake NORMAL to DuckDB using Box-Muller transform (#6654)

- due to [`d6bf569`](https://github.com/tobymao/sqlglot/commit/d6bf569ef2442d5055ab788a2078adf485cc1120) - updates for Snowflake to DuckDB transpilation of TO_TIMESTAMP functions *(PR [#6622](https://github.com/tobymao/sqlglot/pull/6622) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  updates for Snowflake to DuckDB transpilation of TO_TIMESTAMP functions (#6622)

- due to [`13251fd`](https://github.com/tobymao/sqlglot/commit/13251fd7f03d31af69b86a4ef5bb6e940cc9318b) - annotation support for ELT function *(PR [#6659](https://github.com/tobymao/sqlglot/pull/6659) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  annotation support for ELT function (#6659)

- due to [`b287e4e`](https://github.com/tobymao/sqlglot/commit/b287e4ef9e05c0ba1fadc3638977e994eb911834) - Support transpilation for BITMAP_BUCKET_NUMBER *(PR [#6668](https://github.com/tobymao/sqlglot/pull/6668) by [@fivetran-ashashankar](https://github.com/fivetran-ashashankar))*:

  Support transpilation for BITMAP_BUCKET_NUMBER (#6668)

- due to [`d5b84cb`](https://github.com/tobymao/sqlglot/commit/d5b84cbbd1406ebd9f5e2373e12b0d15dcb85d7f) - ignore comments in the semantic differ *(PR [#6675](https://github.com/tobymao/sqlglot/pull/6675) by [@georgesittas](https://github.com/georgesittas))*:

  ignore comments in the semantic differ (#6675)

- due to [`399c80f`](https://github.com/tobymao/sqlglot/commit/399c80f2e66a1acdcd7d567e44c297db7071dc8f) - rename args for CovarSamp/Pop, stop inheriting from Binary *(commit by [@georgesittas](https://github.com/georgesittas))*:

  rename args for CovarSamp/Pop, stop inheriting from Binary

- due to [`c9a70c0`](https://github.com/tobymao/sqlglot/commit/c9a70c004e0ec563e8a712668a30da9856cb0516) - update transpilation of DATE_TRUNC to duckdb *(PR [#6644](https://github.com/tobymao/sqlglot/pull/6644) by [@toriwei](https://github.com/toriwei))*:

  update transpilation of DATE_TRUNC to duckdb (#6644)

- due to [`460b3a2`](https://github.com/tobymao/sqlglot/commit/460b3a2ae62d8294c57068453b5a11e4a7e12a91) - Allow varlen args in exp.MD5Digest *(PR [#6685](https://github.com/tobymao/sqlglot/pull/6685) by [@VaggelisD](https://github.com/VaggelisD))*:

  Allow varlen args in exp.MD5Digest (#6685)

- due to [`dcdee68`](https://github.com/tobymao/sqlglot/commit/dcdee68cb1a77286232865de9df8d8a01898fcc7) - Allow non aggregation functions in PIVOT *(PR [#6687](https://github.com/tobymao/sqlglot/pull/6687) by [@VaggelisD](https://github.com/VaggelisD))*:

  Allow non aggregation functions in PIVOT (#6687)

- due to [`a4be3fa`](https://github.com/tobymao/sqlglot/commit/a4be3faf63c981a2b10b1f8c709581acf59277ce) - support SYSTIMESTAMP as NO_PAREN_FUNCTION *(PR [#6677](https://github.com/tobymao/sqlglot/pull/6677) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  support SYSTIMESTAMP as NO_PAREN_FUNCTION (#6677)

- due to [`243448c`](https://github.com/tobymao/sqlglot/commit/243448ca5ccc6a26d680661dcb7d921a055dbfa9) - Transpile date extraction from Snowflake to DuckDB (YEAR*, WEEK*, DAY*, etc) *(PR [#6666](https://github.com/tobymao/sqlglot/pull/6666) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  Transpile date extraction from Snowflake to DuckDB (YEAR*, WEEK*, DAY*, etc) (#6666)

- due to [`29cff1f`](https://github.com/tobymao/sqlglot/commit/29cff1fbe0011476b22e1b1442af857499f871a6) - bq robust literal/non-literal binary annotation *(PR [#6688](https://github.com/tobymao/sqlglot/pull/6688) by [@geooo109](https://github.com/geooo109))*:

  bq robust literal/non-literal binary annotation (#6688)

- due to [`65acd6c`](https://github.com/tobymao/sqlglot/commit/65acd6c00f023c3279947b23e04139bc9554db85) - transpile snowflake SYSDATE *(PR [#6693](https://github.com/tobymao/sqlglot/pull/6693) by [@fivetran-ashashankar](https://github.com/fivetran-ashashankar))*:

  transpile snowflake SYSDATE (#6693)

- due to [`36ff96c`](https://github.com/tobymao/sqlglot/commit/36ff96c727e40dbe467d6338128f87a74b99a980) - support transpilation of BITSHIFTLEFT and BITSHIFTRIGHT *(PR [#6586](https://github.com/tobymao/sqlglot/pull/6586) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  support transpilation of BITSHIFTLEFT and BITSHIFTRIGHT (#6586)

- due to [`fb8f57a`](https://github.com/tobymao/sqlglot/commit/fb8f57ac54a4e0213941f3bd057ccb03c4e125b7) - Fix UPDATE statement for multi tables *(PR [#6700](https://github.com/tobymao/sqlglot/pull/6700) by [@VaggelisD](https://github.com/VaggelisD))*:

  Fix UPDATE statement for multi tables (#6700)

- due to [`21f9d3c`](https://github.com/tobymao/sqlglot/commit/21f9d3ca7c6fa0d38f0f63000f904589b91c7d0a) - Normalize struct field names when annotating types *(PR [#6674](https://github.com/tobymao/sqlglot/pull/6674) by [@vchan](https://github.com/vchan))*:

  Normalize struct field names when annotating types (#6674)

- due to [`7362c23`](https://github.com/tobymao/sqlglot/commit/7362c2357eba540a12eea079e9f4212f3545c8d1) - interval expressions *(PR [#6648](https://github.com/tobymao/sqlglot/pull/6648) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  interval expressions (#6648)

- due to [`a503a7a`](https://github.com/tobymao/sqlglot/commit/a503a7adbfe3352924e94a21303a2ac23903833c) - annotate Encode for hive dialect hierarchy *(commit by [@georgesittas](https://github.com/georgesittas))*:

  annotate Encode for hive dialect hierarchy

- due to [`c447df7`](https://github.com/tobymao/sqlglot/commit/c447df71a645f55e4798887bde4d42285f5dea4a) - annotate the LOCALTIMESTAMP *(PR [#6709](https://github.com/tobymao/sqlglot/pull/6709) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  annotate the LOCALTIMESTAMP (#6709)

- due to [`e91a14b`](https://github.com/tobymao/sqlglot/commit/e91a14b5b325d3d71fbdbe701e531588723bcd2e) - annotate the CURRENT_TIMEZONE *(PR [#6708](https://github.com/tobymao/sqlglot/pull/6708) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  annotate the CURRENT_TIMEZONE (#6708)

- due to [`b53c8d4`](https://github.com/tobymao/sqlglot/commit/b53c8d47481d735c74ae8704a90594f33263eee8) - annotate the UNIX_TIMESTAMP *(PR [#6717](https://github.com/tobymao/sqlglot/pull/6717) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  annotate the UNIX_TIMESTAMP (#6717)

- due to [`cfb06ab`](https://github.com/tobymao/sqlglot/commit/cfb06ab58c606715e688d39e704b1587fcd256e8) - add support for transpiling ARRAY_AGG with ORDER BY *(PR [#6691](https://github.com/tobymao/sqlglot/pull/6691) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*:

  add support for transpiling ARRAY_AGG with ORDER BY (#6691)

- due to [`2965077`](https://github.com/tobymao/sqlglot/commit/296507773fc4476862cc1ee374f88c7d73cb147f) - add support for JSON_KEYS func *(PR [#6718](https://github.com/tobymao/sqlglot/pull/6718) by [@geooo109](https://github.com/geooo109))*:

  add support for JSON_KEYS func (#6718)

- due to [`bbaba5f`](https://github.com/tobymao/sqlglot/commit/bbaba5fd7d0111947035534168593a5a3ee6839a) - annotate type for snowflake ARRAY_CAT *(PR [#6721](https://github.com/tobymao/sqlglot/pull/6721) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*:

  annotate type for snowflake ARRAY_CAT (#6721)

- due to [`0a1c7ab`](https://github.com/tobymao/sqlglot/commit/0a1c7abddc0cbd2f853a431848c1ae45e6876ba2) - support transpilation of GETBIT from snowflake to duckdb *(PR [#6692](https://github.com/tobymao/sqlglot/pull/6692) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*:

  support transpilation of GETBIT from snowflake to duckdb (#6692)

- due to [`cb320c4`](https://github.com/tobymao/sqlglot/commit/cb320c41361f0bb7ad71522366ff4cb8691607bf) - bq annotate type for raw strings *(PR [#6723](https://github.com/tobymao/sqlglot/pull/6723) by [@geooo109](https://github.com/geooo109))*:

  bq annotate type for raw strings (#6723)

- due to [`09fa467`](https://github.com/tobymao/sqlglot/commit/09fa467461656d5c4d4e57c7044c46ff4fcf3f7f) - Annotate ATAN2 for Spark & DBX *(PR [#6725](https://github.com/tobymao/sqlglot/pull/6725) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  Annotate ATAN2 for Spark & DBX (#6725)

- due to [`b59b3bf`](https://github.com/tobymao/sqlglot/commit/b59b3bfd06c18120db2938748c561495dd885ab4) - Annotate TANH for Spark & DBX  *(PR [#6726](https://github.com/tobymao/sqlglot/pull/6726) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  Annotate TANH for Spark & DBX  (#6726)

- due to [`917071b`](https://github.com/tobymao/sqlglot/commit/917071b42b91f5111d88d599a1caed83e6d7661c) - Support transpilation of TO_TIME and TRY_TO_TIME from snowflake to duckdb *(PR [#6690](https://github.com/tobymao/sqlglot/pull/6690) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  Support transpilation of TO_TIME and TRY_TO_TIME from snowflake to duckdb (#6690)

- due to [`e50a97e`](https://github.com/tobymao/sqlglot/commit/e50a97ed05d50be9fbea7720511a760c11f4a86e) - Type annotate for Snowflake Kurtosis *(PR [#6720](https://github.com/tobymao/sqlglot/pull/6720) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  Type annotate for Snowflake Kurtosis (#6720)

- due to [`33b8a5d`](https://github.com/tobymao/sqlglot/commit/33b8a5d25bf49791fb95ec20132ab6ff0bb885e0) - bump sqlglotrs to 0.11.0 *(commit by [@georgesittas](https://github.com/georgesittas))*:

  bump sqlglotrs to 0.11.0


### :sparkles: New Features
- [`06ce65a`](https://github.com/tobymao/sqlglot/commit/06ce65ab4235d180d30c59a74756bdd8026fddc7) - **snowflake**: support transpilation of bitwise agg functions *(PR [#6580](https://github.com/tobymao/sqlglot/pull/6580) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*
- [`82abf40`](https://github.com/tobymao/sqlglot/commit/82abf4085dd93d23b1612304704fcb89b2cb09e2) - **duckdb**: Add tranwspilation support for CEIL function *(PR [#6589](https://github.com/tobymao/sqlglot/pull/6589) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`9f9994d`](https://github.com/tobymao/sqlglot/commit/9f9994df1582a1b3e16dcf27c618dadfdffd8745) - **duckdb**: Add transpilation support for float/decimal numbers and preserve end-of-month logic & type *(PR [#6576](https://github.com/tobymao/sqlglot/pull/6576) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`8ef9d7b`](https://github.com/tobymao/sqlglot/commit/8ef9d7b2183589217df10004798fd751d4d618d0) - **snowflake**: support transpilation of GREATEST, GREATEST_IGNORE_NULLS, LEAST, LEAST_IGNORE_NULLS from snowflake to duckdb *(PR [#6579](https://github.com/tobymao/sqlglot/pull/6579) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*
- [`b93291b`](https://github.com/tobymao/sqlglot/commit/b93291bee7ada32b4d686db919d8d3d683d95425) - **snowflake**: support transpilation of TRY_TO_BOOLEAN from Snowflake to DuckDB *(PR [#6594](https://github.com/tobymao/sqlglot/pull/6594) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*
- [`00e596d`](https://github.com/tobymao/sqlglot/commit/00e596d1a7a6b806cd1c632afd6b09f4f5270086) - **snowflake**: support transpilation of BOOLXOR_AGG, BOOLAND_AGG, and BOOLOR_AGG from Snowflake to DuckDB *(PR [#6592](https://github.com/tobymao/sqlglot/pull/6592) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`0ba33ad`](https://github.com/tobymao/sqlglot/commit/0ba33ad69abcb718f761090cdd867e45d9481b80) - **duckdb**: Add transpilation support for DAYNAME and MONTHNAME functions. *(PR [#6603](https://github.com/tobymao/sqlglot/pull/6603) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`77ff9d0`](https://github.com/tobymao/sqlglot/commit/77ff9d0c5cec8fdba7e64fe02c0db0a63a64f1e0) - **snowflake**: annotate types for MAP_* functions *(PR [#6605](https://github.com/tobymao/sqlglot/pull/6605) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`e7c1574`](https://github.com/tobymao/sqlglot/commit/e7c1574f89314a304933a2b3b391528d530ea596) - **duckdb**: Add transpilation support for DATE_DIFF function *(PR [#6609](https://github.com/tobymao/sqlglot/pull/6609) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`d26ff44`](https://github.com/tobymao/sqlglot/commit/d26ff4444fba6697f25efa3dea5ab751fe560f6b) - **postgres**: support adjacent ranges operator *(PR [#6611](https://github.com/tobymao/sqlglot/pull/6611) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`8f38887`](https://github.com/tobymao/sqlglot/commit/8f3888746a1f36446544483e920e1287ebf76b4f) - **optimizer**: annotate snowflake array construct *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`870dba4`](https://github.com/tobymao/sqlglot/commit/870dba41cc88dc9e4802f3695f3f715e0c35e0ed) - **snowflake**: support transpilation of LAST_DAY from Snowflake to Duckdb *(PR [#6614](https://github.com/tobymao/sqlglot/pull/6614) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*
- [`fc5800d`](https://github.com/tobymao/sqlglot/commit/fc5800d1359f9b0933622cb355fdb7a34f2f486a) - **snowflake**: support Snowflake to DuckDB transpilation of ZIPF  *(PR [#6618](https://github.com/tobymao/sqlglot/pull/6618) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`4aea018`](https://github.com/tobymao/sqlglot/commit/4aea018c2d4b701ac1b895445ef390307666693f) - **duckdb**: Add transpilation support for nanoseconds used in date/time functions. *(PR [#6617](https://github.com/tobymao/sqlglot/pull/6617) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`dea22ca`](https://github.com/tobymao/sqlglot/commit/dea22ca07076c1ef6a08f06f9fdc6070ca0fecb8) - **singlestore**: support `RECORD` type *(PR [#6635](https://github.com/tobymao/sqlglot/pull/6635) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`77783da`](https://github.com/tobymao/sqlglot/commit/77783da1329ad5d681a7d37928e4dbedd68ab365) - **clickhouse**: support: var-args in `xor` *(PR [#6634](https://github.com/tobymao/sqlglot/pull/6634) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`b75a3e3`](https://github.com/tobymao/sqlglot/commit/b75a3e3b5d948f55df5b0096d116edf3b898e5e1) - **mysql**: handle BINARY keyword *(PR [#6636](https://github.com/tobymao/sqlglot/pull/6636) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`c17878a`](https://github.com/tobymao/sqlglot/commit/c17878a35c761824b58b0e0743585e3a5717d1b4) - add ability to create dialect plugins *(PR [#6627](https://github.com/tobymao/sqlglot/pull/6627) by [@vgvoleg](https://github.com/vgvoleg))*
  - :arrow_lower_right: *addresses issue [#6626](https://github.com/tobymao/sqlglot/issues/6626) opened by [@vgvoleg](https://github.com/vgvoleg)*
- [`e7b5d6f`](https://github.com/tobymao/sqlglot/commit/e7b5d6f1b8031fbf25e1fd7c3b42309c4aae8810) - **snowflake**: support transpilation of TRY_TO_BINARY from Snowflake to DuckDB *(PR [#6629](https://github.com/tobymao/sqlglot/pull/6629) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*
- [`9c54329`](https://github.com/tobymao/sqlglot/commit/9c543291a0e28ad044523747d19262243fed1f5d) - **snowflake**: Type annotation for Snowflake ENCRYPT and DECRYPT functions *(PR [#6643](https://github.com/tobymao/sqlglot/pull/6643) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`7870bd0`](https://github.com/tobymao/sqlglot/commit/7870bd0dc7503d0a1863d7623be7fc12bb9412e4) - **snowflake**: type annotation for COVAR_POP and COVAR_SAMP *(PR [#6656](https://github.com/tobymao/sqlglot/pull/6656) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`2fa29b1`](https://github.com/tobymao/sqlglot/commit/2fa29b1e0feb6d8f9290c105e5fa0f349a011e22) - **snowflake**: Type annotation for ARRAY_REMOVE function *(PR [#6653](https://github.com/tobymao/sqlglot/pull/6653) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*
- [`3d59af5`](https://github.com/tobymao/sqlglot/commit/3d59af557caf5c0fd109ae687b8408264543f9ea) - **snowflake**: Added UNIFORM transpilation for Snowflake to DuckDB *(PR [#6640](https://github.com/tobymao/sqlglot/pull/6640) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`5552f12`](https://github.com/tobymao/sqlglot/commit/5552f121f9c9a8103fc3ccf58fd5eed076fe0575) - annotation support for LOCALTIME function *(PR [#6651](https://github.com/tobymao/sqlglot/pull/6651) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`a669651`](https://github.com/tobymao/sqlglot/commit/a6696518efefabaf815d7c61f1ae923c39ce6107) - **snowflake**: Type annotation for Snowflake LOCALTIMESTAMP *(PR [#6652](https://github.com/tobymao/sqlglot/pull/6652) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`40ccce4`](https://github.com/tobymao/sqlglot/commit/40ccce492746cc969467c234b9c7d345454da683) - **duckdb**: Transpile Snowflake NORMAL to DuckDB using Box-Muller transform *(PR [#6654](https://github.com/tobymao/sqlglot/pull/6654) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`d6bf569`](https://github.com/tobymao/sqlglot/commit/d6bf569ef2442d5055ab788a2078adf485cc1120) - **snowflake**: updates for Snowflake to DuckDB transpilation of TO_TIMESTAMP functions *(PR [#6622](https://github.com/tobymao/sqlglot/pull/6622) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`51af50e`](https://github.com/tobymao/sqlglot/commit/51af50e937dafb80d42cd639b7fb8e139d811ea0) - **snowflake**: support out of range values for DATE_FROM_PARTS when transpiling to DuckDB *(PR [#6671](https://github.com/tobymao/sqlglot/pull/6671) by [@toriwei](https://github.com/toriwei))*
- [`13251fd`](https://github.com/tobymao/sqlglot/commit/13251fd7f03d31af69b86a4ef5bb6e940cc9318b) - **mysql**: annotation support for ELT function *(PR [#6659](https://github.com/tobymao/sqlglot/pull/6659) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`b287e4e`](https://github.com/tobymao/sqlglot/commit/b287e4ef9e05c0ba1fadc3638977e994eb911834) - **snowflake**: Support transpilation for BITMAP_BUCKET_NUMBER *(PR [#6668](https://github.com/tobymao/sqlglot/pull/6668) by [@fivetran-ashashankar](https://github.com/fivetran-ashashankar))*
- [`d5b84cb`](https://github.com/tobymao/sqlglot/commit/d5b84cbbd1406ebd9f5e2373e12b0d15dcb85d7f) - **diff**: ignore comments in the semantic differ *(PR [#6675](https://github.com/tobymao/sqlglot/pull/6675) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *addresses issue [#6673](https://github.com/tobymao/sqlglot/issues/6673) opened by [@GaryLiuGTA](https://github.com/GaryLiuGTA)*
- [`26c16f2`](https://github.com/tobymao/sqlglot/commit/26c16f2f4bb9aaa91253a06c337b8320dc4609c9) - **snowflake**: transpile CORR with NaN-->NULL *(PR [#6619](https://github.com/tobymao/sqlglot/pull/6619) by [@treysp](https://github.com/treysp))*
- [`c9a70c0`](https://github.com/tobymao/sqlglot/commit/c9a70c004e0ec563e8a712668a30da9856cb0516) - **snowflake**: update transpilation of DATE_TRUNC to duckdb *(PR [#6644](https://github.com/tobymao/sqlglot/pull/6644) by [@toriwei](https://github.com/toriwei))*
- [`a4be3fa`](https://github.com/tobymao/sqlglot/commit/a4be3faf63c981a2b10b1f8c709581acf59277ce) - **oracle,exasol**: support SYSTIMESTAMP as NO_PAREN_FUNCTION *(PR [#6677](https://github.com/tobymao/sqlglot/pull/6677) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
  - :arrow_lower_right: *addresses issue [#6686](https://github.com/tobymao/sqlglot/issues/6686) opened by [@Hfuegl](https://github.com/Hfuegl)*
- [`243448c`](https://github.com/tobymao/sqlglot/commit/243448ca5ccc6a26d680661dcb7d921a055dbfa9) - **duckdb**: Transpile date extraction from Snowflake to DuckDB (YEAR*, WEEK*, DAY*, etc) *(PR [#6666](https://github.com/tobymao/sqlglot/pull/6666) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`efa77df`](https://github.com/tobymao/sqlglot/commit/efa77df3734f2e8534ca911d39c9a5c6103a82fa) - **mysql**: translate UPDATE … FROM … syntax to UPDATE … JOIN … when generating MySQL *(PR [#6655](https://github.com/tobymao/sqlglot/pull/6655) by [@brdbry](https://github.com/brdbry))*
- [`65acd6c`](https://github.com/tobymao/sqlglot/commit/65acd6c00f023c3279947b23e04139bc9554db85) - **duckdb**: transpile snowflake SYSDATE *(PR [#6693](https://github.com/tobymao/sqlglot/pull/6693) by [@fivetran-ashashankar](https://github.com/fivetran-ashashankar))*
- [`36ff96c`](https://github.com/tobymao/sqlglot/commit/36ff96c727e40dbe467d6338128f87a74b99a980) - **snowflake**: support transpilation of BITSHIFTLEFT and BITSHIFTRIGHT *(PR [#6586](https://github.com/tobymao/sqlglot/pull/6586) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`142acd2`](https://github.com/tobymao/sqlglot/commit/142acd21ae9e1b3b1495707b2b6811db830fa61f) - pretty formatting for nested data types *(PR [#6707](https://github.com/tobymao/sqlglot/pull/6707) by [@treysp](https://github.com/treysp))*
- [`a503a7a`](https://github.com/tobymao/sqlglot/commit/a503a7adbfe3352924e94a21303a2ac23903833c) - **optimizer**: annotate Encode for hive dialect hierarchy *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`c447df7`](https://github.com/tobymao/sqlglot/commit/c447df71a645f55e4798887bde4d42285f5dea4a) - **spark,databricks**: annotate the LOCALTIMESTAMP *(PR [#6709](https://github.com/tobymao/sqlglot/pull/6709) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`e91a14b`](https://github.com/tobymao/sqlglot/commit/e91a14b5b325d3d71fbdbe701e531588723bcd2e) - **spark,databricks**: annotate the CURRENT_TIMEZONE *(PR [#6708](https://github.com/tobymao/sqlglot/pull/6708) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`b53c8d4`](https://github.com/tobymao/sqlglot/commit/b53c8d47481d735c74ae8704a90594f33263eee8) - **hive,spark,databricks**: annotate the UNIX_TIMESTAMP *(PR [#6717](https://github.com/tobymao/sqlglot/pull/6717) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`cfb06ab`](https://github.com/tobymao/sqlglot/commit/cfb06ab58c606715e688d39e704b1587fcd256e8) - **duckdb**: add support for transpiling ARRAY_AGG with ORDER BY *(PR [#6691](https://github.com/tobymao/sqlglot/pull/6691) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*
- [`2965077`](https://github.com/tobymao/sqlglot/commit/296507773fc4476862cc1ee374f88c7d73cb147f) - **parser**: add support for JSON_KEYS func *(PR [#6718](https://github.com/tobymao/sqlglot/pull/6718) by [@geooo109](https://github.com/geooo109))*
- [`52de0a5`](https://github.com/tobymao/sqlglot/commit/52de0a5c9ebc14c738041e47a385981f072291a9) - **duckdb**: Add transpilation support for float/decimal numbers for TIME functions *(PR [#6719](https://github.com/tobymao/sqlglot/pull/6719) by [@fivetran-amrutabhimsenayachit](https://github.com/fivetran-amrutabhimsenayachit))*
- [`bbeb881`](https://github.com/tobymao/sqlglot/commit/bbeb881016e4b84dc5cb6a29846224784591abba) - **oracle**: Added support for IN/OUT keywords in stored procedure parameters *(PR [#6710](https://github.com/tobymao/sqlglot/pull/6710) by [@rsanchez-xtillion](https://github.com/rsanchez-xtillion))*
- [`0a1c7ab`](https://github.com/tobymao/sqlglot/commit/0a1c7abddc0cbd2f853a431848c1ae45e6876ba2) - **snowflake**: support transpilation of GETBIT from snowflake to duckdb *(PR [#6692](https://github.com/tobymao/sqlglot/pull/6692) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*
- [`cb320c4`](https://github.com/tobymao/sqlglot/commit/cb320c41361f0bb7ad71522366ff4cb8691607bf) - **optimizer**: bq annotate type for raw strings *(PR [#6723](https://github.com/tobymao/sqlglot/pull/6723) by [@geooo109](https://github.com/geooo109))*
- [`09fa467`](https://github.com/tobymao/sqlglot/commit/09fa467461656d5c4d4e57c7044c46ff4fcf3f7f) - **optimizer**: Annotate ATAN2 for Spark & DBX *(PR [#6725](https://github.com/tobymao/sqlglot/pull/6725) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`b59b3bf`](https://github.com/tobymao/sqlglot/commit/b59b3bfd06c18120db2938748c561495dd885ab4) - **optimizer**: Annotate TANH for Spark & DBX  *(PR [#6726](https://github.com/tobymao/sqlglot/pull/6726) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`917071b`](https://github.com/tobymao/sqlglot/commit/917071b42b91f5111d88d599a1caed83e6d7661c) - **snowflake**: Support transpilation of TO_TIME and TRY_TO_TIME from snowflake to duckdb *(PR [#6690](https://github.com/tobymao/sqlglot/pull/6690) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`e50a97e`](https://github.com/tobymao/sqlglot/commit/e50a97ed05d50be9fbea7720511a760c11f4a86e) - **snowflake**: Type annotate for Snowflake Kurtosis *(PR [#6720](https://github.com/tobymao/sqlglot/pull/6720) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`197943f`](https://github.com/tobymao/sqlglot/commit/197943fb56e997cbb7e77a8b2d9f0c2453d052c7) - **postgres**: support index predicate in conflict `INSERT` clause closes [#6727](https://github.com/tobymao/sqlglot/pull/6727) *(commit by [@georgesittas](https://github.com/georgesittas))*

### :bug: Bug Fixes
- [`8f16463`](https://github.com/tobymao/sqlglot/commit/8f16463ebdbb7f51ab778cf34ab89709e018691f) - **presto**: Add support for WEEK function *(PR [#6593](https://github.com/tobymao/sqlglot/pull/6593) by [@chrisqu777](https://github.com/chrisqu777))*
- [`1497ee6`](https://github.com/tobymao/sqlglot/commit/1497ee6197dd1bd15926ab40bb09f03f72a4da34) - **postgres**: corrected handling of ToChar for Postgres *(commit by [@dhawkins1234](https://github.com/dhawkins1234))*
- [`c52705d`](https://github.com/tobymao/sqlglot/commit/c52705dec16390e0651d8a68c3500d8fa11dff12) - **optimizer**: annotate EXISTS, ALL, ANY as BOOLEAN *(PR [#6590](https://github.com/tobymao/sqlglot/pull/6590) by [@doripo](https://github.com/doripo))*
- [`eab6f72`](https://github.com/tobymao/sqlglot/commit/eab6f72a2829def3fd6958f47353ba60ed0e0334) - **parser**: only parse CREATE TABLE pk key ordering in tsql *(PR [#6604](https://github.com/tobymao/sqlglot/pull/6604) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#6596](https://github.com/tobymao/sqlglot/issues/6596) opened by [@osmith42](https://github.com/osmith42)*
- [`62c0ef0`](https://github.com/tobymao/sqlglot/commit/62c0ef0ad4579a55d65498eddd4e649ae464b559) - **duckdb**: Fix BQ's exp.Date transpilation *(PR [#6595](https://github.com/tobymao/sqlglot/pull/6595) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#6581](https://github.com/tobymao/sqlglot/issues/6581) opened by [@nikchha](https://github.com/nikchha)*
- [`d8f0bbd`](https://github.com/tobymao/sqlglot/commit/d8f0bbdbd208acc0eccb7e9be331d2661169ad97) - **tokenizer**: conditionally consume +/- in scientific literal notation *(PR [#6610](https://github.com/tobymao/sqlglot/pull/6610) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#6608](https://github.com/tobymao/sqlglot/issues/6608) opened by [@pjpjean](https://github.com/pjpjean)*
- [`c0b3e5c`](https://github.com/tobymao/sqlglot/commit/c0b3e5c06c7d0ec3335146eab1a86f202298c94a) - **spark**: make_interval week *(PR [#6612](https://github.com/tobymao/sqlglot/pull/6612) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`13014e0`](https://github.com/tobymao/sqlglot/commit/13014e0f01d10f0a078ef8aed4569d3aa8bd741b) - **postgres**: move postgres range parsers to global level *(PR [#6591](https://github.com/tobymao/sqlglot/pull/6591) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`1f3436b`](https://github.com/tobymao/sqlglot/commit/1f3436bfe7ddbbf212d61616153f150d32824450) - **optimizer**: bq robust literal/non-literal type annotation *(PR [#6600](https://github.com/tobymao/sqlglot/pull/6600) by [@geooo109](https://github.com/geooo109))*
- [`870d600`](https://github.com/tobymao/sqlglot/commit/870d600a93108b1a1d68936244564548dec5f683) - **postgres**: support: postgres point *(PR [#6615](https://github.com/tobymao/sqlglot/pull/6615) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`b8b22fa`](https://github.com/tobymao/sqlglot/commit/b8b22fae5cf17ebfe119f815c4cc0ef2dc8132bc) - **trino**: mark as supporting `...EXCEPT ALL` *(PR [#6616](https://github.com/tobymao/sqlglot/pull/6616) by [@NickCrews](https://github.com/NickCrews))*
- [`9382ebd`](https://github.com/tobymao/sqlglot/commit/9382ebdd79c89e9c92ae98a29147c1523cec415f) - **postgres**: Fix exp.WidthBucket required args *(PR [#6621](https://github.com/tobymao/sqlglot/pull/6621) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#6620](https://github.com/tobymao/sqlglot/issues/6620) opened by [@codetalker-ai](https://github.com/codetalker-ai)*
- [`302fda0`](https://github.com/tobymao/sqlglot/commit/302fda0151094bc074b98847390cd554414258bb) - **clickhouse**: support and, or *(PR [#6625](https://github.com/tobymao/sqlglot/pull/6625) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`00c80b1`](https://github.com/tobymao/sqlglot/commit/00c80b12936811dde7e661720c1bd17c6ba9271a) - Parse joins with derived tables in UPDATE *(PR [#6632](https://github.com/tobymao/sqlglot/pull/6632) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#6628](https://github.com/tobymao/sqlglot/issues/6628) opened by [@marktdodds](https://github.com/marktdodds)*
- [`2bf9405`](https://github.com/tobymao/sqlglot/commit/2bf9405adf9c9c72c77f7aa8ab792779a3b9c5f3) - **oracle**: USING keyword in chr *(PR [#6637](https://github.com/tobymao/sqlglot/pull/6637) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`235fc14`](https://github.com/tobymao/sqlglot/commit/235fc14f40c422e6339da0a6b17252ab9eb18ec2) - **mysql,singlestore**: support charset *(PR [#6633](https://github.com/tobymao/sqlglot/pull/6633) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`b0afbac`](https://github.com/tobymao/sqlglot/commit/b0afbaca100e73d094f247371fff13b5a4b5290a) - **exasol**: fix TO_CHAR parsing leaking canonical datetime format tokens *(PR [#6650](https://github.com/tobymao/sqlglot/pull/6650) by [@nnamdi16](https://github.com/nnamdi16))*
- [`6ecbb01`](https://github.com/tobymao/sqlglot/commit/6ecbb01a37708347cc6595ed8f17eb1a623d37eb) - **druid**: array expression should use square brackets *(PR [#6664](https://github.com/tobymao/sqlglot/pull/6664) by [@its-felix](https://github.com/its-felix))*
- [`af50c1c`](https://github.com/tobymao/sqlglot/commit/af50c1ce464d42d4df1fac8876ae25aea556912a) - **duckdb**: Fix NOT precedence for JSON extractions *(PR [#6670](https://github.com/tobymao/sqlglot/pull/6670) by [@kyle-cheung](https://github.com/kyle-cheung))*
- [`460b3a2`](https://github.com/tobymao/sqlglot/commit/460b3a2ae62d8294c57068453b5a11e4a7e12a91) - **exasol**: Allow varlen args in exp.MD5Digest *(PR [#6685](https://github.com/tobymao/sqlglot/pull/6685) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#6683](https://github.com/tobymao/sqlglot/issues/6683) opened by [@Hfuegl](https://github.com/Hfuegl)*
- [`dcdee68`](https://github.com/tobymao/sqlglot/commit/dcdee68cb1a77286232865de9df8d8a01898fcc7) - **spark**: Allow non aggregation functions in PIVOT *(PR [#6687](https://github.com/tobymao/sqlglot/pull/6687) by [@VaggelisD](https://github.com/VaggelisD))*
  - :arrow_lower_right: *fixes issue [#6684](https://github.com/tobymao/sqlglot/issues/6684) opened by [@gauravdawar-e6](https://github.com/gauravdawar-e6)*
- [`29cff1f`](https://github.com/tobymao/sqlglot/commit/29cff1fbe0011476b22e1b1442af857499f871a6) - **optimizer**: bq robust literal/non-literal binary annotation *(PR [#6688](https://github.com/tobymao/sqlglot/pull/6688) by [@geooo109](https://github.com/geooo109))*
- [`f866c83`](https://github.com/tobymao/sqlglot/commit/f866c835b84e9503b2f335a0da4b89e3426aa9e7) - **oracle**: properly parse xmlelement *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`91b3678`](https://github.com/tobymao/sqlglot/commit/91b3678c4e2380b3b344cc37643d12528c3e142b) - **resolver**: correctly resolve unnest alias shadowing for BigQuery *(PR [#6665](https://github.com/tobymao/sqlglot/pull/6665) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*
- [`2a3f1fe`](https://github.com/tobymao/sqlglot/commit/2a3f1fe854746141299303fdae36ce33688722e1) - **spark, databricks**: parse LTRIM/RTRIM *(PR [#6699](https://github.com/tobymao/sqlglot/pull/6699) by [@geooo109](https://github.com/geooo109))*
  - :arrow_lower_right: *fixes issue [#6696](https://github.com/tobymao/sqlglot/issues/6696) opened by [@avinmathew](https://github.com/avinmathew)*
- [`fb8f57a`](https://github.com/tobymao/sqlglot/commit/fb8f57ac54a4e0213941f3bd057ccb03c4e125b7) - **doris, starrocks**: Fix UPDATE statement for multi tables *(PR [#6700](https://github.com/tobymao/sqlglot/pull/6700) by [@VaggelisD](https://github.com/VaggelisD))*
- [`21f9d3c`](https://github.com/tobymao/sqlglot/commit/21f9d3ca7c6fa0d38f0f63000f904589b91c7d0a) - **optimizer**: Normalize struct field names when annotating types *(PR [#6674](https://github.com/tobymao/sqlglot/pull/6674) by [@vchan](https://github.com/vchan))*
- [`7362c23`](https://github.com/tobymao/sqlglot/commit/7362c2357eba540a12eea079e9f4212f3545c8d1) - **oracle**: interval expressions *(PR [#6648](https://github.com/tobymao/sqlglot/pull/6648) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`a84dbc2`](https://github.com/tobymao/sqlglot/commit/a84dbc2de051539ac4dfe10e0fa991c988840874) - **parser**: prevent INTERVAL from consuming GENERATED as interval unit *(PR [#6714](https://github.com/tobymao/sqlglot/pull/6714) by [@harshsinh](https://github.com/harshsinh))*
  - :arrow_lower_right: *fixes issue [#6713](https://github.com/tobymao/sqlglot/issues/6713) opened by [@harshsinh](https://github.com/harshsinh)*

### :recycle: Refactors
- [`399c80f`](https://github.com/tobymao/sqlglot/commit/399c80f2e66a1acdcd7d567e44c297db7071dc8f) - rename args for CovarSamp/Pop, stop inheriting from Binary *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`14b03ae`](https://github.com/tobymao/sqlglot/commit/14b03ae492c9cac67ae1e78b67a83427e4fe6681) - flip Getbit lsb flag to msb since more dialects match lsb *(commit by [@georgesittas](https://github.com/georgesittas))*

### :wrench: Chores
- [`e0193ea`](https://github.com/tobymao/sqlglot/commit/e0193eaeea0036bbe835c49a64a4120365525b89) - Fix make style of 6593 *(PR [#6597](https://github.com/tobymao/sqlglot/pull/6597) by [@VaggelisD](https://github.com/VaggelisD))*
- [`0534db1`](https://github.com/tobymao/sqlglot/commit/0534db11022c9cbaf91015b38039da48945aac2a) - Fix integration test failing with empty PR description *(PR [#6598](https://github.com/tobymao/sqlglot/pull/6598) by [@VaggelisD](https://github.com/VaggelisD))*
- [`c523e2a`](https://github.com/tobymao/sqlglot/commit/c523e2a4ee3b347eacf6f4f9a3629b649e902f47) - Follow up of 6551 *(PR [#6599](https://github.com/tobymao/sqlglot/pull/6599) by [@VaggelisD](https://github.com/VaggelisD))*
- [`26c3c1f`](https://github.com/tobymao/sqlglot/commit/26c3c1fcea5f105432430de3d1e2b00627191706) - Fix integration test condition for skipping fork PRs *(PR [#6606](https://github.com/tobymao/sqlglot/pull/6606) by [@VaggelisD](https://github.com/VaggelisD))*
- [`d0965ba`](https://github.com/tobymao/sqlglot/commit/d0965baa8224f72f01a04f2d9b72399f04b6103e) - Add test for PR 6616 *(commit by [@VaggelisD](https://github.com/VaggelisD))*
- [`020fb05`](https://github.com/tobymao/sqlglot/commit/020fb05971f95ba3962d051c1ef795147ee9e19d) - refactor duckdb `ZIPF` transpilation logic *(commit by [@georgesittas](https://github.com/georgesittas))*
- [`a4e1dec`](https://github.com/tobymao/sqlglot/commit/a4e1dec87fc854ee15789672189753cf2e4450e1) - Refactor PR 6617 *(PR [#6630](https://github.com/tobymao/sqlglot/pull/6630) by [@VaggelisD](https://github.com/VaggelisD))*
- [`7f2a74c`](https://github.com/tobymao/sqlglot/commit/7f2a74c1025ab2c9627889db174a7f6404c2d585) - refactor `RANDSTR` duckdb transpilation logic *(PR [#6631](https://github.com/tobymao/sqlglot/pull/6631) by [@georgesittas](https://github.com/georgesittas))*
- [`9a41cfc`](https://github.com/tobymao/sqlglot/commit/9a41cfcc1b57488373d7a63d17b6a0d91f90c9e8) - Refactor PR 6637 *(commit by [@VaggelisD](https://github.com/VaggelisD))*
- [`2f8ffcf`](https://github.com/tobymao/sqlglot/commit/2f8ffcfac03a09460a4603d66ee7b13affbc5ebf) - **snowflake**: Adding type annotation tests for Snowflake's STDDEV / STDDEV_SAMP, STDDEV_POP  *(PR [#6641](https://github.com/tobymao/sqlglot/pull/6641) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`d46969d`](https://github.com/tobymao/sqlglot/commit/d46969db50ff52b8657d5b33f0a106b69dbd1e2a) - **optimizer**: annotate snowflake ARRAY_APPEND and ARRAY_PREPEND *(PR [#6645](https://github.com/tobymao/sqlglot/pull/6645) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*
- [`4deaabc`](https://github.com/tobymao/sqlglot/commit/4deaabcedb823ff29174674fa3d398cb7966e219) - array_append/array_prepend tests clean up *(commit by [@geooo109](https://github.com/geooo109))*
- [`b3ea996`](https://github.com/tobymao/sqlglot/commit/b3ea99607b4947905bfb55994309c0ee3e646c62) - array_append/array_prepend databricks tests *(commit by [@geooo109](https://github.com/geooo109))*
- [`7f6b9f2`](https://github.com/tobymao/sqlglot/commit/7f6b9f2eda20509b442d28f7fc470e7a0aee63b7) - snowflake remove aead arg gen for DECRYPT *(commit by [@geooo109](https://github.com/geooo109))*
- [`1fdcd63`](https://github.com/tobymao/sqlglot/commit/1fdcd6389587da95dbda34227ec688181055dcf9) - Fix onboarding md paragraph *(commit by [@VaggelisD](https://github.com/VaggelisD))*
- [`5d76a45`](https://github.com/tobymao/sqlglot/commit/5d76a45ff71feb19eee4b294f2c128e30976cb5b) - Follow up 6677 *(commit by [@VaggelisD](https://github.com/VaggelisD))*
- [`8d49255`](https://github.com/tobymao/sqlglot/commit/8d492558ff18b6af53a1aaea47cc0a863fbfe482) - Remove incorrect test of PR6655 *(commit by [@VaggelisD](https://github.com/VaggelisD))*
- [`94e6fe0`](https://github.com/tobymao/sqlglot/commit/94e6fe03752500ddeace317081dc758d36c78c1f) - Follow up of 6693 *(PR [#6698](https://github.com/tobymao/sqlglot/pull/6698) by [@VaggelisD](https://github.com/VaggelisD))*
- [`dd4d55a`](https://github.com/tobymao/sqlglot/commit/dd4d55af58a7602a1924f6b1f9a93f58636f172c) - _annotate_by_args UNKNOWN *(PR [#6703](https://github.com/tobymao/sqlglot/pull/6703) by [@geooo109](https://github.com/geooo109))*
- [`2ecfc27`](https://github.com/tobymao/sqlglot/commit/2ecfc27daf3570bca442797e0b4f4dc4e5363dd6) - Follow up 6648 *(PR [#6716](https://github.com/tobymao/sqlglot/pull/6716) by [@VaggelisD](https://github.com/VaggelisD))*
- [`bbaba5f`](https://github.com/tobymao/sqlglot/commit/bbaba5fd7d0111947035534168593a5a3ee6839a) - **optimizer**: annotate type for snowflake ARRAY_CAT *(PR [#6721](https://github.com/tobymao/sqlglot/pull/6721) by [@fivetran-MichaelLee](https://github.com/fivetran-MichaelLee))*
- [`4968ccd`](https://github.com/tobymao/sqlglot/commit/4968ccd1d74d58dace3793d4dbf7a0bff22d3300) - add deployment instructions in README *(PR [#6722](https://github.com/tobymao/sqlglot/pull/6722) by [@georgesittas](https://github.com/georgesittas))*
- [`2893ac3`](https://github.com/tobymao/sqlglot/commit/2893ac399d24846482b45fd79e99b90f5ef2cca6) - **optimizer**: Remove duplicate INITCAP annotation from Snowflake *(commit by [@VaggelisD](https://github.com/VaggelisD))*
- [`33b8a5d`](https://github.com/tobymao/sqlglot/commit/33b8a5d25bf49791fb95ec20132ab6ff0bb885e0) - bump sqlglotrs to 0.11.0 *(commit by [@georgesittas](https://github.com/georgesittas))*


## [v28.5.0] - 2025-12-17
### :boom: BREAKING CHANGES
- due to [`4dfc810`](https://github.com/tobymao/sqlglot/commit/4dfc810f45d5a617ada2ba4ed57002549c8d1853) - support transpilation of BOOLNOT from snowflake to duckdb *(PR [#6577](https://github.com/tobymao/sqlglot/pull/6577) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*:

  support transpilation of BOOLNOT from snowflake to duckdb (#6577)

- due to [`b857185`](https://github.com/tobymao/sqlglot/commit/b8571850ca55802671484d118560a7b90e893c39) - remove Sysdate in favor of CurrentTimestamp with sysdate arg *(PR [#6584](https://github.com/tobymao/sqlglot/pull/6584) by [@georgesittas](https://github.com/georgesittas))*:

  remove Sysdate in favor of CurrentTimestamp with sysdate arg (#6584)

- due to [`bf217d6`](https://github.com/tobymao/sqlglot/commit/bf217d69f92efcbce5b69d637976e915ca63998d) - make `JSONArrayAgg` an `AggFunc` *(PR [#6585](https://github.com/tobymao/sqlglot/pull/6585) by [@AbhishekASLK](https://github.com/AbhishekASLK))*:

  make `JSONArrayAgg` an `AggFunc` (#6585)

- due to [`604efe5`](https://github.com/tobymao/sqlglot/commit/604efe5cf5812d0b1dd9d625ed278907d0d7fb8f) - Type annotation fixes for TO_TIMESTAMP* *(PR [#6557](https://github.com/tobymao/sqlglot/pull/6557) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*:

  Type annotation fixes for TO_TIMESTAMP* (#6557)


### :sparkles: New Features
- [`4dfc810`](https://github.com/tobymao/sqlglot/commit/4dfc810f45d5a617ada2ba4ed57002549c8d1853) - **snowflake**: support transpilation of BOOLNOT from snowflake to duckdb *(PR [#6577](https://github.com/tobymao/sqlglot/pull/6577) by [@fivetran-felixhuang](https://github.com/fivetran-felixhuang))*
- [`7077981`](https://github.com/tobymao/sqlglot/commit/707798166c1b45e633bd0e8d02d1c0146598b03a) - **snowflake**: Transpilation of Snowflake MONTHS_BETWEEN to DuckDB *(PR [#6561](https://github.com/tobymao/sqlglot/pull/6561) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`604efe5`](https://github.com/tobymao/sqlglot/commit/604efe5cf5812d0b1dd9d625ed278907d0d7fb8f) - **snowflake**: Type annotation fixes for TO_TIMESTAMP* *(PR [#6557](https://github.com/tobymao/sqlglot/pull/6557) by [@fivetran-kwoodbeck](https://github.com/fivetran-kwoodbeck))*
- [`3567880`](https://github.com/tobymao/sqlglot/commit/35678808dafb37c5d37c806682e6af9b6351bced) - add tokens to functions *(commit by [@tobymao](https://github.com/tobymao))*

### :bug: Bug Fixes
- [`b857185`](https://github.com/tobymao/sqlglot/commit/b8571850ca55802671484d118560a7b90e893c39) - **snowflake**: remove Sysdate in favor of CurrentTimestamp with sysdate arg *(PR [#6584](https://github.com/tobymao/sqlglot/pull/6584) by [@georgesittas](https://github.com/georgesittas))*
- [`bf217d6`](https://github.com/tobymao/sqlglot/commit/bf217d69f92efcbce5b69d637976e915ca63998d) - make `JSONArrayAgg` an `AggFunc` *(PR [#6585](https://github.com/tobymao/sqlglot/pull/6585) by [@AbhishekASLK](https://github.com/AbhishekASLK))*
- [`48f5e99`](https://github.com/tobymao/sqlglot/commit/48f5e999d3d3f6ad51c30e7a33a3a574d0e50d2b) - **duckdb**: preserve l/r-trim syntax *(PR [#6588](https://github.com/tobymao/sqlglot/pull/6588) by [@georgesittas](https://github.com/georgesittas))*
  - :arrow_lower_right: *fixes issue [#6587](https://github.com/tobymao/sqlglot/issues/6587) opened by [@baruchoxman](https://github.com/baruchoxman)*

### :wrench: Chores
- [`ea0263a`](https://github.com/tobymao/sqlglot/commit/ea0263aa555591b03b06a4b6dee093fe42b545f9) - Skip integration tests GA for external contributors & fix `git diff` *(PR [#6582](https://github.com/tobymao/sqlglot/pull/6582) by [@VaggelisD](https://github.com/VaggelisD))*


## [v28.4.1] - 2025-12-16
### :boom: BREAKING CHANGES
- due to [`cfc9346`](https://github.com/tobymao/sqlglot/commit/cfc9346ba0477523d3de8f923d83fd09814b22ac) - bump sqlglotrs to 0.10.0 *(commit by [@tobymao](https://github.com/tobymao))*:

  bump sqlglotrs to 0.10.0


### :wrench: Chores
- [`cfc9346`](https://github.com/tobymao/sqlglot/commit/cfc9346ba0477523d3de8f923d83fd09814b22ac) - bump sqlglotrs to 0.10.0 *(commit by [@tobymao](https://github.com/tobymao))*


## [v28.4.0] - 2025-12-16
### :boom: BREAKING CHANGES
- due to [`938f4b6`](https://github.com/tobymao/sqlglot/commit/938f4b6ebc1c0d26bd3c1400883978c79a435189) - annotate type for LAST_DAY *(PR [#5528](https://github.com/tobymao/sqlglot/pull/5528) by [@geooo109](https://github.com/geooo109))*:

  annotate type for LAST_DAY (#5528)

- due to [`7d12dac`](https://github.com/tobymao/sqlglot/commit/7d12dac613ba5119334408f2c52cb270067156d9) - annotate type for bigquery GENERATE_TIMESTAMP_ARRAY *(PR [#5529](https://github.com/tobymao/sqlglot/pull/5529) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery GENERATE_TIMESTAMP_ARRAY (#5529)

- due to [`d50ebe2`](https://github.com/tobymao/sqlglot/commit/d50ebe286dd8e2836b9eb2a3406f15976db3aa05) - annotate type for bigquery TIME_TRUNC *(PR [#5530](https://github.com/tobymao/sqlglot/pull/5530) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery TIME_TRUNC (#5530)

- due to [`29748be`](https://github.com/tobymao/sqlglot/commit/29748be7dfc10edc9f29665c98327883dd25c13d) - annotate type for bigquery TIME *(PR [#5531](https://github.com/tobymao/sqlglot/pull/5531) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery TIME (#5531)

- due to [`7003b3f`](https://github.com/tobymao/sqlglot/commit/7003b3fa39cd455e3643066364696708d1ac4f38) - parse and annotate type for bigquery DATE_FROM_UNIX_DATE *(PR [#5532](https://github.com/tobymao/sqlglot/pull/5532) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery DATE_FROM_UNIX_DATE (#5532)

- due to [`a276ca6`](https://github.com/tobymao/sqlglot/commit/a276ca6fd5f9d47fa8c90fcfa19f9864e7a28f8f) - parse and annotate type for bigquery JUSTIFY funcs *(PR [#5534](https://github.com/tobymao/sqlglot/pull/5534) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery JUSTIFY funcs (#5534)

- due to [`374178e`](https://github.com/tobymao/sqlglot/commit/374178e22fe8d2d2275b65fe08e27ef66c611220) - parse and annotate type for bigquery UNIX_MICROS and UNIX_MILLIS *(PR [#5535](https://github.com/tobymao/sqlglot/pull/5535) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery UNIX_MICROS and UNIX_MILLIS (#5535)

- due to [`1d8d1ab`](https://github.com/tobymao/sqlglot/commit/1d8d1abe459053a135a46525d0a13bb861220927) - annotate type for bigquery DATE_TRUNC *(PR [#5540](https://github.com/tobymao/sqlglot/pull/5540) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery DATE_TRUNC (#5540)

- due to [`306ba65`](https://github.com/tobymao/sqlglot/commit/306ba6531839ea2823f5165de7bde01d17560845) - annotate type for bigquery TIMESTAMP_TRUNC *(PR [#5541](https://github.com/tobymao/sqlglot/pull/5541) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery TIMESTAMP_TRUNC (#5541)

- due to [`d799c5a`](https://github.com/tobymao/sqlglot/commit/d799c5af23010a67c29edb6d45a40fb24903e1a3) - preserve projection names when merging subqueries *(commit by [@snovik75](https://github.com/snovik75))*:

  preserve projection names when merging subqueries

- due to [`8130bd4`](https://github.com/tobymao/sqlglot/commit/8130bd40815803a6781ee8f20fccd30987516192) - WEEKDAY of WEEK as VAR *(PR [#5552](https://github.com/tobymao/sqlglot/pull/5552) by [@geooo109](https://github.com/geooo109))*:

  WEEKDAY of WEEK as VAR (#5552)

- due to [`f3ffe19`](https://github.com/tobymao/sqlglot/commit/f3ffe19ec01533c5f27b9d3a7b6704b83c005118) - annotate type for bigquery format_time *(PR [#5559](https://github.com/tobymao/sqlglot/pull/5559) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery format_time (#5559)

- due to [`6872b43`](https://github.com/tobymao/sqlglot/commit/6872b43ba17a39137172fd2fa9f0d059ce595ef9) - use dialect in DataType.build fixes [#5560](https://github.com/tobymao/sqlglot/pull/5560) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  use dialect in DataType.build fixes #5560

- due to [`3ab3690`](https://github.com/tobymao/sqlglot/commit/3ab369096313b418699b7942b1c513c0c66a5331) - parse and annotate type for bigquery PARSE_DATETIME *(PR [#5558](https://github.com/tobymao/sqlglot/pull/5558) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery PARSE_DATETIME (#5558)

- due to [`e5da951`](https://github.com/tobymao/sqlglot/commit/e5da951542eb55691bc43fbbfbec4a30100de038) - parse and annotate type for bigquery PARSE_TIME *(PR [#5561](https://github.com/tobymao/sqlglot/pull/5561) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery PARSE_TIME (#5561)

- due to [`798e213`](https://github.com/tobymao/sqlglot/commit/798e213fd10c3b61afbd8cef621546de65fa6f26) - improve transpilability of ANY_VALUE closes [#5563](https://github.com/tobymao/sqlglot/pull/5563) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  improve transpilability of ANY_VALUE closes #5563

- due to [`8c0cb76`](https://github.com/tobymao/sqlglot/commit/8c0cb764fd825062fb7334032b8eeffbc39627d5) - more robust CREATE SEQUENCE *(PR [#5566](https://github.com/tobymao/sqlglot/pull/5566) by [@geooo109](https://github.com/geooo109))*:

  more robust CREATE SEQUENCE (#5566)

- due to [`c7041c7`](https://github.com/tobymao/sqlglot/commit/c7041c71250b17192c2f25fb8f33407324d332c2) - parse and annotate type for bigquery BYTE_LENGHT *(PR [#5568](https://github.com/tobymao/sqlglot/pull/5568) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery BYTE_LENGHT (#5568)

- due to [`a6c61c3`](https://github.com/tobymao/sqlglot/commit/a6c61c34f1e168c97dd5c2b8ec071372ba593992) - parse and annotate type for bigquery CODE_POINTS_TO_STRING *(PR [#5569](https://github.com/tobymao/sqlglot/pull/5569) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery CODE_POINTS_TO_STRING (#5569)

- due to [`51e0335`](https://github.com/tobymao/sqlglot/commit/51e0335377fe2bc2e2a94a623475791e9dd19fb9) - parse and annotate type for bigquery REVERSE *(PR [#5571](https://github.com/tobymao/sqlglot/pull/5571) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery REVERSE (#5571)

- due to [`2a33339`](https://github.com/tobymao/sqlglot/commit/2a333395cde71936df911488afcff92cae735e11) - annotate type for bigquery REPLACE *(PR [#5572](https://github.com/tobymao/sqlglot/pull/5572) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery REPLACE (#5572)

- due to [`1e6f813`](https://github.com/tobymao/sqlglot/commit/1e6f81343de641e588f1a05ce7dc01bed72bd849) - annotate type for bigquery REGEXP_EXTRACT_ALL *(PR [#5573](https://github.com/tobymao/sqlglot/pull/5573) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery REGEXP_EXTRACT_ALL (#5573)

- due to [`d0d62ed`](https://github.com/tobymao/sqlglot/commit/d0d62ede6320b3fd0eee04b7073f5708676dc58c) - support `TO_CHAR` with numeric inputs *(PR [#5570](https://github.com/tobymao/sqlglot/pull/5570) by [@jasonthomassql](https://github.com/jasonthomassql))*:

  support `TO_CHAR` with numeric inputs (#5570)

- due to [`7928985`](https://github.com/tobymao/sqlglot/commit/7928985a655c3d0244bc9175a37f502b19a5c5f0) - allow dashes in JSONPath keys *(PR [#5574](https://github.com/tobymao/sqlglot/pull/5574) by [@georgesittas](https://github.com/georgesittas))*:

  allow dashes in JSONPath keys (#5574)

- due to [`eb09e6e`](https://github.com/tobymao/sqlglot/commit/eb09e6e32491a05846488de7b72b1dca0e0a2669) - parse and annotate type for bigquery TRANSLATE *(PR [#5575](https://github.com/tobymao/sqlglot/pull/5575) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery TRANSLATE (#5575)

- due to [`f9a522b`](https://github.com/tobymao/sqlglot/commit/f9a522b26cd5d643b8b18fa64d70f2a3f0ff2d2c) - parse and annotate type for bigquery SOUNDEX *(PR [#5576](https://github.com/tobymao/sqlglot/pull/5576) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery SOUNDEX (#5576)

- due to [`51da41b`](https://github.com/tobymao/sqlglot/commit/51da41b90ce421b154e45add28353ac044640a1c) - annotate type for bigquery MD5 *(PR [#5577](https://github.com/tobymao/sqlglot/pull/5577) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery MD5 (#5577)

- due to [`bcf302f`](https://github.com/tobymao/sqlglot/commit/bcf302ff6ad2d0adfc29f708a8b53b5c0e547619) - annotate type for bigquery MIN/MAX BY *(PR [#5579](https://github.com/tobymao/sqlglot/pull/5579) by [@geooo109](https://github.com/geooo109))*:

  annotate type for bigquery MIN/MAX BY (#5579)

- due to [`c501d9e`](https://github.com/tobymao/sqlglot/commit/c501d9e6f58e4880e4d23f21f53f72dcb5fdaa8c) - parse and annotate type for bigquery GROUPING *(PR [#5581](https://github.com/tobymao/sqlglot/pull/5581) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery GROUPING (#5581)

- due to [`7b180bd`](https://github.com/tobymao/sqlglot/commit/7b180bdc3da9e39946c22970bd2523f7d8beaf29) - raise if query modifier is specified multiple times *(PR [#5608](https://github.com/tobymao/sqlglot/pull/5608) by [@georgesittas](https://github.com/georgesittas))*:

  raise if query modifier is specified multiple times (#5608)

- due to [`36602a2`](https://github.com/tobymao/sqlglot/commit/36602a2ecc9ffca98e89044d23e40f33c6ed71e4) - parse LIST_FILTER into ArrayFilter closes [#5633](https://github.com/tobymao/sqlglot/pull/5633) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  parse LIST_FILTER into ArrayFilter closes #5633

- due to [`0188d21`](https://github.com/tobymao/sqlglot/commit/0188d21d443c991a528eb9d220459890b7dca477) - parse LIST_TRANSFORM into Transform closes [#5634](https://github.com/tobymao/sqlglot/pull/5634) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  parse LIST_TRANSFORM into Transform closes #5634

- due to [`3ab1d44`](https://github.com/tobymao/sqlglot/commit/3ab1d4487279cab3be2d3764e51516c6db21629d) - Wrap CONCAT items with COALESCE less aggressively *(PR [#5641](https://github.com/tobymao/sqlglot/pull/5641) by [@VaggelisD](https://github.com/VaggelisD))*:

  Wrap CONCAT items with COALESCE less aggressively (#5641)

- due to [`af0b299`](https://github.com/tobymao/sqlglot/commit/af0b299561914953b30ab36004e53dcb92d39e1c) - Qualify columns generated by exp.Aliases *(PR [#5647](https://github.com/tobymao/sqlglot/pull/5647) by [@VaggelisD](https://github.com/VaggelisD))*:

  Qualify columns generated by exp.Aliases (#5647)

- due to [`53aa8fe`](https://github.com/tobymao/sqlglot/commit/53aa8fe7f188012f765066f32c4179035fff036d) - support alter table with check closes [#5649](https://github.com/tobymao/sqlglot/pull/5649) *(commit by [@georgesittas](https://github.com/georgesittas))*:

  support alter table with check closes #5649

- due to [`1a60a5a`](https://github.com/tobymao/sqlglot/commit/1a60a5a845c7431d7d3d7ccb71119699316f4b41) - Added parsing/generation of JSON_ARRAY_CONTAINS function *(PR [#5661](https://github.com/tobymao/sqlglot/pull/5661) by [@AdalbertMemSQL](https://github.com/AdalbertMemSQL))*:

  Added parsing/generation of JSON_ARRAY_CONTAINS function (#5661)

- due to [`e0db0a9`](https://github.com/tobymao/sqlglot/commit/e0db0a95d3cb7614242dbd1b439d408e7e7bd475) - add parse and annotate type for bigquery FARM_FINGERPRINT *(PR [#5667](https://github.com/tobymao/sqlglot/pull/5667) by [@geooo109](https://github.com/geooo109))*:

  add parse and annotate type for bigquery FARM_FINGERPRINT (#5667)

- due to [`56588c7`](https://github.com/tobymao/sqlglot/commit/56588c7e22b4db4f0e44696a460483ca1e549163) - Add support for vector_search function. Move predict to BigQuery dialect. *(PR [#5660](https://github.com/tobymao/sqlglot/pull/5660) by [@rloredo](https://github.com/rloredo))*:

  Add support for vector_search function. Move predict to BigQuery dialect. (#5660)

- due to [`a688a0f`](https://github.com/tobymao/sqlglot/commit/a688a0f0d70f87139e531d1419b338b695bec384) - parse and annotate type for bigquery APPROX_TOP_COUNT *(PR [#5670](https://github.com/tobymao/sqlglot/pull/5670) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery APPROX_TOP_COUNT (#5670)

- due to [`3c93fcc`](https://github.com/tobymao/sqlglot/commit/3c93fcce96ec82e78753f6c9dd5fb0e730a82058) - parse and annotate type for bigquery APPROX_TOP_SUM *(PR [#5675](https://github.com/tobymao/sqlglot/pull/5675) by [@geooo109](https://github.com/geooo109))*:

  parse and annotate type for bigquery APPROX_TOP_SUM (#5675)

- due to [`741d45a`](https://github.com/tobymao/sqlglot/commit/741d45a0ca7c1bad67da4393cd10cc9cfa49ea68) - parse and annotate type for bigquery FROM/TO_BASE32 *(PR [#5676](https://github.com/tobymao/sqlglot/pull/5676) by [@geooo109](https://github.com/geooo109))*:

- Improvement: Better nested select parsing [45603f](https://github.com/tobymao/sqlglot/commit/45603f14bf9146dc3f8b330b85a0e25b77630b9b)

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
