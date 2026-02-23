from __future__ import annotations

import typing as t

from sqlglot import exp, generator, jsonpath, parser, tokens
from sqlglot.dialects.dialect import Dialect, inline_array_sql, rename_func
from sqlglot.helper import seq_get

if t.TYPE_CHECKING:
    from sqlglot._typing import E


class Firebolt(Dialect):
    """
    Firebolt SQL dialect.

    Firebolt is a cloud data warehouse built for speed and efficiency.
    This dialect implements Firebolt SQL functions across 15 categories.

    References:
    - https://docs.firebolt.io/sql_reference/functions-reference/
    """

    # Firebolt uses Postgres-style identifier normalization (lowercase)
    INDEX_OFFSET = 1  # Firebolt arrays are 1-indexed
    CREATABLE_KIND_MAPPING = {
        **Dialect.CREATABLE_KIND_MAPPING,
        "FACT": "TABLE",
        "DIMENSION": "TABLE",
        "AGGREGATING": "INDEX",
    }

    dialect = "firebolt"

    class Tokenizer(tokens.Tokenizer):
        # Support PostgreSQL-style escaped strings: E'...'
        BYTE_STRINGS = [("e'", "'"), ("E'", "'")]

        # Support dollar-quoted strings: $$...$$, $tag$...$tag$
        HEREDOC_STRINGS = ["$"]
        HEREDOC_TAG_IS_IDENTIFIER = True
        HEREDOC_STRING_ALTERNATIVE = tokens.TokenType.PARAMETER

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            # Firebolt type aliases
            "INT4": tokens.TokenType.INT,
            "INT8": tokens.TokenType.BIGINT,
            "LONG": tokens.TokenType.BIGINT,
            "FLOAT4": tokens.TokenType.FLOAT,
            "FLOAT8": tokens.TokenType.DOUBLE,
            "BOOL": tokens.TokenType.BOOLEAN,
            "GEOGRAPHY": tokens.TokenType.GEOGRAPHY,
            "TOP": tokens.TokenType.TOP,
            "CANCEL": tokens.TokenType.COMMAND,
        }

    class JSONPathTokenizer(jsonpath.JSONPathTokenizer):
        UNESCAPED_SEQUENCES = Dialect.UNESCAPED_SEQUENCES | {
            "\x1b[0m": "",
            "\x1b[4m": "",
            "\x1b[24m": "",
        }

    class Parser(parser.Parser):
        CLONE_KEYWORDS = {"CLONE", "COPY"}

        UNESCAPED_SEQUENCES = Dialect.UNESCAPED_SEQUENCES | {
            "\x1b[0m": "",
            "\x1b[4m": "",
            "\x1b[24m": "",
        }
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            # ═══════════════════════════════════════════════════════════════
            # NUMERIC FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "CEILING": exp.Ceil.from_arg_list,
            "POWER": exp.Pow.from_arg_list,
            # LOG(x) -> base-10 log; LOG(b, x) keeps two-arg semantics
            "LOG": lambda args: (
                exp.Log(this=exp.Literal.number(10), expression=seq_get(args, 0))
                if len(args) == 1
                else exp.Log.from_arg_list(args)
            ),
            "LOG10": lambda args: exp.Log(this=exp.Literal.number(10), expression=seq_get(args, 0)),
            "IS_FINITE": lambda args: exp.Not(this=exp.IsInf(this=seq_get(args, 0))),
            "IS_INFINITE": lambda args: exp.IsInf(this=seq_get(args, 0)),
            "BIT_SHIFT_LEFT": lambda args: exp.BitwiseLeftShift(
                this=seq_get(args, 0), expression=seq_get(args, 1)
            ),
            "BIT_SHIFT_RIGHT": lambda args: exp.BitwiseRightShift(
                this=seq_get(args, 0), expression=seq_get(args, 1)
            ),
            "ARRAY_ENUMERATE": lambda args: exp.Anonymous(this="ARRAY_ENUMERATE", expressions=args),
            "HLL_COUNT_ESTIMATE": lambda args: exp.Anonymous(
                this="HLL_COUNT_ESTIMATE", expressions=args
            ),
            "PARAM": lambda args: exp.Anonymous(this="PARAM", expressions=args),
            # ═══════════════════════════════════════════════════════════════
            # STRING FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "SUBSTR": exp.Substring.from_arg_list,
            "STRPOS": exp.StrPosition.from_arg_list,
            "BTRIM": lambda args: exp.Trim(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
            ),
            "LTRIM": lambda args: exp.Trim(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                position="LEADING",
            ),
            "RTRIM": lambda args: exp.Trim(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                position="TRAILING",
            ),
            "BASE64_ENCODE": lambda args: exp.ToBase64(this=seq_get(args, 0)),
            "REGEXP_REPLACE_ALL": lambda args: exp.RegexpReplace(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                replacement=seq_get(args, 2),
            ),
            "REGEXP_EXTRACT_ALL": exp.RegexpExtractAll.from_arg_list,
            "REGEXP_LIKE": exp.RegexpLike.from_arg_list,
            "EXTRACT_ALL": exp.RegexpExtractAll.from_arg_list,
            "SPLIT_PART": lambda args: exp.RegexpSplit(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                limit=seq_get(args, 2),
            ),
            "STRING_TO_ARRAY": exp.StringToArray.from_arg_list,
            "SPLIT": exp.Split.from_arg_list,
            "OCTET_LENGTH": exp.ByteLength.from_arg_list,
            "TO_INT": lambda args: exp.Cast(
                this=seq_get(args, 0),
                to=exp.DataType.build("INT"),
            ),
            "TO_FLOAT": lambda args: exp.Cast(
                this=seq_get(args, 0),
                to=exp.DataType.build("REAL"),
            ),
            "TO_DOUBLE": exp.ToDouble.from_arg_list,
            "GEN_RANDOM_UUID_TEXT": lambda args: exp.Anonymous(
                this="GEN_RANDOM_UUID_TEXT", expressions=args
            ),
            "ICU_NORMALIZE": lambda args: exp.Anonymous(this="ICU_NORMALIZE", expressions=args),
            "MATCH_ANY": lambda args: exp.Anonymous(this="MATCH_ANY", expressions=args),
            "REGEXP_LIKE_ANY": lambda args: exp.Anonymous(this="REGEXP_LIKE_ANY", expressions=args),
            "MD5_NUMBER_LOWER64": lambda args: exp.Anonymous(
                this="MD5_NUMBER_LOWER64", expressions=args
            ),
            "MD5_NUMBER_UPPER64": lambda args: exp.Anonymous(
                this="MD5_NUMBER_UPPER64", expressions=args
            ),
            "NGRAM": lambda args: exp.Anonymous(this="NGRAM", expressions=args),
            "TO_BIN": lambda args: exp.Anonymous(this="TO_BIN", expressions=args),
            "TO_OCT": lambda args: exp.Anonymous(this="TO_OCT", expressions=args),
            "URL_ENCODE": lambda args: exp.Anonymous(this="URL_ENCODE", expressions=args),
            "URL_DECODE": lambda args: exp.Anonymous(this="URL_DECODE", expressions=args),
            "LPAD": lambda args: exp.Pad(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                fill_pattern=seq_get(args, 2),
                is_left=True,
            ),
            "RPAD": lambda args: exp.Pad(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                fill_pattern=seq_get(args, 2),
                is_left=False,
            ),
            # ═══════════════════════════════════════════════════════════════
            # AGGREGATION FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "ANY_VALUE": exp.AnyValue.from_arg_list,
            "ANY": exp.AnyValue.from_arg_list,
            "APPROX_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "APPROX_PERCENTILE": exp.ApproxQuantile.from_arg_list,
            "STDDEV_POP": exp.StddevPop.from_arg_list,
            "STDDEV_SAMP": exp.StddevSamp.from_arg_list,
            "VAR_POP": exp.VariancePop.from_arg_list,
            "VAR_SAMP": exp.Variance.from_arg_list,
            "VARIANCE_POP": exp.VariancePop.from_arg_list,
            "VARIANCE_SAMP": exp.Variance.from_arg_list,
            "MEDIAN": exp.Median.from_arg_list,
            "PERCENTILE_CONT": exp.PercentileCont.from_arg_list,
            "PERCENTILE_DISC": exp.PercentileDisc.from_arg_list,
            "MAX_BY": exp.ArgMax.from_arg_list,
            "MIN_BY": exp.ArgMin.from_arg_list,
            "BIT_AND": exp.BitwiseAndAgg.from_arg_list,
            "BIT_OR": exp.BitwiseOrAgg.from_arg_list,
            "BIT_XOR": exp.BitwiseXorAgg.from_arg_list,
            "BOOL_AND": lambda args: exp.LogicalAnd(
                this=seq_get(args, 0),
                expression=seq_get(args, 1) if len(args) > 1 else None,
            ),
            "BOOL_OR": lambda args: exp.LogicalOr(
                this=seq_get(args, 0),
                expression=seq_get(args, 1) if len(args) > 1 else None,
            ),
            "HASH_AGG": lambda args: exp.Anonymous(this="HASH_AGG", expressions=args),
            "HLL_COUNT_BUILD": lambda args: exp.Anonymous(this="HLL_COUNT_BUILD", expressions=args),
            "HLL_COUNT_DISTINCT": lambda args: exp.Anonymous(
                this="HLL_COUNT_DISTINCT", expressions=args
            ),
            "HLL_COUNT_MERGE": lambda args: exp.Anonymous(this="HLL_COUNT_MERGE", expressions=args),
            "ARRAY_COUNT_GLOBAL": lambda args: exp.Anonymous(
                this="ARRAY_COUNT_GLOBAL", expressions=args
            ),
            "ARRAY_MAX_GLOBAL": lambda args: exp.Anonymous(
                this="ARRAY_MAX_GLOBAL", expressions=args
            ),
            "ARRAY_MIN_GLOBAL": lambda args: exp.Anonymous(
                this="ARRAY_MIN_GLOBAL", expressions=args
            ),
            "ARRAY_SUM_GLOBAL": lambda args: exp.Anonymous(
                this="ARRAY_SUM_GLOBAL", expressions=args
            ),
            # ═══════════════════════════════════════════════════════════════
            # ARRAY FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "ARRAY_CONCAT": exp.ArrayConcat.from_arg_list,
            "ARRAY_CONTAINS": exp.ArrayContains.from_arg_list,
            "ARRAY_LENGTH": exp.ArraySize.from_arg_list,
            "FLATTEN": exp.Explode.from_arg_list,
            "ARRAY_DISTINCT": lambda args: exp.ArrayUniqueAgg(this=seq_get(args, 0)),
            "ARRAY_REVERSE": exp.ArrayReverse.from_arg_list,
            "ARRAY_SORT": lambda args: (
                exp.ArraySort(this=seq_get(args, 0), expression=seq_get(args, 1))
                if len(args) == 2 and isinstance(seq_get(args, 1), exp.Lambda)
                else exp.Anonymous(this="ARRAY_SORT", expressions=args)
            ),
            "ARRAY_MAX": lambda args: exp.Anonymous(this="ARRAY_MAX", expressions=args),
            "ARRAY_MIN": lambda args: exp.Anonymous(this="ARRAY_MIN", expressions=args),
            "ARRAY_SUM": exp.ArraySum.from_arg_list,
            "ARRAY_TO_STRING": exp.ArrayToString.from_arg_list,
            "ARRAYS_OVERLAP": exp.ArrayOverlaps.from_arg_list,
            "ARRAY_INTERSECT": exp.ArrayIntersect.from_arg_list,
            "ARRAY_SLICE": exp.ArraySlice.from_arg_list,
            "ARRAY_COUNT": lambda args: exp.Anonymous(this="ARRAY_COUNT", expressions=args),
            "ARRAY_COUNT_DISTINCT": lambda args: exp.Anonymous(
                this="ARRAY_COUNT_DISTINCT", expressions=args
            ),
            "ARRAY_CUMULATIVE_SUM": lambda args: exp.Anonymous(
                this="ARRAY_CUMULATIVE_SUM", expressions=args
            ),
            "ARRAY_FILL": lambda args: exp.Anonymous(this="ARRAY_FILL", expressions=args),
            "ARRAY_FIRST_INDEX": lambda args: exp.Anonymous(
                this="ARRAY_FIRST_INDEX", expressions=args
            ),
            "ARRAY_REPLACE_BACKWARDS": lambda args: exp.Anonymous(
                this="ARRAY_REPLACE_BACKWARDS", expressions=args
            ),
            "ARRAY_REVERSE_SORT": lambda args: exp.Anonymous(
                this="ARRAY_REVERSE_SORT", expressions=args
            ),
            "INDEX_OF": lambda args: exp.Anonymous(this="INDEX_OF", expressions=args),
            # ═══════════════════════════════════════════════════════════════
            # DATE/TIME FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "LOCALTIMESTAMP": exp.CurrentTimestamp.from_arg_list,
            "CURRENT_TIMESTAMPTZ": exp.CurrentTimestamp.from_arg_list,
            "TO_YYYYMM": lambda args: exp.TimeToStr(
                this=seq_get(args, 0),
                format=exp.Literal.string("%Y%m"),
            ),
            "TO_YYYYMMDD": lambda args: exp.TimeToStr(
                this=seq_get(args, 0),
                format=exp.Literal.string("%Y%m%d"),
            ),
            "TO_CHAR": lambda args: exp.TimeToStr(
                this=seq_get(args, 0),
                format=seq_get(args, 1),
            ),
            "AGO": lambda args: exp.Anonymous(this="AGO", expressions=args),
            # ═══════════════════════════════════════════════════════════════
            # JSON FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "JSON_EXTRACT_ARRAY": lambda args: exp.Anonymous(
                this="JSON_EXTRACT_ARRAY", expressions=args
            )
            if len(args) > 2
            else exp.JSONExtractArray(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
            ),
            "JSON_VALUE": lambda args: exp.JSONExtractScalar(
                this=seq_get(args, 0),
                expression=seq_get(args, 1) if len(args) > 1 else exp.Literal.string("$"),
                expressions=[seq_get(args, 2)] if len(args) > 2 else None,
            ),
            "JSON_VALUE_ARRAY": lambda args: exp.JSONValueArray(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
            ),
            "JSON_FORMAT": lambda args: exp.Anonymous(this="JSON_FORMAT", expressions=args),
            "JSON_POINTER_EXTRACT_TEXT": lambda args: exp.Anonymous(
                this="JSON_POINTER_EXTRACT_TEXT", expressions=args
            ),
            "JSON_POINTER_EXTRACT_VALUES": lambda args: exp.Anonymous(
                this="JSON_POINTER_EXTRACT_VALUES", expressions=args
            ),
            "JSON_POINTER_EXTRACT_KEYS": lambda args: exp.Anonymous(
                this="JSON_POINTER_EXTRACT_KEYS", expressions=args
            ),
            # ═══════════════════════════════════════════════════════════════
            # CONDITIONAL FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "IFNULL": lambda args: exp.Coalesce(
                this=seq_get(args, 0),
                expressions=[seq_get(args, 1)],
            ),
            "CITY_HASH": lambda args: exp.Anonymous(this="CITY_HASH", expressions=args),
            "TYPEOF": lambda args: exp.Anonymous(this="TYPEOF", expressions=args),
            "VERSION": lambda args: exp.Anonymous(this="VERSION", expressions=args),
            # ═══════════════════════════════════════════════════════════════
            # LAMBDA FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "FILTER": lambda args: (
                exp.ArrayFilter.from_arg_list(args)
                if len(args) == 2
                else exp.Anonymous(this="FILTER", expressions=args)
            ),
            "ARRAY_FILTER": lambda args: (
                exp.ArrayFilter.from_arg_list(args)
                if len(args) == 2
                else exp.Anonymous(this="ARRAY_FILTER", expressions=args)
            ),
            "TRANSFORM": lambda args: exp.Anonymous(this="TRANSFORM", expressions=args),
            "ARRAY_ALL_MATCH": lambda args: exp.ArrayAll(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
            ),
            "ARRAY_ANY_MATCH": lambda args: exp.ArrayAny(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
            ),
            "ARRAY_FIRST": lambda args: (
                exp.Bracket(this=seq_get(args, 0), expressions=[exp.Literal.number(1)])
                if len(args) == 1
                else exp.Anonymous(this="ARRAY_FIRST", expressions=args)
            ),
            # ═══════════════════════════════════════════════════════════════
            # GEOSPATIAL FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "ST_GEOGPOINT": lambda args: exp.Anonymous(this="ST_GEOGPOINT", expressions=args),
            "ST_GEOGFROMTEXT": lambda args: exp.Anonymous(this="ST_GEOGFROMTEXT", expressions=args),
            "ST_GEOGFROMWKB": lambda args: exp.Anonymous(this="ST_GEOGFROMWKB", expressions=args),
            "ST_GEOGFROMGEOJSON": lambda args: exp.Anonymous(
                this="ST_GEOGFROMGEOJSON", expressions=args
            ),
            "ST_ASTEXT": lambda args: exp.Anonymous(this="ST_ASTEXT", expressions=args),
            "ST_ASBINARY": lambda args: exp.Anonymous(this="ST_ASBINARY", expressions=args),
            "ST_ASEWKB": lambda args: exp.Anonymous(this="ST_ASEWKB", expressions=args),
            "ST_ASGEOJSON": lambda args: exp.Anonymous(this="ST_ASGEOJSON", expressions=args),
            "ST_CONTAINS": lambda args: exp.Anonymous(this="ST_CONTAINS", expressions=args),
            "ST_COVERS": lambda args: exp.Anonymous(this="ST_COVERS", expressions=args),
            "ST_INTERSECTS": lambda args: exp.Anonymous(this="ST_INTERSECTS", expressions=args),
            "ST_DISTANCE": lambda args: exp.Anonymous(this="ST_DISTANCE", expressions=args),
            "ST_X": lambda args: exp.Anonymous(this="ST_X", expressions=args),
            "ST_Y": lambda args: exp.Anonymous(this="ST_Y", expressions=args),
            "ST_S2CELLIDFROMPOINT": lambda args: exp.Anonymous(
                this="ST_S2CELLIDFROMPOINT", expressions=args
            ),
            # ═══════════════════════════════════════════════════════════════
            # BYTEA FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "ENCODE": lambda args: exp.Encode(
                this=seq_get(args, 0),
                charset=seq_get(args, 1),
            ),
            "DECODE": lambda args: exp.Decode(
                this=seq_get(args, 0),
                charset=seq_get(args, 1),
            ),
            "CONVERT_FROM": lambda args: exp.Anonymous(this="CONVERT_FROM", expressions=args),
            # ═══════════════════════════════════════════════════════════════
            # DATASKETCHES FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "APACHE_DATASKETCHES_HLL_BUILD": lambda args: exp.Anonymous(
                this="APACHE_DATASKETCHES_HLL_BUILD", expressions=args
            ),
            "APACHE_DATASKETCHES_HLL_ESTIMATE": lambda args: exp.Anonymous(
                this="APACHE_DATASKETCHES_HLL_ESTIMATE", expressions=args
            ),
            "APACHE_DATASKETCHES_HLL_MERGE": lambda args: exp.Anonymous(
                this="APACHE_DATASKETCHES_HLL_MERGE", expressions=args
            ),
            # ═══════════════════════════════════════════════════════════════
            # TABLE-VALUED FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "GENERATE_SERIES": exp.GenerateSeries.from_arg_list,
            "READ_CSV": lambda args: exp.Anonymous(this="READ_CSV", expressions=args),
            "READ_PARQUET": lambda args: exp.Anonymous(this="READ_PARQUET", expressions=args),
            "READ_AVRO": lambda args: exp.Anonymous(this="READ_AVRO", expressions=args),
            "READ_ICEBERG": lambda args: exp.Anonymous(this="READ_ICEBERG", expressions=args),
            "LIST_OBJECTS": lambda args: exp.Anonymous(this="LIST_OBJECTS", expressions=args),
            "VECTOR_SEARCH": lambda args: exp.Anonymous(this="VECTOR_SEARCH", expressions=args),
            # ═══════════════════════════════════════════════════════════════
            # VECTOR FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "VECTOR_ADD": lambda args: exp.Anonymous(this="VECTOR_ADD", expressions=args),
            "VECTOR_SUBTRACT": lambda args: exp.Anonymous(this="VECTOR_SUBTRACT", expressions=args),
            "VECTOR_COSINE_DISTANCE": lambda args: exp.Anonymous(
                this="VECTOR_COSINE_DISTANCE", expressions=args
            ),
            "VECTOR_COSINE_SIMILARITY": lambda args: exp.Anonymous(
                this="VECTOR_COSINE_SIMILARITY", expressions=args
            ),
            "VECTOR_EUCLIDEAN_DISTANCE": lambda args: exp.Anonymous(
                this="VECTOR_EUCLIDEAN_DISTANCE", expressions=args
            ),
            "VECTOR_MANHATTAN_DISTANCE": lambda args: exp.Anonymous(
                this="VECTOR_MANHATTAN_DISTANCE", expressions=args
            ),
            "VECTOR_SQUARED_EUCLIDEAN_DISTANCE": lambda args: exp.Anonymous(
                this="VECTOR_SQUARED_EUCLIDEAN_DISTANCE", expressions=args
            ),
            "VECTOR_INNER_PRODUCT": lambda args: exp.Anonymous(
                this="VECTOR_INNER_PRODUCT", expressions=args
            ),
            # ═══════════════════════════════════════════════════════════════
            # SESSION FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "CURRENT_ACCOUNT": lambda args: exp.Anonymous(this="CURRENT_ACCOUNT", expressions=args),
            "CURRENT_DATABASE": exp.CurrentSchema.from_arg_list,
            "CURRENT_ENGINE": lambda args: exp.Anonymous(this="CURRENT_ENGINE", expressions=args),
            "SESSION_USER": exp.CurrentUser.from_arg_list,
            # ═══════════════════════════════════════════════════════════════
            # AI FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            "AI_QUERY": lambda args: exp.Anonymous(this="AI_QUERY", expressions=args),
            "AWS_BEDROCK_AI_QUERY": lambda args: exp.Anonymous(
                this="AWS_BEDROCK_AI_QUERY", expressions=args
            ),
        }

        # Support Firebolt-specific ARRAY element nullability and postfix TYPE ARRAY
        def _parse_types(
            self,
            check_func: bool = False,
            schema: bool = False,
            allow_identifiers: bool = True,
        ) -> t.Optional[exp.Expression]:
            index0 = self._index

            # Special-case: ARRAY(<type> [NULL|NOT NULL])
            if self._match(tokens.TokenType.ARRAY) and self._match(tokens.TokenType.L_PAREN):
                elem_type = super()._parse_types(
                    check_func=check_func,
                    schema=schema,
                    allow_identifiers=allow_identifiers,
                )

                # Optional element nullability
                nullable = None
                if self._match_pair(tokens.TokenType.NOT, tokens.TokenType.NULL):
                    nullable = False
                elif self._match(tokens.TokenType.NULL):
                    nullable = True

                if not self._match(tokens.TokenType.R_PAREN):
                    # Fallback to base behavior if this wasn't our grammar
                    self._retreat(index0)
                else:
                    if isinstance(elem_type, exp.DataType) and nullable is not None:
                        elem_type.set("nullable", nullable)
                    return exp.DataType(
                        this=exp.DataType.Type.ARRAY,
                        expressions=[elem_type],
                        nested=True,
                    )

            # Fallback to the base implementation, but with a tweak to handle postfix TYPE ARRAY
            result = super()._parse_types(
                check_func=check_func,
                schema=schema,
                allow_identifiers=allow_identifiers,
            )

            # If we're in a schema (CREATE TABLE) and encounter postfix `TYPE ARRAY`,
            # wrap the type in an ARRAY dimension.
            if result is not None and schema and self._match(tokens.TokenType.ARRAY):
                if not self._match(tokens.TokenType.L_BRACKET, advance=False):
                    return exp.DataType(
                        this=exp.DataType.Type.ARRAY,
                        expressions=[result],
                        nested=True,
                    )

                # If there is a '[', retreat one step so base suffix logic can continue
                self._retreat(self._index - 1)

            return result

        def _parse_unnest(self, with_alias: bool = True) -> t.Optional[exp.Unnest]:
            # Accept UNNEST(<expr> [AS <id>] [, <expr> [AS <id>]] ...)
            if not self._match_pair(
                tokens.TokenType.UNNEST, tokens.TokenType.L_PAREN, advance=False
            ):
                return None

            self._advance()

            # Consume opening parenthesis
            self._match(tokens.TokenType.L_PAREN)

            expressions: t.List[exp.Expression] = []

            while True:
                expr = self._parse_bitwise()
                if not expr:
                    break

                # Optional inner alias after each expression
                if self._match(tokens.TokenType.ALIAS):
                    self._parse_id_var(
                        any_token=False,
                        tokens=(tokens.TokenType.VAR, tokens.TokenType.ANY),
                    )

                expressions.append(expr)

                if not self._match(tokens.TokenType.COMMA):
                    break

            self._match_r_paren()

            offset = self._match_pair(tokens.TokenType.WITH, tokens.TokenType.ORDINALITY)

            alias = self._parse_table_alias() if with_alias else None

            if alias:
                if self.dialect.UNNEST_COLUMN_ONLY:
                    if alias.args.get("columns"):
                        self.raise_error("Unexpected extra column alias in unnest.")

                    alias.set("columns", [alias.this])
                    alias.set("this", None)

                columns = alias.args.get("columns") or []
                if offset and len(expressions) < len(columns):
                    offset = columns.pop()

            if not offset and self._match_pair(tokens.TokenType.WITH, tokens.TokenType.OFFSET):
                self._match(tokens.TokenType.ALIAS)
                offset = self._parse_id_var(
                    any_token=False, tokens=self.UNNEST_OFFSET_ALIAS_TOKENS
                ) or exp.to_identifier("offset")

            return self.expression(exp.Unnest, expressions=expressions, alias=alias, offset=offset)

        def _parse_function(
            self,
            functions: t.Optional[t.Dict[str, t.Callable]] = None,
            anonymous: bool = False,
            optional_parens: bool = True,
            any_token: bool = False,
        ) -> t.Optional[exp.Expression]:
            # Handle vector_search with INDEX keyword specially
            if self._match_text_seq("VECTOR_SEARCH"):
                return self._parse_vector_search()

            return super()._parse_function(functions, anonymous, optional_parens, any_token)

        def _parse_vector_search(self) -> exp.Anonymous:
            # Parse: VECTOR_SEARCH(INDEX <name>, <vector>, <top_k>, <ef_search>)
            self._match(tokens.TokenType.L_PAREN)

            args: t.List[exp.Expression] = []

            # Parse INDEX <identifier>
            if self._match(tokens.TokenType.INDEX):
                index_name = self._parse_id_var()
                args.append(exp.Kwarg(this=exp.var("INDEX"), expression=index_name))
                self._match(tokens.TokenType.COMMA)

            # Parse remaining arguments
            while self._curr and not self._match(tokens.TokenType.R_PAREN):
                arg = self._parse_assignment()
                if arg:
                    args.append(arg)
                if not self._match(tokens.TokenType.COMMA):
                    self._match(tokens.TokenType.R_PAREN)
                    break

            return exp.Anonymous(this="VECTOR_SEARCH", expressions=args)

        def _parse_cte(self) -> t.Optional[exp.CTE]:
            # Allow trailing comma after the last CTE in WITH
            index = self._index

            alias = self._parse_table_alias(self.ID_VAR_TOKENS)
            if not alias or not alias.this:
                if self._prev and self._prev.token_type == tokens.TokenType.COMMA:
                    self._retreat(index)
                    return None
                self.raise_error("Expected CTE to have alias")

            if not self._match(tokens.TokenType.ALIAS) and not self.OPTIONAL_ALIAS_TOKEN_CTE:
                self._retreat(index)
                return None

            comments = self._prev_comments

            if self._match_text_seq("NOT", "MATERIALIZED"):
                materialized = False
            elif self._match_text_seq("MATERIALIZED"):
                materialized = True
            else:
                materialized = None

            cte = self.expression(
                exp.CTE,
                this=self._parse_wrapped(self._parse_statement),
                alias=alias,
                materialized=materialized,
                comments=comments,
            )

            values = cte.this
            if isinstance(values, exp.Values):
                if values.alias:
                    cte.set("this", exp.select("*").from_(values))
                else:
                    cte.set(
                        "this",
                        exp.select("*").from_(exp.alias_(values, "_values", table=True)),
                    )

            return cte

        def _parse_create(self) -> exp.Create | exp.Command:
            # Handle CREATE EXTERNAL TABLE - treat as Command
            index = self._index
            if self._match_text_seq("EXTERNAL", "TABLE"):
                tokens_text = ["CREATE", "EXTERNAL", "TABLE"]

                while self._curr and not self._match(tokens.TokenType.SEMICOLON):
                    tokens_text.append(self._curr.text)
                    self._advance()

                sql_text = " ".join(tokens_text)
                return exp.Command(this="CREATE", expression=sql_text)

            # Handle CREATE [FACT|DIMENSION] TABLE
            if self._match_text_seq("FACT", "TABLE") or self._match_text_seq("DIMENSION", "TABLE"):
                self._retreat(index)
                self._advance()  # Skip FACT or DIMENSION

            return super()._parse_create()

        def _parse_interval(
            self, match_interval: bool = True
        ) -> t.Optional[exp.Add | exp.Interval]:
            # Override to handle interval '4' without explicit unit
            index = self._index

            if not self._match(tokens.TokenType.INTERVAL) and match_interval:
                return None

            if self._match(tokens.TokenType.STRING, advance=False):
                this = self._parse_primary()
            else:
                this = self._parse_term()

            if not this:
                self._retreat(index)
                return None

            # Try to parse unit, but make it optional
            unit = None
            if self._curr and self._curr.text.upper() in self.dialect.VALID_INTERVAL_UNITS:
                unit = self._parse_var(any_token=True, upper=True)
            elif self._curr and self._curr.text.upper() not in (
                "ORDER",
                "GROUP",
                "LIMIT",
                "OFFSET",
                "HAVING",
                "WHERE",
                "UNION",
                "EXCEPT",
                "INTERSECT",
                ")",
                ";",
                ",",
            ):
                saved_index = self._index
                potential_unit = self._parse_var(any_token=True, upper=True)
                if (
                    potential_unit
                    and potential_unit.name.upper() in self.dialect.VALID_INTERVAL_UNITS
                ):
                    unit = potential_unit
                else:
                    self._retreat(saved_index)

            return exp.Interval(this=this, unit=unit)

        @t.overload
        def _parse_query_modifiers(self, this: E) -> E: ...

        @t.overload
        def _parse_query_modifiers(self, this: None) -> None: ...

        def _parse_query_modifiers(self, this):
            # Override to handle redundant TOP + LIMIT syntax
            # In Firebolt, queries can have both TOP and LIMIT (though redundant)
            if isinstance(this, self.MODIFIABLES):
                for join in self._parse_joins():
                    this.append("joins", join)
                for lateral in iter(self._parse_lateral, None):
                    this.append("laterals", lateral)

                while True:
                    if self._match_set(self.QUERY_MODIFIER_PARSERS, advance=False):
                        modifier_token = self._curr
                        parser = self.QUERY_MODIFIER_PARSERS[modifier_token.token_type]
                        key, expression = parser(self)

                        if expression:
                            existing = this.args.get(key)
                            if existing:
                                if key == "limit":
                                    # Allow LIMIT to override TOP
                                    this.set(key, expression)
                                else:
                                    self.raise_error(
                                        f"Found multiple '{modifier_token.text.upper()}' clauses",
                                        token=modifier_token,
                                    )
                            else:
                                this.set(key, expression)

                            if key == "limit":
                                offset = expression.args.get("offset")
                                expression.set("offset", None)

                                if offset:
                                    offset = exp.Offset(expression=offset)
                                    this.set("offset", offset)

                                    limit_by_expressions = expression.expressions
                                    expression.set("expressions", None)
                                    offset.set("expressions", limit_by_expressions)
                            continue
                    break

            if self.SUPPORTS_IMPLICIT_UNNEST and this and this.args.get("from_"):
                this = self._implicit_unnests_to_explicit(this)

            return this

    class Generator(generator.Generator):
        # Firebolt supports TRY_CAST semantics
        TRY_SUPPORTED = True

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            # Array literals use bracket syntax: ['a', 'b']
            exp.Array: inline_array_sql,
            # ═══════════════════════════════════════════════════════════════
            # NUMERIC FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            exp.IsInf: rename_func("IS_INFINITE"),
            exp.BitwiseLeftShift: rename_func("BIT_SHIFT_LEFT"),
            exp.BitwiseRightShift: rename_func("BIT_SHIFT_RIGHT"),
            exp.Pow: rename_func("POW"),
            exp.Rand: rename_func("RANDOM"),
            exp.Mod: rename_func("MOD"),
            # ═══════════════════════════════════════════════════════════════
            # STRING FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            exp.StrPosition: rename_func("STRPOS"),
            exp.ToBase64: rename_func("BASE64_ENCODE"),
            exp.ByteLength: rename_func("OCTET_LENGTH"),
            # ═══════════════════════════════════════════════════════════════
            # AGGREGATION FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ApproxQuantile: rename_func("APPROX_PERCENTILE"),
            exp.StddevPop: rename_func("STDDEV_POP"),
            exp.StddevSamp: rename_func("STDDEV_SAMP"),
            exp.VariancePop: rename_func("VAR_POP"),
            exp.Variance: rename_func("VAR_SAMP"),
            exp.ArgMax: rename_func("MAX_BY"),
            exp.ArgMin: rename_func("MIN_BY"),
            exp.BitwiseAndAgg: rename_func("BIT_AND"),
            exp.BitwiseOrAgg: rename_func("BIT_OR"),
            exp.BitwiseXorAgg: rename_func("BIT_XOR"),
            # ═══════════════════════════════════════════════════════════════
            # ARRAY FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.ArrayUniqueAgg: rename_func("ARRAY_DISTINCT"),
            # ═══════════════════════════════════════════════════════════════
            # SESSION FUNCTIONS
            # ═══════════════════════════════════════════════════════════════
            exp.CurrentSchema: rename_func("CURRENT_DATABASE"),
            # ═══════════════════════════════════════════════════════════════
            # DATE/TIME TRANSFORMATIONS
            # ═══════════════════════════════════════════════════════════════
            exp.DatetimeTrunc: lambda self, e: self._datetime_trunc_sql(e),
            exp.TimeToUnix: lambda self, e: self._time_to_unix_sql(e),
            exp.Timestamp: lambda self, e: self._timestamp_sql(e),
            exp.TimestampFromParts: lambda self, e: self._timestamp_from_parts_sql(e),
            # JSON PARSING
            # Firebolt doesn't have a JSON type - JSON is stored as TEXT
            exp.ParseJSON: lambda self, e: self.sql(e, "this"),
        }

        def _timestamp_from_parts_sql(self, expression: exp.TimestampFromParts) -> str:
            """
            Convert TimestampFromParts to Firebolt CAST.

            BigQuery: DATETIME(2025, 10, 27, 0, 0, 0)
            Firebolt: CAST('2025-10-27 00:00:00' AS TIMESTAMP)
            """
            year = expression.args.get("year")
            month = expression.args.get("month")
            day = expression.args.get("day")
            hour = expression.args.get("hour") or exp.Literal.number(0)
            min_val = expression.args.get("min") or exp.Literal.number(0)
            sec = expression.args.get("sec") or exp.Literal.number(0)

            def get_value(e: t.Optional[exp.Expression]) -> t.Optional[int]:
                if isinstance(e, exp.Literal) and e.is_int:
                    return int(e.this)
                return None

            y = get_value(year)
            m = get_value(month)
            d = get_value(day)
            h = get_value(hour)
            mi = get_value(min_val)
            s = get_value(sec)

            if all(v is not None for v in [y, m, d, h, mi, s]):
                timestamp_str = f"{y:04d}-{m:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}"
                return f"CAST('{timestamp_str}' AS TIMESTAMP)"

            # Fallback for dynamic values
            return self.func(
                "CAST",
                self.func(
                    "CONCAT",
                    year,
                    exp.Literal.string("-"),
                    month,
                    exp.Literal.string("-"),
                    day,
                    exp.Literal.string(" "),
                    hour,
                    exp.Literal.string(":"),
                    min_val,
                    exp.Literal.string(":"),
                    sec,
                ),
                exp.DataType.build("TIMESTAMP"),
            )

        def _timestamp_sql(self, expression: exp.Timestamp) -> str:
            """
            Convert Timestamp to Firebolt's CAST or AT TIME ZONE.

            TIMESTAMP(expr) -> CAST(expr AS TIMESTAMP)
            TIMESTAMP(expr, zone) -> CAST(expr AS TIMESTAMP) AT TIME ZONE zone
            """
            zone = expression.args.get("zone")
            this = expression.this

            if not zone:
                from sqlglot.optimizer.annotate_types import annotate_types

                target_type = (
                    annotate_types(expression, dialect=self.dialect).type
                    or exp.DataType.Type.TIMESTAMP
                )
                return self.sql(exp.cast(this, target_type))

            at_tz = exp.AtTimeZone(
                this=exp.cast(this, exp.DataType.Type.TIMESTAMP),
                zone=zone,
            )
            return self.sql(at_tz)

        def _datetime_trunc_sql(self, expression: exp.DatetimeTrunc) -> str:
            """
            Convert DatetimeTrunc to Firebolt's DATE_TRUNC.

            DATETIME_TRUNC(datetime, DAY) -> DATE_TRUNC('DAY', datetime)
            """
            unit_expr = expression.args.get("unit")
            timestamp = expression.this

            if isinstance(timestamp, exp.Date) and timestamp.this:
                timestamp = timestamp.this

            if isinstance(unit_expr, exp.Interval):
                unit = unit_expr.args.get("unit")
                if unit:
                    unit_str = unit.name if isinstance(unit, exp.Var) else str(unit)
                    return self.func("DATE_TRUNC", exp.Literal.string(unit_str), timestamp)

            return self.func("DATE_TRUNC", unit_expr, timestamp)

        def _time_to_unix_sql(self, expression: exp.TimeToUnix) -> str:
            """
            Convert TimeToUnix to Firebolt's EXTRACT(EPOCH FROM ...).

            UNIX_MILLIS(timestamp) -> EXTRACT(EPOCH FROM timestamp) * 1000
            """
            scale = expression.args.get("scale")
            timestamp_sql = self.sql(expression, "this")

            epoch_expr = f"EXTRACT(EPOCH FROM {timestamp_sql})"

            if scale == exp.UnixToTime.MILLIS:
                return f"({epoch_expr} * 1000)"
            elif scale == exp.UnixToTime.MICROS:
                return f"({epoch_expr} * 1000000)"
            else:
                return epoch_expr

        def lambda_sql(
            self, expression: exp.Lambda, arrow_sep: str = "->", wrap: bool = True
        ) -> str:
            # Firebolt lambda syntax: x -> expr (no parentheses around params)
            return super().lambda_sql(expression, arrow_sep=arrow_sep, wrap=False)

        def filter_sql(self, expression: exp.Filter) -> str:
            if isinstance(expression.this, exp.GroupConcat):
                gc = expression.this
                agg_expr = gc.this.this if isinstance(gc.this, exp.Order) else gc.this
                separator = gc.args.get("separator") or exp.Literal.string(",")

                array_agg = exp.ArrayAgg(this=agg_expr)
                filtered_agg = exp.Filter(this=array_agg, expression=expression.expression)

                return self.func("ARRAY_TO_STRING", filtered_agg, separator)

            return super().filter_sql(expression)

        def groupconcat_sql(self, expression: exp.GroupConcat) -> str:
            # Convert GROUP_CONCAT/LISTAGG to ARRAY_TO_STRING(ARRAY_AGG(...), separator)
            agg_expr = (
                expression.this.this if isinstance(expression.this, exp.Order) else expression.this
            )
            separator = expression.args.get("separator") or exp.Literal.string(",")
            array_agg = exp.ArrayAgg(this=agg_expr)
            return self.func("ARRAY_TO_STRING", array_agg, separator)

        def sub_sql(self, expression: exp.Sub) -> str:
            left = expression.this
            right = expression.expression

            left_is_timestamp = (
                isinstance(left, exp.Cast)
                and left.to
                and left.to.this in (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ)
            )
            right_is_timestamp = (
                isinstance(right, exp.Cast)
                and right.to
                and right.to.this in (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ)
            )

            if left_is_timestamp and right_is_timestamp:
                return self.func(
                    "DATE_DIFF",
                    exp.Literal.string("second"),
                    right,
                    left,
                )

            return super().sub_sql(expression)

        def arraytostring_sql(self, expression: exp.ArrayToString) -> str:
            array_expr = expression.this
            delimiter = expression.expression
            null_replacement = expression.args.get("null")

            if null_replacement:
                lambda_body = exp.Coalesce(
                    this=exp.Cast(
                        this=exp.Identifier(this="x"),
                        to=exp.DataType.build("TEXT"),
                    ),
                    expressions=[null_replacement],
                )
                lambda_expr = exp.Lambda(
                    this=lambda_body,
                    expressions=[exp.Identifier(this="x")],
                )
                transform_expr = exp.Anonymous(
                    this="TRANSFORM",
                    expressions=[lambda_expr, array_expr],
                )
                return self.func("ARRAY_TO_STRING", transform_expr, delimiter)

            return self.func("ARRAY_TO_STRING", array_expr, delimiter)

        def arraysort_sql(self, expression: exp.ArraySort) -> str:
            if isinstance(expression.expression, exp.Lambda):
                return self.func("ARRAY_SORT", expression.expression, expression.this)

            if isinstance(expression.expression, (exp.Boolean, exp.Null, type(None))):
                if isinstance(expression.expression, exp.Boolean):
                    is_ascending = expression.expression.this
                    if not is_ascending:
                        return self.func("ARRAY_REVERSE_SORT", expression.this)

                return self.func("ARRAY_SORT", expression.this)

            return self.func("ARRAY_SORT", expression.expression, expression.this)

        def in_sql(self, expression: exp.In) -> str:
            not_ = expression.args.get("not")
            base = super().in_sql(expression)
            return base.replace(" IN ", " NOT IN ", 1) if not_ else base

        def is_sql(self, expression: exp.Is) -> str:
            not_ = expression.args.get("not")
            if not_:
                expression = expression.copy()
                expression.args.pop("not", None)
                return f"{self.sql(expression, 'this')} IS NOT {self.sql(expression, 'expression')}"
            return super().is_sql(expression)

        def not_sql(self, expression: exp.Not) -> str:
            inner = expression.this
            if isinstance(inner, exp.In):
                base = super().in_sql(inner)
                return base.replace(" IN ", " NOT IN ", 1)
            if isinstance(inner, exp.Is):
                base = super().is_sql(inner)
                return base.replace(" IS ", " IS NOT ", 1)
            return super().not_sql(expression)

        def bracket_sql(self, expression: exp.Bracket) -> str:
            this = self.sql(expression, "this")
            returns_list_for_maps = expression.args.get("returns_list_for_maps")

            expressions = self.bracket_offset_expressions(expression)
            expressions_sql = ", ".join(self.sql(e) for e in expressions)

            if returns_list_for_maps:
                return f"{this}({expressions_sql})"
            return f"{this}[{expressions_sql}]"

        def jsonextractarray_sql(self, expression: exp.JSONExtractArray) -> str:
            this = self.sql(expression, "this")
            path = self.sql(expression, "expression")
            return f"JSON_EXTRACT_ARRAY({this}, {path})"

        def jsonextractscalar_sql(self, expression: exp.JSONExtractScalar) -> str:
            this = self.sql(expression, "this")
            path = self.sql(expression, "expression")

            path_syntax = None
            if expression.expressions:
                path_syntax = self.sql(expression.expressions[0])

            args = [this]

            implicit_default = (
                isinstance(expression.expression, exp.Literal) and expression.expression.this == "$"
            )

            if not implicit_default or path_syntax:
                args.append(path)

            if path_syntax:
                args.append(path_syntax)

            return self.func("JSON_VALUE", *args)

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            # Numeric types
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.TINYINT: "INT",
            exp.DataType.Type.SMALLINT: "INT",
            # String types - Firebolt uses TEXT
            exp.DataType.Type.VARCHAR: "TEXT",
            exp.DataType.Type.NVARCHAR: "TEXT",
            exp.DataType.Type.CHAR: "TEXT",
            exp.DataType.Type.NCHAR: "TEXT",
            exp.DataType.Type.TEXT: "TEXT",
            # Binary type
            exp.DataType.Type.BINARY: "BYTEA",
            exp.DataType.Type.VARBINARY: "BYTEA",
            exp.DataType.Type.BLOB: "BYTEA",
            # Boolean type
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            # Timestamp types
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMPTZ",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMPTZ",
            # Date type
            exp.DataType.Type.DATE: "DATE",
            # Numeric precision type
            exp.DataType.Type.DECIMAL: "NUMERIC",
            # Spatial type
            exp.DataType.Type.GEOGRAPHY: "GEOGRAPHY",
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Firebolt uses PostgreSQL-style postfix array syntax: TEXT[] not ARRAY<TEXT>
            if expression.is_type(exp.DataType.Type.ARRAY):
                if expression.expressions:
                    values = self.expressions(expression, key="values", flat=True)
                    return f"{self.expressions(expression, flat=True)}[{values}]"
                return "ARRAY"

            return super().datatype_sql(expression)

        def anonymous_sql(self, expression: exp.Anonymous) -> str:
            # Special handling for VECTOR_SEARCH with INDEX keyword
            if expression.this == "VECTOR_SEARCH" and expression.expressions:
                first_arg = expression.expressions[0]
                if isinstance(first_arg, exp.Kwarg) and first_arg.this.name == "INDEX":
                    args = [f"INDEX {self.sql(first_arg.expression)}"]
                    args.extend(self.sql(arg) for arg in expression.expressions[1:])
                    return f"VECTOR_SEARCH({', '.join(args)})"

            return super().anonymous_sql(expression)
