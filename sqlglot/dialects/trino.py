from __future__ import annotations

from sqlglot import exp, parser
from sqlglot.dialects.dialect import (
    merge_without_target_sql,
    trim_sql,
    timestrtotime_sql,
    groupconcat_sql,
)
from sqlglot.dialects.presto import Presto
from sqlglot.tokens import TokenType
import typing as t


class Trino(Presto):
    SUPPORTS_USER_DEFINED_TYPES = False
    LOG_BASE_FIRST = True

    class Parser(Presto.Parser):
        FUNCTION_PARSERS = {
            **Presto.Parser.FUNCTION_PARSERS,
            "TRIM": lambda self: self._parse_trim(),
            "JSON_QUERY": lambda self: self._parse_json_query(),
            "LISTAGG": lambda self: self._parse_string_agg(),
        }

        JSON_QUERY_OPTIONS: parser.OPTIONS_TYPE = {
            **dict.fromkeys(
                ("WITH", "WITHOUT"),
                (
                    ("WRAPPER"),
                    ("ARRAY", "WRAPPER"),
                    ("CONDITIONAL", "WRAPPER"),
                    ("CONDITIONAL", "ARRAY", "WRAPPED"),
                    ("UNCONDITIONAL", "WRAPPER"),
                    ("UNCONDITIONAL", "ARRAY", "WRAPPER"),
                ),
            ),
        }

        def _parse_json_query_quote(self) -> t.Optional[exp.JSONExtractQuote]:
            if not (
                self._match_text_seq("KEEP", "QUOTES") or self._match_text_seq("OMIT", "QUOTES")
            ):
                return None

            return self.expression(
                exp.JSONExtractQuote,
                option=self._tokens[self._index - 2].text.upper(),
                scalar=self._match_text_seq("ON", "SCALAR", "STRING"),
            )

        def _parse_json_query(self) -> exp.JSONExtract:
            return self.expression(
                exp.JSONExtract,
                this=self._parse_bitwise(),
                expression=self._match(TokenType.COMMA) and self._parse_bitwise(),
                option=self._parse_var_from_options(self.JSON_QUERY_OPTIONS, raise_unmatched=False),
                json_query=True,
                quote=self._parse_json_query_quote(),
                on_condition=self._parse_on_condition(),
            )

    class Generator(Presto.Generator):
        PROPERTIES_LOCATION = {
            **Presto.Generator.PROPERTIES_LOCATION,
            exp.LocationProperty: exp.Properties.Location.POST_WITH,
        }

        TRANSFORMS = {
            **Presto.Generator.TRANSFORMS,
            exp.ArraySum: lambda self,
            e: f"REDUCE({self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)",
            exp.ArrayUniqueAgg: lambda self, e: f"ARRAY_AGG(DISTINCT {self.sql(e, 'this')})",
            exp.GroupConcat: lambda self, e: groupconcat_sql(self, e, on_overflow=True),
            exp.LocationProperty: lambda self, e: self.property_sql(e),
            exp.Merge: merge_without_target_sql,
            exp.TimeStrToTime: lambda self, e: timestrtotime_sql(self, e, include_precision=True),
            exp.Trim: trim_sql,
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        def jsonextract_sql(self, expression: exp.JSONExtract) -> str:
            if not expression.args.get("json_query"):
                return super().jsonextract_sql(expression)

            json_path = self.sql(expression, "expression")
            option = self.sql(expression, "option")
            option = f" {option}" if option else ""

            quote = self.sql(expression, "quote")
            quote = f" {quote}" if quote else ""

            on_condition = self.sql(expression, "on_condition")
            on_condition = f" {on_condition}" if on_condition else ""

            return self.func(
                "JSON_QUERY",
                expression.this,
                json_path + option + quote + on_condition,
            )
