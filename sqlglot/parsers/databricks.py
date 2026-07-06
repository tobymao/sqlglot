from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_date_delta, build_formatted_time
from sqlglot.helper import seq_get
from sqlglot.parsers.spark import SparkParser
from sqlglot.tokens import TokenType


class DatabricksParser(SparkParser):
    LOG_DEFAULTS_TO_LN = True
    STRICT_CAST = True
    COLON_IS_VARIANT_EXTRACT = True
    COLON_CHAIN_IS_SINGLE_EXTRACT = False

    FUNCTIONS = {
        **SparkParser.FUNCTIONS,
        "IFF": exp.If.from_arg_list,
        "GETDATE": exp.CurrentTimestamp.from_arg_list,
        "DATEDIFF": build_date_delta(exp.DateDiff),
        "DATE_DIFF": build_date_delta(exp.DateDiff),
        "NOW": exp.CurrentTimestamp.from_arg_list,
        "TO_DATE": build_formatted_time(exp.TsOrDsToDate),
        "UNIFORM": lambda args: exp.Uniform(
            this=seq_get(args, 0), expression=seq_get(args, 1), seed=seq_get(args, 2)
        ),
    }

    NO_PAREN_FUNCTION_PARSERS = {
        **SparkParser.NO_PAREN_FUNCTION_PARSERS,
        "CURDATE": lambda self: self._parse_curdate(),
    }

    FACTOR = {
        **SparkParser.FACTOR,
        TokenType.COLON: exp.JSONExtract,
    }

    COLUMN_OPERATORS = {
        **parser.Parser.COLUMN_OPERATORS,
        TokenType.QDCOLON: lambda self, this, to: self.build_cast(
            False,
            this=this,
            to=to,
        ),
    }
    CAST_COLUMN_OPERATORS = {
        *SparkParser.CAST_COLUMN_OPERATORS,
        TokenType.QDCOLON,
    }

    def _parse_curdate(self) -> exp.CurrentDate:
        # CURDATE, an alias for CURRENT_DATE, has optional parentheses
        if self._match(TokenType.L_PAREN):
            self._match_r_paren()
        return self.expression(exp.CurrentDate())

    ALTERABLES = SparkParser.ALTERABLES | {TokenType.SCHEMA, TokenType.DATABASE}

    SCHEMA_ALTER_PARSERS: t.ClassVar[dict[str, t.Callable]] = {
        "DEFAULT": lambda self: self._parse_alter_schema_default(),
        "SET": lambda self: self._parse_alter_schema_set(),
        "OWNER": lambda self: self._parse_alter_schema_owner(),
        "RETAIN": lambda self: self._parse_alter_schema_retain_dropped(),
        "UNSET": lambda self: self._parse_alter_schema_unset_tags(),
        "ENABLE": lambda self: self._parse_alter_schema_predictive_opt("ENABLE"),
        "DISABLE": lambda self: self._parse_alter_schema_predictive_opt("DISABLE"),
        "INHERIT": lambda self: self._parse_alter_schema_predictive_opt("INHERIT"),
    }

    def _parse_alter(self) -> exp.Alter | exp.Command:
        start = self._prev
        # Peek at the alter kind to decide which parser dict to use
        saved = self._index
        self._advance()
        kind_token = self._prev
        self._retreat(saved)

        if kind_token and kind_token.token_type in (TokenType.SCHEMA, TokenType.DATABASE):
            # Temporarily replace ALTER_PARSERS with the schema-specific ones
            orig = self.ALTER_PARSERS
            self.ALTER_PARSERS = self.SCHEMA_ALTER_PARSERS
            try:
                return super()._parse_alter()
            finally:
                self.ALTER_PARSERS = orig

        return super()._parse_alter()

    def _parse_alter_schema_default(self) -> exp.Expression:
        self._match_text_seq("COLLATION")
        return self.expression(exp.AlterSchemaDefaultCollation(this=self._parse_field()))

    def _parse_alter_schema_predictive_opt(self, mode: str) -> exp.Expression:
        self._match_text_seq("PREDICTIVE", "OPTIMIZATION")
        return self.expression(exp.AlterSchemaPredictiveOptimization(this=mode))

    def _parse_alter_schema_set(self) -> exp.Expression:
        if self._match_text_seq("DBPROPERTIES"):
            exprs = self._parse_wrapped_csv(self._parse_property)
            return self.expression(exp.AlterSchemaSetDbProperties(expressions=exprs))
        if self._match_text_seq("TAGS"):
            exprs = self._parse_wrapped_csv(self._parse_assignment)
            return self.expression(exp.AlterSchemaSetTags(expressions=exprs))
        if self._match_text_seq("DEFAULT", "COLLATION"):
            return self.expression(exp.AlterSchemaDefaultCollation(this=self._parse_field()))
        if self._match_text_seq("MANAGED", "LOCATION"):
            return self.expression(exp.AlterSchemaManagedLocation(this=self._parse_field()))
        if self._match_text_seq("OWNER", "TO"):
            return self.expression(exp.AlterSchemaOwner(this=self._parse_id_var()))
        if self._match_text_seq("RETAIN", "DROPPED", "TO"):
            return self._parse_alter_schema_retain_dropped_value()
        return self._parse_alter_table_set()

    def _parse_alter_schema_owner(self) -> exp.Expression:
        self._match_text_seq("TO")
        return self.expression(exp.AlterSchemaOwner(this=self._parse_id_var()))

    def _parse_alter_schema_retain_dropped(self) -> exp.Expression:
        self._match_text_seq("DROPPED", "TO")
        return self._parse_alter_schema_retain_dropped_value()

    def _parse_alter_schema_retain_dropped_value(self) -> exp.Expression:
        number = self._parse_number()
        unit = self._advance_any() and self._prev.text.upper()
        return self.expression(exp.AlterSchemaRetainDropped(this=number, unit=unit))

    def _parse_alter_schema_unset_tags(self) -> exp.Expression:
        self._match_text_seq("TAGS")
        exprs = self._parse_wrapped_csv(self._parse_string)
        return self.expression(exp.AlterSchemaSetTags(expressions=exprs, unset=True))

    def _parse_cluster_property(self):
        if self._match_texts(("AUTO", "NONE")):
            return self.expression(exp.ClusterProperty(this=self._prev.text.upper()))
        return super()._parse_cluster_property()
