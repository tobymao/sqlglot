from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import seq_get
from sqlglot.parsers.mysql import MySQLParser
from sqlglot.tokens import TokenType


# Accept both DATE_TRUNC(datetime, unit) and DATE_TRUNC(unit, datetime)
def _build_date_trunc(args: t.List[exp.Expr]) -> exp.Expr:
    a0, a1 = seq_get(args, 0), seq_get(args, 1)

    def _is_unit_like(e: exp.Expr | None) -> bool:
        if not (isinstance(e, exp.Literal) and e.is_string):
            return False
        text = e.this
        return not any(ch.isdigit() for ch in text)

    # Determine which argument is the unit
    unit, this = (a0, a1) if _is_unit_like(a0) else (a1, a0)

    return exp.TimestampTrunc(this=this, unit=unit)


class DorisParser(MySQLParser):
    FUNCTIONS = {
        **MySQLParser.FUNCTIONS,
        "COLLECT_SET": exp.ArrayUniqueAgg.from_arg_list,
        "DATE_TRUNC": _build_date_trunc,
        "L2_DISTANCE": exp.EuclideanDistance.from_arg_list,
        "MONTHS_ADD": exp.AddMonths.from_arg_list,
        "REGEXP": exp.RegexpLike.from_arg_list,
        "TO_DATE": exp.TsOrDsToDate.from_arg_list,
    }

    FUNCTION_PARSERS = {
        k: v for k, v in MySQLParser.FUNCTION_PARSERS.items() if k != "GROUP_CONCAT"
    }

    NO_PAREN_FUNCTIONS = {
        k: v for k, v in MySQLParser.NO_PAREN_FUNCTIONS.items() if k != TokenType.CURRENT_DATE
    }

    PROPERTY_PARSERS = {
        **MySQLParser.PROPERTY_PARSERS,
        "PROPERTIES": lambda self: self._parse_wrapped_properties(),
        "UNIQUE": lambda self: self._parse_composite_key_property(exp.UniqueKeyProperty),
        # Plain KEY without UNIQUE/DUPLICATE/AGGREGATE prefixes should be treated as UniqueKeyProperty with unique=False
        "KEY": lambda self: self._parse_composite_key_property(exp.UniqueKeyProperty),
        "BUILD": lambda self: self._parse_build_property(),
        "REFRESH": lambda self: self._parse_refresh_property(),
    }

    def _parse_partition_property(
        self,
    ) -> t.Optional[exp.Expr] | t.List[exp.Expr]:
        expr = super()._parse_partition_property()

        if not expr:
            return self._parse_partitioned_by()

        if isinstance(expr, exp.Property):
            return expr

        self._match_l_paren()

        if self._match_text_seq("FROM", advance=False):
            create_expressions = self._parse_csv(self._parse_partitioning_granularity_dynamic)
        else:
            create_expressions = None

        self._match_r_paren()

        return self.expression(
            exp.PartitionByRangeProperty(
                partition_expressions=expr, create_expressions=create_expressions
            )
        )

    def _parse_partitioning_granularity_dynamic(self) -> exp.PartitionByRangePropertyDynamic:
        self._match_text_seq("FROM")
        start = self._parse_wrapped(self._parse_string)
        self._match_text_seq("TO")
        end = self._parse_wrapped(self._parse_string)
        self._match_text_seq("INTERVAL")
        number = self._parse_number()
        unit = self._parse_var(any_token=True)
        every = self.expression(exp.Interval(this=number, unit=unit))
        return self.expression(
            exp.PartitionByRangePropertyDynamic(start=start, end=end, every=every)
        )

    def _parse_partition_range_value(self) -> t.Optional[exp.Expr]:
        expr = super()._parse_partition_range_value()

        if isinstance(expr, exp.Partition):
            return expr

        self._match_text_seq("VALUES")
        name = expr

        # Doris-specific bracket syntax: VALUES [(...), (...))
        self._match(TokenType.L_BRACKET)
        values = self._parse_csv(lambda: self._parse_wrapped_csv(self._parse_expression))

        self._match(TokenType.R_BRACKET)
        self._match(TokenType.R_PAREN)

        part_range = self.expression(exp.PartitionRange(this=name, expressions=values))
        return self.expression(exp.Partition(expressions=[part_range]))

    def _parse_build_property(self) -> exp.BuildProperty:
        return self.expression(exp.BuildProperty(this=self._parse_var(upper=True)))

    def _parse_refresh_property(self) -> exp.RefreshTriggerProperty:
        method = self._parse_var(upper=True)

        self._match(TokenType.ON)

        kind = self._match_texts(("MANUAL", "COMMIT", "SCHEDULE")) and self._prev.text.upper()
        every = self._match_text_seq("EVERY") and self._parse_number()
        unit = self._parse_var(any_token=True) if every else None
        starts = self._match_text_seq("STARTS") and self._parse_string()

        return self.expression(
            exp.RefreshTriggerProperty(
                method=method, kind=kind, every=every, unit=unit, starts=starts
            )
        )
