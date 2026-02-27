"""sqlglot expressions constraints."""

from __future__ import annotations

from sqlglot.expressions.core import Expression, ColumnConstraintKind


class IndexConstraintOption(Expression):
    arg_types = {
        "key_block_size": False,
        "using": False,
        "parser": False,
        "comment": False,
        "visible": False,
        "engine_attr": False,
        "secondary_engine_attr": False,
    }


class ColumnConstraint(Expression):
    arg_types = {"this": False, "kind": True}

    @property
    def kind(self) -> ColumnConstraintKind:
        return self.args["kind"]


class AutoIncrementColumnConstraint(ColumnConstraintKind):
    pass


class ZeroFillColumnConstraint(ColumnConstraint):
    arg_types = {}


class PeriodForSystemTimeConstraint(ColumnConstraintKind):
    arg_types = {"this": True, "expression": True}


class CaseSpecificColumnConstraint(ColumnConstraintKind):
    arg_types = {"not_": True}


class CharacterSetColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True}


class CheckColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True, "enforced": False}


class ClusteredColumnConstraint(ColumnConstraintKind):
    pass


class CollateColumnConstraint(ColumnConstraintKind):
    pass


class CommentColumnConstraint(ColumnConstraintKind):
    pass


class CompressColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": False}


class DateFormatColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True}


class DefaultColumnConstraint(ColumnConstraintKind):
    pass


class EncodeColumnConstraint(ColumnConstraintKind):
    pass


class ExcludeColumnConstraint(ColumnConstraintKind):
    pass


class EphemeralColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": False}


class WithOperator(Expression):
    arg_types = {"this": True, "op": True}


class GeneratedAsIdentityColumnConstraint(ColumnConstraintKind):
    # this: True -> ALWAYS, this: False -> BY DEFAULT
    arg_types = {
        "this": False,
        "expression": False,
        "on_null": False,
        "start": False,
        "increment": False,
        "minvalue": False,
        "maxvalue": False,
        "cycle": False,
        "order": False,
    }


class GeneratedAsRowColumnConstraint(ColumnConstraintKind):
    arg_types = {"start": False, "hidden": False}


class IndexColumnConstraint(ColumnConstraintKind):
    arg_types = {
        "this": False,
        "expressions": False,
        "kind": False,
        "index_type": False,
        "options": False,
        "expression": False,  # Clickhouse
        "granularity": False,
    }


class InlineLengthColumnConstraint(ColumnConstraintKind):
    pass


class NonClusteredColumnConstraint(ColumnConstraintKind):
    pass


class NotForReplicationColumnConstraint(ColumnConstraintKind):
    arg_types = {}


class MaskingPolicyColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True, "expressions": False}


class NotNullColumnConstraint(ColumnConstraintKind):
    arg_types = {"allow_null": False}


class OnUpdateColumnConstraint(ColumnConstraintKind):
    pass


class PrimaryKeyColumnConstraint(ColumnConstraintKind):
    arg_types = {"desc": False, "options": False}


class TitleColumnConstraint(ColumnConstraintKind):
    pass


class UniqueColumnConstraint(ColumnConstraintKind):
    arg_types = {
        "this": False,
        "index_type": False,
        "on_conflict": False,
        "nulls": False,
        "options": False,
    }


class UppercaseColumnConstraint(ColumnConstraintKind):
    arg_types = {}


class WatermarkColumnConstraint(Expression):
    arg_types = {"this": True, "expression": True}


class PathColumnConstraint(ColumnConstraintKind):
    pass


class ProjectionPolicyColumnConstraint(ColumnConstraintKind):
    pass


class ComputedColumnConstraint(ColumnConstraintKind):
    arg_types = {"this": True, "persisted": False, "not_null": False, "data_type": False}


class InOutColumnConstraint(ColumnConstraintKind):
    arg_types = {"input_": False, "output": False, "variadic": False}


class Constraint(Expression):
    arg_types = {"this": True, "expressions": True}


class ForeignKey(Expression):
    arg_types = {
        "expressions": False,
        "reference": False,
        "delete": False,
        "update": False,
        "options": False,
    }


class ColumnPrefix(Expression):
    arg_types = {"this": True, "expression": True}


class PrimaryKey(Expression):
    arg_types = {"this": False, "expressions": True, "options": False, "include": False}


class IndexParameters(Expression):
    arg_types = {
        "using": False,
        "include": False,
        "columns": False,
        "with_storage": False,
        "partition_by": False,
        "tablespace": False,
        "where": False,
        "on": False,
    }


class AddConstraint(Expression):
    arg_types = {"expressions": True}
