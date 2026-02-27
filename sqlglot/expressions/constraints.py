"""sqlglot expressions constraints."""

from __future__ import annotations

from sqlglot.expressions.core import Expression, ExpressionBase, ColumnConstraintKind


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


class AutoIncrementColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class ZeroFillColumnConstraint(ColumnConstraint):
    arg_types = {}


class PeriodForSystemTimeConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"this": True, "expression": True}


class CaseSpecificColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"not_": True}


class CharacterSetColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"this": True}


class CheckColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"this": True, "enforced": False}


class ClusteredColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class CollateColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class CommentColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class CompressColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"this": False}


class DateFormatColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"this": True}


class DefaultColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class EncodeColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class ExcludeColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class EphemeralColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"this": False}


class WithOperator(Expression):
    arg_types = {"this": True, "op": True}


class GeneratedAsIdentityColumnConstraint(ExpressionBase, ColumnConstraintKind):
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


class GeneratedAsRowColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"start": False, "hidden": False}


class IndexColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {
        "this": False,
        "expressions": False,
        "kind": False,
        "index_type": False,
        "options": False,
        "expression": False,  # Clickhouse
        "granularity": False,
    }


class InlineLengthColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class NonClusteredColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class NotForReplicationColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {}


class MaskingPolicyColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"this": True, "expressions": False}


class NotNullColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"allow_null": False}


class OnUpdateColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class PrimaryKeyColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"desc": False, "options": False}


class TitleColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class UniqueColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {
        "this": False,
        "index_type": False,
        "on_conflict": False,
        "nulls": False,
        "options": False,
    }


class UppercaseColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {}


class WatermarkColumnConstraint(Expression):
    arg_types = {"this": True, "expression": True}


class PathColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class ProjectionPolicyColumnConstraint(ExpressionBase, ColumnConstraintKind):
    pass


class ComputedColumnConstraint(ExpressionBase, ColumnConstraintKind):
    arg_types = {"this": True, "persisted": False, "not_null": False, "data_type": False}


class InOutColumnConstraint(ExpressionBase, ColumnConstraintKind):
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
