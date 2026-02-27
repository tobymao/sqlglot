"""sqlglot expressions properties."""

from __future__ import annotations

import typing as t
from enum import auto

from sqlglot.helper import AutoName, mypyc_attr
from sqlglot.expressions.core import Expression, ColumnConstraintKind, Literal, convert


@mypyc_attr(allow_interpreted_subclasses=True)
class Property(Expression):
    arg_types = {"this": True, "value": True}


class GrantPrivilege(Expression):
    arg_types = {"this": True, "expressions": False}


class GrantPrincipal(Expression):
    arg_types = {"this": True, "kind": False}


class AllowedValuesProperty(Expression):
    arg_types = {"expressions": True}


class AlgorithmProperty(Property):
    arg_types = {"this": True}


class AutoIncrementProperty(Property):
    arg_types = {"this": True}


class AutoRefreshProperty(Property):
    arg_types = {"this": True}


class BackupProperty(Property):
    arg_types = {"this": True}


class BuildProperty(Property):
    arg_types = {"this": True}


class BlockCompressionProperty(Property):
    arg_types = {
        "autotemp": False,
        "always": False,
        "default": False,
        "manual": False,
        "never": False,
    }


class CharacterSetProperty(Property):
    arg_types = {"this": True, "default": True}


class ChecksumProperty(Property):
    arg_types = {"on": False, "default": False}


class CollateProperty(Property):
    arg_types = {"this": True, "default": False}


class CopyGrantsProperty(Property):
    arg_types = {}


class DataBlocksizeProperty(Property):
    arg_types = {
        "size": False,
        "units": False,
        "minimum": False,
        "maximum": False,
        "default": False,
    }


class DataDeletionProperty(Property):
    arg_types = {"on": True, "filter_column": False, "retention_period": False}


class DefinerProperty(Property):
    arg_types = {"this": True}


class DistKeyProperty(Property):
    arg_types = {"this": True}


class DistributedByProperty(Property):
    arg_types = {"expressions": False, "kind": True, "buckets": False, "order": False}


class DistStyleProperty(Property):
    arg_types = {"this": True}


class DuplicateKeyProperty(Property):
    arg_types = {"expressions": True}


class EngineProperty(Property):
    arg_types = {"this": True}


class HeapProperty(Property):
    arg_types = {}


class HandlerProperty(Property):
    arg_types = {"this": True}


class ParameterStyleProperty(Property):
    arg_types = {"this": True}


class ToTableProperty(Property):
    arg_types = {"this": True}


class ExecuteAsProperty(Property):
    arg_types = {"this": True}


class ExternalProperty(Property):
    arg_types = {"this": False}


class FallbackProperty(Property):
    arg_types = {"no": True, "protection": False}


class FileFormatProperty(Property):
    arg_types = {"this": False, "expressions": False, "hive_format": False}


class CredentialsProperty(Property):
    arg_types = {"expressions": True}


class FreespaceProperty(Property):
    arg_types = {"this": True, "percent": False}


class GlobalProperty(Property):
    arg_types = {}


class IcebergProperty(Property):
    arg_types = {}


class InheritsProperty(Property):
    arg_types = {"expressions": True}


class InputModelProperty(Property):
    arg_types = {"this": True}


class OutputModelProperty(Property):
    arg_types = {"this": True}


class IsolatedLoadingProperty(Property):
    arg_types = {"no": False, "concurrent": False, "target": False}


class JournalProperty(Property):
    arg_types = {
        "no": False,
        "dual": False,
        "before": False,
        "local": False,
        "after": False,
    }


class LanguageProperty(Property):
    arg_types = {"this": True}


class EnviromentProperty(Property):
    arg_types = {"expressions": True}


class ClusteredByProperty(Property):
    arg_types = {"expressions": True, "sorted_by": False, "buckets": True}


class DictProperty(Property):
    arg_types = {"this": True, "kind": True, "settings": False}


class DictSubProperty(Property):
    pass


class DictRange(Property):
    arg_types = {"this": True, "min": True, "max": True}


class DynamicProperty(Property):
    arg_types = {}


class OnCluster(Property):
    arg_types = {"this": True}


class EmptyProperty(Property):
    arg_types = {}


class LikeProperty(Property):
    arg_types = {"this": True, "expressions": False}


class LocationProperty(Property):
    arg_types = {"this": True}


class LockProperty(Property):
    arg_types = {"this": True}


class LockingProperty(Property):
    arg_types = {
        "this": False,
        "kind": True,
        "for_or_in": False,
        "lock_type": True,
        "override": False,
    }


class LogProperty(Property):
    arg_types = {"no": True}


class MaterializedProperty(Property):
    arg_types = {"this": False}


class MergeBlockRatioProperty(Property):
    arg_types = {"this": False, "no": False, "default": False, "percent": False}


class NoPrimaryIndexProperty(Property):
    arg_types = {}


class OnProperty(Property):
    arg_types = {"this": True}


class OnCommitProperty(Property):
    arg_types = {"delete": False}


class PartitionedByProperty(Property):
    arg_types = {"this": True}


class PartitionedByBucket(Property):
    arg_types = {"this": True, "expression": True}


class PartitionByTruncate(Property):
    arg_types = {"this": True, "expression": True}


class PartitionByRangeProperty(Property):
    arg_types = {"partition_expressions": True, "create_expressions": True}


class PartitionByRangePropertyDynamic(Expression):
    arg_types = {"this": False, "start": True, "end": True, "every": True}


class RollupProperty(Property):
    arg_types = {"expressions": True}


class RollupIndex(Expression):
    arg_types = {"this": True, "expressions": True, "from_index": False, "properties": False}


class PartitionByListProperty(Property):
    arg_types = {"partition_expressions": True, "create_expressions": True}


class PartitionList(Expression):
    arg_types = {"this": True, "expressions": True}


class RefreshTriggerProperty(Property):
    arg_types = {
        "method": False,
        "kind": False,
        "every": False,
        "unit": False,
        "starts": False,
    }


class UniqueKeyProperty(Property):
    arg_types = {"expressions": True}


class PartitionBoundSpec(Expression):
    # this -> IN / MODULUS, expression -> REMAINDER, from_expressions -> FROM (...), to_expressions -> TO (...)
    arg_types = {
        "this": False,
        "expression": False,
        "from_expressions": False,
        "to_expressions": False,
    }


class PartitionedOfProperty(Property):
    # this -> parent_table (schema), expression -> FOR VALUES ... / DEFAULT
    arg_types = {"this": True, "expression": True}


class StreamingTableProperty(Property):
    arg_types = {}


class RemoteWithConnectionModelProperty(Property):
    arg_types = {"this": True}


class ReturnsProperty(Property):
    arg_types = {"this": False, "is_table": False, "table": False, "null": False}


class StrictProperty(Property):
    arg_types = {}


class RowFormatProperty(Property):
    arg_types = {"this": True}


class RowFormatDelimitedProperty(Property):
    # https://cwiki.apache.org/confluence/display/hive/languagemanual+dml
    arg_types = {
        "fields": False,
        "escaped": False,
        "collection_items": False,
        "map_keys": False,
        "lines": False,
        "null": False,
        "serde": False,
    }


class RowFormatSerdeProperty(Property):
    arg_types = {"this": True, "serde_properties": False}


class QueryTransform(Expression):
    arg_types = {
        "expressions": True,
        "command_script": True,
        "schema": False,
        "row_format_before": False,
        "record_writer": False,
        "row_format_after": False,
        "record_reader": False,
    }


class SampleProperty(Property):
    arg_types = {"this": True}


class SecurityProperty(Property):
    arg_types = {"this": True}


class SchemaCommentProperty(Property):
    arg_types = {"this": True}


class SemanticView(Expression):
    arg_types = {
        "this": True,
        "metrics": False,
        "dimensions": False,
        "facts": False,
        "where": False,
    }


class SerdeProperties(Property):
    arg_types = {"expressions": True, "with_": False}


class SetProperty(Property):
    arg_types = {"multi": True}


class SharingProperty(Property):
    arg_types = {"this": False}


class SetConfigProperty(Property):
    arg_types = {"this": True}


class SettingsProperty(Property):
    arg_types = {"expressions": True}


class SortKeyProperty(Property):
    arg_types = {"this": True, "compound": False}


class SqlReadWriteProperty(Property):
    arg_types = {"this": True}


class SqlSecurityProperty(Property):
    arg_types = {"this": True}


class StabilityProperty(Property):
    arg_types = {"this": True}


class StorageHandlerProperty(Property):
    arg_types = {"this": True}


class TemporaryProperty(Property):
    arg_types = {"this": False}


class SecureProperty(Property):
    arg_types = {}


class Tags(Property, ColumnConstraintKind):
    arg_types = {"expressions": True}


class PropertiesLocation(AutoName):
    POST_CREATE = auto()
    POST_NAME = auto()
    POST_SCHEMA = auto()
    POST_WITH = auto()
    POST_ALIAS = auto()
    POST_EXPRESSION = auto()
    POST_INDEX = auto()
    UNSUPPORTED = auto()


class TransformModelProperty(Property):
    arg_types = {"expressions": True}


class TransientProperty(Property):
    arg_types = {"this": False}


class UnloggedProperty(Property):
    arg_types = {}


class UsingTemplateProperty(Property):
    arg_types = {"this": True}


class ViewAttributeProperty(Property):
    arg_types = {"this": True}


class VolatileProperty(Property):
    arg_types = {"this": False}


class WithDataProperty(Property):
    arg_types = {"no": True, "statistics": False}


class WithJournalTableProperty(Property):
    arg_types = {"this": True}


class WithSchemaBindingProperty(Property):
    arg_types = {"this": True}


class WithSystemVersioningProperty(Property):
    arg_types = {
        "on": False,
        "this": False,
        "data_consistency": False,
        "retention_period": False,
        "with_": True,
    }


class WithProcedureOptions(Property):
    arg_types = {"expressions": True}


class EncodeProperty(Property):
    arg_types = {"this": True, "properties": False, "key": False}


class IncludeProperty(Property):
    arg_types = {"this": True, "alias": False, "column_def": False}


class ForceProperty(Property):
    arg_types = {}


class Properties(Expression):
    arg_types = {"expressions": True}

    NAME_TO_PROPERTY: t.ClassVar[t.Dict[str, t.Type[Property]]] = {
        "ALGORITHM": AlgorithmProperty,
        "AUTO_INCREMENT": AutoIncrementProperty,
        "CHARACTER SET": CharacterSetProperty,
        "CLUSTERED_BY": ClusteredByProperty,
        "COLLATE": CollateProperty,
        "COMMENT": SchemaCommentProperty,
        "CREDENTIALS": CredentialsProperty,
        "DEFINER": DefinerProperty,
        "DISTKEY": DistKeyProperty,
        "DISTRIBUTED_BY": DistributedByProperty,
        "DISTSTYLE": DistStyleProperty,
        "ENGINE": EngineProperty,
        "EXECUTE AS": ExecuteAsProperty,
        "FORMAT": FileFormatProperty,
        "LANGUAGE": LanguageProperty,
        "LOCATION": LocationProperty,
        "LOCK": LockProperty,
        "PARTITIONED_BY": PartitionedByProperty,
        "RETURNS": ReturnsProperty,
        "ROW_FORMAT": RowFormatProperty,
        "SORTKEY": SortKeyProperty,
        "ENCODE": EncodeProperty,
        "INCLUDE": IncludeProperty,
    }

    PROPERTY_TO_NAME: t.ClassVar[t.Dict[t.Type[Property], str]] = {
        v: k for k, v in NAME_TO_PROPERTY.items()
    }

    # CREATE property locations
    # Form: schema specified
    #   create [POST_CREATE]
    #     table a [POST_NAME]
    #     (b int) [POST_SCHEMA]
    #     with ([POST_WITH])
    #     index (b) [POST_INDEX]
    #
    # Form: alias selection
    #   create [POST_CREATE]
    #     table a [POST_NAME]
    #     as [POST_ALIAS] (select * from b) [POST_EXPRESSION]
    #     index (c) [POST_INDEX]
    Location: t.ClassVar[t.Type[PropertiesLocation]] = PropertiesLocation

    @classmethod
    def from_dict(cls, properties_dict: t.Dict) -> Properties:
        expressions = []
        for key, value in properties_dict.items():
            property_cls = cls.NAME_TO_PROPERTY.get(key.upper())
            if property_cls:
                expressions.append(property_cls(this=convert(value)))
            else:
                expressions.append(Property(this=Literal.string(key), value=convert(value)))

        return cls(expressions=expressions)
