import typing as t

from sqlglot.dataframe.sql import types

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql._typing import SchemaInput


def get_column_mapping_from_schema_input(schema: "SchemaInput") -> t.Dict[str, str]:
    if isinstance(schema, dict):
        return schema
    elif isinstance(schema, str):
        col_name_type_strs = [x.strip() for x in schema.split(",")]
        return {name_type_str.split(':')[0].strip(): name_type_str.split(':')[1].strip() for name_type_str in
                          col_name_type_strs}
    elif isinstance(schema, types.StructType):
        return {struct_field.name: struct_field.dataType.simpleString() for struct_field in schema}
    return {x.strip(): None for x in schema}
