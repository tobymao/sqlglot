import typing as t

from sqlglot import expressions as exp


def is_in_cte(node: exp.Expression):
    return node.find_ancestor(exp.CTE) is not None


def replace_alias_names_with_sequence_ids(node: exp.Expression, name_to_sequence_id_mapping: t.Dict[str, t.List[str]], **kwargs):
    """
    PySpark DataFrame Specific

    Replaces any alias name references with the CTE name for the latest sequence ID that is used based on the name.
    """
    if isinstance(node, exp.Column) and node.args.get("table") is not None and node.args["table"].alias_or_name in name_to_sequence_id_mapping:
        new_table_name = exp.Identifier(this=name_to_sequence_id_mapping[node.args["table"].alias_or_name][-1])
        node.set("table", new_table_name)
    return node


def replace_branch_and_sequence_ids_with_cte_name(node: exp.Expression, known_ids: t.Set[str], **kwargs):
    """
    PySpark DataFrame Specific

    Replaces branch and sequence id references with cte names
    """
    if isinstance(node, exp.Identifier) and node.alias_or_name in known_ids:
        this_id = node.alias_or_name
        root_select: exp.Select = node.parent_select
        this_cte_name = node.find_ancestor(exp.CTE).alias_or_name if is_in_cte(node) else None
        latest_cte_name = None
        for cte in root_select.ctes:
            if this_cte_name is not None and this_cte_name == cte.alias_or_name:
                break
            branch_id = cte.args["branch_id"]
            sequence_id = cte.args["sequence_id"]
            if this_id == branch_id:
                latest_cte_name = cte.alias_or_name
            elif this_id == sequence_id:
                latest_cte_name = cte.alias_or_name
        if latest_cte_name is None:
            raise RuntimeError("Could not find matching ID")
        node.set("this", exp.Identifier(this=latest_cte_name))
    return node


ORDERED_TRANSFORMS = [
    replace_alias_names_with_sequence_ids,
    replace_branch_and_sequence_ids_with_cte_name
]


if __name__ == '__main__':
    import sqlglot
    expression = sqlglot.parse_one("WITH a1 as (SELECT col_a from tablea) SELECT blah.col_a from blah")
    cte = expression.ctes[0]
    cte.set("branch_id", "a2")
    cte.set("sequence_id", "a3")
    name_to_sequence_id_mapping = {
        "blah": ["a3"],
    }
    known_ids = {"a2", "a3"}
    result = expression.transform(replace_alias_names_with_sequence_ids, name_to_sequence_id_mapping)
    for transform in ORDERED_TRANSFORMS:
        expression = expression.transform(transform, name_to_sequence_id_mapping=name_to_sequence_id_mapping, known_ids=known_ids)
    print("here")
