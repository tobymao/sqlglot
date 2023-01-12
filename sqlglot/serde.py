from __future__ import annotations

import typing as t

from sqlglot import expressions as exp

if t.TYPE_CHECKING:
    JSON = t.Union[dict, list, str, float, int, bool]
    Node = t.Union[t.List["Node"], exp.DataType.Type, exp.Expression, JSON]


def dump(node: Node) -> JSON:
    """
    Recursively dump an AST into a JSON-serializable dict.
    """
    if isinstance(node, list):
        return [dump(i) for i in node]
    if isinstance(node, exp.DataType.Type):
        return {
            "class": "DataType.Type",
            "value": node.value,
        }
    if isinstance(node, exp.Expression):
        klass = node.__class__.__qualname__
        if node.__class__.__module__ != exp.__name__:
            klass = f"{node.__module__}.{klass}"
        obj = {
            "class": klass,
            "args": {k: dump(v) for k, v in node.args.items() if v is not None and v != []},
        }
        if node.type:
            obj["type"] = node.type.sql()
        if node.comments:
            obj["comments"] = node.comments
        return obj
    return node


def load(obj: JSON) -> Node:
    """
    Recursively load a dict (as returned by `dump`) into an AST.
    """
    if isinstance(obj, list):
        return [load(i) for i in obj]
    if isinstance(obj, dict):
        class_name = obj["class"]

        if class_name == "DataType.Type":
            return exp.DataType.Type(obj["value"])

        if "." in class_name:
            module_path, class_name = class_name.rsplit(".", maxsplit=1)
            module = __import__(module_path, fromlist=[class_name])
        else:
            module = exp

        klass = getattr(module, class_name)

        expression = klass(**{k: load(v) for k, v in obj["args"].items()})
        type_ = obj.get("type")
        if type_:
            expression.type = exp.DataType.build(type_)
        comments = obj.get("comments")
        if comments:
            expression.comments = load(comments)
        return expression
    return obj
