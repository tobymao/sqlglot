"""
Hot-path expression functions compiled with mypyc for performance.

These are standalone functions extracted from Expression methods to enable
mypyc compilation without compiling the entire Expression class hierarchy.
"""

from __future__ import annotations

import typing as t
from collections import deque

Expr = t.Any


def _is_expression(v: t.Any) -> bool:
    return hasattr(v, "parent")


def iter_expressions_func(args: t.Dict[str, t.Any], reverse: bool = False) -> t.Iterator[Expr]:
    """Yields expression children from an args dict."""
    values = list(args.values())
    if reverse:
        values = list(reversed(values))

    for vs in values:
        if type(vs) is list:
            items = list(reversed(vs)) if reverse else vs
            for v in items:
                if _is_expression(v):
                    yield v
        elif _is_expression(vs):
            yield vs


def dfs_func(root: Expr, prune: t.Optional[t.Callable[[Expr], t.Any]] = None) -> t.Iterator[Expr]:
    """Depth-first traversal of expression tree."""
    stack = [root]

    while stack:
        node = stack.pop()
        yield node

        if prune is not None and prune(node):
            continue

        for vs in reversed(list(node.args.values())):
            if type(vs) is list:
                for v in reversed(vs):
                    if _is_expression(v):
                        stack.append(v)
            elif _is_expression(vs):
                stack.append(vs)


def bfs_func(root: Expr, prune: t.Optional[t.Callable[[Expr], t.Any]] = None) -> t.Iterator[Expr]:
    """Breadth-first traversal of expression tree."""
    queue = deque([root])

    while queue:
        node = queue.popleft()
        yield node

        if prune is not None and prune(node):
            continue

        for vs in node.args.values():
            if type(vs) is list:
                for v in vs:
                    if _is_expression(v):
                        queue.append(v)
            elif _is_expression(vs):
                queue.append(vs)


def compute_hash_func(root: Expr) -> int:
    """Compute hash for an expression tree using BFS, then bottom-up."""
    nodes = []
    queue = deque([root])

    while queue:
        node = queue.popleft()
        nodes.append(node)

        for vs in node.args.values():
            if type(vs) is list:
                for v in vs:
                    if _is_expression(v) and v._hash is None:
                        queue.append(v)
            elif _is_expression(vs) and vs._hash is None:
                queue.append(vs)

    for node in reversed(nodes):
        h = hash(node.key)
        node_type = type(node).__name__

        if node_type == "Literal" or node_type == "Identifier":
            for k, v in sorted(node.args.items()):
                if v:
                    h = hash((h, k, v))
        else:
            for k, v in sorted(node.args.items()):
                if type(v) is list:
                    for x in v:
                        if x is not None and x is not False:
                            h = hash((h, k, x.lower() if type(x) is str else x))
                        else:
                            h = hash((h, k))
                elif v is not None and v is not False:
                    h = hash((h, k, v.lower() if type(v) is str else v))

        node._hash = h

    return root._hash  # type: ignore


def set_parent_func(
    parent: Expr, arg_key: str, value: t.Any, index: t.Optional[int] = None
) -> None:
    """Set parent references on child expressions."""
    if _is_expression(value):
        value.parent = parent
        value.arg_key = arg_key
        value.index = index
    elif type(value) is list:
        for i, v in enumerate(value):
            if _is_expression(v):
                v.parent = parent
                v.arg_key = arg_key
                v.index = i


def deepcopy_func(root: Expr) -> Expr:
    """Deep copy an expression tree, avoiding stdlib deepcopy overhead."""
    root_copy = root.__class__()
    stack = [(root, root_copy)]

    while stack:
        node, copy = stack.pop()

        if node.comments is not None:
            copy.comments = list(node.comments)

        if node._type is not None:
            copy._type = node._type

        if node._meta is not None:
            copy._meta = dict(node._meta)

        if node._hash is not None:
            copy._hash = node._hash

        copy_args = {}

        for k, vs in node.args.items():
            if _is_expression(vs):
                child_copy = vs.__class__()
                child_copy.parent = copy
                child_copy.arg_key = k
                child_copy.index = None
                copy_args[k] = child_copy
                stack.append((vs, child_copy))
            elif type(vs) is list:
                new_list = []
                for i, v in enumerate(vs):
                    if _is_expression(v):
                        child_copy = v.__class__()
                        child_copy.parent = copy
                        child_copy.arg_key = k
                        child_copy.index = i
                        new_list.append(child_copy)
                        stack.append((v, child_copy))
                    else:
                        new_list.append(v)
                copy_args[k] = new_list
            else:
                copy_args[k] = vs

        copy.args = copy_args

    return root_copy
