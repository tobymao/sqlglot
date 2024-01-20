from __future__ import annotations

import typing as t

from sqlglot.errors import ParseError
from sqlglot.expressions import SAFE_IDENTIFIER_RE
from sqlglot.tokens import Token, Tokenizer, TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import Lit


class JSONPathTokenizer(Tokenizer):
    SINGLE_TOKENS = {
        "(": TokenType.L_PAREN,
        ")": TokenType.R_PAREN,
        "[": TokenType.L_BRACKET,
        "]": TokenType.R_BRACKET,
        ":": TokenType.COLON,
        ",": TokenType.COMMA,
        "-": TokenType.DASH,
        ".": TokenType.DOT,
        "?": TokenType.PLACEHOLDER,
        "@": TokenType.PARAMETER,
        "'": TokenType.QUOTE,
        '"': TokenType.QUOTE,
        "$": TokenType.DOLLAR,
        "*": TokenType.STAR,
    }

    KEYWORDS = {
        "..": TokenType.DOT,
    }

    IDENTIFIER_ESCAPES = ["\\"]
    STRING_ESCAPES = ["\\"]


JSONPathNode = t.Dict[str, t.Any]


def _node(kind: str, value: t.Any = None, **kwargs: t.Any) -> JSONPathNode:
    node = {"kind": kind, **kwargs}

    if value is not None:
        node["value"] = value

    return node


def parse(path: str) -> t.List[JSONPathNode]:
    """Takes in a JSONPath string and converts into a list of nodes."""
    tokens = JSONPathTokenizer().tokenize(path)
    size = len(tokens)

    i = 0

    def _curr() -> t.Optional[TokenType]:
        return tokens[i].token_type if i < size else None

    def _prev() -> Token:
        return tokens[i - 1]

    def _advance() -> Token:
        nonlocal i
        i += 1
        return _prev()

    def _error(msg: str) -> str:
        return f"{msg} at index {i}: {path}"

    @t.overload
    def _match(token_type: TokenType, raise_unmatched: Lit[True] = True) -> Token:
        pass

    @t.overload
    def _match(token_type: TokenType, raise_unmatched: Lit[False] = False) -> t.Optional[Token]:
        pass

    def _match(token_type, raise_unmatched=False):
        if _curr() == token_type:
            return _advance()
        if raise_unmatched:
            raise ParseError(_error(f"Expected {token_type}"))
        return None

    def _parse_literal() -> t.Any:
        token = _match(TokenType.STRING) or _match(TokenType.IDENTIFIER)
        if token:
            return token.text
        if _match(TokenType.STAR):
            return _node("wildcard")
        if _match(TokenType.PLACEHOLDER) or _match(TokenType.L_PAREN):
            script = _prev().text == "("
            start = i

            while True:
                if _match(TokenType.L_BRACKET):
                    _parse_bracket()  # nested call which we can throw away
                if _curr() in (TokenType.R_BRACKET, None):
                    break
                _advance()
            return _node(
                "script" if script else "filter", path[tokens[start].start : tokens[i].end]
            )

        number = "-" if _match(TokenType.DASH) else ""

        token = _match(TokenType.NUMBER)
        if token:
            number += token.text

        if number:
            return int(number)
        return False

    def _parse_slice() -> t.Any:
        start = _parse_literal()
        end = _parse_literal() if _match(TokenType.COLON) else None
        step = _parse_literal() if _match(TokenType.COLON) else None

        if end is None and step is None:
            return start
        return _node("slice", start=start, end=end, step=step)

    def _parse_bracket() -> JSONPathNode:
        literal = _parse_slice()

        if isinstance(literal, str) or literal is not False:
            indexes = [literal]
            while _match(TokenType.COMMA):
                literal = _parse_slice()

                if literal:
                    indexes.append(literal)

            if len(indexes) == 1:
                if isinstance(literal, str):
                    node = _node("key", indexes[0])
                elif isinstance(literal, dict) and literal["kind"] in ("script", "filter"):
                    node = _node("selector", indexes[0])
                else:
                    node = _node("subscript", indexes[0])
            else:
                node = _node("union", indexes)
        else:
            raise ParseError(_error("Cannot have empty segment"))

        _match(TokenType.R_BRACKET, raise_unmatched=True)

        return node

    nodes = []

    while _curr():
        if _match(TokenType.DOLLAR):
            nodes.append(_node("root"))
        elif _match(TokenType.DOT):
            recursive = _prev().text == ".."
            value = _match(TokenType.VAR) or _match(TokenType.STAR)
            nodes.append(
                _node("recursive" if recursive else "child", value=value.text if value else None)
            )
        elif _match(TokenType.L_BRACKET):
            nodes.append(_parse_bracket())
        elif _match(TokenType.VAR):
            nodes.append(_node("key", _prev().text))
        elif _match(TokenType.STAR):
            nodes.append(_node("wildcard"))
        elif _match(TokenType.PARAMETER):
            nodes.append(_node("current"))
        else:
            raise ParseError(_error(f"Unexpected {tokens[i].token_type}"))

    return nodes


MAPPING = {
    "child": lambda n: f".{n['value']}" if n.get("value") is not None else "",
    "filter": lambda n: f"?{n['value']}",
    "key": lambda n: f".{n['value']}"
    if SAFE_IDENTIFIER_RE.match(n["value"])
    else f'[{generate([n["value"]])}]',
    "recursive": lambda n: f"..{n['value']}" if n.get("value") is not None else "..",
    "root": lambda _: "$",
    "script": lambda n: f"({n['value']}",
    "slice": lambda n: ":".join(
        "" if p is False else generate([p])
        for p in [n["start"], n["end"], n["step"]]
        if p is not None
    ),
    "selector": lambda n: f"[{generate([n['value']])}]",
    "subscript": lambda n: f"[{generate([n['value']])}]",
    "union": lambda n: f"[{','.join(generate([p]) for p in n['value'])}]",
    "wildcard": lambda _: "*",
}


def generate(
    nodes: t.List[JSONPathNode],
    mapping: t.Optional[t.Dict[str, t.Callable[[JSONPathNode], str]]] = None,
) -> str:
    mapping = MAPPING if mapping is None else mapping
    path = []

    for node in nodes:
        if isinstance(node, dict):
            path.append(mapping[node["kind"]](node))
        elif isinstance(node, str):
            escaped = node.replace('"', '\\"')
            path.append(f'"{escaped}"')
        else:
            path.append(str(node))

    return "".join(path)
