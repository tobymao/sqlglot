from __future__ import annotations

import logging
import typing as t

import sqlglot.expressions as exp
from sqlglot.errors import ParseError
from sqlglot.tokens import Token, Tokenizer, TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import Lit

logger = logging.getLogger("sqlglot")


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


def parse(path: str) -> t.List[exp.JSONPathPart]:
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
            return exp.JSONPathWildcard()
        if _match(TokenType.PLACEHOLDER) or _match(TokenType.L_PAREN):
            script = _prev().text == "("
            start = i

            while True:
                if _match(TokenType.L_BRACKET):
                    _parse_bracket()  # nested call which we can throw away
                if _curr() in (TokenType.R_BRACKET, None):
                    break
                _advance()

            expr_type = exp.JSONPathScript if script else exp.JSONPathFilter
            return expr_type(this=path[tokens[start].start : tokens[i].end])

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

        return exp.JSONPathSlice(start=start, end=end, step=step)

    def _parse_bracket() -> exp.JSONPathPart:
        literal = _parse_slice()

        if isinstance(literal, str) or literal is not False:
            indexes = [literal]
            while _match(TokenType.COMMA):
                literal = _parse_slice()

                if literal:
                    indexes.append(literal)

            if len(indexes) == 1:
                if isinstance(literal, str):
                    node: exp.JSONPathPart = exp.JSONPathKey(this=indexes[0])
                elif isinstance(literal, exp.JSONPathPart) and isinstance(
                    literal, (exp.JSONPathScript, exp.JSONPathFilter)
                ):
                    node = exp.JSONPathSelector(this=indexes[0])
                else:
                    node = exp.JSONPathSubscript(this=indexes[0])
            else:
                node = exp.JSONPathUnion(expressions=indexes)
        else:
            raise ParseError(_error("Cannot have empty segment"))

        _match(TokenType.R_BRACKET, raise_unmatched=True)

        return node

    nodes: t.List[exp.JSONPathPart] = []

    while _curr():
        if _match(TokenType.DOLLAR):
            nodes.append(exp.JSONPathRoot())
        elif _match(TokenType.DOT):
            recursive = _prev().text == ".."
            value = _match(TokenType.VAR) or _match(TokenType.STAR)
            expr_type = exp.JSONPathRecursive if recursive else exp.JSONPathChild
            nodes.append(expr_type(this=value.text if value else None))
        elif _match(TokenType.L_BRACKET):
            nodes.append(_parse_bracket())
        elif _match(TokenType.VAR):
            nodes.append(exp.JSONPathKey(this=_prev().text))
        elif _match(TokenType.STAR):
            nodes.append(exp.JSONPathWildcard())
        elif _match(TokenType.PARAMETER):
            nodes.append(exp.JSONPathCurrent())
        else:
            raise ParseError(_error(f"Unexpected {tokens[i].token_type}"))

    # We canonicalize the JSON path AST so that it always starts with a
    # "root" element, so paths like "field" will be generated as "$.field"
    if nodes and not isinstance(nodes[0], exp.JSONPathRoot):
        nodes.insert(0, exp.JSONPathRoot())

    return nodes


MAPPING = {
    exp.JSONPathChild: lambda n: f".{n.this}" if n.this is not None else "",
    exp.JSONPathCurrent: lambda _: "@",
    exp.JSONPathFilter: lambda n: f"?{n.this}",
    exp.JSONPathKey: lambda n: f".{n.this}"
    if exp.SAFE_IDENTIFIER_RE.match(n.this)
    else f"[{generate([n.this])}]",
    exp.JSONPathRecursive: lambda n: f"..{n.this}" if n.this is not None else "..",
    exp.JSONPathRoot: lambda _: "$",
    exp.JSONPathScript: lambda n: f"({n.this}",
    exp.JSONPathSlice: lambda n: ":".join(
        "" if p is False else generate([p])
        for p in [n.args.get("start"), n.args.get("end"), n.args.get("step")]
        if p is not None
    ),
    exp.JSONPathSelector: lambda n: f"[{generate([n.this])}]",
    exp.JSONPathSubscript: lambda n: f"[{generate([n.this])}]",
    exp.JSONPathUnion: lambda n: f"[{','.join(generate([p]) for p in n.expressions)}]",
    exp.JSONPathWildcard: lambda _: "*",
}


def generate(
    nodes: t.List[exp.JSONPathPart],
    mapping: t.Optional[
        t.Dict[t.Type[exp.JSONPathPart], t.Callable[[exp.JSONPathPart], str]]
    ] = None,
) -> str:
    unsupported_nodes: t.Set[str] = set()
    mapping = MAPPING if mapping is None else mapping

    path = []
    for node in nodes:
        if isinstance(node, exp.JSONPathPart):
            node_class = node.__class__
            generator = mapping.get(node_class)

            if generator:
                path.append(generator(node))
            else:
                unsupported_nodes.add(node_class.__name__)
        elif isinstance(node, str):
            escaped = node.replace('"', '\\"')
            path.append(f'"{escaped}"')
        else:
            path.append(str(node))

    if unsupported_nodes:
        logger.warning(f"Unsupported JSON path syntax: {', '.join(k for k in unsupported_nodes)}")

    return "".join(path)
