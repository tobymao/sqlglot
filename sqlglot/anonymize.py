from __future__ import annotations

import string
import typing as t

from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.errors import TokenError
from sqlglot.tokens import Token, TokenType


ALPHABET = string.ascii_lowercase
ALPHABET_SIZE = len(ALPHABET)
ANONYMIZED_TYPES = {
    TokenType.BIT_STRING,
    TokenType.BYTE_STRING,
    TokenType.HEX_STRING,
    TokenType.HEREDOC_STRING,
    TokenType.IDENTIFIER,
    TokenType.NATIONAL_STRING,
    TokenType.NUMBER,
    TokenType.RAW_STRING,
    TokenType.STRING,
    TokenType.UNICODE_STRING,
    TokenType.VAR,
}


def anonymize(
    sql_or_tokens: list[Token] | str,
    dialect: DialectType = None,
) -> list[Token]:
    """Replaces sensitive tokens (identifiers, strings, numbers) with fixed-width,
    length-preserving, consistent aliases, and blanks out comments. When a SQL string is
    given, it is tokenized with `dialect` first; any un-tokenized remainder (e.g. an
    unterminated literal) is appended as a blanked UNKNOWN token. Mutates and returns
    `sql_or_tokens`.

    Args:
        sql_or_tokens: The SQL string to anonymize, or its token list.
        dialect: The dialect used to tokenize a SQL string.
    """
    dialect = Dialect.get_or_raise(dialect)
    known_functions = dialect.parser_class.FUNCTIONS

    errored = False
    if isinstance(sql_or_tokens, str):
        sql = sql_or_tokens
        tokenizer = dialect.tokenizer()
        try:
            tokens = tokenizer.tokenize(sql)
        except TokenError:
            tokens = tokenizer.tokens
            errored = True
    else:
        tokens = sql_or_tokens
        sql = None

    seen: dict[tuple[bool, str], str] = {}
    counter = 0

    for i, token in enumerate(tokens):
        token.comments = [
            "".join(char if char.isspace() else "." for char in comment)
            for comment in token.comments
        ]

        if (
            token.token_type == TokenType.VAR
            and i + 1 < len(tokens)
            and tokens[i + 1].token_type == TokenType.L_PAREN
            and token.text.upper() in known_functions
        ):
            continue
        if token.token_type not in ANONYMIZED_TYPES:
            continue
        if not token.text:
            continue

        is_number = token.token_type == TokenType.NUMBER
        key = (is_number, token.text)
        alias = seen.get(key)
        if alias is None:
            seen[key] = (
                _number_alias(counter, token.text) if is_number else _alias(counter, token.text)
            )
            counter += 1
        token.text = seen[key]

    if sql is not None and errored and tokens:
        start = tokens[-1].end + 1
        length = len(sql)
        while start < length and sql[start].isspace():
            start += 1
        if start < length:
            tokens.append(
                Token(
                    TokenType.UNKNOWN,
                    "." * (length - start),
                    start=start,
                    end=length - 1,
                    line=sql.count("\n", 0, start) + 1,
                    col=start - sql.rfind("\n", 0, start),
                )
            )

    return tokens


def render(sql: str, tokens: list[Token], dialect: DialectType = None) -> str:
    """Recreates the (anonymized) SQL string from the original `sql` and token positions,
    inserting anonymized comments in the gaps between tokens.

    Args:
        sql: The original SQL string.
        tokens: The anonymized tokens to render.
        dialect: The dialect used to identify comments in `sql`.
    """
    result = []
    prev = 0
    comments = (comment for token in tokens for comment in token.comments)
    delimiters = [
        (start, end or "")
        for start, end in sorted(
            Dialect.get_or_raise(dialect).tokenizer_class._COMMENTS.items(),
            key=lambda item: len(item[0]),
            reverse=True,
        )
    ]
    for token in tokens:
        result.append(_replace_comments(sql[prev : token.start], comments, delimiters))
        result.append(token.text)
        prev = token.end + 1
    result.append(_replace_comments(sql[prev:], comments, delimiters))
    return "".join(result)


def _alias(counter: int, text: str) -> str:
    remaining = counter
    letters = ""
    while remaining:
        remaining, digit = divmod(remaining, ALPHABET_SIZE)
        letters = ALPHABET[digit] + letters

    letters = letters.rjust(sum(not char.isspace() for char in text), "a")

    alias = ""
    i = 0
    for char in text:
        if char.isspace():
            alias += char
        else:
            alias += letters[i]
            i += 1

    return alias


def _number_alias(counter: int, text: str) -> str:
    if len(text) > 4000:
        digits_seen = False
        result = ""
        for char in text:
            if char.isdigit():
                result += "1" if not digits_seen else "0"
                digits_seen = True
            else:
                result += char
        return result

    sep = "e" if "e" in text else ("E" if "E" in text else "")
    mantissa, _, exponent = text.partition(sep) if sep else (text, "", "")
    sign = ""
    if exponent.startswith(("-", "+")):
        sign, exponent = exponent[0], exponent[1:]
    exponent_length = len(exponent)

    integer, dot, fraction = mantissa.partition(".")
    integer_length = len(integer)
    digits = integer_length + len(fraction)
    mantissa_value = 10 ** (digits - 1) + counter % (9 * 10 ** (digits - 1))
    result = str(mantissa_value)
    if dot:
        result = result[:integer_length] + "." + result[integer_length:]
    if sep:
        if exponent_length:
            exponent_value = 10 ** (exponent_length - 1) + (counter // (9 * 10 ** (digits - 1))) % (
                9 * 10 ** (exponent_length - 1)
            )
            result += sep + sign + str(exponent_value)

    return result


def _replace_comments(
    sql: str, comments: t.Iterator[str], delimiters: list[tuple[str, str]]
) -> str:
    out: list[str] = []
    i = 0
    while i < len(sql):
        for start, end in delimiters:
            if sql.startswith(start, i):
                comment = next(comments)
                out.extend((start, comment, end))
                i += len(start) + len(comment) + len(end)
                break
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)
