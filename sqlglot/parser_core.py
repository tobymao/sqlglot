from __future__ import annotations

import typing as t

from sqlglot.errors import ErrorLevel, ParseError, highlight_sql
from sqlglot.tokenizer_core import Token, TokenType


class ParserCore:
    __slots__ = (
        "error_level",
        "error_message_context",
        "max_errors",
        "dialect",
        "sql",
        "errors",
        "_tokens",
        "_index",
        "_curr",
        "_next",
        "_prev",
        "_prev_comments",
        "_pipe_cte_counter",
        "_chunks",
        "_chunk_index",
    )

    def __init__(
        self,
        error_level: ErrorLevel,
        error_message_context: int,
        max_errors: int,
        dialect: t.Any,
    ) -> None:
        self.error_level: ErrorLevel = error_level
        self.error_message_context = error_message_context
        self.max_errors = max_errors
        self.dialect: t.Any = dialect
        self.reset()

    def reset(self) -> None:
        self.sql: str = ""
        self.errors: t.List[ParseError] = []
        self._tokens: t.List[Token] = []
        self._index: int = 0
        self._curr: t.Optional[Token] = None
        self._next: t.Optional[Token] = None
        self._prev: t.Optional[Token] = None
        self._prev_comments: t.Optional[t.List[str]] = None
        self._pipe_cte_counter: int = 0
        self._chunks: t.List[t.List[Token]] = []
        self._chunk_index: int = 0

    def _advance(self, times: int = 1) -> None:
        index = self._index + times
        self._index = index
        tokens = self._tokens
        size = len(tokens)
        self._curr = tokens[index] if index < size else None
        self._next = tokens[index + 1] if index + 1 < size else None

        if index > 0:
            prev = tokens[index - 1]
            self._prev = prev
            self._prev_comments = prev.comments
        else:
            self._prev = None
            self._prev_comments = None

    def _advance_chunk(self) -> None:
        self._index = -1
        self._tokens = self._chunks[self._chunk_index]
        self._chunk_index += 1
        self._advance()

    def _retreat(self, index: int) -> None:
        if index != self._index:
            self._advance(index - self._index)

    def _add_comments(self, expression: t.Any) -> None:
        if expression and self._prev_comments:
            expression.add_comments(self._prev_comments)
            self._prev_comments = None

    def _match(self, token_type: TokenType, advance: bool = True, expression: t.Any = None) -> bool:
        curr = self._curr
        if curr and curr.token_type == token_type:
            if advance:
                self._advance()
            self._add_comments(expression)
            return True
        return False

    def _match_set(self, types: t.Any, advance: bool = True) -> bool:
        curr = self._curr
        if curr and curr.token_type in types:
            if advance:
                self._advance()
            return True
        return False

    def _match_pair(
        self, token_type_a: TokenType, token_type_b: TokenType, advance: bool = True
    ) -> bool:
        curr = self._curr
        next_ = self._next
        if curr and next_ and curr.token_type == token_type_a and next_.token_type == token_type_b:
            if advance:
                self._advance(2)
            return True
        return False

    def _match_texts(self, texts: t.Any, advance: bool = True) -> bool:
        curr = self._curr
        if curr and curr.token_type != TokenType.STRING and curr.text.upper() in texts:
            if advance:
                self._advance()
            return True
        return False

    def _match_text_seq(self, *texts: str, advance: bool = True) -> bool:
        index = self._index
        string_type = TokenType.STRING
        for text in texts:
            curr = self._curr
            if curr and curr.token_type != string_type and curr.text.upper() == text:
                self._advance()
            else:
                self._retreat(index)
                return False

        if not advance:
            self._retreat(index)

        return True

    def _is_connected(self) -> bool:
        prev = self._prev
        curr = self._curr
        return bool(prev and curr and prev.end + 1 == curr.start)

    def _find_sql(self, start: Token, end: Token) -> str:
        return self.sql[start.start : end.end + 1]

    def raise_error(self, message: str, token: t.Optional[Token] = None) -> None:
        token = token or self._curr or self._prev or Token.string("")
        formatted_sql, start_context, highlight, end_context = highlight_sql(
            sql=self.sql,
            positions=[(token.start, token.end)],
            context_length=self.error_message_context,
        )
        formatted_message = f"{message}. Line {token.line}, Col: {token.col}.\n  {formatted_sql}"

        error = ParseError.new(
            formatted_message,
            description=message,
            line=token.line,
            col=token.col,
            start_context=start_context,
            highlight=highlight,
            end_context=end_context,
        )

        if self.error_level == ErrorLevel.IMMEDIATE:
            raise error

        self.errors.append(error)

    def validate_expression(self, expression: t.Any, args: t.Optional[t.List] = None) -> t.Any:
        if self.error_level != ErrorLevel.IGNORE:
            for error_message in expression.error_messages(args):
                self.raise_error(error_message)
        return expression

    def _try_parse(self, parse_method: t.Callable, retreat: bool = False) -> t.Optional[t.Any]:
        index = self._index
        error_level = self.error_level
        this: t.Optional[t.Any] = None

        self.error_level = ErrorLevel.IMMEDIATE
        try:
            this = parse_method()
        except ParseError:
            this = None
        finally:
            if not this or retreat:
                self._retreat(index)
            self.error_level = error_level

        return this
