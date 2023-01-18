from __future__ import annotations

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType


class Teradata(Dialect):
    class Tokenizer(tokens.Tokenizer):
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,  # type: ignore
            "GRAPHIC": TokenType.CHARSET_TRANSLATOR,
            "KANJI1": TokenType.CHARSET_TRANSLATOR,
            "KANJISJIS": TokenType.CHARSET_TRANSLATOR,
            "LATIN": TokenType.CHARSET_TRANSLATOR,
            "UNICODE": TokenType.CHARSET_TRANSLATOR,
            "GRAPHIC_TO_KANJISJIS": TokenType.CHARSET_TRANSLATOR,
            "GRAPHIC_TO_LATIN": TokenType.CHARSET_TRANSLATOR,
            "GRAPHIC_TO_UNICODE": TokenType.CHARSET_TRANSLATOR,
            "GRAPHIC_TO_UNICODE_PadSpace": TokenType.CHARSET_TRANSLATOR,
            "KANJI1_KanjiEBCDIC_TO_UNICODE": TokenType.CHARSET_TRANSLATOR,
            "KANJI1_KanjiEUC_TO_UNICODE": TokenType.CHARSET_TRANSLATOR,
            "KANJI1_KANJISJIS_TO_UNICODE": TokenType.CHARSET_TRANSLATOR,
            "KANJI1_SBC_TO_UNICODE": TokenType.CHARSET_TRANSLATOR,
            "KANJISJIS_TO_GRAPHIC": TokenType.CHARSET_TRANSLATOR,
            "KANJISJIS_TO_LATIN": TokenType.CHARSET_TRANSLATOR,
            "KANJISJIS_TO_UNICODE": TokenType.CHARSET_TRANSLATOR,
            "LATIN_TO_GRAPHIC": TokenType.CHARSET_TRANSLATOR,
            "LATIN_TO_KANJISJIS": TokenType.CHARSET_TRANSLATOR,
            "LATIN_TO_UNICODE": TokenType.CHARSET_TRANSLATOR,
            "LOCALE_TO_UNICODE": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_GRAPHIC": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_GRAPHIC_PadGraphic": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_GRAPHIC_VarGraphic": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_KANJI1_KanjiEBCDIC": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_KANJI1_KanjiEUC": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_KANJI1_KANJISJIS": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_KANJI1_SBC": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_KANJISJIS": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_LATIN": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_LOCALE": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_UNICODE_FoldSpace": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_UNICODE_Fullwidth": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_UNICODE_Halfwidth": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_UNICODE_NFC": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_UNICODE_NFD": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_UNICODE_NFKC": TokenType.CHARSET_TRANSLATOR,
            "UNICODE_TO_UNICODE_NFKD": TokenType.CHARSET_TRANSLATOR,
        }

    class Parser(parser.Parser):
        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,  # type: ignore
            "TRANSLATE": lambda self: self._parse_translate(),
        }

        def _parse_translate(self) -> exp.Expression:
            this = self._parse_conjunction()

            if not self._match(TokenType.USING):
                self.raise_error("Expected USING in TRANSLATE")

            if self._match(TokenType.CHARSET_TRANSLATOR):
                charset_split = self._prev.text.split("_TO_")
                charset_from = self.expression(exp.CharacterSet, this=charset_split[0])
                charset_to = self.expression(exp.CharacterSet, this=charset_split[1])
            else:
                self.raise_error("Expected a character set translator after USING in TRANSLATE")

            return self.expression(
                exp.TranslateCharacterSet,
                **{  # type: ignore
                    "this": this,
                    "from": charset_from,
                    "to": charset_to,
                },
            )

    class Generator(generator.Generator):
        def translate_sql(self, expression: exp.TranslateCharacterSet):
            return f"TRANSLATE({self.sql(expression, 'this')} USING {self.sql(expression, 'from')}_TO_{self.sql(expression, 'to')}"
