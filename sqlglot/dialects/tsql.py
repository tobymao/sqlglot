from __future__ import annotations

from sqlglot import tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
)
from sqlglot.generators.tsql import TSQLGenerator
from sqlglot.parsers.tsql import TSQLParser
from sqlglot.tokens import TokenType
from sqlglot.typing.tsql import EXPRESSION_METADATA


class TSQL(Dialect):
    LOG_BASE_FIRST = False
    TYPED_DIVISION = True
    CONCAT_COALESCE = True
    CONCAT_WS_COALESCE = True
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = False

    TIME_FORMAT = "'yyyy-mm-dd hh:mm:ss'"

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    DATE_PART_MAPPING = {
        **Dialect.DATE_PART_MAPPING,
        "QQ": "QUARTER",
        "M": "MONTH",
        "Y": "DAYOFYEAR",
        "WW": "WEEK",
        "N": "MINUTE",
        "SS": "SECOND",
        "MCS": "MICROSECOND",
        "TZOFFSET": "TIMEZONE_MINUTE",
        "TZ": "TIMEZONE_MINUTE",
        "ISO_WEEK": "WEEKISO",
        "ISOWK": "WEEKISO",
        "ISOWW": "WEEKISO",
    }

    TIME_MAPPING = {
        "year": "%Y",
        "dayofyear": "%j",
        "day": "%d",
        "dy": "%d",
        "y": "%Y",
        "week": "%W",
        "ww": "%W",
        "wk": "%W",
        "isowk": "%V",
        "isoww": "%V",
        "iso_week": "%V",
        "hour": "%h",
        "hh": "%I",
        "minute": "%M",
        "mi": "%M",
        "n": "%M",
        "second": "%S",
        "ss": "%S",
        "s": "%-S",
        "millisecond": "%f",
        "ms": "%f",
        "weekday": "%w",
        "dw": "%w",
        "month": "%m",
        "mm": "%M",
        "m": "%-M",
        "Y": "%Y",
        "YYYY": "%Y",
        "YY": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "M": "%-m",
        "dddd": "%A",
        "dd": "%d",
        "d": "%-d",
        "HH": "%H",
        "H": "%-H",
        "h": "%-I",
        "ffffff": "%f",
        "yyyy": "%Y",
        "yy": "%y",
    }

    CONVERT_FORMAT_MAPPING = {
        "0": "%b %d %Y %-I:%M%p",
        "1": "%m/%d/%y",
        "2": "%y.%m.%d",
        "3": "%d/%m/%y",
        "4": "%d.%m.%y",
        "5": "%d-%m-%y",
        "6": "%d %b %y",
        "7": "%b %d, %y",
        "8": "%H:%M:%S",
        "9": "%b %d %Y %-I:%M:%S:%f%p",
        "10": "mm-dd-yy",
        "11": "yy/mm/dd",
        "12": "yymmdd",
        "13": "%d %b %Y %H:%M:ss:%f",
        "14": "%H:%M:%S:%f",
        "20": "%Y-%m-%d %H:%M:%S",
        "21": "%Y-%m-%d %H:%M:%S.%f",
        "22": "%m/%d/%y %-I:%M:%S %p",
        "23": "%Y-%m-%d",
        "24": "%H:%M:%S",
        "25": "%Y-%m-%d %H:%M:%S.%f",
        "100": "%b %d %Y %-I:%M%p",
        "101": "%m/%d/%Y",
        "102": "%Y.%m.%d",
        "103": "%d/%m/%Y",
        "104": "%d.%m.%Y",
        "105": "%d-%m-%Y",
        "106": "%d %b %Y",
        "107": "%b %d, %Y",
        "108": "%H:%M:%S",
        "109": "%b %d %Y %-I:%M:%S:%f%p",
        "110": "%m-%d-%Y",
        "111": "%Y/%m/%d",
        "112": "%Y%m%d",
        "113": "%d %b %Y %H:%M:%S:%f",
        "114": "%H:%M:%S:%f",
        "120": "%Y-%m-%d %H:%M:%S",
        "121": "%Y-%m-%d %H:%M:%S.%f",
        "126": "%Y-%m-%dT%H:%M:%S.%f",
    }

    FORMAT_TIME_MAPPING = {
        "y": "%B %Y",
        "d": "%m/%d/%Y",
        "H": "%-H",
        "h": "%-I",
        "s": "%Y-%m-%d %H:%M:%S",
        "D": "%A,%B,%Y",
        "f": "%A,%B,%Y %-I:%M %p",
        "F": "%A,%B,%Y %-I:%M:%S %p",
        "g": "%m/%d/%Y %-I:%M %p",
        "G": "%m/%d/%Y %-I:%M:%S %p",
        "M": "%B %-d",
        "m": "%B %-d",
        "O": "%Y-%m-%dT%H:%M:%S",
        "u": "%Y-%M-%D %H:%M:%S%z",
        "U": "%A, %B %D, %Y %H:%M:%S%z",
        "T": "%-I:%M:%S %p",
        "t": "%-I:%M",
        "Y": "%a %Y",
    }

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = [("[", "]"), '"']
        QUOTES = ["'", '"']
        HEX_STRINGS = [("0x", ""), ("0X", "")]
        VAR_SINGLE_TOKENS = {"@", "$", "#"}

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "CLUSTERED INDEX": TokenType.INDEX,
            "DATETIME2": TokenType.DATETIME2,
            "DATETIMEOFFSET": TokenType.TIMESTAMPTZ,
            "DECLARE": TokenType.DECLARE,
            "EXEC": TokenType.EXECUTE,
            "FOR SYSTEM_TIME": TokenType.TIMESTAMP_SNAPSHOT,
            "GO": TokenType.COMMAND,
            "IMAGE": TokenType.IMAGE,
            "MONEY": TokenType.MONEY,
            "NONCLUSTERED INDEX": TokenType.INDEX,
            "NTEXT": TokenType.TEXT,
            "OPTION": TokenType.OPTION,
            "OUTPUT": TokenType.RETURNING,
            "PRINT": TokenType.COMMAND,
            "PROC": TokenType.PROCEDURE,
            "REAL": TokenType.FLOAT,
            "ROWVERSION": TokenType.ROWVERSION,
            "SMALLDATETIME": TokenType.SMALLDATETIME,
            "SMALLMONEY": TokenType.SMALLMONEY,
            "SQL_VARIANT": TokenType.VARIANT,
            "SYSTEM_USER": TokenType.CURRENT_USER,
            "TOP": TokenType.TOP,
            "TIMESTAMP": TokenType.ROWVERSION,
            "TINYINT": TokenType.UTINYINT,
            "UNIQUEIDENTIFIER": TokenType.UUID,
            "UPDATE STATISTICS": TokenType.COMMAND,
            "XML": TokenType.XML,
        }
        KEYWORDS.pop("/*+")

        COMMANDS = {*tokens.Tokenizer.COMMANDS, TokenType.END} - {TokenType.EXECUTE}

    Parser = TSQLParser

    Generator = TSQLGenerator
