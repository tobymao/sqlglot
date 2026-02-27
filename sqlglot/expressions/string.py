"""sqlglot expressions - string, encoding, hashing, and regex functions."""

from __future__ import annotations

from sqlglot.expressions.core import ExpressionBase, Func, Binary


# String basics


class Ascii(ExpressionBase, Func):
    pass


class BitLength(ExpressionBase, Func):
    pass


class ByteLength(ExpressionBase, Func):
    pass


class Chr(ExpressionBase, Func):
    arg_types = {"expressions": True, "charset": False}
    is_var_len_args = True
    _sql_names = ["CHR", "CHAR"]


class Concat(ExpressionBase, Func):
    arg_types = {"expressions": True, "safe": False, "coalesce": False}
    is_var_len_args = True


class ConcatWs(Concat):
    _sql_names = ["CONCAT_WS"]


class Contains(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "json_scope": False}


class Elt(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class EndsWith(ExpressionBase, Func):
    _sql_names = ["ENDS_WITH", "ENDSWITH"]
    arg_types = {"this": True, "expression": True}


class Format(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Initcap(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


class IsAscii(ExpressionBase, Func):
    pass


class Left(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class Length(ExpressionBase, Func):
    arg_types = {"this": True, "binary": False, "encoding": False}
    _sql_names = ["LENGTH", "LEN", "CHAR_LENGTH", "CHARACTER_LENGTH"]


class Levenshtein(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "expression": False,
        "ins_cost": False,
        "del_cost": False,
        "sub_cost": False,
        "max_dist": False,
    }


class Lower(ExpressionBase, Func):
    _sql_names = ["LOWER", "LCASE"]


class MatchAgainst(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": True, "modifier": False}


class Normalize(ExpressionBase, Func):
    arg_types = {"this": True, "form": False, "is_casefold": False}


class NumberToStr(ExpressionBase, Func):
    arg_types = {"this": True, "format": True, "culture": False}


class Overlay(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "from_": True, "for_": False}


class Pad(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "fill_pattern": False, "is_left": True}


class Repeat(ExpressionBase, Func):
    arg_types = {"this": True, "times": True}


class Replace(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "replacement": False}


class Reverse(ExpressionBase, Func):
    pass


class Right(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class RtrimmedLength(ExpressionBase, Func):
    pass


class Search(ExpressionBase, Func):
    arg_types = {
        "this": True,  # data_to_search / search_data
        "expression": True,  # search_query / search_string
        "json_scope": False,  # BigQuery: JSON_VALUES | JSON_KEYS | JSON_KEYS_AND_VALUES
        "analyzer": False,  # Both: analyzer / ANALYZER
        "analyzer_options": False,  # BigQuery: analyzer_options_values
        "search_mode": False,  # Snowflake: OR | AND
    }


class SearchIp(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class Soundex(ExpressionBase, Func):
    pass


class SoundexP123(ExpressionBase, Func):
    pass


class Space(ExpressionBase, Func):
    """
    SPACE(n) → string consisting of n blank characters
    """

    pass


class Split(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "limit": False}


class SplitPart(ExpressionBase, Func):
    arg_types = {"this": True, "delimiter": False, "part_index": False}


class StartsWith(ExpressionBase, Func):
    _sql_names = ["STARTS_WITH", "STARTSWITH"]
    arg_types = {"this": True, "expression": True}


class StrPosition(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "substr": True,
        "position": False,
        "occurrence": False,
    }


class StrToMap(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "pair_delim": False,
        "key_value_delim": False,
        "duplicate_resolution_callback": False,
    }


class String(ExpressionBase, Func):
    arg_types = {"this": True, "zone": False}


class Stuff(ExpressionBase, Func):
    _sql_names = ["STUFF", "INSERT"]
    arg_types = {"this": True, "start": True, "length": True, "expression": True}


class Substring(ExpressionBase, Func):
    _sql_names = ["SUBSTRING", "SUBSTR"]
    arg_types = {"this": True, "start": False, "length": False}


class SubstringIndex(ExpressionBase, Func):
    """
    SUBSTRING_INDEX(str, delim, count)

    *count* > 0  → left slice before the *count*-th delimiter
    *count* < 0  → right slice after the |count|-th delimiter
    """

    arg_types = {"this": True, "delimiter": True, "count": True}


class Translate(ExpressionBase, Func):
    arg_types = {"this": True, "from_": True, "to": True}


class Trim(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "expression": False,
        "position": False,
        "collation": False,
    }


class Unicode(ExpressionBase, Func):
    pass


class Upper(ExpressionBase, Func):
    _sql_names = ["UPPER", "UCASE"]


# Encoding / base conversion


class Base64DecodeBinary(ExpressionBase, Func):
    arg_types = {"this": True, "alphabet": False}


class Base64DecodeString(ExpressionBase, Func):
    arg_types = {"this": True, "alphabet": False}


class Base64Encode(ExpressionBase, Func):
    arg_types = {"this": True, "max_line_length": False, "alphabet": False}


class CodePointsToBytes(ExpressionBase, Func):
    pass


class CodePointsToString(ExpressionBase, Func):
    pass


class ConvertToCharset(ExpressionBase, Func):
    arg_types = {"this": True, "dest": True, "source": False}


class Decode(ExpressionBase, Func):
    arg_types = {"this": True, "charset": True, "replace": False}


class Encode(ExpressionBase, Func):
    arg_types = {"this": True, "charset": True}


class FromBase(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class FromBase32(ExpressionBase, Func):
    pass


class FromBase64(ExpressionBase, Func):
    pass


class Hex(ExpressionBase, Func):
    pass


class HexDecodeString(ExpressionBase, Func):
    pass


class HexEncode(ExpressionBase, Func):
    arg_types = {"this": True, "case": False}


class LowerHex(Hex):
    pass


class SafeConvertBytesToString(ExpressionBase, Func):
    pass


class ToBase32(ExpressionBase, Func):
    pass


class ToBase64(ExpressionBase, Func):
    pass


class ToBinary(ExpressionBase, Func):
    arg_types = {"this": True, "format": False, "safe": False}


class ToChar(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "format": False,
        "nlsparam": False,
        "is_numeric": False,
    }


class ToCodePoints(ExpressionBase, Func):
    pass


class ToDecfloat(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "format": False,
    }


class ToDouble(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "format": False,
        "safe": False,
    }


class ToFile(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "path": False,
        "safe": False,
    }


class ToNumber(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "format": False,
        "nlsparam": False,
        "precision": False,
        "scale": False,
        "safe": False,
        "safe_name": False,
    }


class TryBase64DecodeBinary(ExpressionBase, Func):
    arg_types = {"this": True, "alphabet": False}


class TryBase64DecodeString(ExpressionBase, Func):
    arg_types = {"this": True, "alphabet": False}


class TryHexDecodeBinary(ExpressionBase, Func):
    pass


class TryHexDecodeString(ExpressionBase, Func):
    pass


class TryToDecfloat(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "format": False,
    }


class Unhex(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


# Regex


class RegexpCount(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "parameters": False,
    }


class RegexpExtract(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "occurrence": False,
        "parameters": False,
        "group": False,
        "null_if_pos_overflow": False,  # for transpilation target behavior
    }


class RegexpExtractAll(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "group": False,
        "parameters": False,
        "position": False,
        "occurrence": False,
    }


class RegexpFullMatch(ExpressionBase, Binary, Func):
    arg_types = {"this": True, "expression": True, "options": False}


class RegexpILike(ExpressionBase, Binary, Func):
    arg_types = {"this": True, "expression": True, "flag": False}


class RegexpInstr(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "occurrence": False,
        "option": False,
        "parameters": False,
        "group": False,
    }


class RegexpReplace(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "replacement": False,
        "position": False,
        "occurrence": False,
        "modifiers": False,
        "single_replace": False,
    }


class RegexpSplit(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "limit": False}


# Hashing / cryptographic


class Compress(ExpressionBase, Func):
    arg_types = {"this": True, "method": False}


class Decrypt(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "passphrase": True,
        "aad": False,
        "encryption_method": False,
        "safe": False,
    }


class DecryptRaw(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "key": True,
        "iv": True,
        "aad": False,
        "encryption_method": False,
        "aead": False,
        "safe": False,
    }


class DecompressBinary(ExpressionBase, Func):
    arg_types = {"this": True, "method": True}


class DecompressString(ExpressionBase, Func):
    arg_types = {"this": True, "method": True}


class Encrypt(ExpressionBase, Func):
    arg_types = {"this": True, "passphrase": True, "aad": False, "encryption_method": False}


class EncryptRaw(ExpressionBase, Func):
    arg_types = {"this": True, "key": True, "iv": True, "aad": False, "encryption_method": False}


class FarmFingerprint(ExpressionBase, Func):
    arg_types = {"expressions": True}
    is_var_len_args = True
    _sql_names = ["FARM_FINGERPRINT", "FARMFINGERPRINT64"]


class MD5(ExpressionBase, Func):
    _sql_names = ["MD5"]


class MD5Digest(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True
    _sql_names = ["MD5_DIGEST"]


class MD5NumberLower64(ExpressionBase, Func):
    pass


class MD5NumberUpper64(ExpressionBase, Func):
    pass


class SHA(ExpressionBase, Func):
    _sql_names = ["SHA", "SHA1"]


class SHA1Digest(ExpressionBase, Func):
    pass


class SHA2(ExpressionBase, Func):
    _sql_names = ["SHA2"]
    arg_types = {"this": True, "length": False}


class SHA2Digest(ExpressionBase, Func):
    arg_types = {"this": True, "length": False}


class StandardHash(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


# Parse


class ParseBignumeric(ExpressionBase, Func):
    pass


class ParseNumeric(ExpressionBase, Func):
    pass


class ParseUrl(ExpressionBase, Func):
    arg_types = {"this": True, "part_to_extract": False, "key": False, "permissive": False}
