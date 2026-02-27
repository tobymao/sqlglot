"""sqlglot expressions - string, encoding, hashing, and regex functions."""

from __future__ import annotations

from sqlglot.expressions.core import Expression, Func, Binary


# String basics


class Ascii(Expression, Func):
    pass


class BitLength(Expression, Func):
    pass


class ByteLength(Expression, Func):
    pass


class Chr(Expression, Func):
    arg_types = {"expressions": True, "charset": False}
    is_var_len_args = True
    _sql_names = ["CHR", "CHAR"]


class Concat(Expression, Func):
    arg_types = {"expressions": True, "safe": False, "coalesce": False}
    is_var_len_args = True


class ConcatWs(Concat):
    _sql_names = ["CONCAT_WS"]


class Contains(Expression, Func):
    arg_types = {"this": True, "expression": True, "json_scope": False}


class Elt(Expression, Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class EndsWith(Expression, Func):
    _sql_names = ["ENDS_WITH", "ENDSWITH"]
    arg_types = {"this": True, "expression": True}


class Format(Expression, Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Initcap(Expression, Func):
    arg_types = {"this": True, "expression": False}


class IsAscii(Expression, Func):
    pass


class Left(Expression, Func):
    arg_types = {"this": True, "expression": True}


class Length(Expression, Func):
    arg_types = {"this": True, "binary": False, "encoding": False}
    _sql_names = ["LENGTH", "LEN", "CHAR_LENGTH", "CHARACTER_LENGTH"]


class Levenshtein(Expression, Func):
    arg_types = {
        "this": True,
        "expression": False,
        "ins_cost": False,
        "del_cost": False,
        "sub_cost": False,
        "max_dist": False,
    }


class Lower(Expression, Func):
    _sql_names = ["LOWER", "LCASE"]


class MatchAgainst(Expression, Func):
    arg_types = {"this": True, "expressions": True, "modifier": False}


class Normalize(Expression, Func):
    arg_types = {"this": True, "form": False, "is_casefold": False}


class NumberToStr(Expression, Func):
    arg_types = {"this": True, "format": True, "culture": False}


class Overlay(Expression, Func):
    arg_types = {"this": True, "expression": True, "from_": True, "for_": False}


class Pad(Expression, Func):
    arg_types = {"this": True, "expression": True, "fill_pattern": False, "is_left": True}


class Repeat(Expression, Func):
    arg_types = {"this": True, "times": True}


class Replace(Expression, Func):
    arg_types = {"this": True, "expression": True, "replacement": False}


class Reverse(Expression, Func):
    pass


class Right(Expression, Func):
    arg_types = {"this": True, "expression": True}


class RtrimmedLength(Expression, Func):
    pass


class Search(Expression, Func):
    arg_types = {
        "this": True,  # data_to_search / search_data
        "expression": True,  # search_query / search_string
        "json_scope": False,  # BigQuery: JSON_VALUES | JSON_KEYS | JSON_KEYS_AND_VALUES
        "analyzer": False,  # Both: analyzer / ANALYZER
        "analyzer_options": False,  # BigQuery: analyzer_options_values
        "search_mode": False,  # Snowflake: OR | AND
    }


class SearchIp(Expression, Func):
    arg_types = {"this": True, "expression": True}


class Soundex(Expression, Func):
    pass


class SoundexP123(Expression, Func):
    pass


class Space(Expression, Func):
    """
    SPACE(n) → string consisting of n blank characters
    """

    pass


class Split(Expression, Func):
    arg_types = {"this": True, "expression": True, "limit": False}


class SplitPart(Expression, Func):
    arg_types = {"this": True, "delimiter": False, "part_index": False}


class StartsWith(Expression, Func):
    _sql_names = ["STARTS_WITH", "STARTSWITH"]
    arg_types = {"this": True, "expression": True}


class StrPosition(Expression, Func):
    arg_types = {
        "this": True,
        "substr": True,
        "position": False,
        "occurrence": False,
    }


class StrToMap(Expression, Func):
    arg_types = {
        "this": True,
        "pair_delim": False,
        "key_value_delim": False,
        "duplicate_resolution_callback": False,
    }


class String(Expression, Func):
    arg_types = {"this": True, "zone": False}


class Stuff(Expression, Func):
    _sql_names = ["STUFF", "INSERT"]
    arg_types = {"this": True, "start": True, "length": True, "expression": True}


class Substring(Expression, Func):
    _sql_names = ["SUBSTRING", "SUBSTR"]
    arg_types = {"this": True, "start": False, "length": False}


class SubstringIndex(Expression, Func):
    """
    SUBSTRING_INDEX(str, delim, count)

    *count* > 0  → left slice before the *count*-th delimiter
    *count* < 0  → right slice after the |count|-th delimiter
    """

    arg_types = {"this": True, "delimiter": True, "count": True}


class Translate(Expression, Func):
    arg_types = {"this": True, "from_": True, "to": True}


class Trim(Expression, Func):
    arg_types = {
        "this": True,
        "expression": False,
        "position": False,
        "collation": False,
    }


class Unicode(Expression, Func):
    pass


class Upper(Expression, Func):
    _sql_names = ["UPPER", "UCASE"]


# Encoding / base conversion


class Base64DecodeBinary(Expression, Func):
    arg_types = {"this": True, "alphabet": False}


class Base64DecodeString(Expression, Func):
    arg_types = {"this": True, "alphabet": False}


class Base64Encode(Expression, Func):
    arg_types = {"this": True, "max_line_length": False, "alphabet": False}


class CodePointsToBytes(Expression, Func):
    pass


class CodePointsToString(Expression, Func):
    pass


class ConvertToCharset(Expression, Func):
    arg_types = {"this": True, "dest": True, "source": False}


class Decode(Expression, Func):
    arg_types = {"this": True, "charset": True, "replace": False}


class Encode(Expression, Func):
    arg_types = {"this": True, "charset": True}


class FromBase(Expression, Func):
    arg_types = {"this": True, "expression": True}


class FromBase32(Expression, Func):
    pass


class FromBase64(Expression, Func):
    pass


class Hex(Expression, Func):
    pass


class HexDecodeString(Expression, Func):
    pass


class HexEncode(Expression, Func):
    arg_types = {"this": True, "case": False}


class LowerHex(Hex):
    pass


class SafeConvertBytesToString(Expression, Func):
    pass


class ToBase32(Expression, Func):
    pass


class ToBase64(Expression, Func):
    pass


class ToBinary(Expression, Func):
    arg_types = {"this": True, "format": False, "safe": False}


class ToChar(Expression, Func):
    arg_types = {
        "this": True,
        "format": False,
        "nlsparam": False,
        "is_numeric": False,
    }


class ToCodePoints(Expression, Func):
    pass


class ToDecfloat(Expression, Func):
    arg_types = {
        "this": True,
        "format": False,
    }


class ToDouble(Expression, Func):
    arg_types = {
        "this": True,
        "format": False,
        "safe": False,
    }


class ToFile(Expression, Func):
    arg_types = {
        "this": True,
        "path": False,
        "safe": False,
    }


class ToNumber(Expression, Func):
    arg_types = {
        "this": True,
        "format": False,
        "nlsparam": False,
        "precision": False,
        "scale": False,
        "safe": False,
        "safe_name": False,
    }


class TryBase64DecodeBinary(Expression, Func):
    arg_types = {"this": True, "alphabet": False}


class TryBase64DecodeString(Expression, Func):
    arg_types = {"this": True, "alphabet": False}


class TryHexDecodeBinary(Expression, Func):
    pass


class TryHexDecodeString(Expression, Func):
    pass


class TryToDecfloat(Expression, Func):
    arg_types = {
        "this": True,
        "format": False,
    }


class Unhex(Expression, Func):
    arg_types = {"this": True, "expression": False}


# Regex


class RegexpCount(Expression, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "parameters": False,
    }


class RegexpExtract(Expression, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "occurrence": False,
        "parameters": False,
        "group": False,
        "null_if_pos_overflow": False,  # for transpilation target behavior
    }


class RegexpExtractAll(Expression, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "group": False,
        "parameters": False,
        "position": False,
        "occurrence": False,
    }


class RegexpFullMatch(Expression, Binary, Func):
    arg_types = {"this": True, "expression": True, "options": False}


class RegexpILike(Expression, Binary, Func):
    arg_types = {"this": True, "expression": True, "flag": False}


class RegexpInstr(Expression, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "occurrence": False,
        "option": False,
        "parameters": False,
        "group": False,
    }


class RegexpReplace(Expression, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "replacement": False,
        "position": False,
        "occurrence": False,
        "modifiers": False,
        "single_replace": False,
    }


class RegexpSplit(Expression, Func):
    arg_types = {"this": True, "expression": True, "limit": False}


# Hashing / cryptographic


class Compress(Expression, Func):
    arg_types = {"this": True, "method": False}


class Decrypt(Expression, Func):
    arg_types = {
        "this": True,
        "passphrase": True,
        "aad": False,
        "encryption_method": False,
        "safe": False,
    }


class DecryptRaw(Expression, Func):
    arg_types = {
        "this": True,
        "key": True,
        "iv": True,
        "aad": False,
        "encryption_method": False,
        "aead": False,
        "safe": False,
    }


class DecompressBinary(Expression, Func):
    arg_types = {"this": True, "method": True}


class DecompressString(Expression, Func):
    arg_types = {"this": True, "method": True}


class Encrypt(Expression, Func):
    arg_types = {"this": True, "passphrase": True, "aad": False, "encryption_method": False}


class EncryptRaw(Expression, Func):
    arg_types = {"this": True, "key": True, "iv": True, "aad": False, "encryption_method": False}


class FarmFingerprint(Expression, Func):
    arg_types = {"expressions": True}
    is_var_len_args = True
    _sql_names = ["FARM_FINGERPRINT", "FARMFINGERPRINT64"]


class MD5(Expression, Func):
    _sql_names = ["MD5"]


class MD5Digest(Expression, Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True
    _sql_names = ["MD5_DIGEST"]


class MD5NumberLower64(Expression, Func):
    pass


class MD5NumberUpper64(Expression, Func):
    pass


class SHA(Expression, Func):
    _sql_names = ["SHA", "SHA1"]


class SHA1Digest(Expression, Func):
    pass


class SHA2(Expression, Func):
    _sql_names = ["SHA2"]
    arg_types = {"this": True, "length": False}


class SHA2Digest(Expression, Func):
    arg_types = {"this": True, "length": False}


class StandardHash(Expression, Func):
    arg_types = {"this": True, "expression": False}


# Parse


class ParseBignumeric(Expression, Func):
    pass


class ParseNumeric(Expression, Func):
    pass


class ParseUrl(Expression, Func):
    arg_types = {"this": True, "part_to_extract": False, "key": False, "permissive": False}
