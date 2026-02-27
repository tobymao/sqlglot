"""sqlglot expressions - string, encoding, hashing, and regex functions."""

from __future__ import annotations

from sqlglot.expressions.core import Func, Binary


# String basics


class Ascii(Func):
    pass


class BitLength(Func):
    pass


class ByteLength(Func):
    pass


class Chr(Func):
    arg_types = {"expressions": True, "charset": False}
    is_var_len_args = True
    _sql_names = ["CHR", "CHAR"]


class Concat(Func):
    arg_types = {"expressions": True, "safe": False, "coalesce": False}
    is_var_len_args = True


class ConcatWs(Concat):
    _sql_names = ["CONCAT_WS"]


class Contains(Func):
    arg_types = {"this": True, "expression": True, "json_scope": False}


class Elt(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class EndsWith(Func):
    _sql_names = ["ENDS_WITH", "ENDSWITH"]
    arg_types = {"this": True, "expression": True}


class Format(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Initcap(Func):
    arg_types = {"this": True, "expression": False}


class IsAscii(Func):
    pass


class Left(Func):
    arg_types = {"this": True, "expression": True}


class Length(Func):
    arg_types = {"this": True, "binary": False, "encoding": False}
    _sql_names = ["LENGTH", "LEN", "CHAR_LENGTH", "CHARACTER_LENGTH"]


class Levenshtein(Func):
    arg_types = {
        "this": True,
        "expression": False,
        "ins_cost": False,
        "del_cost": False,
        "sub_cost": False,
        "max_dist": False,
    }


class Lower(Func):
    _sql_names = ["LOWER", "LCASE"]


class MatchAgainst(Func):
    arg_types = {"this": True, "expressions": True, "modifier": False}


class Normalize(Func):
    arg_types = {"this": True, "form": False, "is_casefold": False}


class NumberToStr(Func):
    arg_types = {"this": True, "format": True, "culture": False}


class Overlay(Func):
    arg_types = {"this": True, "expression": True, "from_": True, "for_": False}


class Pad(Func):
    arg_types = {"this": True, "expression": True, "fill_pattern": False, "is_left": True}


class Repeat(Func):
    arg_types = {"this": True, "times": True}


class Replace(Func):
    arg_types = {"this": True, "expression": True, "replacement": False}


class Reverse(Func):
    pass


class Right(Func):
    arg_types = {"this": True, "expression": True}


class RtrimmedLength(Func):
    pass


class Search(Func):
    arg_types = {
        "this": True,  # data_to_search / search_data
        "expression": True,  # search_query / search_string
        "json_scope": False,  # BigQuery: JSON_VALUES | JSON_KEYS | JSON_KEYS_AND_VALUES
        "analyzer": False,  # Both: analyzer / ANALYZER
        "analyzer_options": False,  # BigQuery: analyzer_options_values
        "search_mode": False,  # Snowflake: OR | AND
    }


class SearchIp(Func):
    arg_types = {"this": True, "expression": True}


class Soundex(Func):
    pass


class SoundexP123(Func):
    pass


class Space(Func):
    """
    SPACE(n) → string consisting of n blank characters
    """

    pass


class Split(Func):
    arg_types = {"this": True, "expression": True, "limit": False}


class SplitPart(Func):
    arg_types = {"this": True, "delimiter": False, "part_index": False}


class StartsWith(Func):
    _sql_names = ["STARTS_WITH", "STARTSWITH"]
    arg_types = {"this": True, "expression": True}


class StrPosition(Func):
    arg_types = {
        "this": True,
        "substr": True,
        "position": False,
        "occurrence": False,
    }


class StrToMap(Func):
    arg_types = {
        "this": True,
        "pair_delim": False,
        "key_value_delim": False,
        "duplicate_resolution_callback": False,
    }


class String(Func):
    arg_types = {"this": True, "zone": False}


class Stuff(Func):
    _sql_names = ["STUFF", "INSERT"]
    arg_types = {"this": True, "start": True, "length": True, "expression": True}


class Substring(Func):
    _sql_names = ["SUBSTRING", "SUBSTR"]
    arg_types = {"this": True, "start": False, "length": False}


class SubstringIndex(Func):
    """
    SUBSTRING_INDEX(str, delim, count)

    *count* > 0  → left slice before the *count*-th delimiter
    *count* < 0  → right slice after the |count|-th delimiter
    """

    arg_types = {"this": True, "delimiter": True, "count": True}


class Translate(Func):
    arg_types = {"this": True, "from_": True, "to": True}


class Trim(Func):
    arg_types = {
        "this": True,
        "expression": False,
        "position": False,
        "collation": False,
    }


class Unicode(Func):
    pass


class Upper(Func):
    _sql_names = ["UPPER", "UCASE"]


# Encoding / base conversion


class Base64DecodeBinary(Func):
    arg_types = {"this": True, "alphabet": False}


class Base64DecodeString(Func):
    arg_types = {"this": True, "alphabet": False}


class Base64Encode(Func):
    arg_types = {"this": True, "max_line_length": False, "alphabet": False}


class CodePointsToBytes(Func):
    pass


class CodePointsToString(Func):
    pass


class ConvertToCharset(Func):
    arg_types = {"this": True, "dest": True, "source": False}


class Decode(Func):
    arg_types = {"this": True, "charset": True, "replace": False}


class Encode(Func):
    arg_types = {"this": True, "charset": True}


class FromBase(Func):
    arg_types = {"this": True, "expression": True}


class FromBase32(Func):
    pass


class FromBase64(Func):
    pass


class Hex(Func):
    pass


class HexDecodeString(Func):
    pass


class HexEncode(Func):
    arg_types = {"this": True, "case": False}


class LowerHex(Hex):
    pass


class SafeConvertBytesToString(Func):
    pass


class ToBase32(Func):
    pass


class ToBase64(Func):
    pass


class ToBinary(Func):
    arg_types = {"this": True, "format": False, "safe": False}


class ToChar(Func):
    arg_types = {
        "this": True,
        "format": False,
        "nlsparam": False,
        "is_numeric": False,
    }


class ToCodePoints(Func):
    pass


class ToDecfloat(Func):
    arg_types = {
        "this": True,
        "format": False,
    }


class ToDouble(Func):
    arg_types = {
        "this": True,
        "format": False,
        "safe": False,
    }


class ToFile(Func):
    arg_types = {
        "this": True,
        "path": False,
        "safe": False,
    }


class ToNumber(Func):
    arg_types = {
        "this": True,
        "format": False,
        "nlsparam": False,
        "precision": False,
        "scale": False,
        "safe": False,
        "safe_name": False,
    }


class TryBase64DecodeBinary(Func):
    arg_types = {"this": True, "alphabet": False}


class TryBase64DecodeString(Func):
    arg_types = {"this": True, "alphabet": False}


class TryHexDecodeBinary(Func):
    pass


class TryHexDecodeString(Func):
    pass


class TryToDecfloat(Func):
    arg_types = {
        "this": True,
        "format": False,
    }


class Unhex(Func):
    arg_types = {"this": True, "expression": False}


# Regex


class RegexpCount(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "parameters": False,
    }


class RegexpExtract(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "occurrence": False,
        "parameters": False,
        "group": False,
        "null_if_pos_overflow": False,  # for transpilation target behavior
    }


class RegexpExtractAll(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "group": False,
        "parameters": False,
        "position": False,
        "occurrence": False,
    }


class RegexpFullMatch(Binary, Func):
    arg_types = {"this": True, "expression": True, "options": False}


class RegexpILike(Binary, Func):
    arg_types = {"this": True, "expression": True, "flag": False}


class RegexpInstr(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "occurrence": False,
        "option": False,
        "parameters": False,
        "group": False,
    }


class RegexpReplace(Func):
    arg_types = {
        "this": True,
        "expression": True,
        "replacement": False,
        "position": False,
        "occurrence": False,
        "modifiers": False,
        "single_replace": False,
    }


class RegexpSplit(Func):
    arg_types = {"this": True, "expression": True, "limit": False}


# Hashing / cryptographic


class Compress(Func):
    arg_types = {"this": True, "method": False}


class Decrypt(Func):
    arg_types = {
        "this": True,
        "passphrase": True,
        "aad": False,
        "encryption_method": False,
        "safe": False,
    }


class DecryptRaw(Func):
    arg_types = {
        "this": True,
        "key": True,
        "iv": True,
        "aad": False,
        "encryption_method": False,
        "aead": False,
        "safe": False,
    }


class DecompressBinary(Func):
    arg_types = {"this": True, "method": True}


class DecompressString(Func):
    arg_types = {"this": True, "method": True}


class Encrypt(Func):
    arg_types = {"this": True, "passphrase": True, "aad": False, "encryption_method": False}


class EncryptRaw(Func):
    arg_types = {"this": True, "key": True, "iv": True, "aad": False, "encryption_method": False}


class FarmFingerprint(Func):
    arg_types = {"expressions": True}
    is_var_len_args = True
    _sql_names = ["FARM_FINGERPRINT", "FARMFINGERPRINT64"]


class MD5(Func):
    _sql_names = ["MD5"]


class MD5Digest(Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True
    _sql_names = ["MD5_DIGEST"]


class MD5NumberLower64(Func):
    pass


class MD5NumberUpper64(Func):
    pass


class SHA(Func):
    _sql_names = ["SHA", "SHA1"]


class SHA1Digest(Func):
    pass


class SHA2(Func):
    _sql_names = ["SHA2"]
    arg_types = {"this": True, "length": False}


class SHA2Digest(Func):
    arg_types = {"this": True, "length": False}


class StandardHash(Func):
    arg_types = {"this": True, "expression": False}


# Parse


class ParseBignumeric(Func):
    pass


class ParseNumeric(Func):
    pass


class ParseUrl(Func):
    arg_types = {"this": True, "part_to_extract": False, "key": False, "permissive": False}
