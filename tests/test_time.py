import re
import unittest
import sys

from sqlglot.dialects.dialect import Dialect, Dialects
from sqlglot.generators.hive import CANONICAL_TIME_FORMAT, LAX_TO_NON_PADDED_FORMATS
from sqlglot.time import format_time, subsecond_precision

KNOWN_CANONICAL_ATOM = re.compile(r"%(?:[mdHIMS]strict|[-:].|[\w%])")


class TestTime(unittest.TestCase):
    def test_format_time(self):
        self.assertEqual(format_time("", {}), None)
        self.assertEqual(format_time(" ", {}), " ")
        mapping = {"a": "b", "aa": "c"}
        self.assertEqual(format_time("a", mapping), "b")
        self.assertEqual(format_time("aa", mapping), "c")
        self.assertEqual(format_time("aaada", mapping), "cbdb")
        self.assertEqual(format_time("da", mapping), "db")

    def test_canonical_time_format_vocabulary(self):
        # _lenient_parse_format assumes CANONICAL_TIME_FORMAT tokenizes every canonical time
        # format atomically; fail loudly when a dialect introduces an atom it can't recognize,
        # instead of silently mis-rewriting formats generated for strict dialects
        for dialect in Dialects:
            if not dialect.value:
                continue

            d = Dialect.get_or_raise(dialect.value)
            for value in (*d.TIME_MAPPING.values(), *d.FORMAT_MAPPING.values()):
                for part in CANONICAL_TIME_FORMAT.split(value):
                    self.assertNotIn("%", part, f"{dialect.value}: unconsumed '%' in {value!r}")
                for atom in CANONICAL_TIME_FORMAT.findall(value):
                    self.assertTrue(
                        KNOWN_CANONICAL_ATOM.fullmatch(atom),
                        f"{dialect.value}: unexpected canonical atom {atom!r} in {value!r} - "
                        "update CANONICAL_TIME_FORMAT in sqlglot/generators/hive.py",
                    )
                self.assertIsNone(
                    re.search(r"%[mdHIMS](?!strict)\w", value),
                    f"{dialect.value}: a lax %m/%d/%H/%I/%M/%S directly followed by a word "
                    f"character in {value!r} would be mis-tokenized by CANONICAL_TIME_FORMAT",
                )

            if d.TIME_MAPPING.get("MM") == "%mstrict":
                # _lenient_parse_format rewrites the format before the inverse trie walk, so
                # strict dialects must not rely on composite inverse keys containing a lax specifier
                lax = set(LAX_TO_NON_PADDED_FORMATS)
                for key in d.INVERSE_TIME_MAPPING:
                    atoms = CANONICAL_TIME_FORMAT.findall(key)
                    if len(atoms) > 1:
                        self.assertFalse(
                            lax & set(atoms),
                            f"{dialect.value}: composite inverse key {key!r} contains a lax specifier",
                        )

    def test_subsecond_precision(self):
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.123456+00:00"))
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.000123+00:00"))
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.000001+00:00"))
        self.assertEqual(3, subsecond_precision("2023-01-01 12:13:14.123+00:00"))
        self.assertEqual(0, subsecond_precision("2023-01-01 12:13:14+00:00"))
        self.assertEqual(0, subsecond_precision("2023-01-01 12:13:14"))
        self.assertEqual(0, subsecond_precision("garbage"))

    @unittest.skipUnless(
        sys.version_info >= (3, 11),
        "Python 3.11 relaxed datetime.fromisoformat() parsing with regards to microseconds",
    )
    def test_subsecond_precision_python311(self):
        # ref: https://docs.python.org/3/whatsnew/3.11.html#datetime
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.123456789+00:00"))
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.12345+00:00"))
        self.assertEqual(6, subsecond_precision("2023-01-01 12:13:14.1234+00:00"))
        self.assertEqual(3, subsecond_precision("2023-01-01 12:13:14.12+00:00"))
        self.assertEqual(3, subsecond_precision("2023-01-01 12:13:14.1+00:00"))
