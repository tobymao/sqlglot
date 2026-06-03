import unittest
from unittest.mock import Mock, patch

import sqlglot
from sqlglot import Dialect
from sqlglot.dialects.dialect import Dialect as DialectBase
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer


class FakeDialect(DialectBase):
    pass


class PluginDialect(DialectBase):
    """A pure-Python dialect plugin, mimicking an external entry-point dialect.

    Its nested Tokenizer/Parser/Generator are interpreted subclasses of the
    (possibly mypyc-compiled) base classes. Instantiating them must not raise
    "interpreted classes cannot inherit from compiled" under the sqlglot[c] build.
    """

    class Tokenizer(Tokenizer):
        pass

    class Parser(Parser):
        pass

    class Generator(Generator):
        pass


class TestDialectEntryPoints(unittest.TestCase):
    def setUp(self):
        Dialect._classes.clear()

    def tearDown(self):
        Dialect._classes.clear()

    def test_entry_point_plugin_discovery_modern_api(self):
        fake_entry_point = Mock()
        fake_entry_point.name = "fakedb"
        fake_entry_point.load.return_value = FakeDialect

        mock_selectable = Mock()
        mock_selectable.select.return_value = [fake_entry_point]

        mock_entry_points = Mock(return_value=mock_selectable)

        with patch("sqlglot.dialects.dialect.entry_points", mock_entry_points):
            dialect = Dialect.get("fakedb")

        self.assertIsNotNone(dialect)
        self.assertEqual(dialect, FakeDialect)
        fake_entry_point.load.assert_called_once()
        mock_selectable.select.assert_called_once_with(group="sqlglot.dialects", name="fakedb")

    def test_entry_point_plugin_discovery_legacy_api(self):
        fake_entry_point = Mock()
        fake_entry_point.name = "fakedb"
        fake_entry_point.load.return_value = FakeDialect

        mock_dict = Mock(spec=["get"])
        mock_dict.get.return_value = [fake_entry_point]

        mock_entry_points = Mock(return_value=mock_dict)

        with patch("sqlglot.dialects.dialect.entry_points", mock_entry_points):
            dialect = Dialect.get("fakedb")

        self.assertIsNotNone(dialect)
        self.assertEqual(dialect, FakeDialect)
        fake_entry_point.load.assert_called_once()
        mock_dict.get.assert_called_once_with("sqlglot.dialects", [])

    def test_entry_point_plugin_not_found(self):
        mock_selectable = Mock()
        mock_selectable.select.return_value = []

        mock_entry_points = Mock(return_value=mock_selectable)

        with patch("sqlglot.dialects.dialect.entry_points", mock_entry_points):
            dialect = Dialect.get("nonexistent")

        self.assertIsNone(dialect)

    def test_interpreted_subclass_roundtrip(self):
        # A pure-Python dialect whose Parser/Generator/Tokenizer subclass the
        # base classes must parse and generate even when those bases are
        # mypyc-compiled (sqlglot[c]). Regression test for external dialect
        # plugins breaking with "interpreted classes cannot inherit from compiled".
        self.assertEqual(
            sqlglot.transpile("SELECT 1 AS x FROM t", read=PluginDialect, write=PluginDialect),
            ["SELECT 1 AS x FROM t"],
        )

    def test_interpreted_subclass_instantiation(self):
        dialect = PluginDialect()
        self.assertIsInstance(dialect.tokenizer(), Tokenizer)
        self.assertIsInstance(dialect.parser(), Parser)
        self.assertIsInstance(dialect.generator(), Generator)
