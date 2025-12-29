import unittest
from unittest.mock import Mock, patch

from sqlglot import Dialect
from sqlglot.dialects.dialect import Dialect as DialectBase


class FakeDialect(DialectBase):
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
