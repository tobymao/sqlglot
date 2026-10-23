import os
import runpy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from setuptools import Distribution, Extension
from setuptools.command.build_ext import build_ext


class TestSQLGlotC(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        version = mock.patch.dict(os.environ, {"SETUPTOOLS_SCM_PRETEND_VERSION": "0.0.0"})
        version.start()
        cls.addClassCleanup(version.stop)

        with (
            mock.patch("setuptools.setup"),
            mock.patch.dict(sys.modules, {"mypyc.build": mock.Mock(mypycify=lambda *a, **k: [])}),
            mock.patch.dict(os.environ),
        ):
            cls.build_ext = runpy.run_path(
                str(Path(__file__).resolve().parents[1] / "sqlglotc" / "setup.py")
            )["build_ext"]

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "sqlglot"
        self.source.mkdir()

        # setuptools_scm's finalize hook reads pyproject.toml from the cwd; the repo root's
        # [tool.setuptools_scm] would make it warn that the version is already set.
        cwd = os.getcwd()
        os.chdir(self.root)
        try:
            distribution = Distribution(
                {
                    "version": "0.0.0",
                    "ext_modules": [
                        Extension("sqlglot.optimizer.scope", ["scope.c"]),
                        Extension("sqlglot.optimizer.scope__mypyc", ["native.c"]),
                        Extension("standalone__mypyc", ["runtime.c"]),
                    ],
                }
            )
        finally:
            os.chdir(cwd)

        self.command = self.build_ext(distribution)
        self.command.build_lib = str(self.root / "build" / "lib")
        self.command.ensure_finalized()
        for extension in self.command.extensions:
            extension.sources = [str(self.root / source) for source in extension.sources]
            for source in extension.sources:
                Path(source).touch()
        self.expected = [Path(path) for path in self.command.get_outputs()]
        for path in self.expected:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"current extension")

    def test_build_removes_obsolete_extensions(self):
        build = Path(self.command.build_lib)
        obsolete = [
            build / "sqlglot" / "schema.so",
            build / "sqlglot.schema__mypyc.so",
            build / "sqlglot" / "optimizer" / "removed.pyd",
        ]
        for path in obsolete:
            path.write_bytes(b"obsolete extension")
        metadata = build / "sqlglot" / "py.typed"
        metadata.touch()
        mtimes = [path.stat().st_mtime_ns for path in self.expected]

        self.command.compiler = mock.Mock()
        self.command.build_extensions()

        self.assertTrue(all(not path.exists() for path in obsolete))
        self.assertEqual(mtimes, [path.stat().st_mtime_ns for path in self.expected])
        self.assertTrue(metadata.exists())
        self.command.compiler.compile.assert_not_called()
        self.command.compiler.link_shared_object.assert_not_called()

    def test_editable_install_removes_obsolete_extensions(self):
        (self.source / "optimizer").mkdir()
        obsolete = [
            self.source / "schema.so",
            self.source / "sqlglot.schema__mypyc.so",
            self.source / "optimizer" / "removed.pyd",
        ]
        for path in obsolete:
            path.write_bytes(b"obsolete extension")
        schema = self.source / "schema.py"
        schema.write_text("class MappingSchema: pass\n")

        with mock.patch.dict(
            self.build_ext.copy_extensions_to_source.__globals__,
            {"SQLGLOT_SRC": str(self.source)},
        ):
            self.command.copy_extensions_to_source()

        self.assertTrue(all(not path.exists() for path in obsolete))
        self.assertTrue(schema.exists())
        self.assertEqual(
            (self.source / "optimizer" / self.expected[0].name).read_bytes(),
            b"current extension",
        )
        self.assertEqual(
            (self.source / "optimizer" / self.expected[1].name).read_bytes(),
            b"current extension",
        )
        self.assertEqual((self.source / self.expected[2].name).read_bytes(), b"current extension")

    def test_dry_run_preserves_obsolete_extensions(self):
        obsolete = Path(self.command.build_lib) / "sqlglot" / "schema.so"
        obsolete.touch()
        self.command.dry_run = True

        with mock.patch.object(build_ext, "build_extensions"):
            self.command.build_extensions()

        self.assertTrue(obsolete.exists())
