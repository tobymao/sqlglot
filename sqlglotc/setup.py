import os
import shutil
import sys

import mypyc
from setuptools import setup
from setuptools.command.build_ext import build_ext as _build_ext
from setuptools.command.sdist import sdist as _sdist
from mypyc.build import mypycify


def _log(msg: str) -> None:
    """Print to stderr so pip surfaces the message even without -v."""
    print(f"[sqlglotc-setup] {msg}", file=sys.stderr, flush=True)


_log(f"python={sys.version.split()[0]} platform={sys.platform}")
_log(f"mypyc={mypyc.__file__}")

SQLGLOT_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sqlglot")


def _find_sqlglot_dir():
    """Find the sqlglot source directory: repo source, or installed package.

    When the installed package is in site-packages, copy it to a clean temp
    directory so mypy doesn't discover unrelated modules (mypy_extensions,
    typing_extensions) that cause shadowing errors.
    """
    if os.path.isdir(SQLGLOT_SRC):
        return SQLGLOT_SRC

    # Fall back to the installed sqlglot package (build dependency).
    import sqlglot
    import tempfile

    installed = os.path.dirname(sqlglot.__file__)
    tmp = tempfile.mkdtemp(prefix="sqlglotc_build_")
    dst = os.path.join(tmp, "sqlglot")
    shutil.copytree(installed, dst)
    return dst


def _subpkg_files(src_dir, subpkg, files=None):
    """List source files from a sqlglot subpackage. Compiles all .py files if `files` is None."""
    if files is None:
        files = sorted(
            f
            for f in os.listdir(os.path.join(src_dir, subpkg))
            if f.endswith(".py") and f != "__init__.py"
        )
    return [os.path.join(subpkg, f) for f in files]


def _source_files(src_dir):
    return [
        "errors.py",
        "generator.py",
        "helper.py",
        "parser.py",
        "schema.py",
        "serde.py",
        "time.py",
        "tokenizer_core.py",
        "trie.py",
        *_subpkg_files(src_dir, "expressions"),
        *_subpkg_files(src_dir, "generators"),
        *_subpkg_files(
            src_dir,
            "optimizer",
            [
                "scope.py",
                "resolver.py",
                "isolate_table_selects.py",
                "normalize_identifiers.py",
                "qualify.py",
                "qualify_tables.py",
                "qualify_columns.py",
                "simplify.py",
                "annotate_types.py",
            ],
        ),
        *_subpkg_files(src_dir, "parsers"),
        *_subpkg_files(src_dir, "executor", ["table.py"]),
    ]


SRC_DIR = _find_sqlglot_dir()
SOURCE_FILES = _source_files(SRC_DIR)
_log(f"src_dir={SRC_DIR} source_files={len(SOURCE_FILES)}")

# Set MYPYPATH to the parent of the sqlglot source so mypy resolves
# `import sqlglot` from there — not from site-packages where
# mypy_extensions.py / typing_extensions.py can cause shadowing errors.
os.environ["MYPYPATH"] = os.path.dirname(SRC_DIR)


def _source_paths():
    return [os.path.join(SRC_DIR, f) for f in SOURCE_FILES]


class build_ext(_build_ext):
    def copy_extensions_to_source(self):
        """For editable installs, put sqlglot.* .so files in the sqlglot source dir."""
        for ext in self.extensions:
            fullname = self.get_ext_fullname(ext.name)
            filename = self.get_ext_filename(fullname)
            src = os.path.join(self.build_lib, filename)
            parts = fullname.split(".")
            if parts[0] == "sqlglot" and os.path.isdir(SQLGLOT_SRC):
                # Place compiled sqlglot.* / sqlglot.sub.* modules in the sqlglot source tree.
                sub_module = ".".join(parts[1:])
                dst = os.path.join(SQLGLOT_SRC, self.get_ext_filename(sub_module))
            else:
                # Place the mypyc runtime helper (e.g., HASH__mypyc) inside sqlglot/.
                # sqlglot/__init__.py bootstraps it into sys.modules for editable installs.
                dst = os.path.join(SQLGLOT_SRC, os.path.basename(filename))
            self.copy_file(src, dst, level=self.verbose)

    def run(self):
        super().run()
        # TEMPORARY: always fail with full diagnostics so pip surfaces output.
        # Used to debug why integration tests produce a 530KB wheel without
        # shared-lib .so files. Revert once root-caused.
        present, missing = [], []
        for ext in self.extensions:
            filename = self.get_ext_filename(self.get_ext_fullname(ext.name))
            path = os.path.join(self.build_lib, filename)
            (present if os.path.exists(path) else missing).append(path)
        _log(
            f"build_ext finished; build_lib={self.build_lib} inplace={self.inplace} "
            f"present={len(present)} missing={len(missing)}"
        )
        # Sample a shim and a shared lib so we can see what got built
        for sample in present[:3]:
            try:
                size = os.path.getsize(sample)
            except OSError:
                size = -1
            _log(f"  PRESENT {sample} ({size} bytes)")
        for path in missing[:5]:
            _log(f"  MISSING {path}")
        # Also list the actual contents of build_lib so we see what setuptools
        # will package into the wheel.
        if os.path.isdir(self.build_lib):
            for root, _dirs, files in os.walk(self.build_lib):
                for f in files:
                    if f.endswith(".so"):
                        full = os.path.join(root, f)
                        _log(f"  build_lib has: {os.path.relpath(full, self.build_lib)}")
        raise SystemExit("[sqlglotc-setup] DEBUG: aborting after build_ext run")


class sdist(_sdist):
    """Bundle sqlglot source files into the sdist as a fallback."""

    def run(self):
        local_sqlglot = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlglot")
        os.makedirs(local_sqlglot, exist_ok=True)
        subpkgs = {os.path.dirname(f) for f in SOURCE_FILES if os.path.dirname(f)}
        for subpkg in subpkgs:
            pkg_dir = os.path.join(local_sqlglot, subpkg)
            os.makedirs(pkg_dir, exist_ok=True)
        for fname in SOURCE_FILES:
            dst_path = os.path.join(local_sqlglot, fname)
            os.makedirs(os.path.dirname(dst_path), exist_ok=True)
            shutil.copy2(os.path.join(SQLGLOT_SRC, fname), dst_path)
        try:
            super().run()
        finally:
            shutil.rmtree(local_sqlglot, ignore_errors=True)


_ext_modules = mypycify(
    _source_paths(),
    opt_level=os.environ.get("MYPYC_OPT", "2"),
    separate=True,
    verbose=True,
)
_shared_libs = [e for e in _ext_modules if e.name.endswith("__mypyc")]
_shims = [e for e in _ext_modules if not e.name.endswith("__mypyc")]
_log(
    f"mypycify produced {len(_ext_modules)} extensions: {len(_shared_libs)} shared libs + {len(_shims)} shims"
)
# Show what sources each kind of Extension has, plus whether those source files exist.
for kind, ext in (
    ("shared_lib", _shared_libs[0] if _shared_libs else None),
    ("shim", _shims[0] if _shims else None),
):
    if ext is None:
        continue
    _log(f"  sample {kind}: name={ext.name} sources={ext.sources!r}")
    for s in ext.sources or []:
        _log(f"    source exists={os.path.exists(s)} path={s}")
if not _shared_libs:
    raise SystemExit(
        "[sqlglotc-setup] mypycify returned no shared-lib extensions; "
        "compilation must have been skipped. Refusing to build a broken wheel."
    )

setup(
    name="sqlglotc",
    packages=[],
    ext_modules=_ext_modules,
    cmdclass={"build_ext": build_ext, "sdist": sdist},
)
