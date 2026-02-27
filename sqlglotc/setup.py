import os
import shutil

from setuptools import setup
from setuptools.command.build_ext import build_ext as _build_ext
from setuptools.command.sdist import sdist as _sdist
from mypyc.build import mypycify

here = os.path.dirname(os.path.abspath(__file__))
sqlglot_src = os.path.join(here, "..", "sqlglot")


SOURCE_FILES = [
    "errors.py",
    "helper.py",
    "parser_core.py",
    "schema.py",
    "serde.py",
    "time.py",
    "tokenizer_core.py",
    "trie.py",
]


def _source_paths():
    if os.path.isdir(sqlglot_src):
        # Building from the git repo: compile directly from sqlglot source, no copies.
        return [os.path.join(sqlglot_src, f) for f in SOURCE_FILES]
    # Building from an sdist: source files are bundled in ./sqlglot/.
    return [os.path.join(here, "sqlglot", f) for f in SOURCE_FILES]


class build_ext(_build_ext):
    def copy_extensions_to_source(self):
        """For editable installs, put sqlglot.* .so files in the sqlglot source dir."""
        build_py = self.get_finalized_command("build_py")
        for ext in self.extensions:
            fullname = self.get_ext_fullname(ext.name)
            filename = self.get_ext_filename(fullname)
            src = os.path.join(self.build_lib, filename)
            parts = fullname.split(".")
            if parts[0] == "sqlglot" and os.path.isdir(sqlglot_src):
                # Place compiled sqlglot.* / sqlglot.sub.* modules in the sqlglot source tree.
                sub_module = ".".join(parts[1:])
                dst = os.path.join(sqlglot_src, self.get_ext_filename(sub_module))
            else:
                # Default: mypyc runtime helper (e.g., HASH__mypyc) goes in current dir.
                package = ".".join(parts[:-1])
                package_dir = build_py.get_package_dir(package)
                dst = (
                    os.path.join(package_dir, os.path.basename(filename))
                    if package_dir
                    else os.path.basename(filename)
                )
            self.copy_file(src, dst, level=self.verbose)


class sdist(_sdist):
    """Bundle sqlglot source files into the sdist so sqlglotc can compile on install."""

    def run(self):
        local_sqlglot = os.path.join(here, "sqlglot")
        os.makedirs(local_sqlglot, exist_ok=True)
        open(os.path.join(local_sqlglot, "__init__.py"), "w").close()
        subpkgs = {os.path.dirname(f) for f in SOURCE_FILES if os.path.dirname(f)}
        for subpkg in subpkgs:
            pkg_dir = os.path.join(local_sqlglot, subpkg)
            os.makedirs(pkg_dir, exist_ok=True)
            open(os.path.join(pkg_dir, "__init__.py"), "w").close()
        for fname in SOURCE_FILES:
            dst_path = os.path.join(local_sqlglot, fname)
            os.makedirs(os.path.dirname(dst_path), exist_ok=True)
            shutil.copy2(os.path.join(sqlglot_src, fname), dst_path)
        try:
            super().run()
        finally:
            shutil.rmtree(local_sqlglot, ignore_errors=True)


setup(
    name="sqlglotc",
    packages=[],
    ext_modules=mypycify(_source_paths(), opt_level="3"),
    cmdclass={"build_ext": build_ext, "sdist": sdist},
)
