# Compiling SQLGlot with mypyc

## The problem

SQLGlot is a pure Python SQL parser, transpiler, and optimizer. It supports 34 SQL dialects and has zero dependencies. It powers a growing number of open and closed source projects like [SQLMesh](https://github.com/TobikoData/sqlmesh), [Apache Superset](https://github.com/apache/superset), and many others, which collectively process millions of queries. People love that it's pure Python because it's easy to install, easy to hack on, and runs anywhere, but "pure Python" and "fast" don't usually show up in the same sentence.

For most use cases SQLGlot is plenty fast. But when you're parsing millions of queries in a data pipeline or running the optimizer over hundreds of TPC-H queries, those milliseconds add up. We wanted to make it faster without giving up the things that make it great.

## Why not Cython, Rust, or PyPy?

We looked at all the usual suspects before landing on mypyc.

**Cython** can work in "pure Python" mode, but in practice you still end up with Cython-specific annotations and build concerns. Our goal was to maintain a single, 100% pure Python codebase with no special syntax or modes.

**Rust** is the one we actually tried. SQLGlot had a Rust-based tokenizer (`sqlglotrs`) for over a year. It worked, but it came with real pain points. We needed a separate build and test pipeline to interoperate with Rust, we needed Rust expertise whenever things broke or packages needed upgrading, and the versioning was a headache since `sqlglotrs` had its own release cycle independent of sqlglot.

**PyPy** is great but it's a different runtime entirely. Users would need to switch their Python installation, which is a non-starter for most production environments.

What we really wanted was to keep our codebase as it is but somehow make it faster.

## Enter mypyc

[mypyc](https://mypyc.readthedocs.io/) is a compiler that takes type-annotated Python and compiles it to C extension modules. It's built on top of [mypy](https://github.com/python/mypy), so it understands your type annotations and uses them to generate efficient C code.

The main idea behind mypyc is that it combines compiled CPython code with native operations. Instead of going through Python's interpreter for every attribute access or function call, mypyc generates C code that operates directly on C-level data structures. String comparisons, integer arithmetic, method calls, and attribute lookups all become native C operations rather than going through Python's dynamic dispatch machinery. If you already have good type annotations (and we did, since we were already running mypy), you can get significant speedups without changing your source code at all.

## Getting it to compile

SQLGlot follows a classic compiler architecture: tokenizer, parser, AST (expressions), generator (turning the AST back into a query), and optimizer; our goal was to compile as much of this as possible.

Remember how we said you can get significant speedups without changing your source code? Yeah, about that: getting a codebase this size through mypyc was a project in itself. We hit compiler bugs, ran into design constraints we didn't expect, and ended up contributing fixes back to mypyc to unblock ourselves.

### Compiler bugs that blocked us

mypyc wasn't designed to handle a module with ~950 classes. Our AST definition lives in a single module, and when the compiler tried to process it, one of its internal compilation passes was doing redundant work on every instruction. On a module that large, it would eat all available memory and die with OOM even on a 64 GB machine. We had to dig into mypyc's internals and optimize that pass just to get compilation to finish ([python/mypy#20897](https://github.com/python/mypy/pull/20897)).

That was just the first of several blockers. Dictionary comprehensions containing lambdas would crash the compiler entirely ([#21009](https://github.com/python/mypy/pull/21009)). Method resolution would silently break with deep inheritance hierarchies ([#20917](https://github.com/python/mypy/pull/20917)). Property getters and setters would get mixed up in the generated internal method tables, so calling a getter might run the setter instead ([#21010](https://github.com/python/mypy/pull/21010)). Class variables that referenced themselves or other class variables in the same body would produce incorrect initialization code ([#21011](https://github.com/python/mypy/pull/21011)). And `__init_subclass__` hooks were running before class-level constants were even set up, so any logic that depended on those constants would see garbage values ([#20916](https://github.com/python/mypy/pull/20916)).

We could have worked around most of these on the SQLGlot side, but it would have meant massive refactors: breaking apart files, pulling every lambda out into a named function, restructuring class hierarchies, removing `__init_subclass__` in favor of something more manual. That would have made the codebase significantly harder to maintain for a questionable tradeoff. Instead we went into the mypyc codebase, understood the code generation, wrote fixes, and got them merged upstream. It took real time and effort, but the alternative was either carrying a fork of mypyc forever or rewriting large parts of SQLGlot into something less natural.

### Adapting SQLGlot

Even after fixing the compiler bugs, we had to adapt parts of our codebase to work within mypyc's model. None of these changes were huge individually, but together they touched a lot of files.

The biggest category was **class attribute annotations**. SQLGlot's dialect system is built on class-level dictionaries that child dialects extend. Every Parser and Generator overrides things like `FUNCTIONS`, `TRANSFORMS`, and `KEYWORDS`. In normal Python this just works, but mypyc changed how class attributes work under the hood: without a `ClassVar` annotation, accessing `Parent.FUNCTIONS` would return a low-level property descriptor instead of the actual dictionary. The fix is straightforward (annotate with `ClassVar` and mypyc leaves it alone) but we had to go through the entire codebase and add these annotations everywhere they were needed.

Beyond that, there was a grab bag of smaller adaptations: replacing metaclasses with `__init_subclass__` (mypyc only supports `ABCMeta`), converting lazy `from X import Y` imports inside methods to qualified access (mypyc doesn't support `from` imports in compiled functions), and fixing type annotations that mypyc's strict runtime enforcement flagged as incorrect. That last category was actually a nice surprise; mypyc caught real bugs we'd been carrying without knowing it, like generator methods annotated with the wrong expression type or variables that could be `None` but weren't marked as `Optional`.

---

## Optimizing for mypyc

Once everything compiled and the tests passed, we turned our attention to helping mypyc generate the best possible code. This is where things get interesting, because small changes to how you write Python can have outsized effects on the compiled output.

### Making the parser's hot loop faster

The parser is one of the most performance-sensitive parts of SQLGlot, and we found two patterns that made a real difference in how mypyc compiled it.

The first was eliminating `None` checks. The parser maintains a cursor into the token stream with `_curr`, `_next`, and `_prev` fields. Originally these were `Optional[Token]`, which meant every access needed to handle the `None` case, and in compiled code that adds real overhead since mypyc has to wrap and unwrap values as Python objects at each boundary. We replaced `None` with a sentinel token, a special `TokenType.SENTINEL` value where `Token.__bool__` returns `False`. You can still write `if self._curr:` and it works the same way, but the fields are now always `Token` instead of `Optional[Token]`, which lets mypyc skip the `None` handling entirely.

The second was using native integers. The parser's index fields (`_index`, `_tokens_size`) are incremented and compared millions of times during parsing. Python integers carry more overhead than you'd expect because they support arbitrary sizes, but mypyc supports a special `i64` type annotation that tells the compiler to use a plain 64-bit integer instead. The difference is small per operation but compounds across millions of iterations.

### Why we inlined instead of using generators

mypyc can't optimize code that yields values through generators the way it can optimize a plain loop. Nested generators, which are common in tree traversal code, are especially expensive because each one adds a layer of overhead the compiler can't see through. We discovered this when profiling the optimizer's scope traversal, which used `expression.walk()`, a generator-based tree walker. By rewriting the traversal as a direct loop we got a [1.8x speedup](https://github.com/tobymao/sqlglot/pull/7196) on scope analysis. Similarly, inlining the tokenizer's whitespace-skipping logic into its inner `_scan` method (bypassing the normal `_advance()` calls) made a [significant difference](https://github.com/tobymao/sqlglot/pull/7226) for queries with lots of whitespace or string literals.

### Faster dispatch in the generator

SQLGlot's generator converts AST nodes to SQL strings by dispatching on the expression type. The original code did this with string concatenation (`expression.key + "_sql"`) and `getattr()` on every node. We replaced this with a [pre-built dispatch dictionary](https://github.com/tobymao/sqlglot/pull/7404) that maps expression types directly to their generation methods, built once per Generator subclass and cached at the module level. This gave us a 6-23% speedup on SQL generation depending on query complexity, with the biggest gains on queries that visit many small nodes.

### Small hints that add up

A few smaller patterns round out the picture. Annotating module-level constants with `typing.Final` lets mypyc inline values directly into the generated C code instead of looking them up at runtime. Before we contributed native string primitives to mypyc (more on that below), we also used pre-built lookup tables for character classification in the tokenizer, since a dictionary lookup was cheaper than a Python method call in compiled code.

---

## Contributing performance primitives to mypyc

SQLGlot's tokenizer is the hottest code path in the entire library. It scans SQL strings character by character, checking whether each character is whitespace, a digit, alphanumeric, and so on. These string operations happen millions of times per query, and before our work mypyc didn't have optimized native versions of them. It would fall back to calling the regular Python methods through the C API, which works but leaves performance on the table.

We contributed five string operation primitives back to mypyc:

| Primitive | Speedup | PR |
|-----------|---------|-----|
| `str.isspace()` | ~1.3x | [#20842](https://github.com/python/mypy/pull/20842) |
| `str.isalnum()` | up to 3.2x | [#20852](https://github.com/python/mypy/pull/20852) |
| `str.isdigit()` | up to 3.5x | [#20893](https://github.com/python/mypy/pull/20893) |
| `str.lower()` / `str.upper()` | up to 2.6x | [#20948](https://github.com/python/mypy/pull/20948) |
| `str[i]` indexing (ASCII cache) | 3.9x | [#21035](https://github.com/python/mypy/pull/21035) |

The string indexing optimization deserves a closer look. The tokenizer constantly indexes into the SQL string with `self.sql[i]`. In CPython, single-character ASCII strings are cached internally, so `"hello"[0]` always returns the same pre-existing `"h"` object without allocating anything new. But mypyc's string indexing was creating a brand new string object on every access. By teaching mypyc to use CPython's cached ASCII characters, we got a 3.9x speedup on what is arguably the single most frequent operation in the entire tokenizer.

---

## Why we still ship pure Python

mypyc compiled classes are C extension types, not regular Python classes, and that comes with constraints that matter in practice.

You can't monkey-patch compiled classes or add attributes to them at runtime, because the type namespace is frozen after class creation. You can't freely subclass them from regular Python without opting in with a special decorator, and even then there are edge cases. Compiled instances don't have a `__dict__` (similar to classes with `__slots__`), so only attributes declared in the class body exist. And compiled functions don't expose `__code__`, so any code that inspects function signatures at runtime needs a fallback path.

These aren't theoretical limitations. Some of our own tests create ad-hoc subclasses of expression types or patch methods for testing purposes, and our users do the same in their code. The compiled version is faster, but it can't support every usage pattern, so pure Python needs to be a first-class option rather than just a fallback. We run the full test suite in both modes and any divergence between them is a bug we investigate.

## Packaging and distribution

We ship the compiled extensions as a separate package called `sqlglotc`. Users install it with `pip install "sqlglot[c]"`. If `sqlglotc` is installed, SQLGlot automatically picks up the compiled modules; if it's not, everything works exactly the same at pure Python speed.

We use [cibuildwheel](https://cibuildwheel.readthedocs.io/) to build pre-compiled wheels for Python 3.9 through 3.14 on Linux, macOS, and Windows, so most users never have to compile anything themselves. This was a big improvement over the old `sqlglotrs` setup, where we had to maintain separate Rust toolchains in CI and deal with cross-compilation headaches.

---

## Where we are now

Today we compile over 100 modules to C extensions: the core tokenizer, parser, and generator, all ~950 expression classes that make up the AST, all 33+ dialect parsers, all 32+ dialect generators, and the most performance-critical optimizer passes. We deliberately leave some modules as pure Python, things like the less frequently used optimizer passes and the executor, since they change often and don't run frequently enough to justify the compilation overhead.

Here's where things stand across SQLGlot's main components:

| Component | Speedup (compiled vs pure Python) |
|-----------|----------------------------------|
| Parsing (tokenizer + parser) | ~5x |
| SQL generation | ~2.5x |
| Optimizer (annotate_types, simplify) | ~2x to 2.5x |

The parsing speedup is the most dramatic because the tokenizer benefits from all the string primitives we contributed, and the parser benefits from the sentinel pattern, native integers, and inlined hot paths. The generator and optimizer see more modest but still significant improvements, mostly from tighter dispatch and eliminating dynamic overhead.

We benchmark against a wide range of query shapes to make sure the speedups are consistent and not just artifacts of one particular pattern. The suite includes TPC-H queries, 20,000-item IN clauses, 500 levels of nested arithmetic, 200 JOINs, 500 UNIONs, 1,000-branch CASE statements, and more.

---

## What we learned

If we had to boil this whole experience down to a few takeaways, it would be these.

**Type annotations pay double.** We already had decent type annotations for mypy checking, but mypyc pushed us to make them tighter. Better types mean faster compiled code, and mypyc's strict runtime enforcement caught real bugs where our annotations didn't match reality. What started as a correctness tool became a performance tool at the same time.

**Fix it upstream.** We spent a lot of time debugging mypyc internals, writing test cases, and submitting patches. It would have been tempting to just work around every issue on our side. But by fixing things in mypyc itself, we made the compiler better for everyone and we don't have to carry workarounds that might break in future releases. The 13 PRs we merged into python/mypy fix real issues that any large mypyc project would eventually hit.

**Keep one foot in pure Python.** The compiled version is faster, but it can never be the only option. Some users can't build C extensions, some want to monkey-patch internals, some environments are locked down. Treating pure Python as a first-class path rather than a fallback kept us honest about the constraints of compilation.

**Get your hands dirty.** mypyc produces real speedups and it's not a toy, but it's also not a "flip a switch and go faster" tool. For a large codebase you'll need to understand how it works, read its source code, and probably fix a bug or two. If you're comfortable with that, it's a great fit. Start with the hottest modules, get them compiling, and expand from there. You'll learn a lot about your own code along the way.

## Links

- [SQLGlot repository](https://github.com/tobymao/sqlglot)
- [mypyc documentation](https://mypyc.readthedocs.io/)
- [Our mypyc contributions](https://github.com/python/mypy/pulls?q=is%3Apr+author%3AVaggelisD+is%3Amerged)
- [Black's mypyc blog series](https://ichard26.github.io/blog/2022/05/compiling-black-with-mypyc-part-1/) (the inspiration for this series)
