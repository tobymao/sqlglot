# Primer on SQLGlot's Abstract Syntax Tree

SQLGlot is a powerful tool for analyzing and transforming SQL, but the learning curve can be intimidating.
 
This post is intended to familiarize newbies with SQLGlot's abstract syntax trees, how to traverse them, and how to mutate them.

## The tree

SQLGlot parses SQL into an abstract syntax tree (AST).
```python
from sqlglot import parse_one

ast = parse_one("SELECT a FROM (SELECT a FROM x) AS x")
```

An AST is a data structure that represents a SQL statement. The best way to glean the structure of a particular AST is python's builtin `repr` function:
```python
repr(ast)

# Select(
#   expressions=[
#     Column(
#       this=Identifier(this=a, quoted=False))],
#   from=From(
#     this=Subquery(
#       this=Select(
#         expressions=[
#           Column(
#             this=Identifier(this=a, quoted=False))],
#         from=From(
#           this=Table(
#             this=Identifier(this=x, quoted=False)))),
#       alias=TableAlias(
#         this=Identifier(this=x, quoted=False)))))
```

This is a textual representation of the internal data structure. Here's a breakdown of some of its components:
```
`Select` is the expression type
  |
Select(
  expressions=[  ------------------------------- `expressions` is a child key of `Select`
    Column(  ----------------------------------- `Column` is the expression type of the child
      this=Identifier(this=a, quoted=False))],
  from=From(  ---------------------------------- `from` is another child key of `Select`
  ...
```

## Nodes of the tree

The nodes in this tree are instances of `sqlglot.Expression`. Nodes reference their children in `args` and their parent in `parent`:
```python
ast.args
# {
#    "expressions": [Column(this=...)],
#    "from": From(this=...),
#    ...
# }

ast.args["expressions"][0]
# Column(this=...)

ast.args["expressions"][0].args["this"]
# Identifier(this=...)

ast.args["from"]
# From(this=...)

assert ast.args["expressions"][0].args["this"].parent.parent is ast
```

Children can either be:
1. An Expression instance
2. A list of Expression instances
3. Another Python object, such as str or bool. This will always be a leaf node in the tree.

Navigating this tree requires an understanding of the different Expression types. The best way to browse Expression types is directly in the code at [expressions.py](../sqlglot/expressions.py). Let's look at a simplified version of one Expression type:
```python
class Column(Expression):
    arg_types = {
      "this": True, 
      "table": False,
      ...
    }
```

`Column` subclasses `Expression`.

`arg_types` is a class attribute that specifies the possible children. The `args` keys of an Expression instance correspond to the `arg_types` keys of its class. The values of the `arg_types` dict are `True` if the key is required.

There are some common `arg_types` keys:
- "this": This is typically used for the primary child. In `Column`, "this" is the identifier for the column's name.
- "expression": This is typically used for the secondary child
- "expressions": This is typically used for a primary list of children

There aren't strict rules for when these keys are used, but they help with some of the convenience methods available on all Expression types:
- `Expression.this`: shorthand for `self.args.get("this")`
- `Expression.expression`: similarly, shorthand for the expression arg
- `Expression.expressions`: similarly, shorthand for the expressions list arg
- `Expression.name`: text name for whatever `this` is

`arg_types` don't specify the possible Expression types of children. This can be a challenge when you are writing code to traverse a particular AST and you don't know what to expect. A common trick is to parse an example query and print out the `repr`.

You can traverse an AST using just args, but there are some higher-order functions for programmatic traversal.

> [!NOTE]
> SQLGlot can parse and generate SQL for many different dialects. However, there is only a single set of Expression types for all dialects. We like to say that the AST can represent the _superset_ of all dialects. 
> 
> Sometimes, SQLGlot will parse SQL from a dialect into Expression types you didn't expect:
> 
> ```python
> ast = parse_one("SELECT NOW()", dialect="postgres")
> 
> repr(ast)
> # Select(
> #   expressions=[
> #     CurrentTimestamp()])
> ```
> 
> This is because SQLGlot tries to converge dialects on a standard AST. This means you can often write one piece of code that handles multiple dialects.

## Traversing the AST

Analyzing a SQL statement requires traversing this data structure. There are a few ways to do this:

### Args

If you know the structure of an AST, you can use `Expression.args` just like above. However, this can be very limited if you're dealing with arbitrary SQL.

### Walk methods

The walk methods of `Expression` (`find`, `find_all`, and `walk`) are the simplest way to analyze an AST. 

`find` and `find_all` search an AST for specific Expression types:
```python
from sqlglot import exp

ast.find(exp.Select)
# Select(
#   expressions=[
#     Column(
#       this=Identifier(this=a, quoted=False))],
# ...

list(ast.find_all(exp.Select))
# [Select(
#   expressions=[
#     Column(
#       this=Identifier(this=a, quoted=False))],
# ...
```

Both `find` and `find_all` are built on `walk`, which gives finer grained control:
```python
for (
    node,  # the current AST node
    parent,  # parent of the current AST node (this will be None for the root node)
    key  # The 'key' of this node in its parent's args
) in ast.walk():
    ...
```

> [!WARNING]
> Here's a common pitfall of the walk methods:
> ```python
> ast.find_all(exp.Table)
> ```
> At first glance, this seems like a great way to find all tables in a query. However, `Table` instances are not always tables in your database. Here's an example where this fails:
> ```python
> ast = parse_one("""
> WITH x AS (
>   SELECT a FROM y
> )
> SELECT a FROM x
> """)
> 
> # This is NOT a good way to find all tables in the query!
> for table in ast.find_all(exp.Table):
>     print(table)
> 
> # x  -- this is a common table expression, NOT an actual table
> # y
> ```
> 
> For programmatic traversal of ASTs that requires deeper semantic understanding of a query, you need "scope".

### Scope

Scope is a traversal module that handles more semantic context of SQL queries. It's harder to use than the `walk` methods but is more powerful:
```python
from sqlglot.optimizer.scope import build_scope

ast = parse_one("""
WITH x AS (
  SELECT a FROM y
)
SELECT a FROM x
""")

root = build_scope(ast)
for scope in root.traverse():
    print(scope)

# Scope<SELECT a FROM y>
# Scope<WITH x AS (SELECT a FROM y) SELECT a FROM x>
```

Let's use this for a better way to find all tables in a query:
```python
tables = [
    source

    # Traverse the Scope tree, not the AST
    for scope in root.traverse()

    # `selected_sources` contains sources that have been selected in this scope, e.g. in a FROM or JOIN clause.
    # `alias` is the name of this source in this particular scope.
    # `node` is the AST node instance
    # if the selected source is a subquery (including common table expressions), 
    #     then `source` will be the Scope instance for that subquery.
    # if the selected source is a table, 
    #     then `source` will be a Table instance.
    for alias, (node, source) in scope.selected_sources.items()
    if isinstance(source, exp.Table)
]

for table in tables:
    print(table)

# y  -- Success!
```

`build_scope` returns an instance of the `Scope` class. `Scope` has numerous methods for inspecting a query. The best way to browse these methods is directly in the code at [scope.py](../sqlglot/optimizer/scope.py). You can also look for examples of how Scope is used throughout SQLGlot's [optimizer](../sqlglot/optimizer) module. 

Many methods of Scope depend on a fully qualified SQL expression. For example, let's say we want to trace the lineage of columns in this query:
```python
ast = parse_one("""
SELECT
  a,
  c
FROM (
  SELECT 
    a, 
    b 
  FROM x
) AS x
JOIN (
  SELECT 
    b, 
    c 
  FROM y
) AS y
  ON x.b = y.b
""")
```

Just looking at the outer query, it's not obvious that column `a` comes from table `x` without looking at the columns of the subqueries.

We can use the [qualify](../sqlglot/optimizer/qualify.py) function to prefix all columns in an AST with their table name like so:
```python
from sqlglot.optimizer.qualify import qualify

qualify(ast)
# SELECT
#   x.a AS a,
#   y.c AS c
# FROM (
#   SELECT
#     x.a AS a,
#     x.b AS b
#   FROM x AS x
# ) AS x
# JOIN (
#   SELECT
#     y.b AS b,
#     y.c AS c
#   FROM y AS y
# ) AS y
#   ON x.b = y.b
```

Now we can trace a column to its source. Here's how we might find the table or subquery for all columns in a qualified AST:
```python
from sqlglot.optimizer.scope import find_all_in_scope

root = build_scope(ast)

# `find_all_in_scope` is similar to `Expression.find_all`, except it doesn't traverse into subqueries
for column in find_all_in_scope(root.expression, exp.Column):
    print(f"{column} => {root.sources[column.table]}")

# x.a => Scope<SELECT x.a AS a, x.b AS b FROM x AS x>
# y.c => Scope<SELECT y.b AS b, y.c AS c FROM y AS y>
# x.b => Scope<SELECT x.a AS a, x.b AS b FROM x AS x>
# y.b => Scope<SELECT y.b AS b, y.c AS c FROM y AS y>
```

For a complete example of tracing column lineage, check out the [lineage](../sqlglot/lineage.py) module.

> [!NOTE]
> Some queries require the database schema for disambiguation. For example:
> 
> ```sql
> SELECT a FROM x CROSS JOIN y
> ```
> 
> Column `a` might come from table `x` or `y`. In these cases, you must pass the `schema` into `qualify`.

## Mutating the tree

You can also modify an AST or build one from scratch. There are a few ways to do this.

### High-level builder methods

SQLGlot has methods for programmatically building up expressions similar to how you might in an ORM:
```python
ast = (
    exp
    .select("a", "b")
    .from_("x")
    .where("b < 4")
    .limit(10)
)
```

> [!WARNING]
> High-level builder methods will attempt to parse string arguments into Expressions. This can be very convenient, but make sure to keep in mind the dialect of the string. If its written in a specific dialect, you need to set the `dialect` argument.
> 
> You can avoid parsing by passing Expressions as arguments, e.g. `.where(exp.column("b") < 4)` instead of `.where("b < 4")`

These methods can be used on any AST, including ones you've parsed:
```python
ast = parse_one("""
SELECT * FROM (SELECT a, b FROM x) 
""")

# To modify the AST in-place, set `copy=False`
ast.args["from"].this.this.select("c", copy=False)

print(ast)
# SELECT * FROM (SELECT a, b, c FROM x)
```

The best place to browse all the available high-level builder methods and their parameters is, as always, directly in the code at [expressions.py](../sqlglot/expressions.py).

### Low-level builder methods 

High-level builder methods don't account for all possible expressions you might want to build. In the case where a particular high-level method is missing, use the low-level methods. Here are some examples:
```python
node = ast.args["from"].this.this

# These all do the same thing:

# high-level
node.select("c", copy=False)
# low-level
node.set("expressions", node.expressions + [exp.column("c")])
node.append("expressions", exp.column("c"))
node.replace(node.copy().select("c"))
```
> [!NOTE]
> In general, you should use `Expression.set` and `Expression.append` instead of mutating `Expression.args` directly. `set` and `append` take care to update node references like `parent`.

You can also instantiate AST nodes directly:

```python
col = exp.Column(
    this=exp.to_identifier("c")
)
node.append("expressions", col)
```

> [!WARNING]
> Because SQLGlot doesn't verify the types of args, it's easy to instantiate an invalid AST Node that won't generate to SQL properly. Take extra care to inspect the expected types of a node using the methods described above.

### Transform

The `Expression.transform` method applies a function to all nodes in an AST in depth-first, pre-order. 

```python
def transformer(node):
    if isinstance(node, exp.Column) and node.name == "a":
        # Return a new node to replace `node`
        return exp.func("FUN", node)
    # Or return `node` to do nothing and continue traversing the tree
    return node

print(parse_one("SELECT a, b FROM x").transform(transformer))
# SELECT FUN(a), b FROM x
```

> [!WARNING]
> As with the walk methods, `transform` doesn't manage scope. For safely transforming the columns and tables in complex expressions, you should probably use Scope. 

## Summed up

SQLGlot parses SQL statements into an abstract syntax tree (AST) where nodes are instances of `sqlglot.Expression`.

There are 3 ways to traverse an AST:
1. **args** - use this when you know the exact structure of the AST you're dealing with. 
2. **walk methods** - this is the easiest way. Use this for simple cases.
3. **scope** - this is the hardest way. Use this for more complex cases that must handle the semantic context of a query.

There are 3 ways to mutate an AST
1. **high-level builder methods** - use this when you know the exact structure of the AST you're dealing with. 
2. **low-level builder methods** - use this only when high-level builder methods don't exist for what you're trying to build.
3. **transform** - use this for simple transformations on arbitrary statements.

And, of course, these mechanisms can be mixed and matched. For example, maybe you need to use scope to traverse an arbitrary AST and the high-level builder methods to mutate it in-place.

Still need help? [Get in touch!](../README.md#get-in-touch)
