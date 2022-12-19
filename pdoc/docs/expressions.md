# Expressions

Every AST node in SQLGlot is represented by a subclass of `Expression`. Each such expression encapsulates any necessary context, such as its child expressions, their names, or arg keys, and whether each child expression is optional or not.

Furthermore, the following attributes are common across all expressions:

#### key

A unique key for each class in the `Expression` hierarchy. This is useful for hashing and representing expressions as strings.

#### args

A dictionary used for mapping child arg keys, to the corresponding expressions. A value in this mapping is usually either a single or a list of `Expression` instances, but SQLGlot doesn't impose any constraints on the actual type of the value.

#### arg_types

A dictionary used for mapping arg keys to booleans that determine whether the corresponding expressions are optional or not. Consider the following example:

```python
class Limit(Expression):
    arg_types = {"this": False, "expression": True}

```

Here, `Limit` declares that it expects to have one optional and one required child expression, which can be referenced through `this` and `expression`, correspondingly. The arg keys are generally arbitrary, but there are helper methods for keys like `this`, `expression` and `expressions` that abstract away dictionary lookups and related checks. For this reason, these keys are common throughout SQLGlot's codebase.

#### parent

A reference to the parent expression (may be `None`).

#### arg_key

The arg key an expression is associated with, i.e. the name its parent expression uses to refer to it.

#### comments

A list of comments that are associated with a given expression. This is used in order to preserve comments when transpiling SQL code.

#### type

The data type of an expression, as inferred by SQLGlot's optimizer.
