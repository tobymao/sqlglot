# Spark SQL 变更治理：列级血缘、Schema 契约与 AST Diff 实战

## 背景

数据仓库治理的核心难题之一是：**Spark SQL 变更上线前，如何自动校验列级血缘与 schema 契约？** 人工 review 容易遗漏，而 SQLGlot 提供了四个可组合的模块来构建自动化治理流水线：

| 模块 | 职责 |
|------|------|
| `sqlglot/lineage.py` | 追踪列级数据血缘，构建 `Node` 链表 |
| `sqlglot/schema.py` | 绑定表结构元数据，提供类型推断 |
| `sqlglot/diff.py` | 对两棵 AST 做语义 diff，产出编辑脚本 |
| `sqlglot/serde.py` | AST 序列化/反序列化，用于存储与传输 |

本文面向数据工程读者，讲解如何将这四个模块串起来，构建一套完整的 **变更审批 → 血缘校验 → schema 契约 → AST diff → 结果归档** 流水线。

---

## 整体流水线概览

```
┌─────────────────────────────────────────────────────────────────┐
│                    Spark SQL 变更治理流水线                        │
│                                                                 │
│  ① 解析 SQL          ② 绑定 Schema        ③ 追踪列级血缘         │
│  ┌──────────┐       ┌──────────┐         ┌──────────┐          │
│  │ parse_one│──────▶│MappingSch│────────▶│ lineage()│          │
│  │ (AST)    │       │ ema      │         │ → Node   │          │
│  └──────────┘       └──────────┘         └──────────┘          │
│       │                                       │                 │
│       ▼                                       ▼                 │
│  ④ AST Diff          ⑤ AST 序列化                              │
│  ┌──────────┐       ┌──────────┐                                │
│  │ diff()   │       │ dump() / │                                │
│  │ → Edit[] │       │ load()   │                                │
│  └──────────┘       └──────────┘                                │
└─────────────────────────────────────────────────────────────────┘
```

**步骤说明：**

1. 用 `parse_one()` 将新旧 SQL 解析为 AST
2. 用 `MappingSchema` 绑定数仓表结构元数据
3. 用 `lineage()` 追踪每个输出列的上游来源
4. 用 `diff()` 对比新旧 AST，生成语义级变更列表
5. 用 `dump()` / `load()` 将 AST 序列化存储，用于审计归档

---

## Step 1：解析 SQL 为 AST

一切从 `sqlglot.parse_one()` 开始。它将 Spark SQL 字符串转换为结构化的 AST：

```python
from sqlglot import parse_one

old_sql = """
SELECT user_id, amount * 1.1 AS adjusted_amount
FROM orders
WHERE status = 'completed'
"""

new_sql = """
SELECT user_id, amount * 1.2 AS adjusted_amount, created_at
FROM orders
WHERE status = 'completed'
  AND created_at >= '2024-01-01'
"""

old_ast = parse_one(old_sql, dialect="spark")
new_ast = parse_one(new_sql, dialect="spark")
```

`parse_one()` 返回一个 `exp.Select` 节点，它是整棵 AST 的根。后续所有模块都基于这棵树工作。

---

## Step 2：绑定 Schema 元数据

### MappingSchema：表结构的内存表示

`MappingSchema` 是 `schema.py` 中的核心类，它用嵌套字典描述数仓的 catalog → db → table → column 层级结构：

```python
from sqlglot.schema import MappingSchema

schema = MappingSchema(
    schema={
        "analytics": {  # catalog
            "sales": {  # db
                "orders": {  # table
                    "user_id": "BIGINT",
                    "amount": "DECIMAL(10,2)",
                    "status": "STRING",
                    "created_at": "TIMESTAMP",
                },
                "users": {
                    "user_id": "BIGINT",
                    "user_name": "STRING",
                    "region": "STRING",
                },
            }
        }
    },
    dialect="spark",
)
```

### Schema 的三种嵌套深度

`MappingSchema` 根据字典嵌套层数自动推断 `supported_table_args`：

| 深度 | 结构 | 支持的引用方式 |
|------|------|----------------|
| 1 | `{table: {col: type}}` | `orders` |
| 2 | `{db: {table: {col: type}}}` | `sales.orders` |
| 3 | `{catalog: {db: {table: {col: type}}}}` | `analytics.sales.orders` |

`AbstractMappingSchema` 内部使用 Trie（`new_trie()`）索引表路径，`find()` 方法通过 `_find_in_trie()` 做前缀匹配查找。这意味着 `orders` 可以匹配到 `analytics.sales.orders`，前提是路径无歧义。

### 动态注册表

如果 schema 无法在初始化时全量提供，可以用 `add_table()` 动态注册：

```python
schema.add_table(
    "analytics.sales.refunds",
    column_mapping={"refund_id": "BIGINT", "amount": "DECIMAL(10,2)"},
)
```

### 查询列类型

`get_column_type()` 返回 `exp.DataType` 对象，这在后续的类型推断和血缘追踪中至关重要：

```python
col_type = schema.get_column_type(
    "analytics.sales.orders", "amount", dialect="spark"
)
print(col_type)  # DECIMAL(10, 2)
```

### 列可见性控制

`MappingSchema` 支持 `visible` 参数控制列的可见性，这对治理场景非常有用——可以隐藏内部实现列，只暴露契约列：

```python
schema = MappingSchema(
    schema={"orders": {"user_id": "BIGINT", "amount": "DECIMAL", "_internal_flag": "BOOLEAN"}},
    visible={"orders": {"user_id", "amount"}},  # 只暴露这两列
)

# column_names(only_visible=True) 会过滤掉 _internal_flag
visible_cols = schema.column_names("orders", only_visible=True)
```

---

## Step 3：列级血缘追踪

### lineage() 函数

`lineage.py` 的核心入口是 `lineage()` 函数。它接受目标列名、SQL 语句和 schema，返回一个 `Node` 链表：

```python
from sqlglot.lineage import lineage

node = lineage(
    column="adjusted_amount",
    sql=new_sql,
    schema=schema,
    dialect="spark",
)
```

### Node 数据结构

每个 `Node` 包含：

| 字段 | 说明 |
|------|------|
| `name` | 列名（含作用域前缀，如 `orders.amount`） |
| `expression` | 该列对应的 AST 表达式节点 |
| `source` | 包含该表达式的完整 SELECT 语句 |
| `downstream` | 下游 `Node` 列表（上游依赖列） |
| `source_name` | 数据源名称（用于跨查询追踪） |
| `reference_node_name` | CTE 或子查询的引用名 |
| `payload` | 调用方可注入的自定义数据字典 |

### 遍历血缘链

`Node.walk()` 方法以深度优先方式遍历整条血缘链：

```python
for n in node.walk():
    print(f"{n.name} <- {n.expression.sql(dialect='spark')}")
```

输出示例：

```
adjusted_amount <- amount * 1.2
orders.amount <- amount
```

### 跨查询追踪：sources 参数

当 SQL 引用了其他查询的结果（如 dbt model 依赖），用 `sources` 参数传入上游查询定义：

```python
node = lineage(
    column="total_revenue",
    sql="SELECT user_id, SUM(adjusted_amount) AS total_revenue FROM staging_orders GROUP BY user_id",
    sources={
        "staging_orders": "SELECT user_id, amount * 1.2 AS adjusted_amount FROM analytics.sales.orders WHERE status = 'completed'",
    },
    schema=schema,
    dialect="spark",
)
```

`lineage()` 内部调用 `exp.expand()` 将 `sources` 中的查询内联展开到主查询中，然后继续追踪。

### on_node 回调

`on_node` 参数允许在遍历每个 `Node` 时注入自定义逻辑，例如记录元数据：

```python
def tag_node(node):
    node.payload["owner"] = "data-platform-team"
    node.payload["pii"] = node.name.endswith("_email")

lineage("user_id", sql, schema=schema, dialect="spark", on_node=tag_node)
```

### 全列血缘

当 `column=None` 时，`lineage()` 返回 `dict[str, Node]`，键为每个输出列名：

```python
all_lineage = lineage(column=None, sql=new_sql, schema=schema, dialect="spark")
for col_name, node in all_lineage.items():
    print(f"{col_name}: {[n.name for n in node.downstream]}")
```

### 可视化

`Node.to_html()` 方法基于 vis.js 生成可交互的血缘图：

```python
html = node.to_html(dialect="spark")
# 返回 GraphHTML 对象，可直接在 Jupyter Notebook 中渲染
```

`GraphHTML` 类将 `Node` 树转换为 vis-network 格式的节点和边数据，支持层级布局。

### 内部工作机制

`lineage()` 内部依赖优化器的 `qualify.qualify()` 来规范化标识符和补全表限定名，然后通过 `build_scope()` 构建作用域树。`to_node()` 递归函数负责：

1. 在当前 `Scope` 中找到目标列的 SELECT 表达式
2. 创建 `Node` 并关联到上游
3. 查找表达式中引用的所有 `exp.Column`
4. 如果列来自子查询或 CTE（`Scope` 类型），递归进入该作用域
5. 如果列来自物理表（`exp.Table`），创建叶子节点

对于 `UNION` / `UNION ALL`（`exp.SetOperation`），`to_node()` 会创建一个 `UNION` 节点，然后对每个分支递归追踪。

---

## Step 4：AST 语义 Diff

### diff() 函数

`diff.py` 实现了 Change Distiller 算法，对两棵 AST 做语义级比较：

```python
from sqlglot.diff import diff, Insert, Remove, Update, Move, Keep

changes = diff(old_ast, new_ast)
```

### 编辑操作类型

`diff()` 返回一个编辑脚本（`list[Edit]`），每个元素是以下五种之一：

| 类型 | 含义 | 治理关注点 |
|------|------|-----------|
| `Insert(expression)` | 目标中新增的节点 | 新增列或条件 |
| `Remove(expression)` | 源中被删除的节点 | 删除列或条件 |
| `Update(source, target)` | 节点值被修改 | 逻辑变更（如系数变化） |
| `Move(source, target)` | 节点位置移动 | 通常为结构重构，无功能影响 |
| `Keep(source, target)` | 未变化的节点 | 可忽略 |

### 治理场景：变更分类

```python
from sqlglot.diff import diff, Insert, Remove, Update

changes = diff(old_ast, new_ast, delta_only=True)  # delta_only 排除 Keep 节点

structural_changes = []
logic_changes = []

for change in changes:
    if isinstance(change, Update):
        # Update 意味着同类型节点的值发生了变化
        logic_changes.append(f"逻辑变更: {change.source.sql()} → {change.target.sql()}")
    elif isinstance(change, Insert):
        structural_changes.append(f"新增: {change.expression.sql()}")
    elif isinstance(change, Remove):
        structural_changes.append(f"删除: {change.expression.sql()}")

print("=== 逻辑变更（需要回刷数据） ===")
for c in logic_changes:
    print(c)

print("\n=== 结构变更（需要 schema 校验） ===")
for c in structural_changes:
    print(c)
```

### ChangeDistiller 参数调优

`ChangeDistiller` 类接受两个阈值参数：

- `f`（默认 0.6）：叶子节点匹配的 Dice 系数阈值
- `t`（默认 0.6）：内部节点匹配的子叶相似度阈值

对于治理场景，建议保持默认值。降低阈值会产生更多 `Update`（匹配更宽松），升高阈值会产生更多 `Insert` + `Remove`（匹配更严格）。

### UPDATABLE_EXPRESSION_TYPES

`diff.py` 定义了 `UPDATABLE_EXPRESSION_TYPES`，只有这些类型的节点才会产生 `Update` 编辑：

```python
UPDATABLE_EXPRESSION_TYPES = (
    exp.Alias, exp.Boolean, exp.Column, exp.DataType,
    exp.Lambda, exp.Literal, exp.Table, exp.Window,
)
```

这意味着 `amount * 1.1` 变为 `amount * 1.2` 时，只有 `Literal("1.1")` → `Literal("1.2")` 会被标记为 `Update`，而外层的 `Mul` 节点会被标记为 `Keep`。

### 预匹配节点

当你知道某些子树在新旧 AST 中完全对应时，可以通过 `matchings` 参数提供预匹配对，帮助算法产出更精准的结果：

```python
changes = diff(old_ast, new_ast, matchings=[(old_subtree, new_subtree)])
```

---

## Step 5：AST 序列化与归档

### dump() 与 load()

`serde.py` 提供了 AST 的 JSON 序列化能力，用于审计归档和跨服务传输：

```python
from sqlglot.serde import dump, load
import json

# 序列化
payload = dump(new_ast)
json_str = json.dumps(payload)  # 可存入数据库或消息队列

# 反序列化
restored_ast = load(json.loads(json_str))
assert restored_ast.sql(dialect="spark") == new_ast.sql(dialect="spark")
```

### 序列化格式

`dump()` 将 AST 展平为一个 `list[dict]`，每个字典代表一个节点：

| 键 | 含义 |
|---|------|
| `c` (CLASS) | 节点的类名（如 `Select`、`Column`） |
| `i` (INDEX) | 父节点在列表中的索引 |
| `k` (ARG_KEY) | 在父节点 `args` 中的键名 |
| `a` (IS_ARRAY) | 是否为数组元素 |
| `t` (TYPE) | 类型信息（如果已推断） |
| `o` (COMMENTS) | 注释 |
| `m` (META) | 元数据 |
| `v` (VALUE) | 叶子值（如字面量） |

`load()` 按照索引顺序重建父子关系：先创建根节点，然后依次通过 `parent.set(arg_key, node)` 或 `parent.append(arg_key, node)` 挂载子节点。

### 类型信息保留

`dump()` 会序列化 `node.type`（如果存在），这意味着经过 `annotate_types()` 标注后的 AST 在反序列化后仍然保留类型信息。这对治理场景很重要——你可以在 CI 阶段标注类型，在审批阶段直接使用。

---

## 完整治理流水线示例

将以上模块组合起来，构建一个完整的 Spark SQL 变更校验函数：

```python
import json
from sqlglot import parse_one
from sqlglot.schema import MappingSchema
from sqlglot.lineage import lineage
from sqlglot.diff import diff, Insert, Remove, Update, Move
from sqlglot.serde import dump
from sqlglot.optimizer import annotate_types

def govern_sql_change(
    old_sql: str,
    new_sql: str,
    schema_dict: dict,
    dialect: str = "spark",
) -> dict:
    """校验 Spark SQL 变更，返回治理报告。"""

    # 1. 绑定 schema
    schema = MappingSchema(schema_dict, dialect=dialect)

    # 2. 解析新旧 AST
    old_ast = parse_one(old_sql, dialect=dialect)
    new_ast = parse_one(new_sql, dialect=dialect)

    # 3. 类型标注（用于后续血缘和类型校验）
    old_typed = annotate_types(old_ast, schema=schema, dialect=dialect)
    new_typed = annotate_types(new_ast, schema=schema, dialect=dialect)

    # 4. AST diff
    changes = diff(old_typed, new_typed, delta_only=True)

    report = {
        "inserts": [],
        "removes": [],
        "updates": [],
        "moves": [],
        "lineage_changes": {},
        "schema_violations": [],
        "ast_snapshot": None,
    }

    for change in changes:
        if isinstance(change, Insert):
            report["inserts"].append(change.expression.sql(dialect=dialect))
        elif isinstance(change, Remove):
            report["removes"].append(change.expression.sql(dialect=dialect))
        elif isinstance(change, Update):
            report["updates"].append({
                "from": change.source.sql(dialect=dialect),
                "to": change.target.sql(dialect=dialect),
            })
        elif isinstance(change, Move):
            report["moves"].append(change.source.sql(dialect=dialect))

    # 5. 新 AST 的列级血缘
    new_lineage = lineage(column=None, sql=new_ast, schema=schema, dialect=dialect)
    for col_name, node in new_lineage.items():
        upstream_cols = [n.name for n in node.walk() if n is not node]
        report["lineage_changes"][col_name] = upstream_cols

    # 6. Schema 契约校验：检查新 SQL 的输出列是否都在契约中
    contract_columns = {"user_id", "adjusted_amount"}  # 示例：从契约配置读取
    new_columns = {sel.alias_or_name for sel in new_ast.selects}
    violations = new_columns - contract_columns
    if violations:
        report["schema_violations"] = [
            f"输出列 '{c}' 不在 schema 契约中" for c in violations
        ]

    # 7. 归档新 AST
    report["ast_snapshot"] = dump(new_typed)

    return report
```

调用示例：

```python
schema_dict = {
    "orders": {
        "user_id": "BIGINT",
        "amount": "DECIMAL(10,2)",
        "status": "STRING",
        "created_at": "TIMESTAMP",
    }
}

result = govern_sql_change(old_sql, new_sql, schema_dict)

print(f"新增 {len(result['inserts'])} 个节点")
print(f"删除 {len(result['removes'])} 个节点")
print(f"更新 {len(result['updates'])} 个节点")
print(f"Schema 违规: {result['schema_violations']}")
```

---

## 治理检查清单

将以下检查项集成到 CI/CD 流水线中：

```
□  1. 解析检查：parse_one() 不抛异常（SQL 语法合法）
□  2. Schema 绑定：所有引用的表都能在 MappingSchema 中找到
□  3. 列存在性：schema.has_column() 确认所有引用列都存在
□  4. 类型一致性：annotate_types() 标注后，关键列类型符合契约
□  5. 血缘完整性：lineage() 能追踪到每个输出列的物理表来源
□  6. 变更分类：diff() 区分逻辑变更 vs 结构重构
□  7. 契约符合：输出列集合与 schema 契约一致
□  8. 审计归档：dump() 序列化 AST 存入审计日志
```

---

## 关键 API 速查表

| 函数/类 | 模块 | 用途 |
|---------|------|------|
| `parse_one(sql, dialect)` | `sqlglot` | SQL → AST |
| `MappingSchema(schema, dialect)` | `schema.py` | 创建表结构映射 |
| `schema.add_table(table, column_mapping)` | `schema.py` | 动态注册表 |
| `schema.get_column_type(table, column)` | `schema.py` | 查询列类型 |
| `schema.has_column(table, column)` | `schema.py` | 检查列是否存在 |
| `schema.column_names(table, only_visible)` | `schema.py` | 获取列名列表 |
| `ensure_schema(schema, dialect)` | `schema.py` | dict → Schema 自动转换 |
| `lineage(column, sql, schema, sources)` | `lineage.py` | 列级血缘追踪 |
| `Node.walk()` | `lineage.py` | 遍历血缘链 |
| `Node.to_html(dialect)` | `lineage.py` | 血缘可视化 |
| `diff(source, target, delta_only)` | `diff.py` | AST 语义 diff |
| `Insert / Remove / Update / Move / Keep` | `diff.py` | 编辑操作类型 |
| `ChangeDistiller(f, t)` | `diff.py` | 底层 diff 算法 |
| `dump(expression)` | `serde.py` | AST → JSON |
| `load(payloads)` | `serde.py` | JSON → AST |
| `annotate_types(expression, schema)` | `optimizer` | 类型推断标注 |
