from typing import Dict, List, Set
from sqlglot import expressions
from sqlglot.helper import ensure_list
from sqlglot.errors import ErrorLevel, SqlglotError

class PerformanceWarning(SqlglotError):
    """
    Custom warning class to handle performance-related issues.
    """
    pass

class PerformanceAnalyzer:
    """
    Scans a SQLGlot Expression (AST) and logs or raises warnings about
    potential performance issues—e.g., SELECT *, unindexed column usage, etc.
    """

    def __init__(
        self,
        schema: Dict[str, List[str]],
        indexes: Dict[str, Set[str]],
        error_level=ErrorLevel.WARN,
    ):
        """
        :param schema: Table -> list of columns. E.g. {"users": ["id", "name", "email"]}
        :param indexes: Table -> set of indexed columns. E.g. {"users": {"id"}}
        :param error_level: RAISE, WARN, or IGNORE. By default we WARN for performance issues.
        """
        self.schema = {t.lower(): [c.lower() for c in cols] for t, cols in schema.items()}
        self.indexes = {t.lower(): {c.lower() for c in cols} for t, cols in indexes.items()}
        self.error_level = error_level

    def analyze(self, expression: expressions.Expression) -> None:
        """
        Public method to run the performance checks on the entire AST.
        """
        self._analyze_expression(expression, scope={})

    def _analyze_expression(self, expr: expressions.Expression, scope: Dict[str, str]) -> None:
        """
        Recursively walk through the AST, checking for performance issues.
        """
        if isinstance(expr, expressions.Select):
            select_items = ensure_list(expr.args.get("expressions"))
            for item in select_items:
                if isinstance(item, expressions.Star):
                    self._handle_warning(
                        "Query uses SELECT * — consider selecting specific columns for better performance."
                    )

            from_expr = expr.args.get("from")
            if from_expr:
                table_aliases = self._extract_table_aliases(from_expr)
                scope.update(table_aliases)

            where_expr = expr.args.get("where")
            if where_expr:
                self._check_where_for_indexes(where_expr, scope)

        for key, child in expr.args.items():
            if isinstance(child, expressions.Expression):
                self._analyze_expression(child, dict(scope))
            elif isinstance(child, list):
                for subchild in child:
                    if isinstance(subchild, expressions.Expression):
                        self._analyze_expression(subchild, dict(scope))

    def _extract_table_aliases(self, from_expr: expressions.Expression) -> Dict[str, str]:
        """
        Extract table alias info from a FROM or JOIN expression.
        """
        scope_map = {}
        for node in from_expr.find_all(expressions.Table):
            table_name_expr = node.args.get("this")
            alias_expr = node.args.get("alias")
            if table_name_expr and isinstance(table_name_expr, expressions.Identifier):
                table_name_str = table_name_expr.name.lower()
                alias_str = alias_expr.name.lower() if alias_expr else table_name_str
                scope_map[alias_str] = table_name_str
        return scope_map

    def _check_where_for_indexes(self, where_expr: expressions.Expression, scope: Dict[str, str]):
        """
        Traverse the WHERE expression to detect columns that are not indexed.
        """
        for col in where_expr.find_all(expressions.Column):
            table_part = col.args.get("table")
            column_part = col.args.get("this")
            if table_part and isinstance(table_part, expressions.Identifier):
                table_alias = table_part.name.lower()
                if column_part and isinstance(column_part, expressions.Identifier):
                    column_name = column_part.name.lower()
                    if table_alias in scope:
                        actual_table_name = scope[table_alias]
                        if not self._column_indexed(actual_table_name, column_name):
                            self._handle_warning(
                                f"Column '{column_name}' in table '{actual_table_name}' "
                                f"(alias '{table_alias}') is used in WHERE but not indexed."
                            )
            else:
                if column_part and isinstance(column_part, expressions.Identifier):
                    column_name = column_part.name.lower()
                    if len(scope) == 1:
                        (_, actual_table_name) = next(iter(scope.items()))
                        if not self._column_indexed(actual_table_name, column_name):
                            self._handle_warning(
                                f"Column '{column_name}' in table '{actual_table_name}' "
                                f"is used in WHERE but not indexed."
                            )

    def _column_indexed(self, table_name: str, column: str) -> bool:
        """
        Check if a given column is in the indexes dictionary for this table.
        """
        indexed_cols = self.indexes.get(table_name, set())
        return column in indexed_cols

    def _handle_warning(self, message: str) -> None:
        """
        Depending on error_level, either raise or print a performance warning.
        """
        if self.error_level == ErrorLevel.RAISE:
            raise PerformanceWarning(message)
        elif self.error_level == ErrorLevel.WARN:
            print(f"[PERF WARNING] {message}")
        
