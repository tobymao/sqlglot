from sqlglot.executor.env import ENV


class Context:
    """
    Execution context for sql expressions.

    Context is used to hold relevant data tables which can then be queried on with eval.

    References to columns can either be scalar or vectors. When set_row is used, column references
    evaluate to scalars while set_range evaluates to vectors. This allows convenient and efficient
    evaluation of aggregation functions.
    """

    def __init__(self, tables, env=None):
        """
        Args
            tables (dict): table_name -> Table, representing the scope of the current execution context
            env (Optional[dict]): dictionary of functions within the execution context
        """
        self.tables = tables
        self._table = None
        self.range_readers = {name: table.range_reader for name, table in self.tables.items()}
        self.row_readers = {name: table.reader for name, table in tables.items()}
        self.env = {**(env or {}), "scope": self.row_readers}

    def eval(self, code):
        return eval(code, ENV, self.env)

    def eval_tuple(self, codes):
        return tuple(self.eval(code) for code in codes)

    @property
    def table(self):
        if self._table is None:
            self._table = list(self.tables.values())[0]
            for other in self.tables.values():
                if self._table.columns != other.columns:
                    raise Exception(f"Columns are different.")
                if len(self._table.rows) != len(other.rows):
                    raise Exception(f"Rows are different.")
        return self._table

    @property
    def columns(self):
        return self.table.columns

    def __iter__(self):
        self.env["scope"] = self.row_readers
        for i in range(len(self.table.rows)):
            for table in self.tables.values():
                reader = table[i]
            yield reader, self

    def table_iter(self, table):
        self.env["scope"] = self.row_readers

        for reader in self.tables[table]:
            yield reader, self

    def sort(self, key):
        table = self.table

        def sort_key(row):
            table.reader.row = row
            return self.eval_tuple(key)

        table.rows.sort(key=sort_key)

    def set_row(self, row):
        for table in self.tables.values():
            table.reader.row = row
        self.env["scope"] = self.row_readers

    def set_index(self, index):
        for table in self.tables.values():
            table[index]
        self.env["scope"] = self.row_readers

    def set_range(self, start, end):
        for name in self.tables:
            self.range_readers[name].range = range(start, end)
        self.env["scope"] = self.range_readers

    def __contains__(self, table):
        return table in self.tables
