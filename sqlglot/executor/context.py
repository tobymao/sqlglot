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
        self.range_readers = {
            name: table.range_reader for name, table in self.tables.items()
        }
        self.row_readers = {name: table.reader for name, table in tables.items()}
        self.env = {**(env or {}), "scope": self.row_readers}

    def eval(self, code):
        # pylint: disable=eval-used
        return eval(code, ENV, self.env)

    def eval_tuple(self, codes):
        return tuple(self.eval(code) for code in codes)

    def __iter__(self):
        return self.table_iter(list(self.tables)[0])

    def table_iter(self, table):
        self.env["scope"] = self.row_readers

        for reader in self.tables[table]:
            yield reader, self

    def sort(self, table, key):
        table = self.tables[table]

        def sort_key(row):
            table.reader.row = row
            return self.eval_tuple(key)

        table.rows.sort(key=sort_key)

    def set_row(self, table, row):
        self.row_readers[table].row = row
        self.env["scope"] = self.row_readers

    def set_index(self, table, index):
        self.row_readers[table].row = self.tables[table].rows[index]
        self.env["scope"] = self.row_readers

    def set_range(self, table, start, end):
        self.range_readers[table].range = range(start, end)
        self.env["scope"] = self.range_readers

    def __getitem__(self, table):
        return self.env["scope"][table]

    def __contains__(self, table):
        return table in self.tables
