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

    def table_iter(self, table):
        for i in range(self.tables[table].length):
            self.set_row(table, i)
            yield self

    def sort(self, table, key):
        def _sort(i):
            self.set_row(table, i)
            return key(self)

        data_table = self.tables[table]
        index = list(range(data_table.length))
        index.sort(key=_sort)

        for column, rows in data_table.data.items():
            data_table.data[column] = [rows[i] for i in index]

    def set_row(self, table, row):
        self.row_readers[table].row = row
        self.env["scope"] = self.row_readers

    def set_range(self, table, start, end):
        self.range_readers[table].range = range(start, end)
        self.env["scope"] = self.range_readers

    def __getitem__(self, table):
        return self.env["scope"][table]

    def __contains__(self, table):
        return table in self.tables
