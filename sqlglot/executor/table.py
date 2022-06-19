class Table:
    def __init__(self, *columns, _columnar=True, **table):
        if table:
            self.data = table
        else:
            self.data = {column: [] for column in columns}
        self.columns = tuple(self.data)
        self.column_indicies = dict(enumerate(self.columns))
        self.width = len(self.columns)
        self.length = len(tuple(self.data.values())[0])
        self.reader = (
            ColumnarReader(self.data) if _columnar else RowReader(self.columns)
        )
        self.range_reader = RangeReader(self.data)

    def add(self, row):
        for i in range(self.width):
            self.data[self.column_indicies[i]].append(row[i])
        self.length += 1

    def pop(self):
        for column in self.data.values():
            column.pop()
        self.length -= 1

    def __iter__(self):
        return TableIter(self)

    def __getitem__(self, column):
        return self.data[column]

    def __repr__(self):
        widths = {column: len(column) for column in self.data}
        lines = [" ".join(column for column in self.data)]

        for i, row in enumerate(self):
            if i > 10:
                break

            lines.append(
                " ".join(
                    str(row[column]).rjust(widths[column])[0 : widths[column]]
                    for column in self.data
                )
            )
        return "\n".join(lines)


class TableIter:
    def __init__(self, data_table):
        self.data_table = data_table
        self.index = -1

    def __iter__(self):
        return self

    def __next__(self):
        self.index += 1
        if self.index < self.data_table.length:
            self.data_table.reader.row = self.index
            return self.data_table.reader
        raise StopIteration


class RangeReader:
    def __init__(self, table):
        self.table = table
        self.range = range(0)

    def __len__(self):
        return len(self.range)

    def __getitem__(self, column):
        return (self.table[column][i] for i in self.range)


class ColumnarReader:
    def __init__(self, table):
        self.table = table
        self.row = None

    def tuple(self):
        return tuple(self[column] for column in self.table)

    def __getitem__(self, column):
        return self.table[column][self.row]


class RowReader:
    def __init__(self, columns):
        self.columns = {column: i for i, column in enumerate(columns)}
        self.row = None

    def tuple(self):
        return tuple(self.row)

    def __getitem__(self, column):
        return self.row[self.columns[column]]
