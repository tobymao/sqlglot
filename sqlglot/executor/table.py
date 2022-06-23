class Table:
    def __init__(self, *columns, rows=None):
        self.columns = tuple(columns if isinstance(columns[0], str) else columns[0])
        self.rows = rows or []
        if rows:
            assert len(rows[0]) == len(self.columns)
        self.reader = RowReader(self.columns)
        self.range_reader = RangeReader(self)

    def append(self, row):
        assert len(row) == len(self.columns)
        self.rows.append(row)

    def pop(self):
        self.rows.pop()

    @property
    def width(self):
        return len(self.columns)

    def __len__(self):
        return len(self.rows)

    def __iter__(self):
        return TableIter(self)

    def __getitem__(self, index):
        self.reader.row = self.rows[index]
        return self.reader

    def __repr__(self):
        widths = {column: len(column) for column in self.columns}
        lines = [" ".join(column for column in self.columns)]

        for i, row in enumerate(self):
            if i > 10:
                break

            lines.append(
                " ".join(
                    str(row[column]).rjust(widths[column])[0 : widths[column]]
                    for column in self.columns
                )
            )
        return "\n".join(lines)


class TableIter:
    def __init__(self, table):
        self.table = table
        self.index = -1

    def __iter__(self):
        return self

    def __next__(self):
        self.index += 1
        if self.index < len(self.table):
            return self.table[self.index]
        raise StopIteration


class RangeReader:
    def __init__(self, table):
        self.table = table
        self.range = range(0)

    def __len__(self):
        return len(self.range)

    def __getitem__(self, column):
        return (self.table[i][column] for i in self.range)


class RowReader:
    def __init__(self, columns):
        self.columns = {column: i for i, column in enumerate(columns)}
        self.row = None

    def __getitem__(self, column):
        return self.row[self.columns[column]]
