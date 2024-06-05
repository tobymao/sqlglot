import typing as t


def border(columns: t.Iterable[str]) -> str:
    columns = " | ".join(columns)
    return f"| {columns} |"


def ascii_table(table: list[dict[str, t.Any]]) -> str:
    columns = []
    for row in table:
        for key in row:
            if key not in columns:
                columns.append(key)

    widths = {column: max(len(column), 15) for column in columns}

    lines = [
        border(column.rjust(width) for column, width in widths.items()),
        border(str("-" * width) for width in widths.values()),
    ]

    for row in table:
        lines.append(
            border(str(row[column]).rjust(width)[0:width] for column, width in widths.items())
        )

    return "\n".join(lines)
