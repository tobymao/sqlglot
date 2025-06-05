from sqlglot import transpile


def read_sql_file(filepath: str) -> str:
    """
    Reads the contents of a SQL file.

    Args:
        filepath (str): The path to the SQL file.

    Returns:
         str: The entire content of the SQL file as a single string.
    """
    with open(filepath, "r", encoding="utf-8") as file:
        sql_content = file.read()
    return sql_content


def write_sql_file(filepath: str, sql_content: str):
    """
    Writes a string containing SQL content to a specified file.
    If the file exists, it will be overwritten.

    Args:
        filepath (str): The path to the SQL file where the content will be written.
        sql_content (str): The string containing the SQL code to write.
    """
    with open(filepath, "w", encoding="utf-8") as file:
        file.write(sql_content)
    print(f"Successfully wrote SQL content to '{filepath}'")


def transpile_sql_content(
    filepath: str | None = None,
    query: str | None = None,
    input_dialect: str = "databricks",
    output_dialect: str = "exasol",
):
    """
    Transpiles SQL from one dialect to another dialect.

    Args:
        filepath (str): The filepath to a SQL file. Has precedence over query.
        query (str): SQL content as string. Default None.
        input_dialect (str): Dialect of the input content.
        output_dialect(str): Dialect of the output content.
    Returns:
        str: The query transpiled to the output_dialect.
    """
    if filepath is not None:
        query = read_sql_file(filepath)
    if query is None:
        raise ValueError("Either function arg filepath or query needs to be provided.")
    return ";\n".join(transpile(query, input_dialect, output_dialect))


if __name__ == "__main__":
    input_dialect = "databricks"
    output_dialect = "exasol"
    read_filepath = "./databricks"
    # insert queries to transpile here
    databricks_query = """
        DATEDIFF(DAY, created_at, CURRENT_DATE);
    """
    content = transpile_sql_content(
        # filepath=read_filepath + ".sql", # use path to sql file instead of string
        query=databricks_query,
        input_dialect=input_dialect,
        output_dialect=output_dialect,
    )
    # write the transpiled content to a file
    write_filepath = read_filepath + "_to_" + output_dialect + ".sql"
    write_sql_file(write_filepath, content)
