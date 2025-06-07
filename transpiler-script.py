from sqlglot import transpile


def read_sql_file(filepath: str) -> str:
    """
    Reads the contents of a SQL file.

    Args:
        filepath (str): The path to the SQL file.

    Returns:
         str: The entire content of the SQL file as a single string.
    """
    # try:
    with open(filepath, "r", encoding="utf-8") as file:
        sql_content = file.read()
    return sql_content
    # except FileNotFoundError as e:
    #     raise e
    # except IOError as e:
    #     # Catch other I/O related errors (e.g., permission issues) and re-raise them
    #     # You could also wrap this in a custom exception if you prefer
    #     raise IOError(f"An error occurred while reading the file '{filepath}': {e}") from e
    # except Exception as e:
    #     # Catch any other unexpected errors and raise a generic Exception
    #     raise Exception(f"An unexpected error occurred: {e}") from e

    # except FileNotFoundError:
    #     print(f"Error: The file '{filepath}' was not found.")
    #     return None
    # except Exception as e:
    #     print(f"An error occurred while reading the file: {e}")
    #     return None


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
    return "\n".join(transpile(query, input_dialect, output_dialect))


if __name__ == "__main__":
    # read_filepath = ""
    # content = transpile_sql_content(filepath=read_filepath)
    # print("Transpiled file content:")
    # print(content)
    # write_filepath = ""

    databricks_query = """
        SELECT CAST(CAST('2025-04-29 18.47.18' AS DATE) AS TIMESTAMP);
        SELECT DATE_FORMAT(CAST(FROM_UTC_TIMESTAMP(CAST(foo AS TIMESTAMP), 'America/Los_Angeles') AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS foo FROM t;
        DATEDIFF(DAY, created_at, CURRENT_DATE);
    """
    content = transpile_sql_content(query=databricks_query)
    print("Transpiled string query:")
    print(content)
    write_sql_file("./output.sql", content)
