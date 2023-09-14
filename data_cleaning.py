import logging
import re
import time
import pandas as pd
from data_processing import infer_and_set_datatypes
import random
import string


logging.basicConfig(filename='/tmp/app.log', level=logging.DEBUG)

def sanitize_column_name(name):
    """
    Sanitize column names.
    - Replace spaces with underscores.
    - Remove special characters except underscores.
    - Convert to lowercase.
    - Remove leading and trailing underscores.

    Parameters:
        name (str): Original column name.

    Returns:
        str: Sanitized column name.
    """
    sanitized_name = re.sub(r'[^\w\s_]', '', name)  # remove special characters but keep underscores and spaces
    sanitized_name = sanitized_name.replace(' ', '_').lower()
    sanitized_name = sanitized_name.strip('_')  # remove leading and trailing underscores
    return sanitized_name


def escape_string(value):
    return value.replace("'", "''")

def handle_missing_data(conn, relation_name):
    """
    This function handles missing data in a relation. It drops columns and rows where all values are missing, 
    and imputes missing values column-wise.

    Parameters:
        conn (Connection): The DuckDB connection object.
        relation_name (str): The name of the relation to be cleaned.

    Returns:
        new_relation_name (str): The name of the cleaned relation.
    """

    # Get the column names and types
    columns = [desc[0] for desc in conn.execute(f"DESCRIBE {relation_name}").fetchall()]
    types = [desc[1] for desc in conn.execute(f"DESCRIBE {relation_name}").fetchall()]

    # Drop columns where all values are missing
    for column in columns.copy():
        count_not_null = conn.execute(f"SELECT COUNT({column}) FROM {relation_name} WHERE {column} IS NOT NULL").fetchone()[0]
        if count_not_null == 0:
            conn.execute(f"ALTER TABLE {relation_name} DROP COLUMN {column}")
            columns.remove(column)

    # Check if all columns are deleted
    if not columns:
        logging.warning(f"All columns are missing in relation {relation_name}. The relation will be empty.")
        return relation_name

    # Delete rows where all values are missing
    condition = " AND ".join([f"{col} IS NULL" for col in columns])
    conn.execute(f"DELETE FROM {relation_name} WHERE {condition}")

    # Check if all rows are deleted
    count_rows = conn.execute(f"SELECT COUNT(*) FROM {relation_name}").fetchone()[0]
    if count_rows == 0:
        logging.warning(f"All rows are missing in relation {relation_name}. The relation will be empty.")
        return relation_name

    # Create a DataFrame from the relation
    df = conn.execute(f"SELECT * FROM {relation_name}").fetch_df()

    # Handle missing values column-wise
    for column, type in zip(columns, types):
        # Check if column is numeric or not based on the type
        is_numeric = type in ('INTEGER', 'BIGINT', 'DOUBLE', 'DECIMAL')

        if is_numeric:
            # Use median as the imputation value
            median_value = conn.execute(f"SELECT MEDIAN({column}) FROM {relation_name}").fetchone()[0]
            df[column] = df[column].fillna(median_value)
        else:
            # Use mode as the imputation value
            mode_values_query = f"""
                SELECT {column}, COUNT(*) as count
                FROM {relation_name}
                GROUP BY {column}
                ORDER BY count DESC
            """
            mode_values = conn.execute(mode_values_query).fetchall()
            mode_value = mode_values[0][0] if mode_values else None
            df[column] = df[column].fillna(mode_value) if mode_value else df[column].fillna('')

    # Create a new relation from the updated DataFrame
    new_relation_name = f"{relation_name}_cleaned"
    conn.register('df', df)  # Registering the DataFrame before creating a table
    conn.execute(f"CREATE TEMPORARY TABLE {new_relation_name} AS SELECT * FROM df")

    # Return the name of the new relation
    return new_relation_name


def handle_outliers(conn, relation_name, z_score_threshold=3):
    """
    Handle outliers in a DuckDB relation.
    Remove rows that have a z-score greater than the specified threshold.
    Non-numeric columns and columns with zero standard deviation are skipped.

    Parameters:
        conn: DuckDB connection object.
        relation_name (str): The name of the relation/table to be cleaned.
        z_score_threshold (float): The z-score threshold for outlier detection. Default is 3.

    Returns:
        relation_name (str): The name of the cleaned relation/table.
    """

    columns_info = conn.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{relation_name}'").fetchall()
    z_score_clauses = []

    for column, data_type in columns_info:
        if data_type in ['INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE']:
            avg_col = conn.execute(f"SELECT AVG({column}) FROM {relation_name}").fetchone()[0]
            stddev_col = conn.execute(f"SELECT STDDEV_SAMP({column}) FROM {relation_name}").fetchone()[0]

            # Skip columns with zero or null standard deviation
            if not stddev_col:
                continue

            z_score_clause = f"ABS({column} - {avg_col}) / {stddev_col} < {z_score_threshold}"
            z_score_clauses.append(z_score_clause)

    if z_score_clauses:
        z_score_filter = " AND ".join(z_score_clauses)
        conn.execute(f"DELETE FROM {relation_name} WHERE NOT ({z_score_filter})")

    return relation_name

def clean_and_store_data(file_path, file, conn):
    """
    This function cleans and stores data from a CSV file. It loads the data into a relation,
    applies data cleaning methods, infers and sets data types, generates a unique upload ID,
    and stores the relation into the in-memory DuckDB database.

    Parameters:
        file_path (str): The path of the CSV file to be cleaned and stored.
        file (FileStorage): The file to be cleaned and stored.
        conn (duckdb.DuckDBPyConnection): The DuckDB in-memory database connection.

    Returns:
        upload_id (str): The unique ID for the uploaded file.
        sample_data (dict): A sample of the cleaned data.
    """

    # Unique temporary table suffix
    temp_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    
    # Check if file is a valid CSV
    try:
        df = pd.read_csv(file_path)
    except pd.errors.ParserError:
        logging.error("Invalid CSV file format.")
        return None, None

    # Generate unique temporary relation name
    filename = re.sub(r'\W|^(?=\d)', '', file.filename.rsplit('.', 1)[0].replace(' ', '_')).lower()
    timestamp = str(int(time.time()))
    upload_id = f"{filename}_{timestamp}_{temp_suffix}"
    relation_name = f"tmp_{upload_id}"

    # Load CSV into a temporary DuckDB relation
    conn.execute(f"CREATE TEMPORARY TABLE {relation_name} AS SELECT * FROM read_csv_auto('{file_path}',  header=True)")

    # Sanitize column names
    columns_info = conn.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{relation_name}'").fetchall()
    for column_info in columns_info:
        old_column_name = column_info[0]
        new_column_name = sanitize_column_name(old_column_name)
        conn.execute(f"ALTER TABLE {relation_name} RENAME COLUMN \"{old_column_name}\" TO {new_column_name}")

    # Apply data cleaning
    relation_after_missing_data = handle_missing_data(conn, relation_name)
    relation_after_outliers = handle_outliers(conn, relation_after_missing_data)

    # Infer and set datatypes using the previously discussed methodology
    infer_and_set_datatypes(conn, relation_after_outliers)

    # Move this cleaned data to a non-temporary table
    conn.execute(f"CREATE TABLE {upload_id} AS SELECT * FROM {relation_after_outliers}")

    # Fetch a sample for the return
    sample_data = conn.execute(f"SELECT * FROM {upload_id} LIMIT 100").fetch_df()

    # Convert boolean columns to integers
    sample_data = sample_data.astype({col: int for col in sample_data.columns if sample_data[col].dtype == 'bool'})

    # Cleanup: Drop the temporary table
    conn.execute(f"DROP TABLE {relation_name}")

    return upload_id, sample_data
