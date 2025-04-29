import pandas as pd
import pyodbc  # For connecting to SQL Server
import logging
from typing import List  # Added import
import re
import db_config # Import db_config
from difflib import SequenceMatcher

# ------------------------------
# Configuration
# ------------------------------

# Path to the CSV file
csv_file_path = "./data/input/kg.csv"  # Update the path

# Database connection string (replace with your actual connection details)
SQL_SERVER_CONNECTION_STRING = db_config.SQL_SERVER_CONNECTION_STRING # Use the connection string from db_config

# Table name in SQL Server
table_name = "KGPrime_db"  # You can change this if needed

# CSV Header
csv_header = ['relation', 'display_relation', 'x_index', 'x_id', 'x_type', 'x_name', 'x_source', 'y_index', 'y_id', 'y_type', 'y_name', 'y_source']
icd11_table_name = "ICD11_Codes"
icd11_chapter_6_code = "6"
icd11_match_threshold = 0.8

# ------------------------------
# Logging Setup
# ------------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------------------
# Helper Functions
# ------------------------------

def create_table_if_not_exists(connection_string: str, table_name: str, header: List[str]) -> None:
    """
    Creates the specified table in SQL Server if it does not already exist.  Uses the provided header
    to define the table schema.  All columns are created as NVARCHAR(MAX).

    Args:
        connection_string (str): The SQL Server connection string.
        table_name (str): The name of the table to create.
        header (list): A list of column names for the table.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Check if the table exists
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = ?", (table_name,))
        table_exists = cursor.fetchone()

        if not table_exists:
            # Construct the CREATE TABLE statement.  All columns are NVARCHAR(MAX)
            columns = ", ".join(f"[{col}] NVARCHAR(MAX)" for col in header)
            create_table_sql = f"CREATE TABLE {table_name} ({columns})"

            cursor.execute(create_table_sql)
            cnxn.commit()
            logging.info(f"Table {table_name} created successfully.")
        else:
            logging.info(f"Table {table_name} already exists.")
        cursor.close()
        cnxn.close()
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")
        raise  # Re-raise the exception to be handled in main()



def insert_data_into_table(connection_string: str, table_name: str, df: pd.DataFrame, chunk_size: int = 10000) -> None:
    """
    Inserts the data from the Pandas DataFrame into the specified SQL Server table in chunks.
    Uses fast_executemany for efficient insertion of large datasets.

    Args:
        connection_string (str): The SQL Server connection string.
        table_name (str): The name of the table to insert data into.
        df (pd.DataFrame): The Pandas DataFrame containing the data to insert.
        chunk_size (int, optional): The number of rows to insert in each chunk. Defaults to 10000.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()
        total_rows = len(df)
        logging.info(f"Inserting {total_rows} rows into {table_name} in chunks of {chunk_size}.")

        for i in range(0, total_rows, chunk_size):
            chunk_end = min(i + chunk_size, total_rows)
            df_chunk = df.iloc[i:chunk_end]  # Get a chunk of the DataFrame
            data = [tuple(row) for row in df_chunk.itertuples(index=False)]  # Convert DataFrame chunk to a list of tuples

            # Prepare the SQL insert statement.
            placeholders = ", ".join("?" * len(df.columns))
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

            cursor.fast_executemany = True
            cursor.executemany(insert_sql, data)
            cnxn.commit()
            logging.info(f"Inserted rows {i + 1} to {chunk_end} into {table_name}.")

        cursor.close()
        cnxn.close()
        logging.info("Data transfer to SQL Server table complete.")

    except Exception as e:
        logging.error(f"Error inserting data into table {table_name}: {e}")
        cnxn.rollback()
        raise  # Re-raise the exception to be handled in main()



def main():
    """
    Reads the CSV file into a Pandas DataFrame, filters the DataFrame and
    creates the table in SQL Server (if it doesn't exist), and inserts
    the data into the table.
    """
    try:
        # 1. Read the CSV file into a Pandas DataFrame
        logging.info(f"Reading CSV file from {csv_file_path}")
        df = pd.read_csv(csv_file_path, encoding='utf-8', header=0)
        logging.info(f"CSV data read into Pandas DataFrame with {len(df)} rows and columns: {', '.join(df.columns)}.")

        # 2. Filter the DataFrame for rows where 'relation' is 'indication'
        logging.info("Filtering DataFrame: relation == 'indication'")
        df_filtered = df[df["relation"] == "indication"]
        logging.info(f"DataFrame filtered to {len(df_filtered)} rows.")

        # 3. Connect to SQL Server to query the ICD-11 table
        conn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
        cursor = conn.cursor()

        # 4. Fetch the titles of diseases from ICD-11 Chapter 6
        logging.info(f"Fetching diseases from {icd11_table_name} in Chapter 6")
        cursor.execute(f"SELECT title FROM {icd11_table_name} WHERE code LIKE '{icd11_chapter_6_code}%'")
        icd11_chapter_6_diseases = {row[0].lower() for row in cursor.fetchall()}  # Store titles in a set, for faster matching, and lower case them
        logging.info(f"Found {len(icd11_chapter_6_diseases)} diseases in ICD-11 Chapter 6.")

        # 5. Further filter the DataFrame to include only diseases from ICD-11 Chapter 6
        def find_closest_match(disease_name: str, icd11_diseases: List[str], threshold:float) -> str:
            """Checks if a disease name is present in the set of ICD-11 Chapter 6 diseases."""
            best_match = None
            best_similarity = 0
            for icd11_disease in icd11_diseases:
                similarity = SequenceMatcher(None, disease_name.lower(), icd11_disease).ratio()
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = icd11_disease
            if best_similarity >= threshold:
                return best_match
            else:
                return None

        def is_psychological_disease(disease_name: str) -> bool:
            """Checks if a disease name is present in the set of ICD-11 Chapter 6 diseases."""
            match = find_closest_match(disease_name, icd11_chapter_6_diseases, icd11_match_threshold)
            return match is not None

        logging.info("Filtering DataFrame by ICD-11 Chapter 6 diseases")
        df_filtered = df_filtered[df_filtered["y_name"].apply(is_psychological_disease)]
        logging.info(f"DataFrame filtered to {len(df_filtered)} rows after ICD-11 Chapter 6 filtering.")

        # 5. Create the table in SQL Server (if it doesn't exist)
        create_table_if_not_exists(SQL_SERVER_CONNECTION_STRING, table_name, csv_header)

        # 6. Insert the data into the SQL Server table
        insert_data_into_table(SQL_SERVER_CONNECTION_STRING, table_name, df_filtered)

        logging.info("Data transfer to SQL Server complete.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        #  Consider more sophisticated error handling (e.g., logging, user notification)

if __name__ == "__main__":
    main()
