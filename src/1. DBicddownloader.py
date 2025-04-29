import requests
import json
from typing import List, Dict
import pyodbc  # For connecting to SQL Server
import db_config


# -----------------------------------------------------------------------------
# Configuration Section
# -----------------------------------------------------------------------------

# API base URL and endpoint templates
BASE_URI_TEMPLATE = 'http://llmind-icd-api-1/icd/release/11/2025-01/mms/{}?include=diagnosticCriteria'
ROOT_URI = 'http://llmind-icd-api-1/icd/release/11/2025-01/mms'

# HTTP headers for API requests
HEADERS = {
    'Accept': 'application/json',
    'Accept-Language': 'en',
    'API-Version': 'v2'
}

# Database connection string.  **Replace with your actual connection details.**
# Important:  Store this securely, e.g., in environment variables.  DO NOT hardcode in production code.
# Example (using Windows Authentication - trusted connection):
# SQL_SERVER_CONNECTION_STRING = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server_name;DATABASE=your_database_name;TRUSTED_CONNECTION=yes;"
# Example (using SQL Server Authentication):
SQL_SERVER_CONNECTION_STRING = db_config.SQL_SERVER_CONNECTION_STRING

# Note:  You might need to adjust the DRIVER.  "ODBC Driver 17 for SQL Server" is common, but check your system.

# -----------------------------------------------------------------------------
# Data Retrieval Functions
# -----------------------------------------------------------------------------

def retrieve_code(uri: str, session: requests.Session, results: List[Dict]) -> None:
    """
    Recursively retrieves ICD code data from the API starting at the given URI.
    Processes each node: if the node is a 'category' without children (i.e. a leaf node),
    its details are saved into the results list. If the node contains children, each child
    URI is constructed and processed recursively.

    Args:
        uri (str): The API endpoint URI for the current ICD node.
        session (requests.Session): A persistent session for HTTP requests.
        results (List[Dict]): A list to accumulate the processed ICD entries.
    """
    try:
        response = session.get(uri, headers=HEADERS, verify=False)
        response.raise_for_status()  # Raise an error for HTTP error codes
        data = response.json()
    except Exception as e:
        print(f"Error retrieving data from {uri}: {e}")
        return

    # If this node is a leaf category (i.e., it has no 'child' field), process and record it.
    if data.get('classKind') == 'category' and 'child' not in data:
        entry = {
            'code': data.get('code', '').replace(";", "~"),
            'title': data.get('title', {}).get('@value', '').replace(";", "~"),
            'definition': data.get('definition', {}).get('@value', '').replace(";", "~"),
            'longdefinition': data.get('longdefinition', {}).get('@value', '').replace(";", "~"),
            'inclusions': "; ".join([inc.get('label', {}).get('@value', '').replace(";", "~") for inc in data.get('inclusion', [])]),
            'exclusions': "; ".join([exc.get('label', {}).get('@value', '').replace(";", "~") for exc in data.get('exclusion', [])]),
            'diagnosticCriteria': data.get('diagnosticCriteria', {}).get('@value', '').replace(";", "~"),
        }
        results.append(entry)

    # If the node contains children, build their URIs and recursively process each one.
    if 'child' in data:
        for child in data['child']:
            # The unique identifier is extracted from the child URL string.
            child_id = child.split("/mms/")[-1]
            child_uri = BASE_URI_TEMPLATE.format(child_id)
            retrieve_code(child_uri, session, results)

# -----------------------------------------------------------------------------
# Database Interaction Functions
# -----------------------------------------------------------------------------

def create_table_if_not_exists(connection_string: str) -> None:
    """
    Creates the ICD-11 table in the SQL Server database if it does not already exist.

    Args:
        connection_string (str): The connection string for the SQL Server database.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Check if the table exists
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'ICD11_Codes'")
        table_exists = cursor.fetchone()

        if not table_exists:
            # Create the table with appropriate data types.  Use NVARCHAR for Unicode support.
            cursor.execute("""
                CREATE TABLE ICD11_Codes (
                    code NVARCHAR(255) PRIMARY KEY,
                    title NVARCHAR(MAX),
                    definition NVARCHAR(MAX),
                    longdefinition NVARCHAR(MAX),
                    inclusions NVARCHAR(MAX),
                    exclusions NVARCHAR(MAX),
                    diagnosticCriteria NVARCHAR(MAX)
                )
            """)
            cnxn.commit()
            print("ICD11_Codes table created successfully.")
        else:
            print("ICD11_Codes table already exists.")
        cursor.close()
        cnxn.close()
    except Exception as e:
        print(f"Error creating table: {e}")
        # Consider raising the exception or handling it more robustly (e.g., logging, retrying).
        raise  # Re-raise to stop execution if table creation fails.

def insert_data_into_table(connection_string: str, data: List[Dict]) -> None:
    """
    Inserts the ICD-11 code data into the SQL Server table.

    Args:
        connection_string (str): The connection string for the SQL Server database.
        data (List[Dict]): A list of dictionaries, where each dictionary represents an ICD-11 code entry.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Use a parameterized query to prevent SQL injection.
        sql = """
            INSERT INTO ICD11_Codes (code, title, definition, longdefinition, inclusions, exclusions,diagnosticCriteria)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        for row in data:
            try:
                cursor.execute(sql, (
                    row['code'],
                    row['title'],
                    row['definition'],
                    row['longdefinition'],
                    row['inclusions'],
                    row['exclusions'],
                    row['diagnosticCriteria']
                ))
            except pyodbc.Error as e:
                print(f"Error inserting row: {e}.  Row data: {row}")
                cnxn.rollback() # Rollback the current transaction on error.
                continue #  Consider whether to continue or stop on error. Here I continue to the next row.

        cnxn.commit()
        print(f"{len(data)} rows successfully inserted into ICD11_Codes.")
        cursor.close()
        cnxn.close()
    except Exception as e:
        print(f"Error inserting data: {e}")
        #  Consider more sophisticated error handling (e.g., logging, retrying)
        raise  # Re-raise to stop execution after logging

# -----------------------------------------------------------------------------
# Main Execution Function
# -----------------------------------------------------------------------------

def main():
    """
    Main function to:
    1. Retrieve ICD code data starting from the ROOT_URI.
    2. Create the ICD-11 table in the SQL Server database.
    3. Insert the retrieved data into the SQL Server table.
    """
    results: List[Dict] = []

    # Using a persistent session for better performance (reuse connections)
    with requests.Session() as session:
        retrieve_code(ROOT_URI, session, results)

    # Create the table if it doesn't exist.
    create_table_if_not_exists(SQL_SERVER_CONNECTION_STRING)

    # Insert the data into the SQL Server table.
    insert_data_into_table(SQL_SERVER_CONNECTION_STRING, results)

if __name__ == "__main__":
    main()