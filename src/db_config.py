# db_config.py
SQL_SERVER_CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=tcp:sql1,1433;Database=llmind;Uid=sa;Pwd=LLMind2025!;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
import pyodbc

def create_database_if_not_exists(server, database_name, username, password):
    """
    Connects to the SQL Server and creates the specified database if it does not exist.

    Args:
        server (str): The server name or address.
        database_name (str): The name of the database to create.
        username (str): The username for the SQL Server.
        password (str): The password for the SQL Server.
    """
    try:
        # Construct the connection string.  Using Trusted_Connection for Windows Authentication
        # connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=master;UID={username};PWD={password};TrustServerCertificate=yes" #removed Trusted_Connection
        # Establish a connection to the SQL Server's master database
        SQL_SERVER_CONNECTION_STRING_FIRST = "Driver={ODBC Driver 17 for SQL Server};Server=tcp:sql1,1433;Database=master;Uid=sa;Pwd=LLMind2025!;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"

        conn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING_FIRST, autocommit=True)
        cursor = conn.cursor()

        # Check if the database exists
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{database_name}'")
        if cursor.fetchone():
            print(f"Database '{database_name}' already exists.")
        else:
            # Create the database if it does not exist
            print(f"Database '{database_name}' does not exist. Creating...")
            cursor.execute(f"CREATE DATABASE {database_name}")
            conn.commit()
            print(f"Database '{database_name}' created successfully.")

    except pyodbc.Error as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Configure the database connection details
    server = "sql1"  # Replace with your SQL Server instance
    database_name = "llmind"  # The name of the database you want to create
    username = "sa"  # Replace with your SQL Server username
    password = "LLMind2025!"  # Replace with your SQL Server password

    # Call the function to create the database if it doesn't exist
    create_database_if_not_exists(server, database_name, username, password)
