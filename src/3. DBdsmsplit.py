import re
from pathlib import Path
import pyodbc  # For database interaction
import db_config  # Import the db_config.py file

# ------------------------------
# Configuration
# ------------------------------------------------------

# Path to the input text file
input_txt_path = Path("./data/input/DSM-5-TR_Clinical_Cases.txt")

# Database connection string (replace with your actual connection details)
SQL_SERVER_CONNECTION_STRING = db_config.SQL_SERVER_CONNECTION_STRING  # Use the connection string from db_config.py

# ------------------------------
# Helper Functions
# ------------------------------

def create_table_if_not_exists(connection_string: str) -> None:
    """
    Creates the DSM5_Cases table in the SQL Server database if it does not already exist.

    Args:
        connection_string (str): The connection string for the SQL Server database.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Check if the table exists
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'DSM5_Cases'")
        table_exists = cursor.fetchone()

        if not table_exists:
            # Create the table with appropriate data types.  Use NVARCHAR for Unicode support and specify collation for all NVARCHAR columns.
            cursor.execute("""
                CREATE TABLE DSM5_Cases (
                    Case_Number INT PRIMARY KEY,
                    Introduction NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS,
                    Discussion NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS,
                    Diagnosis NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS
                )
            """)
            cnxn.commit()
            print("DSM5_Cases table created successfully.")
        else:
            print("DSM5_Cases table already exists.")
        cursor.close()
        cnxn.close()
    except Exception as e:
        print(f"Error creating table: {e}")
        raise  # Re-raise to stop execution if table creation fails.


def insert_or_update_case_data(connection_string: str, case_number: int, introduction: str, discussion: str, diagnosis: str) -> None:
    """
    Inserts or updates a single case's data in the DSM5_Cases table.
    It checks if the case_number exists.  If it does, it updates the row, otherwise it inserts a new row.

    Args:
        connection_string (str): The connection string for the SQL Server database.
        case_number (int): The case number.
        introduction (str): The introduction text.
        discussion (str): The discussion text.
        diagnosis (str): The diagnosis text.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Check if the case_number already exists
        cursor.execute("SELECT Introduction, Discussion, Diagnosis FROM DSM5_Cases WHERE Case_Number = ?", (case_number,))
        existing_row = cursor.fetchone()

        if existing_row:
            # Case exists, check if the data is different
            existing_introduction, existing_discussion, existing_diagnosis = existing_row
            if (introduction != existing_introduction or
                    discussion != existing_discussion or
                    diagnosis != existing_diagnosis):
                # Data is different, update the row
                sql = """
                    UPDATE DSM5_Cases
                    SET Introduction = ?, Discussion = ?, Diagnosis = ?
                    WHERE Case_Number = ?
                """
                cursor.execute(sql, (introduction, discussion, diagnosis, case_number))
                cnxn.commit()
                print(f"Case {case_number} data updated.")
            else:
                print(f"Case {case_number} data is the same, no update needed.")
        else:
            # Case does not exist, insert a new row
            sql = """
                INSERT INTO DSM5_Cases (Case_Number, Introduction, Discussion, Diagnosis)
                VALUES (?, ?, ?, ?)
            """
            cursor.execute(sql, (case_number, introduction, discussion, diagnosis))
            cnxn.commit()
            print(f"Case {case_number} data inserted.")
        cursor.close()
        cnxn.close()
    except Exception as e:
        print(f"Error inserting/updating data for case {case_number}: {e}")
        cnxn.rollback()
        raise  # Re-raise to stop execution after logging


def clean_text(text: str) -> str:
    """
    Cleans the input text by removing unwanted characters and patterns.  This version is more strict.

    Args:
        text (str): The text to clean.

    Returns:
        str: The cleaned text.
    """
    # Remove control characters, including newlines, tabs, and carriage returns
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)
    # Replace curly quotes, smart quotes, and similar characters with standard quotes
    text = re.sub(r'[\u2018\u2019\u201c\u201d]', '"', text)
    # Replace non-breaking spaces and other whitespace variants with standard spaces
    text = re.sub(r'[\u00A0\u2002-\u200B\u202F\u3000]', ' ', text)
    # Remove any remaining unusual unicode characters
    text = re.sub(r'[^\x00-\x7F\u00A0-\uFFFF]', '', text)
    # Replace multiple spaces with single spaces
    text = re.sub(r'\s+', ' ', text)
    return text.strip()



def main():
    """
    Main function to:
    1.  Read the DSM-5-TR Clinical Cases text file.
    2.  Parse the text to extract individual cases.
    3.  Create the DSM5_Cases table in the SQL Server database.
    4.  Insert or update the extracted case data into the database.
    """
    # Read the input text file
    try:
        with input_txt_path.open("r", encoding="utf-8") as file:
            text = file.read()
    except Exception as e:
        print(f"Error reading input file: {e}")
        return

    # Split the text into individual cases
    cases = re.split(r"Case \d+.*", text)

    # Create the table in the database if it doesn't exist
    create_table_if_not_exists(SQL_SERVER_CONNECTION_STRING)

    # Process each case and insert/update the database
    for idx, case_text in enumerate(cases[1:], start=1):
        case_text_clean = clean_text(case_text.replace('\n', ' ')) # Clean the text *before* further processing.
        try:
            discussion_idx = case_text_clean.index("Discussion")
        except ValueError:
            print(f"Warning: 'Discussion' section not found in case {idx}. Skipping.")
            continue

        try:
            diagnosis_idx = case_text_clean.index("Diagnoses")
        except ValueError:
            try:
                diagnosis_idx = case_text_clean.index("Diagnosis")
            except ValueError:
                print(f"Warning: Neither 'Diagnoses' nor 'Diagnosis' found in case {idx}. Skipping.")
                continue

        introduction = clean_text(case_text_clean[:discussion_idx].strip()) # Clean each extracted part
        discussion = clean_text(case_text_clean[discussion_idx:diagnosis_idx].strip())
        diagnosis = clean_text(case_text_clean[diagnosis_idx:].strip())

        # Insert/update the data into the database
        try:
            insert_or_update_case_data(SQL_SERVER_CONNECTION_STRING, idx, introduction, discussion, diagnosis)
        except Exception:
            print(f"Failed to insert/update case {idx}.  Continuing to the next case.")

    print("Data processing and database operations complete.")


if __name__ == "__main__":
    main()