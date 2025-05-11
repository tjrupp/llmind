#DA CANCELLARE
#SOSTITUITO DA CHIAMATA IN FILE NUMERO 1

import pandas as pd
import numpy as np
import re
import pdfplumber
import pyodbc  # For database interaction
import db_config

# ------------------------------
# Configuration
# ------------------------------

# Path to the PDF file
pdf_path = "./data/input/ICD-11-CDDR.pdf"

# Database connection string (replace with your actual connection details)
SQL_SERVER_CONNECTION_STRING = db_config.SQL_SERVER_CONNECTION_STRING


# ------------------------------
# Helper Functions
# ------------------------------

def clean_text(text):
    """Removes unwanted patterns from text."""
    if isinstance(text, str):
        return text.replace("\\xc2\\xa7", "")
    return text


def dataframe_to_prompts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a prompt for each disorder based on its title, code, definition,
    inclusions, and exclusions from a DataFrame structured like the output
    of the retrieve_code process.

    Args:
        df (pd.DataFrame): DataFrame containing disorder information with columns
                           'code', 'title', 'definition', 'inclusions', 'exclusions'.

    Returns:
        pd.DataFrame: A DataFrame with columns 'code', 'name', and 'prompt'.
    """
    def create_prompt(row):
        """Helper function to create a prompt string for a single row.
         Create a prompt for each disorder based on its title, code, definition, 
         inclusions, and exclusions. This function remains largely the same as it's used
         to structure the prompt *before* writing to the database.
         """
        prompt = ""
        if isinstance(row['definition'], str):
            prompt = (
                f"Disorder Name: {row['title']}\n"
                f"Disorder Code: {row['code']}\n"
                f"Disorder symptoms: {row['definition']}\n"
                )
        # Include inclusions if present
        if isinstance(row.get('inclusions'), str) and row['inclusions']:
             prompt += f"If you have {row['inclusions']} other inclusions could be: {row['inclusions']}\n"
        # Include exclusions if present
        if isinstance(row.get('exclusions'), str) and row['exclusions']:
             prompt += f"If you diagnose this disease exclude: {row['exclusions']}"
        # Include diagnostic criteria if present
        if isinstance(row.get('diagnosticCriteria'), str) and row['diagnosticCriteria']:
             prompt += f"The Diagnostic Criteria for this disorder: {row['diagnosticCriteria']}\n"

        return prompt.strip() # Remove leading/trailing whitespace

    # Apply the create_prompt function to each row of the DataFrame
    # axis=1 ensures the function is applied row-wise
    prompts_series = df.apply(create_prompt, axis=1)

    # Create the resulting DataFrame
    return pd.DataFrame({
        "code": df["code"],
        "name": df["title"],
        "prompt": prompts_series
    })


def read_data_from_sql(connection_string: str, table_name: str, query: str = None) -> pd.DataFrame:
    """
    Reads data from a SQL Server table into a Pandas DataFrame.

    Args:
        connection_string (str): The SQL Server connection string.
        table_name (str): The name of the table to read from.
        query (str, optional):  A custom SQL query.  If provided, overrides table_name.

    Returns:
        pd.DataFrame: The data from the table.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        if query:
            df = pd.read_sql(query, cnxn)
        else:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, cnxn)
        cnxn.close()
        return df
    except Exception as e:
        print(f"Error reading data: {e}")
        raise  # Re-raise to handle it in main()


def write_dataframe_to_sql(connection_string: str, df: pd.DataFrame, table_name: str) -> None:
    """
    Writes a Pandas DataFrame to a SQL Server table.  Creates the table if it doesn't exist.

    Args:
        connection_string (str): The connection string for the SQL Server database.
        df (pd.DataFrame): The DataFrame to write.
        table_name (str): The name of the table to write to.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Check if the table exists
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = ?",
            (table_name,),
        )
        table_exists = cursor.fetchone()

        if not table_exists:
            # Create table DDL based on DataFrame columns. Use NVARCHAR(MAX) for unicode.
            columns = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])  # Use NVARCHAR(MAX)
            create_table_sql = f"CREATE TABLE {table_name} ({columns})"
            cursor.execute(create_table_sql)
            cnxn.commit()
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists.")

        # Insert or Update data
        for _, row in df.iterrows():
            placeholders = ", ".join("?" * len(df.columns))
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            update_sql = f"UPDATE {table_name} SET " + ", ".join([f"[{col}] = ?" for col in df.columns]) + f" WHERE code = ?"
            select_sql = f"SELECT * FROM {table_name} WHERE code = ?"

            cursor.execute(select_sql, (row['code'],))
            existing_row = cursor.fetchone()

            if existing_row:
                # Check if the existing data is the same as the new data
                existing_data = dict(zip([col[0] for col in cursor.description], existing_row))
                same_data = True
                for col in df.columns:
                    if str(existing_data[col]) != str(row[col]):
                        same_data = False
                        break
                if not same_data:
                    # Data is different, so update
                    update_values = list(row.values) + [row['code']]
                    cursor.execute(update_sql, update_values)
                    cnxn.commit()
                    print(f"Updated row with code: {row['code']}")
                else:
                    print(f"Data for code {row['code']} is the same, no update needed.")
            else:
                # Row does not exist, so insert
                cursor.execute(insert_sql, tuple(row))
                cnxn.commit()
                print(f"Inserted row with code: {row['code']}")

        cursor.close()
        cnxn.close()

    except Exception as e:
        print(f"Error writing data to table {table_name}: {e}")
        raise  # Re-raise to handle in main()



def create_icd11_pdf_content_table(connection_string):
    """Creates the ICD11_PDF_Content table if it does not exist."""
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'ICD11_PDF_Content'"
        )
        table_exists = cursor.fetchone()
        if not table_exists:
            cursor.execute(
                """
                CREATE TABLE ICD11_PDF_Content (
                    code NVARCHAR(255) NOT NULL,
                    content NVARCHAR(MAX),
                    page_number INT NOT NULL,
                    PRIMARY KEY (code, page_number)
                )
                """
            )
            cnxn.commit()
            print("ICD11_PDF_Content table created.")
        else:
            print("ICD11_PDF_Content table already exists.")
        cursor.close()
        cnxn.close()
    except Exception as e:
        print(f"Error creating ICD11_PDF_Content table: {e}")
        raise



def main():
    """
    Main function to:
    1.  Create the ICD11_PDF_Content table.
    2.  Extract and process content from the PDF file and store it in the database.
    3.  Read data from the ICD11_Codes table in SQL Server.
    4.  Join the data from SQL Server and the PDF, and create prompts using SQL.
    5.  Save the final processed data to a new table in SQL Server.
    """
    try:
        cnxn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
        cursor = cnxn.cursor()

        # Step 1: Create the ICD11_PDF_Content table.
        create_icd11_pdf_content_table(SQL_SERVER_CONNECTION_STRING)


        # Step 2: Extract and Process PDF Content and store in DB
        pages_content = []
        start_page = 111
        end_page = 694
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            for page_num in range(start_page - 1, min(end_page, total_pages)):
                try:
                    page = pdf.pages[page_num]
                    text = page.extract_text()
                    pages_content.append(
                        {
                            "page_number": page_num + 1,
                            "content": text.replace("\"", "").replace("“", "").replace("”", "").replace("\n", ""),
                        }
                    )
                except Exception as e:
                    print(f"Error reading page {page_num + 1}: {str(e)}")

        #  Insert PDF data into SQL Server
        if pages_content: # Check if there is data to insert
            pdf_df = pd.DataFrame(pages_content)
            # Use a list of tuples for  fast_executemany
            pdf_data = [(row['content'], row['page_number'], re.search(r'(6[A-E]\w{2}(?:\.\w)?)', row['content'])) for row in pages_content]
            pdf_data_to_insert = []
            for content, page_number, match in pdf_data:
                if match:
                    code = match.group(0)
                    pdf_data_to_insert.append((code, content, page_number))

            if pdf_data_to_insert:
                insert_pdf_sql = "INSERT INTO ICD11_PDF_Content (code, content, page_number) VALUES (?, ?, ?)"
                update_pdf_sql = "UPDATE ICD11_PDF_Content SET content = ?, page_number = ? WHERE code = ? AND page_number = ?"
                select_pdf_sql = "SELECT content, page_number FROM ICD11_PDF_Content WHERE code = ? AND page_number = ?"

                for code, content, page_number in pdf_data_to_insert:
                    cursor.execute(select_pdf_sql, (code, page_number))
                    existing_row = cursor.fetchone()
                    if existing_row:
                        #compare
                        if existing_row[0] != content:
                            cursor.execute(update_pdf_sql, (content, page_number, code, page_number))
                            cnxn.commit()
                            print(f"Updated ICD11_PDF_Content for code: {code}, page_number: {page_number}")
                        else:
                            print(f"ICD11_PDF_Content for code: {code}, page_number: {page_number} is the same, no update.")
                    else:
                        cursor.execute(insert_pdf_sql, (code, content, page_number))
                        cnxn.commit()
                        print(f"Inserted into ICD11_PDF_Content for code: {code}, page_number: {page_number}")

            else:
                print("No ICD-11 codes found in PDF content to insert.")

        # Step 3: Read data from SQL Server, filtering in the query
        df_csv = read_data_from_sql(
            SQL_SERVER_CONNECTION_STRING,
            "ICD11_Codes",
            query="SELECT code, title, definition, inclusions, exclusions FROM ICD11_Codes WHERE code LIKE '6%'",
        )

        # Step 4: Join data and create prompts using SQL
        #  This is the most important change:  We do the join in SQL.
        join_query = """
            SELECT
                c.code,
                c.title,
                c.definition,
                c.inclusions,
                c.exclusions,
                p.content,
                (
                    'Disorder Name: ' + c.title + '\\n' +
                    'Disorder Code: ' + c.code + '\\n' +
                    'Disorder symptoms: ' + ISNULL(c.definition, '') + '\\n' +  -- Handle NULLs
                    'If you have ' + ISNULL(c.inclusions, '') + ' other inclusions could be: ' + ISNULL(c.inclusions, '') + '\\n' +  -- Handle NULLs
                    'If you diagnose this disease exclude: ' + ISNULL(c.exclusions, '') + '\\n' +  -- Handle NULLs
                    ISNULL(p.content, '')
                ) AS prompt
            FROM
                ICD11_Codes c  --  c is aliased
            LEFT JOIN
                ICD11_PDF_Content p ON c.code = p.code  -- p is aliased
            WHERE c.code LIKE '6%'
        """
        final_join_df = pd.read_sql(join_query, cnxn)

        # Step 5: Group by code and get the longest prompt.
        #  This is done in Pandas because SQL Server's string aggregation functions can be more complex
        #  and less efficient for very long strings.  We're assuming this is a relatively small
        #  number of rows compared to the base data.
        idx = final_join_df.groupby("code")["prompt"].apply(lambda x: x.str.len().idxmax())
        final_join_df = final_join_df.loc[idx].reset_index(drop=True)

        # Step 6: Save the final processed data to SQL Server
        write_dataframe_to_sql(SQL_SERVER_CONNECTION_STRING, final_join_df, "ICD11_Final_Data")

        print("Data processing and saving to SQL Server complete.")
        cursor.close()
        cnxn.close()

    except Exception as e:
        print(f"An error occurred: {e}")
        # Consider more robust error handling (e.g., logging, user notification)


if __name__ == "__main__":
    main()
