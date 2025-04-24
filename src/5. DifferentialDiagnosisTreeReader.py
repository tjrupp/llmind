import json
import os
import re
import pyodbc

# Import the database connection string from your config file
from db_config import SQL_SERVER_CONNECTION_STRING

def parse_json_leaves(json_data, level=0, parent_condition=None):
    """
    Recursively parses a JSON-like structure to find leaf nodes
    and returns the data in a structured format for database insertion.
    Handles potential errors during JSON parsing.

    Args:
        json_data (dict or str): The JSON data to parse.
        level (int, optional): The nesting level. Defaults to 0.
        parent_condition (str, optional): The condition from the parent node.

    Returns:
        list: A list of dictionaries, where each dictionary represents a leaf
              node and its associated information.
    """
    results = []
    try:
        if isinstance(json_data, dict):
            for key, value in json_data.items():
                if isinstance(value, (dict, list)):
                    parse_results = parse_json_leaves(value, level + 1, key)
                    results.extend(parse_results)
                else:
                    type_str = "yes" if "yes" in key.lower() else \
                               "no" if "no" in key.lower() else "condition"
                    leaf_data = {
                        "level": level,
                        "type": type_str,
                        "value": value,
                        "parent_condition": parent_condition
                    }
                    results.append(leaf_data)
        elif isinstance(json_data, list):
            for item in json_data:
                parse_results = parse_json_leaves(item, level + 1, parent_condition)
                results.extend(parse_results)
    except Exception as e:
        print(f"Error processing data at level {level}: {e}")
        return []
    return results

def process_json_file(file_path, level_0_info, conn):
    """
    Reads a JSON file, parses it, extracts leaf nodes, and inserts data
    into the database. Handles file and JSON errors.

    Args:
        file_path (str): Path to the JSON file.
        level_0_info (str): Level 0 information from the filename.
        conn: Database connection object.
    """
    try:
        with open(file_path, 'r') as f:
            json_data = json.load(f)
        leaf_data_list = parse_json_leaves(json_data)

        if not leaf_data_list:
            print(f"No data to insert from {file_path}")
            return

        cursor = conn.cursor()

        # Insert data into the database
        for leaf_data in leaf_data_list:
            sql = """
                    INSERT INTO DiagnosisData (level_0_info, level, type, value, parent_condition)
                    VALUES (?, ?, ?, ?, ?)
                  """
            params = (level_0_info, leaf_data["level"], leaf_data["type"],
                      leaf_data["value"], leaf_data["parent_condition"])
            try:
                cursor.execute(sql, params)
            except Exception as e:
                print(f"Error inserting data from {file_path}: {e}")
                conn.rollback()
                return

        conn.commit()
        print(f"Successfully inserted data from {file_path} into DiagnosisData.")

    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file {file_path}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while processing {file_path}: {e}")
        conn.rollback()

def main():
    """
    Processes JSON files in a folder, inserts data into the database.
    Creates the table if it doesn't exist and drops it before processing.
    """
    json_folder_path = "./data/input/jsonTrees"
    try:
        if not os.path.isdir(json_folder_path):
            print(f"Error: Folder not found at {json_folder_path}")
            return

        try:
            conn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
            cursor = conn.cursor()

            # Drop the table if it exists
            cursor.execute("IF OBJECT_ID('DiagnosisData', 'U') IS NOT NULL DROP TABLE DiagnosisData")
            conn.commit()
            print("Table DiagnosisData dropped (if it existed).")

            # Create the table.
            cursor.execute("""
                CREATE TABLE DiagnosisData (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    level_0_info VARCHAR(255),
                    level INT,
                    type VARCHAR(50),
                    value VARCHAR(MAX),
                    parent_condition VARCHAR(MAX)
                )
            """)
            conn.commit()
            print('Table DiagnosisData created.')

        except Exception as e:
            print(f"Error connecting to or creating table: {e}")
            return

        for filename in os.listdir(json_folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(json_folder_path, filename)
                match = re.search(r"Decision Tree for (.*?) \d+", filename)
                level_0_info = match.group(1) if match else "N/A"
                print(f"Level 0: {level_0_info}")
                process_json_file(file_path, level_0_info, conn)
        conn.close()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
    print("Finished processing JSON files.")
