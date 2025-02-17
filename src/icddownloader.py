import requests
import json
import csv
from typing import List, Dict

# -----------------------------------------------------------------------------
# Configuration Section
# -----------------------------------------------------------------------------

# API base URL and endpoint templates
BASE_URI_TEMPLATE = 'http://localhost/icd/release/11/2024-01/mms/{}'
ROOT_URI = 'http://localhost/icd/release/11/2024-01/mms'

# HTTP headers for API requests
HEADERS = {
    'Accept': 'application/json',
    'Accept-Language': 'en',
    'API-Version': 'v2'
}

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
            'inclusions': [inc.get('label', {}).get('@value', '').replace(";", "~") for inc in data.get('inclusion', [])],
            'exclusions': [exc.get('label', {}).get('@value', '').replace(";", "~") for exc in data.get('exclusion', [])],
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
# File Saving and Conversion Functions
# -----------------------------------------------------------------------------

def save_results_to_json(results: List[Dict], filename: str) -> None:
    """
    Saves the list of ICD code entries to a JSON file with pretty-print formatting.

    Args:
        results (List[Dict]): The list of ICD code entries.
        filename (str): The filename for the JSON output.
    """
    try:
        with open(filename, "w+", encoding="utf-8") as json_file:
            json.dump(results, json_file, ensure_ascii=False, indent=4)
        print(f"Results successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving JSON file {filename}: {e}")

def convert_json_to_csv(json_filename: str, csv_filename: str) -> None:
    """
    Converts the JSON file containing ICD code entries into a CSV file.
    List fields such as 'inclusions' and 'exclusions' are converted to semicolon-separated strings.

    Args:
        json_filename (str): The source JSON file containing ICD code entries.
        csv_filename (str): The destination CSV file.
    """
    try:
        with open(json_filename, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
    except Exception as e:
        print(f"Error reading JSON file {json_filename}: {e}")
        return

    # Define the CSV column headers
    fieldnames = ["code", "title", "definition", "longdefinition", "inclusions", "exclusions"]

    try:
        with open(csv_filename, "w+", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                # Join list fields into a semicolon-separated string for CSV output.
                row["inclusions"] = "; ".join(row.get("inclusions", []))
                row["exclusions"] = "; ".join(row.get("exclusions", []))
                writer.writerow(row)
        print(f"Results successfully converted to CSV and saved to {csv_filename}")
    except Exception as e:
        print(f"Error writing CSV file {csv_filename}: {e}")

# -----------------------------------------------------------------------------
# Main Execution Function
# -----------------------------------------------------------------------------

def main():
    """
    Main function to:
      1. Retrieve ICD code data starting from the ROOT_URI.
      2. Save the accumulated results to a JSON file.
      3. Convert the JSON file to CSV.
    """
    results: List[Dict] = []

    # Using a persistent session for better performance (reuse connections)
    with requests.Session() as session:
        retrieve_code(ROOT_URI, session, results)

    # Define filenames for JSON and CSV outputs
    json_filename = "./results/dsm_results.json"
    csv_filename = "./data/input/ICD-11.csv"

    # Save results to JSON and convert to CSV
    save_results_to_json(results, json_filename)
    convert_json_to_csv(json_filename, csv_filename)

if __name__ == "__main__":
    main()