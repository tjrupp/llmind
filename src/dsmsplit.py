import re
from pathlib import Path
import pandas as pd

# Define file paths using pathlib for cross-platform compatibility.
input_txt_path = Path("./data/input/DSM-5-TR_Clinical_Cases.txt")
split_csv_path = Path("./data/input/DSM-5-TR_Clinical_Cases_splitted.csv")

# Read the input text file using a context manager to ensure the file is closed after reading.
with input_txt_path.open("r", encoding="utf-8") as file:
    text = file.read()

# Use a regular expression to split the text into individual cases.
# The pattern looks for the string "Case " followed by one or more digits.
# Note: You might want to refine this regex if case numbers have more complex formats.
cases = re.split(r"Case \d+.*", text)

# Initialize a list to store data for each case.
data_rows = []

# Process each case (skip the first element, which is the text before the first case).
for idx, case_text in enumerate(cases[1:], start=1):
    # Remove newline characters to work with a single-line version.
    case_text_clean = case_text.replace('\n', ' ')

    # Find the index where the "Discussion" section begins.
    try:
        discussion_idx = case_text_clean.index("Discussion")
    except ValueError:
        # If "Discussion" is not found, log a warning and skip this case.
        print(f"Warning: 'Discussion' section not found in case {idx}. Skipping.")
        continue

    # Find the index where the "Diagnoses" (or "Diagnosis") section begins.
    try:
        diagnosis_idx = case_text_clean.index("Diagnoses")
    except ValueError:
        try:
            diagnosis_idx = case_text_clean.index("Diagnosis")
        except ValueError:
            print(f"Warning: Neither 'Diagnoses' nor 'Diagnosis' found in case {idx}. Skipping.")
            continue

    # Extract the three parts of the case:
    # - Introduction: from the beginning to the start of the Discussion.
    # - Discussion: from the Discussion start to the start of the Diagnosis section.
    # - Diagnosis: from the Diagnosis section to the end of the case.
    introduction = case_text_clean[:discussion_idx].strip()
    discussion = case_text_clean[discussion_idx:diagnosis_idx].strip()
    diagnosis = case_text_clean[diagnosis_idx:].strip()

    # Append the parsed data as a dictionary to the list.
    data_rows.append({
        "Case_Number": idx,
        "Introduction": introduction,
        "Discussion": discussion,
        "Diagnosis": diagnosis
    })

# Convert the list of dictionaries into a DataFrame.
cases_df = pd.DataFrame(data_rows)

# Save the parsed cases to a CSV file using the custom separator (§).
cases_df.to_csv(split_csv_path, sep='§', index=False, encoding='utf-8')