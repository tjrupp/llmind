import chardet
import pandas as pd
import numpy as np
import re
import pdfplumber

# ------------------------------
# Step 1. Read and Clean CSV Data
# ------------------------------

# Path to CSV file
csv_path = "./data/input/ICD-11.csv"

# Detect encoding for the CSV file
with open(csv_path, 'rb') as file:
    raw_data = file.read()
    detected = chardet.detect(raw_data)
    encoding = detected['encoding']
print(f"Detected CSV encoding: {encoding}")

# Read CSV using the detected encoding
df_csv = pd.read_csv(csv_path, sep=',', quoting=0, encoding=encoding)

# Rename columns to fix unwanted characters in names
#df_csv.rename(columns={"b'codice": 'codice', "esclusioni'": 'esclusioni'}, inplace=True)

# Clean the 'codice' column by removing the "b'" substring
df_csv['code'] = df_csv['code'].str.replace("b'", "", regex=False)

# Drop the last row (if it is not needed)
df_csv.drop(df_csv.tail(1).index, inplace=True)

# Filter rows where 'codice' starts with '6'
filtered_df = df_csv[df_csv['code'].str.startswith('6')].copy()

# Drop the 'definizione lunga' column which is not needed
filtered_df.drop(['longdefinition'], axis=1, inplace=True)

# Replace single quotes with NaN across the DataFrame
filtered_df.replace("'", np.nan, inplace=True)


# Helper function to remove unwanted patterns from text
def clean_text(text):
    if isinstance(text, str):
        return text.replace("\\xc2\\xa7", "")
    return text


# Clean the 'inclusioni' and 'esclusioni' columns
filtered_df["inclusions"] = filtered_df["inclusions"].apply(clean_text)
filtered_df["exclusions"] = filtered_df["exclusions"].apply(clean_text)

# Reset the DataFrame index
filtered_df.reset_index(drop=True, inplace=True)


# ------------------------------
# Step 2. Create Prompts from CSV Data
# ------------------------------

def dataframe_to_prompts(df):
    """
    Create a prompt for each disorder based on its title, code, definition,
    inclusions, and exclusions.
    """

    def create_prompt(row):
        prompt = ""
        if isinstance(row['definition'], str):
            prompt = (
                f"Disorder Name: {row['title']}\n"
                f"Disorder Code: {row['code']}\n"
                f"Disorder symptoms: {row['definition']}\n"
            )
        if isinstance(row['inclusions'], str):
            prompt += f"If you have {row['inclusions']} other inclusions could be: {row['inclusions']}\n"
        if isinstance(row['exclusions'], str):
            prompt += f"If you diagnose this disease exclude: {row['exclusions']}"
        return prompt

    # Apply the prompt creation row-wise
    prompts_series = df.apply(create_prompt, axis=1)

    return pd.DataFrame({
        "code": df["code"],
        "name": df["title"],
        "prompt": prompts_series
    })


# Create the prompts DataFrame and save it to CSV
prompts_df = dataframe_to_prompts(filtered_df)
prompts_df.to_csv("./data/input/ICD-11_filtered.csv", index=True)

# ------------------------------
# Step 3. Extract and Process PDF Content
# ------------------------------

# Define path to the PDF file
pdf_path = "./data/input/ICD-11-CDDR.pdf"

# Initialize list to store page content
pages_content = []
start_page = 111  # starting page number (1-indexed)
end_page = 694  # ending page number (exclusive upper bound)

# Open the PDF and iterate over specified pages
with pdfplumber.open(pdf_path) as pdf:
    total_pages = len(pdf.pages)
    # Ensure we do not exceed available pages
    for page_num in range(start_page - 1, min(end_page, total_pages)):
        try:
            page = pdf.pages[page_num]
            text = page.extract_text()
            pages_content.append({
                'page_number': page_num + 1,
                'content': text.replace("\"","").replace("“","").replace("”","").replace("\n","")
            })
        except Exception as e:
            print(f"Error reading page {page_num + 1}: {str(e)}")

# Create a DataFrame from the extracted PDF pages
pdf_df = pd.DataFrame(pages_content)

# ------------------------------
# Step 4. Split PDF Content by Disorder Codes
# ------------------------------

# Regex pattern to match disorder codes (e.g., 6A00.0)
code_pattern = r'(6[A-E]\w{2}(?:\.\w)?)'

# Validate codes in filtered CSV (print any that do not match the pattern)
for code in filtered_df["code"]:
    if not re.match(code_pattern, code):
        print(f"{code} does NOT match the pattern")

# Instead of building many mini-DataFrames, accumulate records as dictionaries
records = []
for _, row in pdf_df.iterrows():
    # Split the page content based on the code pattern and remove extra spaces
    parts = [part.strip() for part in re.split(code_pattern, row['content']) if part.strip()]
    for part in parts:
        records.append({
            "page_number": row['page_number'],
            "content": part.replace("\"","").replace("“","").replace("”","").replace("\n","")
        })

# Create a DataFrame from the records and filter out very short strings
split_df = pd.DataFrame(records)
split_df = split_df[split_df['content'].str.len() >= 3].reset_index(drop=True)

# Identify rows that likely represent disorder codes using the regex
code_indexes = split_df[split_df['content'].str.contains(code_pattern)].index

# Initialize lists to store codes and their associated descriptions
codes_list = []
descriptions_list = []

# For each detected code, concatenate the following text until the next code is found
for i, idx in enumerate(code_indexes):
    current_code = split_df.loc[idx, 'content']
    if i == len(code_indexes) - 1:
        # Last code: join all remaining text
        description_content = ' '.join(split_df.loc[idx + 1:, 'content'].tolist())
    else:
        next_idx = code_indexes[i + 1]
        description_content = ' '.join(split_df.loc[idx + 1:next_idx - 1, 'content'].tolist())
    codes_list.append(current_code)
    descriptions_list.append(description_content)

# Build a DataFrame mapping each disorder code to its description
result_df = pd.DataFrame({
    'code': codes_list,
    'content': descriptions_list
})

# ------------------------------
# Step 5. Merge Data from CSV, PDF, and Prompts
# ------------------------------

# Merge the CSV data (filtered_df), PDF extraction (result_df), and prompts (prompts_df) on 'codice'
final_join_df = (
    filtered_df
    .merge(result_df, on="code", how="inner")
    .merge(prompts_df, on="code", how="inner")
)
for index, row in final_join_df.iterrows():
        final_join_df.loc[index, 'prompt'] = str(row['prompt']).replace("\"","").replace("“","").replace("”","").replace("\n","") + str(row['content']).replace("\"","").replace("“","").replace("”","").replace("\n","") # String conversion crucial!

# Group by 'code' and find the index of the row with the longest prompt
idx = final_join_df.groupby('code')['prompt'].apply(lambda x: x.str.len().idxmax())

# Use the indices to select the rows with the longest prompts
final_join_df = final_join_df.loc[idx].reset_index(drop=True)
# Save the final merged data to CSV
final_join_df.to_csv("./data/input/ICD-11_joined.csv", index=True)
