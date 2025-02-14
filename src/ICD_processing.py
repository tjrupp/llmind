import chardet
import pandas as pd
import numpy as np
import math
import re
import pdfplumber

output_file_path = "../data/input/ICD-11.csv"
with open(output_file_path, 'rb') as file:
    raw_data = file.read()
    detected = chardet.detect(raw_data)
    encoding = detected['encoding']
    
print(encoding)
df = pd.read_csv(output_file_path, sep=';', quoting=3, encoding=encoding)
df.rename(columns={"b'codice": 'codice', "esclusioni'": 'esclusioni'}, inplace=True)
df['codice'] = df['codice'].str.replace("b'", "", regex=False)
df.drop(df.tail(1).index,inplace=True) # drop last n rows

filtered_df = df[df['codice'].str.startswith('6')]
filtered_df = filtered_df.drop(['definizione lunga'], axis=1)
filtered_df.replace("'", np.nan, inplace=True)
filtered_df["inclusioni"] = [
    str(item).replace("\\xc2\\xa7", "") if isinstance(item, str) else item
    for item in filtered_df["inclusioni"]
]
filtered_df["esclusioni"] = [
    str(item).replace("\\xc2\\xa7", "") if isinstance(item, str) else item
    for item in filtered_df["esclusioni"]
]
filtered_df= filtered_df.reset_index(drop=True)
filtered_df.head(10)

filtered_df.iloc[629:631]

def dataframe_to_prompts(df):
    prompts = []
    for _, row in df.iterrows():
        if type(row['definizione']) == str:
            prompt = f"Disorder Name: {row['titolo']}\n Disorder Code: {row['codice']}\n Disorder symptoms: {row['definizione']}\n"
        else: 
            prompt = ""
        
        if type(row['inclusioni']) == str:
            prompt += f"If you have {row['inclusioni']} other inclusions could be: {row['inclusioni']}\n"
        
        if type(row['esclusioni']) == str:
            prompt += f"If you diagnose this disease exclude: {row['esclusioni']}"
        
        prompts.append(prompt)

    return pd.DataFrame({
        "codice": df["codice"],
        "nome": df["titolo"],
        "prompt": prompts
    })
    
prompts = dataframe_to_prompts(filtered_df)
prompts.to_csv("ICD-11_filtered.csv")

with pdfplumber.open("../data/input/ICD-11-CDDR.pdf") as pdf:
    pages_content = []
    
    # Get total number of pages
    total_pages = len(pdf.pages)
    start_page=111
    # Read pages starting from start_page
    for page_num in range(start_page - 1, 694):
        try:
            # Get the page
            page = pdf.pages[page_num]
            
            # Extract text from page
            text = page.extract_text()
            
            # Add to list with page number
            pages_content.append({
                'page_number': page_num + 1,
                'content': text
            })
            
        except Exception as e:
            print(f"Error reading page {page_num + 1}: {str(e)}")
            
# Create DataFrame
df = pd.DataFrame(pages_content)
df


df.iloc[0] = {'page_number': 111, 'content':"6A00.0 Disorder of intellectual development, mild\n• In mild disorder of intellectual development, intellectual functioning and adaptive behaviour\nare found to be approximately 2–3 standard deviations below the mean (approximately\n0.1–2.3 percentile), based on appropriately normed, individually administered standardized\ntests. Where standardized tests are not available, assessment of intellectual functioning and\nadaptive behaviour requires greater reliance on clinical judgement, which may include\nthe use of behavioural indicators provided in Tables 6.1–6.4. People with mild disorder of\nintellectual development often exhibit difficulties in the acquisition and comprehension of\ncomplex language concepts and academic skills. Most master basic self-care, domestic and\npractical activities. Affected people can generally achieve relatively independent living and\nemployment as adults, but may require appropriate support.\nNeurodevelopmental disorders | Disorders of intellectual development'"}


pattern = r'(6[A-E]{1}\w{2}(\.\w)?)'
for code in filtered_df["codice"]:
    if re.match(pattern, code):
        continue
    else:
        print(f"{code} does NOT match the pattern")


pattern = r'(6[A-E]{1}\w{2}(\.\w?))'
splitted = re.split(pattern, df.iloc[1].content)
charapters = re.findall(pattern, df.iloc[1].content)

new_dfs = []

for index, row in df.iterrows():
    parts = re.split(pattern, row['content'])
    
    for part in parts:
        if part:
            new_df = pd.DataFrame({
                "page_number": [row['page_number']],
                "content": [part.strip()] 
            })
            new_dfs.append(new_df)

final_df = pd.concat(new_dfs, ignore_index=True)
final_df


df = final_df[final_df['content'].str.len() >= 3]
df = df.reset_index(drop=True)

indexes = df[df['content'].str.contains(pattern)].index
codes = df.iloc[indexes].reset_index(drop=True)
codes = codes.rename(columns={"content": "codice"})
description = df.iloc[indexes+1].reset_index(drop=True)

# Initialize lists to store codes and their descriptions
codes_list = []
descriptions_list = []

# Process each code and its following content until the next code
for i in range(len(indexes)):
    current_code = df.iloc[indexes[i]]['content']
    
    # If this is the last code, get all remaining content
    if i == len(indexes) - 1:
        description_content = ' '.join(df.iloc[indexes[i]+1:]['content'].tolist())
    else:
        # Get content until the next code
        description_content = ' '.join(df.iloc[indexes[i]+1:indexes[i+1]]['content'].tolist())
    
    codes_list.append(current_code)
    descriptions_list.append(description_content)

# Create the final DataFrame
result_df = pd.DataFrame({
    'codice': codes_list,
    'content': descriptions_list
})

result_df[result_df["codice"]=="6A62"]

# Merge with the filtered DataFrame
final_join_df = pd.merge(filtered_df, result_df, on="codice")
df = pd.concat([codes, description], axis=1).drop("page_number", axis=1)

final_join_df = pd.merge(filtered_df, df, on="codice")
final_join_df = pd.merge(final_join_df, prompts, on="codice")
final_join_df.to_csv("ICD-11_joined.csv")





