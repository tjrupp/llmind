import re
from pathlib import Path
import pandas as pd
# -------------------------------------------------------------------
# Now, load the splitted cases CSV into a DataFrame.
# -------------------------------------------------------------------
split_csv_path = Path("./data/input/DSM-5-TR_Clinical_Cases_splitted.csv")
df = pd.read_csv(split_csv_path, sep='§', encoding='utf-8')
# Define a dictionary mapping the column names for the answer data to their respective file paths.
data_sources = {
    "gemma2latest_answer": "./data/output/gemma2latest/answers-cases.csv",
    "gemma227b_answer": "./data/output/gemma227b/answers-cases.csv",
    "llama3latest_answer": "./data/output/llama3latest/answers-cases.csv",
    "mistral_nemolatest_answer": "./data/output/mistral-nemolatest/answers-cases.csv",
    "mixtral8x7b_answer": "./data/output/mixtral8x7b/answers-cases.csv",
    "phi35latest_answer": "./data/output/phi3.5latest/answers-cases.csv",
    "phi3medium_answer": "./data/output/phi3medium/answers-cases.csv",
}

# Iterate over each data source and add its 'answer' column to the main DataFrame.
for column_name, file_path in data_sources.items():
    # Read the CSV file containing answers.
    # The engine is set to 'python' if needed for complex separators.
    df_source = pd.read_csv(file_path, sep='§', engine='python')

    # It's assumed that the answer CSV has a column named 'answer' and that rows align with the cases.
    df[column_name] = df_source['answer']

# Optionally, if you want to add a "Question" column from the phi3medium data,
# check for a typo (e.g., "quesion" vs. "question") and add it if present.
# phi3medium_df = pd.read_csv(data_sources["phi3medium_answer"], sep='§', engine='python')
# if 'quesion' in phi3medium_df.columns:
#     df["Question"] = phi3medium_df["quesion"]
# elif 'question' in phi3medium_df.columns:
#     df["Question"] = phi3medium_df["question"]
# else:
#     print("Warning: No question column found in phi3medium data.")

# Save the final DataFrame with all cases and answers to a CSV file.
df.to_csv('./results/dsm_results.csv', index=False, encoding='utf-8')


