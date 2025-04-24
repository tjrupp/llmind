from flask import Flask, request, jsonify
from langchain_chroma import Chroma
from langchain_community.embeddings import OllamaEmbeddings
from langchain import hub
from langchain_community.llms import Ollama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import pyodbc  # Import the pyodbc library
import importlib.util  # Import the importlib module
import re  # Import the re module for regular expressions
from difflib import SequenceMatcher # Import SequenceMatcher
import db_config # Import db_config

app = Flask(__name__)

# Load database configuration from file
def load_db_config(config_file_path):
    try:
        # Load the module from the file path.
        spec = importlib.util.spec_from_file_location("db_config", config_file_path)
        db_config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(db_config)
        connection_string = db_config.SQL_SERVER_CONNECTION_STRING
        if not connection_string:
            raise ValueError("Missing SQL_SERVER_CONNECTION_STRING in db_config.py")
        return connection_string
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: Database configuration file not found at {config_file_path}")
    except AttributeError:
        raise AttributeError(
            f"Error: Could not find SQL_SERVER_CONNECTION_STRING in {config_file_path}.  Make sure it is defined correctly.")
    except Exception as e:
        raise Exception(f"An unexpected error occurred while reading the database configuration: {e}")

# Function to fetch differential diagnoses from the database
def get_differential_diagnoses(conn, initial_diagnosis, confidence_interval=0.8):
    """
    Fetches differential diagnoses from the SQL Server database based on the initial diagnosis,
    using a confidence interval for fuzzy matching.

    Args:
        conn (pyodbc.Connection): The database connection object.
        initial_diagnosis (str): The initial diagnosis from the LLM.
        confidence_interval (float, optional): The minimum similarity score (0 to 1) for a match.
            Defaults to 0.8.

    Returns:
        list: A list of dictionaries, where each dictionary represents a differential diagnosis.
              Returns an empty list if no differential diagnoses are found.
    """
    try:
        cursor = conn.cursor()
        # SQL query to fetch differential diagnoses
        query = """
            SELECT 
                [level], 
                [type], 
                [value],
                [parent_condition]
            FROM 
                [dbo].[DiagnosisData]
            WHERE 
                [level_0_info] LIKE ?
            ORDER BY 
                [level];  -- Order by level for a more organized result
        """
        # Convert the initial diagnosis to a SQL Server compatible pattern.
        pattern = f"%{initial_diagnosis}%"
        cursor.execute(query, pattern)
        results = cursor.fetchall()

        # Process the results into a list of dictionaries
        diagnoses = []
        for row in results:
            diagnoses.append({
                "level": row.level,
                "type": row.type,
                "value": row.value,
                "parent_condition": row.parent_condition
            })
        return diagnoses
    except pyodbc.Error as e:
        print(f"Error fetching differential diagnoses: {e}")
        return []  # Return an empty list in case of error
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

def get_similar_case(conn, case_description, confidence_threshold=0.8):
    """
    Finds the most similar case in the database based on the introduction text.

    Args:
        conn (pyodbc.Connection): The database connection object.
        case_description (str): The input clinical case description.
        confidence_threshold (float): The minimum similarity score.

    Returns:
        dict: A dictionary containing the case details if a similar case is found,
              otherwise None.
    """
    try:
        cursor = conn.cursor()
        query = "SELECT [Case_Number], [Introduction], [Diagnosis] FROM [dbo].[DSM5_Cases]"
        cursor.execute(query)
        cases = cursor.fetchall()

        best_match = None
        best_similarity = 0

        for case in cases:
            similarity = SequenceMatcher(None, case_description, case.Introduction).ratio()
            if similarity > best_similarity and similarity >= confidence_threshold:
                best_similarity = similarity
                best_match = {
                    "Case_Number": case.Case_Number,
                    "Introduction": case.Introduction,
                    "Diagnosis": case.Diagnosis,
                    "similarity": similarity
                }

        return best_match
    except pyodbc.Error as e:
        print(f"Error fetching similar case: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    
def get_icd11_codes(conn, diagnosis_name, use_inclusions=True):
    """
    Fetches ICD-11 codes based on the diagnosis name, including related codes from inclusions.

    Args:
        conn (pyodbc.Connection): The database connection.
        diagnosis_name (str): The diagnosis name.
        use_inclusions (bool): Whether to include codes from the inclusions field.

    Returns:
        list: A list of dictionaries containing ICD-11 code details.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT [code], [title], [definition], [longdefinition], [inclusions], [exclusions]
            FROM [dbo].[ICD11_Codes]
            WHERE [code] LIKE ? 
        """
        params = [f"%{diagnosis_name}%"]
        if use_inclusions:
            query += "OR [inclusions] LIKE ?"
            params.append(f"%{diagnosis_name}%")
        cursor.execute(query, params)
        results = cursor.fetchall()

        codes = []
        for row in results:
            codes.append({
                "code": row.code,
                "title": row.title,
                "definition": row.definition,
                "longdefinition": row.longdefinition,
                "inclusions": row.inclusions,
                "exclusions": row.exclusions
            })
        return codes
    except pyodbc.Error as e:
        print(f"Error fetching ICD-11 codes: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []


def get_next_question(diagnoses, current_level, previous_answer=None):
    """
    Determines the next question to ask the user based on the differential diagnoses.

    Args:
        diagnoses (list): A list of dictionaries representing the differential diagnoses.
        current_level (int): The current level in the diagnosis graph.
        previous_answer (str, optional): The user's answer to the previous question ("yes" or "no").

    Returns:
        tuple: (str, int) - The next question to ask, and the next level, or (None, current_level) if no further questions.
    """
    next_level = current_level + 1
    possible_questions = []

    for diagnosis in diagnoses:
        if diagnosis['level'] == next_level:
            if previous_answer is None or diagnosis['type'] == previous_answer:
                if diagnosis['type'] == 'condition':
                    return diagnosis['value'], next_level
                elif diagnosis['level'] == next_level:
                    possible_questions.append(diagnosis)

    if possible_questions:
        # Prioritize questions
        for question in possible_questions:
            if question['type'] == 'condition':
                return question['value'], next_level

    return None, current_level  # No suitable question found


@app.route('/askLLM', methods=['POST'])
def askLLM():
    """
    Endpoint that takes a clinical case as input, queries the LLM, and guides the user
    through a series of questions to refine the differential diagnosis.
    """
    # Get the "input_string" parameter from the JSON request body
    input_data = request.get_json()
    input_string = input_data.get('input_string')
    previous_answers = input_data.get('previous_answers', [])  # Get previous answers

    # Return an error if "input_string" is missing
    if input_string is None:
        return jsonify({"error": "Missing 'input_string' parameter"}), 400

    # Model name to be used
    modello = "gemma2:27b"

    # Initialize the vector store with the specified model
    vectorstore = Chroma(persist_directory=f"./vectorstore/chroma_db-full-{modello.replace(':','')}",
                        embedding_function=OllamaEmbeddings(model=modello))

    # Load the Llama3 model
    llm = Ollama(model=modello)

    # Use the vector store as the retriever
    retriever = vectorstore.as_retriever()

    # Function to format documents for the QA chain
    def format_docs(docs):
        return "\n\n".join(doc.page_content for doc in docs)

    # Load the QA chain prompt from langchain hub
    rag_prompt = hub.pull("rlm/rag-prompt")

    # Create the QA chain
    qa_chain = (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | rag_prompt
        | llm
        | StrOutputParser()
    )

    # Prepare the question from the input string
    question = f"{input_string}"

    # Invoke the QA chain to get the answer
    answer = qa_chain.invoke(question)
    pattern = "6[0-9a-zA-Z]{3}"
    match = re.search(pattern, answer)
    if match:
        first_occurrence = match.group(0)
        print(f"The first occurrence found is: {first_occurrence}")
        initial_diagnosis = first_occurrence
    else:
        print("No occurrence found.")
        initial_diagnosis = answer.replace("\n", "")

    # Get differential diagnoses from the database
    try:
        SQL_SERVER_CONNECTION_STRING = db_config.SQL_SERVER_CONNECTION_STRING  # Use the connection string from db_config.py
        conn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
        similar_case = get_similar_case(conn, input_string, confidence_threshold=0.7) # Get similar case
        icd11_codes = get_icd11_codes(conn, initial_diagnosis) # Get ICD11 codes
        if similar_case:
            icd11_codes_similar = get_icd11_codes(conn, similar_case["Diagnosis"])
        else:
            icd11_codes_similar = []
        
        # Check for perfect ICD-11 code match
        for code_data in icd11_codes:
            if code_data["code"] in initial_diagnosis:
                conn.close()
                return jsonify({
                    "output_string": initial_diagnosis,
                    "icd11_codes": [code_data],  # Return only the matching code
                    "match_type": "ICD11_match"
                })

        differential_diagnoses = get_differential_diagnoses(conn, initial_diagnosis,
                                                        confidence_interval=0.7)  # Set confidence interval
    except Exception as e:
        print(f"Error: {e}")
        conn = None  # Set conn to None to avoid using an invalid connection
        differential_diagnoses = []  # Ensure that differential_diagnoses is always defined
        icd11_codes = []
        icd11_codes_similar = []

    # if not differential_diagnoses:
    #    if conn:
    #      conn.close()
    #    return jsonify({"output_string": initial_diagnosis, "differential_diagnoses": []})

    # Start interactive questioning
    current_level = 0
    final_diagnosis = None
    
    if similar_case:
        if conn:
            conn.close()
        return jsonify(
            {"output_string": initial_diagnosis, "similar_case": similar_case, "icd11_codes": icd11_codes_similar})  # Return similar case
    else:
        for previous_answer in previous_answers:
            next_question, current_level = get_next_question(differential_diagnoses, current_level, previous_answer)
            if next_question is None:
                break

        next_question, current_level = get_next_question(differential_diagnoses, current_level)

        if next_question is None:
            # If no more questions, get the final diagnosis
            for diagnosis in differential_diagnoses:
                if diagnosis['level'] == current_level + 1:
                    final_diagnosis = diagnosis['value']
                    break
            if conn:
                conn.close()
            return jsonify(
                {"output_string": initial_diagnosis, "final_diagnosis": final_diagnosis,
                 "differential_diagnoses": differential_diagnoses, "icd11_codes": icd11_codes})  # Return the final diagnosis

        # Return the next question
        if conn:
            conn.close()
        return jsonify(
            {"output_string": initial_diagnosis, "next_question": next_question, "current_level": current_level,
             "differential_diagnoses": differential_diagnoses, "icd11_codes": icd11_codes})
    

if __name__ == '__main__':
    # Run the server
    app.run(host="0.0.0.0", debug=True)
