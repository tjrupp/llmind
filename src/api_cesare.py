from flask import Flask, request, jsonify
from langchain_chroma import Chroma
from langchain_community.embeddings import OllamaEmbeddings
from langchain import hub
from langchain_community.llms import Ollama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import re  # Import the regular expression module
import pyodbc  # Import the pyodbc module
import difflib

app = Flask(__name__)

# Endpoint that takes a string input and returns a string output
@app.route('/askLLM', methods=['POST'])
def askLLM():
    # Get the "input_string" parameter from the JSON request body
    input_data = request.get_json()
    input_string = input_data.get('input_string')

    # Return an error if "input_string" is missing
    if input_string is None:
        return jsonify({"error": "Missing 'input_string' parameter"}), 400

    # Model name to be used
    modello = "gemma2:27b"
    
    # Initialize the vector store with the specified model
    vectorstore = Chroma(persist_directory=f"./vectorstore/chroma_db-full-{modello.replace(':','')}", embedding_function=OllamaEmbeddings(model=modello))

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
    
    # Remove newline characters from the answer
    answer = answer.replace("\n", "")
    
    short_answer = qa_chain.invoke(f"starting from the previous answer: {answer}, please give me just the name of the disease and nothing else according to icd11")
    short_answer = short_answer.replace("\n", "").strip()
    # # Extract the disease code using a regular expression
    # disease_code_pattern = r"6[a-zA-Z0-9]{3}"
    # match = re.search(disease_code_pattern, answer)
    # if match:
    #     disease_code = match.group(0)
    # else:
    #     disease_code = None  # Or handle the case where no code is found, e.g., "" or an error

    # import db_config
    # # Construct the connection string
    # cnxn_str = db_config.SQL_SERVER_CONNECTION_STRING
    
    # # Initialize an empty list to store drug information
    # drugs = []
    # disease_definition = None

    # # If a disease code was found, query the database
    # if disease_code:
    #     try:
    #         # Establish a database connection
    #         cnxn = pyodbc.connect(cnxn_str)
    #         cursor = cnxn.cursor()

    #         # SQL query to find disease definition
    #         sql_query_icd11 = """
    #             SELECT title
    #             FROM [dbo].[ICD11_Codes]
    #             WHERE code = ?
    #         """
    #         cursor.execute(sql_query_icd11, disease_code)
    #         icd11_result = cursor.fetchone()
    #         if icd11_result:
    #             disease_definition = icd11_result[0]

    #         # SQL query to find drugs for the given disease definition
    #         sql_query = """
    #             SELECT x_name
    #             FROM [dbo].[KGPrime_db]
    #             WHERE y_name = ?
    #         """
    #         cursor.execute(sql_query, disease_definition)
            
    #         # Fetch all rows and append drug names to the list
    #         rows = cursor.fetchall()
    #         for row in rows:
    #             drugs.append(row[0])  # Assuming x_name is the first column

    #         # Close the cursor and connection
    #         cursor.close()
    #         cnxn.close()

    #     except pyodbc.Error as ex:
    #         sqlstate = ex.args[0]
    #         if sqlstate == '08001':
    #             return jsonify({"error": "Error: Could not connect to the database.  Check your server name, instance name, and port number."}), 500
    #         elif sqlstate == '28000':
    #             return jsonify({"error": "Error: Login failed. Check your username and password."}), 500
    #         else:
    #             return jsonify({"error": f"Database error: {ex}"}), 500
    #     except Exception as e:
    #          return jsonify({"error": f"An unexpected error occurred: {e}"}), 500
    
    # # Return the response, including the disease code and drug list
    # return jsonify({"output_string": answer, "disease_code": disease_code, "drugs": drugs, "disease_definition": disease_definition})

    # Find the disease name by matching answer text against all titles
    
    # Initialize an empty list to store drug information
    disease_name = None
    disease_definition = None
    drugs = []

    import db_config
    # Construct the connection string
    cnxn_str = db_config.SQL_SERVER_CONNECTION_STRING
    try:
        # Establish a database connection
        cnxn = pyodbc.connect(cnxn_str)
        cursor = cnxn.cursor()

        # Get all disease titles
        cursor.execute("SELECT title FROM [dbo].[ICD11_Codes] where code like '6%'")
        titles = [row[0] for row in cursor.fetchall()]

        # Step 1: Exact match
        exact_matches = [title for title in titles if title in short_answer]

        if exact_matches:
            # If there are multiple, pick the first exact match
            disease_definition = exact_matches[0]
            disease_name = disease_definition
        else:
            # Step 2: Fuzzy match if no exact match found
            best_match = difflib.get_close_matches(short_answer, titles, n=1, cutoff=0.6)
            if best_match:
                disease_definition = best_match[0]
                disease_name = disease_definition

        if disease_definition:
            # Now find drugs linked to the disease
            sql_query = """
                SELECT x_name
                FROM [dbo].[KGPrime_db]
                WHERE y_name = ?
            """
            cursor.execute(sql_query, disease_definition)
            rows = cursor.fetchall()
            for row in rows:
                drugs.append(row[0])

        # Close the cursor and connection
        cursor.close()
        cnxn.close()

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        if sqlstate == '08001':
            return jsonify({"error": "Error: Could not connect to the database.  Check your server name, instance name, and port number."}), 500
        elif sqlstate == '28000':
            return jsonify({"error": "Error: Login failed. Check your username and password."}), 500
        else:
            return jsonify({"error": f"Database error: {ex}"}), 500
    except Exception as e:
            return jsonify({"error": f"An unexpected error occurred: {e}"}), 500

    # Return the response, including the disease code and drug list
    return jsonify({"output_string": answer, "disease_name": disease_name, "drugs": drugs, "disease_definition": disease_definition})


if __name__ == '__main__':
    # Run the server
    app.run(host="0.0.0.0", debug=True)
