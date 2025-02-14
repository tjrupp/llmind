from flask import Flask, request, jsonify
from langchain_chroma import Chroma
from langchain_community.embeddings import OllamaEmbeddings
from langchain import hub
from langchain_community.llms import Ollama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

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
    
    # Return the response with the output string
    return jsonify({"output_string": answer})

if __name__ == '__main__':
    # Run the server
    app.run(host="0.0.0.0", debug=True)

# Test URL: http://127.0.0.1:5000/askLLM
# Test prompt: {"input_string": "I go about my day like everything is fine. I smile, I laugh, I joke around, but inside, it’s like I’m drowning. I’m exhausted, but I don’t let anyone see it. I keep pushing through, pretending I’m okay, because I don’t want to burden anyone with my feelings. People see me and think I’m fine, but the truth is, I feel numb and disconnected. It’s like I’m wearing a mask, and the longer I wear it, the harder it is to take off. I keep everything bottled up, afraid that if I show how I really feel, I’ll be judged or misunderstood. I just want to be seen for who I really am, but I don’t know how to let anyone in."}
# curl --location 'http://127.0.0.1:5000/askLLM' \
# --header 'Content-Type: application/json' \
# --data '{"input_string": "I go about my day like everything is fine. I smile, I laugh, I joke around, but inside, it’s like I’m drowning. I’m exhausted, but I don’t let anyone see it. I keep pushing through, pretending I’m okay, because I don’t want to burden anyone with my feelings. People see me and think I’m fine, but the truth is, I feel numb and disconnected. It’s like I’m wearing a mask, and the longer I wear it, the harder it is to take off. I keep everything bottled up, afraid that if I show how I really feel, I’ll be judged or misunderstood. I just want to be seen for who I really am, but I don’t know how to let anyone in."}'