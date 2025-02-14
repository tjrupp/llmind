from langchain_chroma import Chroma
from langchain_community.embeddings import OllamaEmbeddings
from langchain import hub
from langchain_community.llms import Ollama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import csv 
import os
import time

# List of models to be used
models = ["medllama2", "cniongolo/biomistral"]

# Iterate over each model in the list
for model in models:
    print("-------------------------------------------------------------")
    print(f"model: {model} -- {models.index(model)+1}/{len(models)}")
    
    # Initialize the vector store with the specified model
    vectorstore = Chroma(persist_directory=f"./vectorstore/chroma_db-full-{model.replace(':','')}", embedding_function=OllamaEmbeddings(model=model))

    # Load the LLM model
    llm = Ollama(model=model)

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

    # Open the input CSV file
    with open('../data/input/splitCases.csv', encoding="utf-8") as inputFile:
        
        # Skip the header row
        heading = next(inputFile) 
        
        # Create a CSV reader object
        reader_obj = csv.reader(inputFile, delimiter="§") 
        
        # Create output directory if it doesn't exist
        directory = f"./output/{model.replace(':','')}"
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # Initialize counters and timers
        i = 1
        startTime = time.time()
        prevTime = time.time() - startTime
        
        # Iterate over each row in the CSV file
        for row in reader_obj: 
            # Print progress and timing information
            print(f"model: {model} -- Row {i}/103 -- Elapsed time: {(time.time() - startTime)-prevTime}s Total time: {time.time() - startTime}s -- Missing time: {(((time.time() - startTime)/i)*103)-(time.time() - startTime)}s ({((((time.time() - startTime)/i)*103)-(time.time() - startTime))/3600}h)")
            
            # Log progress and timing information
            with open("../data/log.txt", "a", encoding="utf-8") as log:
                log.write(f"model: {model} -- Row {i}/103 -- Elapsed time: {(time.time() - startTime)-prevTime}s Total time: {time.time() - startTime}s -- Missing time: {(((time.time() - startTime)/i)*103)-(time.time() - startTime)}s ({((((time.time() - startTime)/i)*103)-(time.time() - startTime))/3600}h)\n")
            
            # Update previous time
            prevTime = time.time() - startTime
            
            try:
                # Formulate the question based on the row data
                question = f"based on the icd-11 make a diagnosis for this case: {row[i]}"
                
                # Invoke the QA chain to get the answer
                answer = qa_chain.invoke(question)
                
                # Remove newline characters from the answer
                answer = answer.replace("\n", "")
                
                # Write the question and answer to the output CSV file
                with open(directory+"/answers-cases.csv", "a", encoding="utf-8") as outputFile:
                    if i == 1:
                        outputFile.write("row§question§answer\n")
                    outputFile.write(f"{i}§{question}§{answer}\n")
            except:
                # Handle exceptions by writing a placeholder question and answer
                question = "no question"
                with open(directory+"/answers-cases.csv", "a", encoding="utf-8") as outputFile:
                    outputFile.write(f"{i}§{question}§NO ANSWER\n")
            
            # Increment the row counter
            i = i + 1

