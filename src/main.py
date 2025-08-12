import gradio as gr
from langchain_community.utilities import SQLDatabase
from langchain_community.chat_models import ChatOllama
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from operator import itemgetter
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough

db = SQLDatabase.from_uri("sqlite:///../data/database/sakila_master.db")

llm = ChatOllama(model="llama2", temperature=0)

def get_database_schema():
    """Dynamically retrieve the database schema information"""
    
    # Get all table names
    tables_query = """
    SELECT name FROM sqlite_master 
    WHERE type='table' 
    AND name NOT LIKE 'sqlite_%'
    ORDER BY name;
    """
    
    # Get all view names
    views_query = """
    SELECT name FROM sqlite_master 
    WHERE type='view'
    ORDER BY name;
    """
    
    try:
        # Execute queries to get schema info
        tables_result = db.run(tables_query)
        views_result = db.run(views_query)
        
        # Get table schemas
        table_schemas = []
        table_names = [row[0] for row in eval(tables_result)] if tables_result != "[]" else []
        
        for table_name in table_names:
            # Get column information for each table
            pragma_query = f"PRAGMA table_info({table_name});"
            columns_info = db.run(pragma_query)
            
            if columns_info and columns_info != "[]":
                table_schemas.append(f"\n{table_name}:")
                columns = eval(columns_info)
                for col in columns:
                    col_name = col[1]
                    col_type = col[2]
                    is_pk = " (PRIMARY KEY)" if col[5] else ""
                    table_schemas.append(f"  - {col_name}: {col_type}{is_pk}")
        
        # Get view names
        view_names = [row[0] for row in eval(views_result)] if views_result != "[]" else []
        
        # Build the schema string
        schema_info = "Database Schema Information:\n\n"
        schema_info += "TABLES:\n"
        schema_info += "\n".join(table_schemas)
        
        if view_names:
            schema_info += "\n\nVIEWS:\n"
            for view in view_names:
                schema_info += f"- {view}\n"
        
        return schema_info
        
    except Exception as e:
        print(f"Error fetching schema: {e}")
        # Fallback to basic schema info
        return "Unable to fetch detailed schema. Basic table info available through standard SQL queries."

SCHEMA_INFO = get_database_schema()
print("Database Schema Retrieved:")
print(SCHEMA_INFO)

sql_generation_template = """
You are a SQLite expert. Given a user question, create a syntactically correct SQLite query to run.
Never query for all columns from a specific table, only ask for the relevant columns given the question.
Pay attention to the schema of the tables below.

{schema}

Question: {question}

Return ONLY the SQL query without any explanation, markdown formatting, or additional text.
SQL Query:
"""

sql_generation_prompt = ChatPromptTemplate.from_template(sql_generation_template)

answer_template = """
CONTEXT: You are analyzing the Sakila sample database, which is a fictional DVD rental store database created by MySQL for educational and testing purposes. All data is sample/fictional data for learning SQL.

Given the following user question, the corresponding SQL query, and the SQL result from the Sakila sample database, provide a helpful answer based on the data.

If the SQL result is empty, say that you could not find any results.

Question: {question}
SQL Query: {query}
SQL Result: {result}
Answer:
"""
answer_prompt = ChatPromptTemplate.from_template(answer_template)

sql_query_generation_chain = (
    {"schema": lambda x: SCHEMA_INFO, "question": itemgetter("question")}
    | sql_generation_prompt
    | llm
    | StrOutputParser()
)

execute_query_tool = QuerySQLDataBaseTool(db=db)

answer_chain = (
    RunnablePassthrough.assign(query=sql_query_generation_chain).assign(
        result=itemgetter("query") | execute_query_tool
    )
    | answer_prompt
    | llm
    | StrOutputParser()
)

def process_question(question):
    try:
        print(f"\n--- Processing Question ---")
        print(f"Question: {question}")
        
        # Generate the SQL query first
        sql_query = sql_query_generation_chain.invoke({"question": question})
        print(f"Generated SQL Query: {sql_query}")
        
        # Now run the full answer chain
        response = answer_chain.invoke({"question": question})
        print(f"Final Answer: {response}")
        print("--- End Processing ---\n")
        
        return response
    except Exception as e:
        error_msg = f"An error occurred: {e}"
        print(f"Error: {error_msg}")
        return error_msg

iface = gr.Interface(
    fn=process_question,
    inputs=gr.Textbox(lines=2, placeholder="Ask a question about the Sakila DVD rental database..."),
    outputs="text",
    title="Sakila DB Assistant 🦙 (Local with Ollama)",
    description="Ask questions in natural language about the Sakila DVD rental database. This version runs entirely on your local machine!",
    allow_flagging="never"
)

iface.launch()