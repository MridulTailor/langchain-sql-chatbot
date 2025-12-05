from langchain_community.chat_models import ChatOllama
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from operator import itemgetter

class LLMEngine:
    def __init__(self):
        self.llm = ChatOllama(model="llama3:latest", temperature=0)
    

    def get_chain(self, db):
        template = """
You are an expert SQL assistant specializing in Industrial IoT data analysis. 
You are writing queries for a SQLite database with multiple attached databases.

Your Goal: Generate an optimized, syntactically correct SQL query to answer the user's question based strictly on the provided schema.

Schema Context:
{table_info}

Security Constraints (CRITICAL):
1.  **READ-ONLY:** You are a READ-ONLY assistant.
2.  **Prohibited Actions:** NEVER generate SQL that creates, modifies, updates, deletes, or drops tables, columns, or rows.
3.  **Refusal:** ONLY if the user explicitly asks to modify data (e.g., "Change revenue", "Delete table"), return: "I cannot execute modification queries."

General Guidelines:
1.  **Schema Adherence:** Use ONLY the tables and columns defined in the Schema Context.
2.  **Table Selection (CRITICAL):**
    - **"Work Orders" -> `maintenance.work_orders`**
    - **"Sensor Readings" -> `sensor_readings`**
    - **"Employees" -> `employees_shared`**
    - **"Assets" -> `assets_shared`**
    - **"Revenue" -> `revenue.asset_revenue`** (DO NOT query `sensor_readings`)
    - If the user asks for "work orders", you MUST query `maintenance.work_orders`. DO NOT query `sensor_readings`.
    - If the user asks for "revenue", you MUST join `revenue.asset_revenue`. NEVER try to select "revenue" or "amount_usd" from `sensor_readings`.
3.  **Attached Database Syntax:** - Tables in attached databases MUST be referenced with their prefix (e.g., `maintenance.work_orders`).
    - **Aliasing Strategy:** ALWAYS assign short aliases to tables (e.g., `maintenance.work_orders AS wo`).
    - **Column Qualification:** Use the **alias** to qualify columns (e.g., `wo.cost`), not the full database name.
4.  **Join Strategy:** - Identify the central entity (usually `assets_shared` or similar) and treat it as the primary table.
    - JOIN other tables to this primary table using the common identifier (e.g., `asset_id`).
    - DO NOT join unrelated tables (e.g., `employees_shared`) unless the question specifically requires their data.
5.  **Ambiguity & Semantics:** - Map terms like "revenue" to financial columns (e.g., `amount_usd`) and "cost" to expense columns.
    - ALWAYS select the identifier column (e.g., `asset_id`) alongside metrics to make results identifiable.
6.  **Aggregation:** When asked for "top", "highest", or "ranking," use aggregate functions (SUM, AVG, COUNT) combined with ORDER BY and LIMIT.

Output Format rules:
- Return ONLY the raw SQL code.
- No Markdown formatting (no ```sql ... ```).
- No explanations.
- The output must start with SELECT and end with ;
- Default to LIMIT {top_k} if no specific number is requested.

Question: {input}
"""

        prompt = PromptTemplate.from_template(template=template)
        generate_query = create_sql_query_chain(self.llm, db, prompt)
        
        # Custom execution function to return dicts with column names
        def execute_query_with_columns(query):
            # Layer 2: Python Validation
            is_safe, message = self.is_safe_query(query)
            if not is_safe:
                return f"Security Alert: {message}"

            from sqlalchemy import text
            # Clean query: remove markdown, strip whitespace, remove trailing text after ;
            clean_query = query.strip().replace("```sql", "").replace("```", "")
            if ";" in clean_query:
                clean_query = clean_query.split(";")[0] + ";"
            
            try:
                with db._engine.connect() as connection:
                    result = connection.execute(text(clean_query))
                    # Convert to list of dicts
                    return [dict(row._mapping) for row in result]
            except Exception as e:
                return f"Error: {str(e)}"

        from langchain_core.runnables import RunnableLambda
        execute_query = RunnableLambda(execute_query_with_columns)
        
        chain = (
            RunnablePassthrough.assign(query=generate_query).assign(result=itemgetter("query") | execute_query)
        )
        return chain

    def is_safe_query(self, sql_query):
        """Layer 2: Python Logic Guardrail"""
        # Normalize query
        sql_upper = sql_query.upper()
        
        # List of forbidden keywords
        forbidden = [
            "DROP ", "DELETE ", "INSERT ", "UPDATE ", "ALTER ", 
            "TRUNCATE ", "CREATE ", "GRANT ", "REVOKE ", "COMMIT"
        ]
        
        for word in forbidden:
            if word in sql_upper:
                return False, f"Forbidden keyword detected: {word}"
                
        return True, "Query is safe"
