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
        You are a SQL assistant for an Industrial IoT database.
        
        Database schema:
        {table_info}
        
        Rules:
        - Use 'maintenance.work_orders' for work_orders table
        - Use 'revenue.asset_revenue' for asset_revenue table
        - Use 'sensor_readings', 'assets_shared', 'employees_shared' without prefix
        - Column names: 'amount_usd' for revenue, 'cost' for maintenance costs
        - Use aggregate functions or ORDER BY with LIMIT for "high" or "top" values
        - Use GROUP BY asset_id when joining multiple tables
        - Join tables using 'asset_id' as the key
        - Return ONLY the SQL query without markdown or explanations
        - Return top {top_k} results unless specified otherwise
        
        Question: {input}
        """
        prompt = PromptTemplate.from_template(template=template)
        generate_query = create_sql_query_chain(self.llm, db, prompt)
        execute_query = QuerySQLDataBaseTool(db=db)
        chain = (
            RunnablePassthrough.assign(query=generate_query).assign(result=itemgetter("query") | execute_query)
        )
        return chain
