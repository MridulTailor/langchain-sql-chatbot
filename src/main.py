import ast
import gradio as gr
from database_manager import DBManager
from llm_engine import LLMEngine
import pandas as pd

db_manager = DBManager()
llm_engine = LLMEngine()

def process_query(message, history, role):
    try:
        user_db = db_manager.get_db_for_session(role=role)
        chain = llm_engine.get_chain(user_db)
        response = chain.invoke({"question": message})
        
        result_str = str(response['result']).lower()
        
        if 'error' in result_str:
            if 'no such table' in result_str:
                if 'revenue' in result_str or 'asset_revenue' in result_str:
                    return f"Access denied: Your role ({role}) cannot access revenue data.", pd.DataFrame()
                elif 'maintenance' in result_str or 'work_orders' in result_str:
                    return f"Access denied: Your role ({role}) cannot access maintenance data.", pd.DataFrame()
                else:
                    return "Table not found. Please verify your query.", pd.DataFrame()
            elif 'no such column' in result_str:
                return "Invalid column. Please rephrase your question.", pd.DataFrame()
            elif 'syntax error' in result_str:
                return "SQL syntax error. Please rephrase your question.", pd.DataFrame()
            elif 'ambiguous' in result_str:
                return "Ambiguous column reference. Please be more specific.", pd.DataFrame()
            else:
                return f"Database error: {response['result']}", pd.DataFrame()
        
        result_data = response['result']
        if result_data == '[]' or result_data == '' or result_data == '()' or (isinstance(result_data, list) and len(result_data) == 0):
            return f"No data found.\n\nSQL:\n```sql\n{response['query']}\n```", pd.DataFrame()

        result_list = ast.literal_eval(response['result'])
        df = pd.DataFrame(result_list)
        
        output = f"SQL:\n```sql\n{response['query']}\n```\n\nResults: {len(df)} rows"
        return output, df
        
    except Exception as e:
        error_msg = str(e).lower()
        if 'no such table' in error_msg:
            if 'revenue' in error_msg or 'asset_revenue' in error_msg:
                return f"Access denied: Your role ({role}) cannot access revenue data.", pd.DataFrame()
            elif 'maintenance' in error_msg or 'work_orders' in error_msg:
                return f"Access denied: Your role ({role}) cannot access maintenance data.", pd.DataFrame()
            else:
                return "Table not found.", pd.DataFrame()
        else:
            return f"Error: {str(e)}", pd.DataFrame()
with gr.Blocks(theme=gr.themes.Soft()) as interface:
    with gr.Row():
        with gr.Column():
            gr.Markdown('# Multi-DB SQL Query System')
            gr.Markdown('Query across Sensors, Maintenance, and Revenue databases with role-based access control.')
        role_selector = gr.Dropdown(
            choices=["SensorViewer", "MaintenanceManager", "RevenueAnalyst", "PlantDirector"],
            value="PlantDirector",
            label="Select Role"
        )
    
    gr.ChatInterface(
        fn=process_query,
        additional_inputs=[role_selector],
        additional_outputs=[gr.Dataframe(label="Results", interactive=False)],
        examples=[
            ["Show me the top 5 assets with highest vibration", "PlantDirector"],
            ["List work orders for Asset-001", "MaintenanceManager"],
            ["Which critical assets have high revenue but high maintenance costs?", "PlantDirector"],
            ["Show me the revenue for all assets", "SensorViewer"]
        ]
    )

if __name__ == "__main__":
    interface.launch()