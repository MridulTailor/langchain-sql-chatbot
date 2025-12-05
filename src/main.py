import streamlit as st
import pandas as pd
import ast
from database_manager import DBManager
from llm_engine import LLMEngine

# Initialize resources
@st.cache_resource
def get_resources():
    return DBManager(), LLMEngine()

db_manager, llm_engine = get_resources()

# Page configuration
st.set_page_config(
    page_title="Multi-DB SQL Query System",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

def process_query(prompt, role):
    """Process a query and update chat history."""
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user"):
        st.markdown(prompt)

    # Display assistant response
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        
        try:
            # Process query
            user_db = db_manager.get_db_for_session(role=role)
            chain = llm_engine.get_chain(user_db)
            response = chain.invoke({"question": prompt})
            
            result_str = str(response['result']).lower()
            
            # Error handling logic
            error_message = None
            df = pd.DataFrame()
            
            if 'error' in result_str:
                if 'no such table' in result_str:
                    if 'revenue' in result_str or 'asset_revenue' in result_str:
                        error_message = f"Access denied: Your role ({role}) cannot access revenue data."
                    elif 'maintenance' in result_str or 'work_orders' in result_str:
                        error_message = f"Access denied: Your role ({role}) cannot access maintenance data."
                    else:
                        error_message = "Table not found. Please verify your query."
                elif 'no such column' in result_str:
                    error_message = "Invalid column. Please rephrase your question."
                elif 'syntax error' in result_str:
                    error_message = "SQL syntax error. Please rephrase your question."
                elif 'ambiguous' in result_str:
                    error_message = "Ambiguous column reference. Please be more specific."
                else:
                    error_message = f"Database error: {response['result']}"
            
            if error_message:
                message_placeholder.markdown(error_message)
                st.session_state.messages.append({"role": "assistant", "content": error_message})
            else:
                result_data = response['result']
                
                # Check for empty results
                if not result_data or (isinstance(result_data, list) and len(result_data) == 0):
                    msg = f"No data found.\n\n**SQL Query:**\n```sql\n{response['query']}\n```"
                    message_placeholder.markdown(msg)
                    st.session_state.messages.append({"role": "assistant", "content": msg})
                else:
                    # Result is now a list of dicts, directly create DataFrame
                    df = pd.DataFrame(result_data)
                    
                    msg = f"**SQL Query:**\n```sql\n{response['query']}\n```\n\n**Results:** {len(df)} rows"
                    message_placeholder.markdown(msg)
                    st.dataframe(df)
                    
                    # Save to history with dataframe
                    st.session_state.messages.append({
                        "role": "assistant", 
                        "content": msg,
                        "dataframe": df
                    })

        except Exception as e:
            error_msg = str(e).lower()
            final_error = f"Error: {str(e)}"
            
            if 'no such table' in error_msg:
                if 'revenue' in error_msg or 'asset_revenue' in error_msg:
                    final_error = f"Access denied: Your role ({role}) cannot access revenue data."
                elif 'maintenance' in error_msg or 'work_orders' in error_msg:
                    final_error = f"Access denied: Your role ({role}) cannot access maintenance data."
                else:
                    final_error = "Table not found."
            
            message_placeholder.markdown(final_error)
            st.session_state.messages.append({"role": "assistant", "content": final_error})

# Sidebar for Role Selection and Examples
with st.sidebar:
    st.title("Configuration")
    role = st.selectbox(
        "Select Role",
        ["PlantDirector", "MaintenanceManager", "RevenueAnalyst", "SensorViewer"],
        index=0,
        key="role_selector"
    )
    st.markdown("---")
    st.markdown("**Role Access:**")
    st.markdown("- **PlantDirector**: Full Access")
    st.markdown("- **MaintenanceManager**: Sensors & Maintenance")
    st.markdown("- **RevenueAnalyst**: Sensors & Revenue")
    st.markdown("- **SensorViewer**: Sensors Only")
    
    st.markdown("---")
    st.markdown("### Example Queries")
    
    examples = [
        ("Show me the top 5 assets with highest vibration", "PlantDirector"),
        ("List work orders for Asset-001", "MaintenanceManager"),
        ("Which critical assets have high revenue but high maintenance costs?", "PlantDirector"),
        ("Show me the revenue for all assets", "SensorViewer")
    ]
    
    selected_example = None
    for query, example_role in examples:
        if st.button(query[:40] + "...", help=f"{query} ({example_role})"):
            selected_example = query

# Main Chat Interface
st.title("Multi-DB SQL Query System")
st.markdown("Query across Sensors, Maintenance, and Revenue databases with role-based access control.")

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "dataframe" in message:
            st.dataframe(message["dataframe"])

# Handle Input
prompt = st.chat_input("Ask a question about your data...")

if prompt:
    process_query(prompt, role)
elif selected_example:
    process_query(selected_example, role)