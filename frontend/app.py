# streamlit_app.py
import streamlit as st
import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
import plotly.express as px
import time
from typing import Dict, List, Any, Optional

# Load environment variables
load_dotenv()

# Constants
API_URL = os.getenv("API_URL", "http://localhost:8000")
DEFAULT_DB_TYPE = "mssql"  # Changed default to MSSQL

# Page configuration
st.set_page_config(
    page_title="AI Database Agent",
    page_icon="🤖",
    layout="wide"
)

# Initialize session state
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "connection_verified" not in st.session_state:
    st.session_state.connection_verified = False
if "connection_string" not in st.session_state:
    st.session_state.connection_string = ""

# Helper functions
def validate_connection(connection_string: str, db_type: str) -> bool:
    """Validate database connection string"""
    try:
        response = requests.post(
            f"{API_URL}/validate-connection",
            json={"query": "Test connection", "connection_string": connection_string, "db_type": db_type},
            timeout=10
        )
        
        if response.status_code == 200:
            st.success("Connection successful! Database is ready to use.")
            st.session_state.connection_verified = True
            st.session_state.connection_string = connection_string
            return True
        else:
            error_detail = response.json().get("detail", "Unknown error")
            st.error(f"Connection failed: {error_detail}")
            st.session_state.connection_verified = False
            return False
    except Exception as e:
        st.error(f"Error testing connection: {str(e)}")
        st.session_state.connection_verified = False
        return False

def process_query(user_query: str, connection_string: Optional[str], db_type: str) -> Dict:
    """Send query to FastAPI backend for processing"""
    try:
        with st.spinner("Processing your query..."):
            response = requests.post(
                f"{API_URL}/query",
                json={
                    "query": user_query,
                    "connection_string": connection_string,
                    "db_type": db_type
                },
                timeout=30  # Longer timeout for complex queries
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                error_detail = response.json().get("detail", "Unknown error")
                st.error(f"Query processing failed: {error_detail}")
                return {
                    "answer": f"Error: {error_detail}",
                    "needs_db": False,
                    "sql_query": None,
                    "query_results": None,
                    "error": error_detail
                }
    except Exception as e:
        st.error(f"Error communicating with backend: {str(e)}")
        return {
            "answer": f"Error communicating with backend: {str(e)}",
            "needs_db": False,
            "sql_query": None,
            "query_results": None,
            "error": str(e)
        }

def create_mssql_connection_string() -> Optional[str]:
    """Create an MSSQL connection string from user inputs"""
    try:
        auth_type = st.radio(
            "Authentication Type",
            options=["SQL Server Authentication", "Windows Authentication (Trusted Connection)"],
            horizontal=True
        )
        
        server = st.text_input("Server", placeholder="localhost or server address", help="SQL Server hostname or IP address")
        database = st.text_input("Database", placeholder="database name", help="Name of the database you want to connect to")
        
        if auth_type == "SQL Server Authentication":
            username = st.text_input("Username", placeholder="SQL Server username")
            password = st.text_input("Password", type="password", placeholder="SQL Server password")
            trusted_connection = False
        else:
            username = None
            password = None
            trusted_connection = True
        
        driver = st.selectbox(
            "ODBC Driver",
            options=[
                "ODBC Driver 17 for SQL Server",
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 13 for SQL Server",
                "SQL Server Native Client 11.0",
                "SQL Server"
            ]
        )
        
        if st.button("Create Connection String"):
            if not server or not database:
                st.error("Server and Database are required fields")
                return None
                
            if auth_type == "SQL Server Authentication" and (not username or not password):
                st.error("Username and Password are required for SQL Server Authentication")
                return None
                
            # Call the API to create the connection string
            response = requests.post(
                f"{API_URL}/create-mssql-connection",
                json={
                    "server": server,
                    "database": database,
                    "username": username,
                    "password": password,
                    "trusted_connection": trusted_connection,
                    "driver": driver
                }
            )
            
            if response.status_code == 200:
                conn_str = response.json().get("connection_string")
                st.session_state.connection_string = conn_str
                st.code(conn_str, language="text")
                st.info("Connection string created successfully! Click 'Test Connection' to verify.")
                return conn_str
            else:
                error_detail = response.json().get("detail", "Unknown error")
                st.error(f"Failed to create connection string: {error_detail}")
                return None
        
        return st.session_state.connection_string
    except Exception as e:
        st.error(f"Error creating connection string: {str(e)}")
        return None

def display_query_results(results: List[Dict]) -> None:
    """Display query results in a table and offer visualization options"""
    if not results:
        st.info("No results returned from the database.")
        return
    
    # Convert to DataFrame for easier handling
    df = pd.DataFrame(results)
    
    # Display basic statistics
    st.subheader("Query Results")
    st.write(f"Retrieved {len(df)} rows with {len(df.columns)} columns")
    
    # Display results in a table
    st.dataframe(df)
    
    # Add download button
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        "Download as CSV",
        csv,
        "query_results.csv",
        "text/csv",
        key="download-csv"
    )
    
    # Basic visualization if appropriate columns are available
    st.subheader("Visualization Options")
    
    # Only offer visualization if there are enough rows and columns
    if len(df) > 1 and len(df.columns) >= 2:
        # Check for numeric and categorical columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist() + df.select_dtypes(include=['category']).columns.tolist()
        datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        
        # Only proceed if we have both numeric and categorical columns
        if numeric_cols and (categorical_cols or datetime_cols):
            # Create columns for side-by-side options
            col1, col2 = st.columns(2)
            
            with col1:
                chart_type = st.selectbox(
                    "Select Chart Type",
                    options=["Bar Chart", "Line Chart", "Scatter Plot", "Box Plot", "Histogram"],
                    key="chart_type"
                )
            
            with col2:
                # Different options based on chart type
                if chart_type in ["Bar Chart", "Line Chart", "Box Plot"]:
                    x_axis = st.selectbox("Select X-Axis", options=categorical_cols + datetime_cols, key="x_axis") if categorical_cols + datetime_cols else None
                    y_axis = st.selectbox("Select Y-Axis", options=numeric_cols, key="y_axis") if numeric_cols else None
                    
                    if x_axis and y_axis:
                        if chart_type == "Bar Chart":
                            fig = px.bar(df, x=x_axis, y=y_axis)
                        elif chart_type == "Line Chart":
                            fig = px.line(df, x=x_axis, y=y_axis)
                        elif chart_type == "Box Plot":
                            fig = px.box(df, x=x_axis, y=y_axis)
                        
                        st.plotly_chart(fig, use_container_width=True)
                
                elif chart_type == "Scatter Plot":
                    x_axis = st.selectbox("Select X-Axis", options=numeric_cols, key="x_axis") if numeric_cols else None
                    y_axis = st.selectbox("Select Y-Axis", options=numeric_cols, key="y_axis") if len(numeric_cols) > 1 else None
                    
                    if x_axis and y_axis:
                        fig = px.scatter(df, x=x_axis, y=y_axis)
                        st.plotly_chart(fig, use_container_width=True)
                
                elif chart_type == "Histogram":
                    column = st.selectbox("Select Column", options=numeric_cols, key="histogram_column") if numeric_cols else None
                    
                    if column:
                        fig = px.histogram(df, x=column)
                        st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need both numeric and categorical/date columns for visualization")
    else:
        st.info("Not enough data for visualization")

def display_chat_history() -> None:
    """Display chat history in a conversational format"""
    for i, message in enumerate(st.session_state.chat_history):
        if message["role"] == "user":
            st.chat_message("user").write(message["content"])
        else:
            with st.chat_message("assistant"):
                st.write(message["content"]["answer"])
                
                # Display SQL if available
                if message["content"].get("sql_query"):
                    with st.expander("View SQL Query"):
                        st.code(message["content"]["sql_query"], language="sql")
                
                # Display results if available
                if message["content"].get("query_results"):
                    with st.expander("View Query Results"):
                        display_query_results(message["content"]["query_results"])

# Main UI
st.title("🤖 AI Database Agent")
st.markdown("""
This application lets you query SQL Server databases using natural language. 
Simply enter your question, and the AI will generate and execute the appropriate SQL Server query.
""")

# Sidebar for connection settings
with st.sidebar:
    st.header("Database Connection")
    
    # Database type selection (with MSSQL as default)
    db_type = st.selectbox(
        "Database Type",
        options=["mssql", "postgresql", "mysql"],
        index=0,  # Default to MSSQL
        help="Select your database type"
    )
    
    # Different UI based on database type
    if db_type == "mssql":
        st.subheader("SQL Server Connection")
        
        # Show connection options
        connection_option = st.radio(
            "Connection Method",
            options=["Connection Builder", "Direct Connection String"],
            horizontal=True
        )
        
        if connection_option == "Connection Builder":
            connection_string = create_mssql_connection_string()
        else:
            connection_string = st.text_area(
                "Connection String",
                value=st.session_state.connection_string,
                placeholder="mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server",
                help="Enter your SQL Server connection string"
            )
    else:
        # For other database types
        connection_string = st.text_area(
            "Connection String",
            value=st.session_state.connection_string,
            placeholder=f"Enter your {db_type} connection string",
            help=f"Enter your {db_type} connection string"
        )
    
    # Test connection button
    if connection_string and st.button("Test Connection"):
        validate_connection(connection_string, db_type)

    st.divider()
    
    # Examples section
    st.subheader("Example Queries")
    st.markdown("""
    Try these example queries:
    - Show me the first 10 customers
    - What are the top 5 products by sales?
    - How many orders were placed last month?
    - Show me average order value by customer
    """)

# Note: To run this app, use the following command in your terminal:
# streamlit run frontend/app.py