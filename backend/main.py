# main.py
import os
from typing import Dict, Optional, List, Union
import json
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import google.generativeai as genai
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import logging
import asyncio
from db import DatabaseConnector
from gemini import GeminiHandler
from gemini.gemini import GeminiHandler

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="AI Database Agent", description="An AI agent that uses Google Gemini to process natural language queries and execute database operations")

# Add CORS middleware to allow Streamlit to communicate with the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your Streamlit domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Gemini AI
try:
    gemini_handler = GeminiHandler(api_key=os.getenv("GEMINI_API_KEY"))
    logger.info("Gemini AI initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Gemini AI: {str(e)}")
    raise RuntimeError(f"Failed to initialize Gemini AI: {str(e)}")

# Define request and response models
class QueryRequest(BaseModel):
    query: str = Field(..., description="Natural language query from the user")
    connection_string: Optional[str] = Field(None, description="Database connection string (optional)")
    db_type: Optional[str] = Field("mssql", description="Database type (mssql, postgresql, mysql)")

class ConnectionRequest(BaseModel):
    server: str = Field(..., description="SQL Server hostname or IP")
    database: str = Field(..., description="Database name")
    username: Optional[str] = Field(None, description="SQL Server username")
    password: Optional[str] = Field(None, description="SQL Server password")
    trusted_connection: bool = Field(False, description="Use Windows authentication")
    driver: Optional[str] = Field("ODBC Driver 17 for SQL Server", description="ODBC driver to use")

class QueryResponse(BaseModel):
    answer: str = Field(..., description="AI-generated answer to the query")
    needs_db: bool = Field(..., description="Whether the query requires database access")
    sql_query: Optional[str] = Field(None, description="Generated SQL query, if applicable")
    query_results: Optional[List[Dict]] = Field(None, description="Results from the database query, if applicable")
    error: Optional[str] = Field(None, description="Error message, if any")

# Helper function to initialize database connector
def get_db_connector(connection_string: str, db_type: str = "mssql") -> DatabaseConnector:
    """Create a database connector based on the connection string and type"""
    try:
        if not connection_string:
            raise ValueError("Connection string is required for database operations")
        
        connector = DatabaseConnector(connection_string, db_type)
        return connector
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Database connection error: {str(e)}")

# Endpoint to create MSSQL connection string
@app.post("/create-mssql-connection", response_model=Dict[str, str])
async def create_mssql_connection(request: ConnectionRequest):
    """Create an MSSQL connection string from provided parameters"""
    try:
        connection_string = DatabaseConnector.create_mssql_connection_string(
            server=request.server,
            database=request.database,
            username=request.username,
            password=request.password,
            trusted_connection=request.trusted_connection,
            driver=request.driver
        )
        
        return {"connection_string": connection_string}
    except Exception as e:
        logger.error(f"Failed to create connection string: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to create connection string: {str(e)}")

# Endpoint to validate database connection
@app.post("/validate-connection")
async def validate_connection(request: QueryRequest):
    """Validate the database connection string"""
    if not request.connection_string:
        raise HTTPException(status_code=400, detail="Connection string is required")
    
    try:
        connector = get_db_connector(request.connection_string, request.db_type)
        success, error = connector.test_connection()
        
        if success:
            return {"status": "success", "message": "Connection successful"}
        else:
            raise HTTPException(status_code=400, detail=f"Connection failed: {error}")
    except Exception as e:
        logger.error(f"Connection validation failed: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")

# Main endpoint to process user queries
@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """Process a natural language query"""
    try:
        connector = None
        schema_info = None
        
        # If connection string is provided, try to get schema info
        if request.connection_string:
            try:
                connector = get_db_connector(request.connection_string, request.db_type)
                schema_info = await connector.get_schema_info()
            except Exception as e:
                logger.warning(f"Could not get schema info: {str(e)}")
        
        # Analyze query intent with Gemini
        intent_analysis = await gemini_handler.analyze_query_intent(request.query, schema_info)
        needs_db = intent_analysis.get("needs_db", False)
        sql_query = intent_analysis.get("sql_query")
        
        query_results = None
        error = None
        
        # Execute database query if needed
        if needs_db:
            if not request.connection_string:
                error = "Database connection string is required for this query"
                needs_db = False
            else:
                if not connector:
                    connector = get_db_connector(request.connection_string, request.db_type)
                
                try:
                    query_results, error = await connector.execute_query(sql_query)
                    if error:
                        # If there's an error in the query, try to diagnose and fix it
                        fix_prompt = f"""
                        The SQL query:
                        ```sql
                        {sql_query}
                        ```
                        
                        Failed with error:
                        {error}
                        
                        Please fix the query to address this error. If the error relates to syntax specific to SQL Server (MSSQL),
                        adjust the query to use proper MSSQL syntax. Return only the fixed SQL query with no explanation.
                        """
                        
                        try:
                            response = genai.GenerativeModel('gemini-pro').generate_content(fix_prompt)
                            fixed_sql = response.text.strip().replace("```sql", "").replace("```", "").strip()
                            
                            # Try again with the fixed query
                            logger.info(f"Attempting with fixed query: {fixed_sql}")
                            query_results, error = await connector.execute_query(fixed_sql)
                            
                            if not error:
                                # If the fixed query worked, update the sql_query
                                sql_query = fixed_sql
                        except Exception as fix_err:
                            logger.error(f"Failed to fix SQL query: {str(fix_err)}")
                except Exception as e:
                    error = str(e)
        
        # Generate response
        answer = await gemini_handler.generate_response(
            request.query, 
            needs_db, 
            sql_query, 
            query_results
        )
        
        # Close connector if it was created
        if connector:
            connector.close()
        
        return QueryResponse(
            answer=answer,
            needs_db=needs_db,
            sql_query=sql_query if needs_db else None,
            query_results=query_results,
            error=error
        )
    
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        return QueryResponse(
            answer=f"I apologize, but I encountered an error processing your query: {str(e)}",
            needs_db=False,
            sql_query=None,
            query_results=None,
            error=str(e)
        )

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)