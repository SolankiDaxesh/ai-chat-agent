# gemini_handler.py
import os
import json
import google.generativeai as genai
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class GeminiHandler:
    def __init__(self, api_key: Optional[str] = None):
        """Initialize Gemini AI handler with API key"""
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("Gemini API key is required. Set GEMINI_API_KEY environment variable or pass it directly.")
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-pro')
        logger.info("Gemini AI handler initialized successfully")
    
    async def analyze_query_intent(self, query: str, db_schema: Optional[Dict] = None) -> Dict:
        """
        Analyze if the query requires database access and generate SQL if needed
        
        Args:
            query (str): User's natural language query
            db_schema (Dict, optional): Database schema information to help with SQL generation
            
        Returns:
            Dict: Analysis result containing needs_db flag, generated SQL, and explanation
        """
        schema_info = ""
        if db_schema:
            schema_info = f"""
            DATABASE SCHEMA INFORMATION:
            {json.dumps(db_schema, indent=2)}
            
            Use this schema information to generate more accurate SQL queries.
            """
            
        prompt = f"""
        Analyze the following user query and determine if it requires database access.
        If it does, generate a safe SQL query to fulfill the request.
        
        USER QUERY: {query}
        
        {schema_info}
        
        Return your response in the following JSON format:
        {{
            "needs_db": true/false,
            "sql_query": "SQL query here if needed, otherwise null",
            "explanation": "Brief explanation of your decision"
        }}
        
        IMPORTANT RULES:
        1. NEVER generate DELETE, DROP, or any destructive SQL commands
        2. Only generate SELECT statements for data retrieval
        3. If unsure, set needs_db to false
        4. Keep queries simple and efficient
        5. Use proper SQL syntax based on standard SQL
        6. Use double quotes for table and column names if they might be reserved words
        7. If multiple tables are involved, use appropriate JOIN clauses
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = json.loads(response.text)
            logger.info(f"Query intent analysis: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to analyze query intent: {str(e)}")
            return {
                "needs_db": False,
                "sql_query": None,
                "explanation": f"Error analyzing query: {str(e)}"
            }
    
    async def generate_response(self, query: str, needs_db: bool, sql_query: Optional[str], query_results: Optional[List[Dict]]) -> str:
        """
        Generate a human-readable response based on query and results
        
        Args:
            query (str): Original user query
            needs_db (bool): Whether the query required database access
            sql_query (str, optional): The SQL query that was executed
            query_results (List[Dict], optional): Results from the database query
            
        Returns:
            str: Generated response for the user
        """
        if needs_db and query_results:
            # Format results for prompt
            results_str = json.dumps(query_results[:10], indent=2)  # Limit to first 10 rows
            total_rows = len(query_results)
            
            prompt = f"""
            The user asked: "{query}"
            
            I executed the following SQL query:
            ```sql
            {sql_query}
            ```
            
            And got {total_rows} results. Here are the first {min(10, total_rows)} rows:
            ```json
            {results_str}
            ```
            
            Please provide a clear, concise response that answers the user's question based on these results.
            Include relevant data points but don't just repeat all the raw data.
            If there are important patterns, trends, or insights in the data, highlight them.
            If there are more than 10 rows, mention that there are additional results not shown.
            """
        elif needs_db and not query_results:
            prompt = f"""
            The user asked: "{query}"
            
            I attempted to query the database with:
            ```sql
            {sql_query}
            ```
            
            However, no results were returned. Please provide a helpful response explaining that
            no matching data was found and suggest possible reasons or alternative approaches.
            """
        else:
            prompt = f"""
            The user asked: "{query}"
            
            This query doesn't require database access. Please provide a helpful, informative response.
            Be concise but thorough, and if you don't have enough information to answer properly,
            suggest what additional information would be helpful.
            """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            logger.error(f"Failed to generate AI response: {str(e)}")
            return f"I apologize, but I encountered an error generating a response: {str(e)}"
            
    async def get_schema_from_query(self, query: str) -> Dict:
        """
        Attempt to extract potential schema information from a query
        
        Args:
            query (str): User's natural language query
            
        Returns:
            Dict: Potential schema information extracted from the query
        """
        prompt = f"""
        Based on the following user query, identify potential database tables and fields that might be relevant.
        
        USER QUERY: {query}
        
        Return your response in the following JSON format:
        {{
            "potential_tables": ["table1", "table2"],
            "potential_fields": ["field1", "field2"],
            "relationships": ["table1 might be related to table2"]
        }}
        
        Be conservative in your estimates. Only include tables and fields that are directly mentioned
        or strongly implied by the query.
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = json.loads(response.text)
            return result
        except Exception as e:
            logger.error(f"Failed to extract schema information: {str(e)}")
            return {
                "potential_tables": [],
                "potential_fields": [],
                "relationships": []
            }