# db_connector.py
from sqlalchemy import create_engine, text, MetaData, inspect
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import logging
import re
import urllib.parse

logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self, connection_string: str, db_type: str = "mssql"):
        """
        Initialize database connector
        
        Args:
            connection_string (str): Database connection string
            db_type (str): Type of database (mssql, postgresql, mysql)
        """
        self.connection_string = connection_string
        self.db_type = db_type.lower()
        self.engine = None
        
        # Validate connection string based on database type
        if not self._validate_connection_string():
            raise ValueError(f"Invalid connection string format for {db_type}")
        
        # Create engine
        try:
            self.engine = create_engine(connection_string)
            logger.info(f"Database connector initialized for {db_type}")
        except Exception as e:
            logger.error(f"Failed to create database engine: {str(e)}")
            raise ValueError(f"Failed to create database engine: {str(e)}")
    
    def _validate_connection_string(self) -> bool:
        """Validate connection string format based on database type"""
        # MSSQL can have several connection string formats
        if self.db_type == "mssql":
            # Check for standard SQL Server connection string formats
            mssql_patterns = [
                # Standard SQLAlchemy format
                r"mssql(\+pyodbc)?:\/\/.*:.*@.*\/.*",
                # DSN format
                r"mssql(\+pyodbc)?:\/\/\?odbc_connect=.*",
                # Trusted connection format
                r"mssql(\+pyodbc)?:\/\/.*\/.*\?trusted_connection=yes",
                # Basic format
                r"mssql(\+pyodbc)?:\/\/.*"
            ]
            
            for pattern in mssql_patterns:
                if re.match(pattern, self.connection_string):
                    return True
                    
            # Also allow ODBC connection strings for flexibility with MSSQL
            if "Driver=" in self.connection_string:
                return True
                
            logger.error(f"Invalid MSSQL connection string format: {self.connection_string}")
            return False
            
        # Other database types
        patterns = {
            "postgresql": r"postgresql(\+psycopg2)?:\/\/.*:.*@.*\/.*",
            "mysql": r"mysql(\+pymysql)?:\/\/.*:.*@.*\/.*"
        }
        
        if self.db_type not in patterns:
            logger.error(f"Unsupported database type: {self.db_type}")
            return False
            
        pattern = patterns[self.db_type]
        return bool(re.match(pattern, self.connection_string))
    
    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """Test database connection"""
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True, None
        except Exception as e:
            error_message = str(e)
            logger.error(f"Connection test failed: {error_message}")
            return False, error_message
    
    async def execute_query(self, sql_query: str) -> Tuple[List[Dict], Optional[str]]:
        """
        Execute SQL query and return results
        
        Args:
            sql_query (str): SQL query to execute
            
        Returns:
            Tuple[List[Dict], Optional[str]]: Results and error message (if any)
        """
        try:
            # Validate query (basic safety check)
            self._validate_query(sql_query)
            
            # Execute query
            with self.engine.connect() as connection:
                result = connection.execute(text(sql_query))
                columns = result.keys()
                rows = result.fetchall()
                
                # Convert to list of dictionaries
                results = [dict(zip(columns, row)) for row in rows]
                return results, None
                
        except Exception as e:
            error_message = str(e)
            logger.error(f"Query execution failed: {error_message}")
            return [], error_message
    
    def _validate_query(self, sql_query: str) -> None:
        """
        Validate SQL query for safety
        
        Args:
            sql_query (str): SQL query to validate
            
        Raises:
            ValueError: If query contains unsafe operations
        """
        unsafe_patterns = [
            r"\bDROP\b",
            r"\bDELETE\b",
            r"\bTRUNCATE\b",
            r"\bALTER\b",
            r"\bUPDATE\b",
            r"\bINSERT\b",
            r"\bCREATE\b",
            r"\bGRANT\b",
            r"\bREVOKE\b",
            r"--",  # SQL comment (could be used for SQL injection)
            r"/\*"  # Start of multi-line comment
        ]
        
        query_upper = sql_query.upper()
        for pattern in unsafe_patterns:
            if re.search(pattern, query_upper, re.IGNORECASE):
                logger.warning(f"Unsafe SQL pattern detected: {pattern}")
                raise ValueError(f"Unsafe SQL operation detected: {pattern}")
        
        # Ensure query starts with SELECT
        if not re.match(r"^\s*SELECT\b", query_upper, re.IGNORECASE):
            logger.warning("Query does not start with SELECT")
            raise ValueError("Only SELECT queries are allowed")
    
    async def get_schema_info(self) -> Dict[str, Any]:
        """
        Get database schema information
        
        Returns:
            Dict[str, Any]: Schema information including tables and columns
        """
        try:
            metadata = MetaData()
            inspector = inspect(self.engine)
            
            schema_info = {
                "tables": {}
            }
            
            # Handle multiple schemas in MSSQL
            schemas = inspector.get_schema_names()
            
            for schema in schemas:
                # Skip system schemas in SQL Server
                if schema in ['sys', 'INFORMATION_SCHEMA', 'guest', 'db_owner', 'db_accessadmin', 
                            'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 
                            'db_datawriter', 'db_denydatareader', 'db_denydatawriter']:
                    continue
                    
                for table_name in inspector.get_table_names(schema=schema):
                    full_table_name = f"{schema}.{table_name}" if schema != 'dbo' else table_name
                    
                    columns = []
                    for column in inspector.get_columns(table_name, schema=schema):
                        columns.append({
                            "name": column["name"],
                            "type": str(column["type"])
                        })
                    
                    # Get primary keys
                    pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
                    primary_keys = pk_constraint.get('constrained_columns', []) if pk_constraint else []
                    
                    # Get foreign keys
                    foreign_keys = []
                    for fk in inspector.get_foreign_keys(table_name, schema=schema):
                        foreign_keys.append({
                            "columns": fk.get("constrained_columns", []),
                            "referred_schema": fk.get("referred_schema", schema),
                            "referred_table": fk.get("referred_table", ""),
                            "referred_columns": fk.get("referred_columns", [])
                        })
                    
                    schema_info["tables"][full_table_name] = {
                        "schema": schema,
                        "columns": columns,
                        "primary_keys": primary_keys,
                        "foreign_keys": foreign_keys
                    }
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get schema info: {str(e)}")
            return {"tables": {}, "error": str(e)}
            
    @staticmethod
    def create_mssql_connection_string(server: str, database: str, username: Optional[str] = None, password: Optional[str] = None, trusted_connection: bool = False, driver: str = "ODBC Driver 17 for SQL Server") -> str:
        """
        Create an MSSQL connection string
        
        Args:
            server (str): SQL Server hostname or IP
            database (str): Database name
            username (str, optional): SQL Server username
            password (str, optional): SQL Server password
            trusted_connection (bool): Use Windows authentication
            driver (str): ODBC driver to use
            
        Returns:
            str: Formatted connection string
        """
        # For Windows authentication
        if trusted_connection:
            conn_str = f"mssql+pyodbc://{server}/{database}?driver={urllib.parse.quote_plus(driver)}&trusted_connection=yes"
        else:
            # For SQL Server authentication
            if not username or not password:
                raise ValueError("Username and password are required for SQL Server authentication")
            conn_str = f"mssql+pyodbc://{username}:{urllib.parse.quote_plus(password)}@{server}/{database}?driver={urllib.parse.quote_plus(driver)}"
        
        return conn_str
    
    def close(self) -> None:
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")