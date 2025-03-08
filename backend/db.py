from typing import Dict, List, Optional, Tuple, Any
import sqlalchemy
from sqlalchemy import create_engine, inspect, text
import logging
import pandas as pd
import urllib.parse
import pyodbc

logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self, connection_string: str, db_type: str = "mssql"):
        """Initialize database connector with connection string"""
        self.connection_string = connection_string
        self.db_type = db_type
        self.engine = create_engine(connection_string)

    @staticmethod
    def get_available_drivers() -> List[str]:
        """Get list of available ODBC drivers"""
        return [driver for driver in pyodbc.drivers() if 'SQL Server' in driver]

    @staticmethod
    def create_mssql_connection_string(
        server: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        trusted_connection: bool = False,
        driver: str = "ODBC Driver 17 for SQL Server"
    ) -> str:
        """Create MSSQL connection string from parameters"""
        available_drivers = DatabaseConnector.get_available_drivers()
        if not available_drivers:
            raise ValueError("No SQL Server ODBC drivers found on the system")
        
        if driver not in available_drivers:
            # Try to use the latest available driver
            driver = available_drivers[-1]
            logger.warning(f"Specified driver not found. Using {driver} instead")

        params = {
            "DRIVER": f"{{{driver}}}",
            "SERVER": server,
            "DATABASE": database,
        }

        if trusted_connection:
            params["Trusted_Connection"] = "yes"
        elif username and password:
            params["UID"] = username
            params["PWD"] = password

        param_str = ";".join(f"{k}={v}" for k, v in params.items())
        return f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(param_str)}"

    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """Test the database connection"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True, None
        except Exception as e:
            error_msg = str(e)
            if "IM002" in error_msg:
                available_drivers = self.get_available_drivers()
                error_msg += f"\nAvailable SQL Server drivers: {', '.join(available_drivers)}"
            return False, error_msg

    async def get_schema_info(self) -> Dict[str, Any]:
        """Get database schema information"""
        try:
            inspector = inspect(self.engine)
            schema_info = {
                "tables": {}
            }

            for table_name in inspector.get_table_names():
                columns = inspector.get_columns(table_name)
                schema_info["tables"][table_name] = {
                    "columns": [
                        {
                            "name": col["name"],
                            "type": str(col["type"]),
                        }
                        for col in columns
                    ]
                }

            return schema_info
        except Exception as e:
            logger.error(f"Error getting schema info: {str(e)}")
            raise

    async def execute_query(self, query: str) -> Tuple[Optional[List[Dict]], Optional[str]]:
        """Execute SQL query and return results"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                if result.returns_rows:
                    rows = result.fetchall()
                    columns = result.keys()
                    return [dict(zip(columns, row)) for row in rows], None
                return [], None
        except Exception as e:
            return None, str(e)

    def close(self):
        """Close database connection"""
        self.engine.dispose()
