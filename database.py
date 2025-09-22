# database.py
"""
Database operations for SARSIT3tApp using pymssql (pure Python)
No ODBC driver or environment variables required
"""

import pymssql
from contextlib import contextmanager
from config import Config
from typing import Tuple, List, Any, Optional


@contextmanager
def get_db_connection() -> Tuple['pymssql.Connection', 'pymssql.Cursor']:
    """Context manager for Azure SQL connections using pymssql"""

    config = Config.get_db_config()
    connection = None
    cursor = None

    try:
        # Create connection using pure Python pymssql driver
        connection = pymssql.connect(
            server=config['server'],
            user=config['username'],
            password=config['password'],
            database=config['database'],
            port=config['port'],
            ssl=config['use_encryption'],
            login_timeout=config['connection_timeout'],
            timeout=config['query_timeout']
        )

        cursor = connection.cursor()

        # Yield connection and cursor
        yield connection, cursor

    except pymssql.Error as e:
        print(f"Azure SQL connection failed: {e}")
        raise ValueError(f"Failed to connect to Azure SQL: {str(e)}")
    except Exception as e:
        print(f"Unexpected database error: {e}")
        raise ValueError(f"Database connection error: {str(e)}")
    finally:
        # Ensure cleanup
        try:
            if cursor:
                cursor.close()
        except:
            pass

        try:
            if connection:
                connection.close()
        except:
            pass


def execute_query(query: str, params: Optional[tuple] = None) -> Tuple[List[str], List[tuple]]:
    """Execute query and return columns and rows"""
    with get_db_connection() as (conn, cursor):
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        # Get all rows
        rows = cursor.fetchall()

        return columns, rows


def execute_query_single(query: str, params: Optional[tuple] = None) -> Any:
    """Execute query and return single row"""
    with get_db_connection() as (conn, cursor):
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        return cursor.fetchone()


def execute_insert(query: str, params: tuple) -> int:
    """Execute INSERT/UPDATE and return row count"""
    with get_db_connection() as (conn, cursor):
        cursor.execute(query, params)
        conn.commit()
        return cursor.rowcount


def execute_multiple(query: str, params_list: List[tuple]) -> int:
    """Execute multiple INSERTs efficiently"""
    total_rows = 0
    with get_db_connection() as (conn, cursor):
        for params in params_list:
            cursor.execute(query, params)
            total_rows += cursor.rowcount
        conn.commit()
        return total_rows