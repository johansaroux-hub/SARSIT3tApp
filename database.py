# database.py - CORRECTED VERSION
"""
Database operations for SARSIT3tApp using pymssql (pure Python)
No ODBC driver or environment variables required
"""

import pymssql
from contextlib import contextmanager
from flask import current_app
from typing import Tuple, List, Any, Optional
from config import Config


@contextmanager
def get_db_connection() -> Tuple['pymssql.Connection', 'pymssql.Cursor']:
    """Context manager for Azure SQL connections using pymssql"""

    config = Config.get_db_config()
    connection = None
    cursor = None

    try:
        # Ensure all required config keys exist
        required_keys = ['server', 'database', 'username', 'password', 'port']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")

        # Set default values for optional parameters
        use_ssl = config.get('use_encryption', True)
        login_timeout = config.get('connection_timeout', 30)
        query_timeout = config.get('query_timeout', 60)
        trust_cert = config.get('trust_server_certificate', False)

        # pymssql connection parameters - CORRECTED
        connection_params = {
            'host': config['server'],  # pymssql uses 'host' not 'server'
            'user': config['username'],
            'password': config['password'],
            'database': config['database'],
            'port': config['port'],
            'login_timeout': login_timeout,
            'timeout': query_timeout,  # Query execution timeout
        }

        # Add SSL parameters for Azure SQL (pymssql specific)
        if use_ssl:
            connection_params['tds_version'] = '7.4'  # Required for SSL
            # pymssql handles SSL automatically for Azure SQL - no explicit 'ssl' parameter
            if trust_cert:
                # For self-signed certificates (not recommended for production)
                connection_params['ca_pem'] = False
            else:
                # Use system certificate store for Azure SQL
                pass
        else:
            # Disable encryption (not recommended for Azure SQL)
            connection_params['tds_version'] = '7.3'

        # Create connection using pure Python pymssql driver
        current_app.logger.info(
            f"Connecting to {config['server']}:{config['port']} as {config['username'].split('@')[0]}@***")
        connection = pymssql.connect(**connection_params)

        cursor = connection.cursor()

        # Test connection with simple query
        cursor.execute("SELECT 1")
        test_result = cursor.fetchone()
        if test_result[0] != 1:
            raise ValueError("Connection test query failed")

        current_app.logger.info(f"✅ Successfully connected to database: {config['database']}")

        # Yield connection and cursor
        yield connection, cursor

    except KeyError as e:
        current_app.logger.error(f"Configuration error: Missing key {e}")
        raise ValueError(f"Database configuration error: Missing '{e}' in config")
    except pymssql.Error as e:
        error_msg = f"Azure SQL connection failed: {str(e)}"
        current_app.logger.error(error_msg)
        raise ValueError(error_msg)
    except Exception as e:
        error_msg = f"Unexpected database error: {str(e)}"
        current_app.logger.error(error_msg)
        raise ValueError(error_msg)
    finally:
        # Ensure cleanup
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if connection:
                connection.close()
        except Exception:
            pass


def execute_query(query: str, params: Optional[tuple] = None) -> Tuple[List[str], List[tuple]]:
    """Execute query and return columns and rows"""
    try:
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
    except Exception as e:
        current_app.logger.error(f"Query execution failed: {e}")
        raise


def execute_query_single(query: str, params: Optional[tuple] = None) -> Any:
    """Execute query and return single row"""
    try:
        with get_db_connection() as (conn, cursor):
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            return cursor.fetchone()
    except Exception as e:
        current_app.logger.error(f"Single query execution failed: {e}")
        raise


def execute_insert(query: str, params: tuple) -> int:
    """Execute INSERT/UPDATE and return row count"""
    try:
        with get_db_connection() as (conn, cursor):
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
    except Exception as e:
        current_app.logger.error(f"Insert execution failed: {e}")
        raise


def execute_multiple(query: str, params_list: List[tuple]) -> int:
    """Execute multiple INSERTs efficiently"""
    total_rows = 0
    try:
        with get_db_connection() as (conn, cursor):
            for params in params_list:
                cursor.execute(query, params)
                total_rows += cursor.rowcount
            conn.commit()
            return total_rows
    except Exception as e:
        current_app.logger.error(f"Multiple insert execution failed: {e}")
        raise