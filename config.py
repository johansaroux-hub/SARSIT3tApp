# config.py
"""
Azure SQL Server configuration for SARSIT3tApp
Pure Python implementation - no ODBC required
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv


class Config:
    """Azure SQL Server configuration for SARSIT3tApp"""

    # Azure SQL configuration - UPDATE THESE VALUES WITH YOUR ACTUAL SETTINGS
    DATABASE_CONFIG = {
        'server': 'jdlsoft-sarsit3t-sql.database.windows.net',  # e.g., 'sarsit3t.database.windows.net'
        'database': 'SARSIT3tDB',  # e.g., 'SARSIT3tDB'
        'username': 'jdlsoftadmin@jdlsoft-sarsit3t-sql.database.windows.net',  # e.g., 'sarsuser@sarsit3t'
        'password': 'f@incC66',  # Your SQL password
        'port': 1433,
        'use_encryption': True,
        'trust_server_certificate': False,
        'connection_timeout': 30,
        'query_timeout': 60  # ← THIS WAS MISSING - ADD THIS LINE
    }

    @classmethod
    def load_config(cls):
        """Load configuration from .env file if available"""
        load_dotenv()  # Load .env file if it exists

        # Override with environment variables (optional fallback)
        cls.DATABASE_CONFIG['server'] = os.getenv('DB_SERVER', cls.DATABASE_CONFIG['server'])
        cls.DATABASE_CONFIG['database'] = os.getenv('DB_NAME', cls.DATABASE_CONFIG['database'])
        cls.DATABASE_CONFIG['username'] = os.getenv('DB_USERNAME', cls.DATABASE_CONFIG['username'])
        cls.DATABASE_CONFIG['password'] = os.getenv('DB_PASSWORD', cls.DATABASE_CONFIG['password'])
        cls.DATABASE_CONFIG['port'] = int(os.getenv('DB_PORT', cls.DATABASE_CONFIG['port']))
        cls.DATABASE_CONFIG['connection_timeout'] = int(
            os.getenv('DB_CONNECTION_TIMEOUT', cls.DATABASE_CONFIG['connection_timeout']))
        cls.DATABASE_CONFIG['query_timeout'] = int(os.getenv('DB_QUERY_TIMEOUT', cls.DATABASE_CONFIG['query_timeout']))

        # Validate required config
        required_keys = ['server', 'database', 'username', 'password']
        missing_keys = [key for key in required_keys if not cls.DATABASE_CONFIG[key] or
                        (key == 'server' and 'your-server' in str(cls.DATABASE_CONFIG[key]))]

        if missing_keys:
            raise ValueError(
                f"Missing required database configuration: {', '.join(missing_keys)} - Please update config.py with your actual Azure SQL details")

    @classmethod
    def get_db_config(cls) -> Dict[str, Any]:
        """Get Azure SQL configuration"""
        return cls.DATABASE_CONFIG.copy()

    @classmethod
    def test_connection(cls) -> bool:
        """Test Azure SQL connection"""
        try:
            # Import inside try block to avoid circular import issues
            from database import execute_query_single
            result = execute_query_single("SELECT 1 as test_connection")
            return result is not None and result[0] == 1
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False


# Load configuration on import
Config.load_config()

if __name__ == "__main__":
    print("🔧 Azure SQL Configuration Loaded:")
    config = Config.get_db_config()
    print(f"   Server: {config['server']}")
    print(f"   Database: {config['database']}")
    print(f"   User: {config['username'].split('@')[0] if '@' in config['username'] else config['username']}@***")
    print(f"   Port: {config['port']}")
    print(f"   Connection Timeout: {config['connection_timeout']}s")
    print(f"   Query Timeout: {config['query_timeout']}s")

    # Test connection
    if Config.test_connection():
        print("✅ Azure SQL connection successful!")
    else:
        print("❌ Azure SQL connection failed! Please check your configuration.")