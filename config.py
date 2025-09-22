# config.py
"""
Configuration for SARSIT3tApp - Azure SQL Server only
Uses pymssql (pure Python) - no ODBC driver required
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv


class Config:
    """Azure SQL Server configuration for SARSIT3tApp"""

    # Azure SQL configuration - UPDATE THESE VALUES
    DATABASE_CONFIG = {
        'server': 'jdlsoft-sarsit3t-sql.database.windows.net',
        'database': 'SARSIT3tDB',
        'username': 'jdlsoftadmin',
        'password': 'f@incC66',
        'port': 1433,
        'use_encryption': True,
        'trust_server_certificate': False,
        'connection_timeout': 30
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

        # Validate required config
        required_keys = ['server', 'database', 'username', 'password']
        for key in required_keys:
            if not cls.DATABASE_CONFIG[key]:
                raise ValueError(f"Missing required database configuration: {key}")

    @classmethod
    def get_db_config(cls) -> Dict[str, Any]:
        """Get Azure SQL configuration"""
        return cls.DATABASE_CONFIG.copy()

    @classmethod
    def test_connection(cls) -> bool:
        """Test Azure SQL connection"""
        try:
            from database import get_db_connection
            with get_db_connection() as (conn, cursor):
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False


# Load configuration on import
Config.load_config()

if __name__ == "__main__":
    print("🔧 Azure SQL Configuration:")
    config = Config.get_db_config()
    print(f"   Server: {config['server']}")
    print(f"   Database: {config['database']}")
    print(f"   User: {config['username'].split('@')[0]}@***")

    # Test connection
    if Config.test_connection():
        print("✅ Azure SQL connection successful!")
    else:
        print("❌ Azure SQL connection failed!")



