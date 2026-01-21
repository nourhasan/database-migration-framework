"""
Configuration module for database migration framework.

This module loads environment variables from .env file and provides
application-wide configuration settings and logging setup.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
# Look for .env in the project root (parent of config directory)
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


class Config:
    """
    Configuration class that loads and validates environment variables.
    
    All database credentials and configuration are loaded from environment
    variables. No values are hardcoded.
    """
    
    # Database configuration
    DB_ENGINE = os.getenv('DB_ENGINE')  # sqlserver, postgresql, or mysql
    DB_SERVER = os.getenv('DB_SERVER')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_PORT = os.getenv('DB_PORT')  # Optional, uses default if not set
    
    # Application configuration
    EXCEL_PATH = os.getenv('EXCEL_PATH')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate(cls):
        """
        Validate that required configuration values are present.
        
        Raises:
            ValueError: If required configuration is missing
        """
        required_vars = {
            'DB_ENGINE': cls.DB_ENGINE,
            'DB_SERVER': cls.DB_SERVER,
            'DB_NAME': cls.DB_NAME,
            'DB_USER': cls.DB_USER,
            'DB_PASSWORD': cls.DB_PASSWORD
        }
        
        missing = [key for key, value in required_vars.items() if not value]
        
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}. "
                f"Please check your .env file."
            )
        
        # Validate DB_ENGINE value
        supported_engines = ['sqlserver', 'postgresql', 'mysql']
        if cls.DB_ENGINE not in supported_engines:
            raise ValueError(
                f"DB_ENGINE '{cls.DB_ENGINE}' is not supported. "
                f"Supported engines: {', '.join(supported_engines)}"
            )
    
    @classmethod
    def get_db_config(cls):
        """
        Get database configuration as a dictionary.
        
        Returns:
            dict: Database configuration parameters
        """
        return {
            'engine': cls.DB_ENGINE,
            'server': cls.DB_SERVER,
            'database': cls.DB_NAME,
            'user': cls.DB_USER,
            'password': cls.DB_PASSWORD,
            'port': cls.DB_PORT
        }


def setup_logging(name: str = None) -> logging.Logger:
    """
    Set up and configure logging for migrations.
    
    Args:
        name: Logger name (typically __name__ from calling module)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name or __name__)
    
    # Only configure if no handlers exist (avoid duplicate configuration)
    if not logger.handlers:
        logger.setLevel(getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO))
        
        # Create console handler with formatting
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
    
    return logger


# Validate configuration on module import
try:
    Config.validate()
except ValueError as e:
    # Log the error but don't fail on import - let migrations handle it
    logger = setup_logging(__name__)
    logger.warning(f"Configuration validation failed: {e}")
