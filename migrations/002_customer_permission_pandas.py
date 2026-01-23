"""
Migration: Customer Permission Import with Pandas and Bulk Insert

This migration demonstrates:
- Using pandas for efficient Excel data manipulation
- Bulk lookup mapping for users, roles, and customers
- Fast bulk insert with executemany
- Comprehensive validation before insert
- Idempotent inserts using WHERE NOT EXISTS
- File and console logging

Excel Structure:
- Sheet: Users (column: Email)
- Sheet: Roles (column: RoleName)  
- Sheet: Customers (column: CustomerName)
- Rows are positionally aligned across sheets

Database Tables Expected:
- Users (UserId, Email)
- Roles (RoleId, RoleName)
- Customers (CustomerId, CustomerName)
- CustomerPermission (UserId, RoleId, CustomerId)
"""

import sys
from pathlib import Path
from datetime import datetime
import logging

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.env_config import Config, setup_logging

try:
    import pandas as pd
except ImportError:
    raise ImportError(
        "pandas is required for this migration. "
        "Install it with: pip install pandas"
    )

try:
    import pyodbc
except ImportError:
    raise ImportError(
        "pyodbc is required for SQL Server connections. "
        "Install it with: pip install pyodbc"
    )

# Set up logging for this migration (both file and console)
LOG_FILE = f"customer_permission_migration_{datetime.now():%Y%m%d_%H%M%S}.log"

logger = logging.getLogger("CustomerPermissionMigration")
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)


def load_excel_data(excel_path: str) -> pd.DataFrame:
    """
    Load and combine data from three Excel sheets.
    
    Args:
        excel_path: Path to Excel file
    
    Returns:
        DataFrame with Email, RoleName, CustomerName columns
    
    Raises:
        FileNotFoundError: If Excel file doesn't exist
        ValueError: If sheets are missing or row counts don't match
    """
    logger.info("Loading Excel file from %s", excel_path)
    
    if not Path(excel_path).exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")
    
    try:
        users_df = pd.read_excel(excel_path, sheet_name="Users")
        roles_df = pd.read_excel(excel_path, sheet_name="Roles")
        customers_df = pd.read_excel(excel_path, sheet_name="Customers")
    except ValueError as e:
        raise ValueError(f"Error reading Excel sheets: {e}")
    
    # Validate row counts match
    if not (len(users_df) == len(roles_df) == len(customers_df)):
        raise ValueError(
            f"Excel sheets row counts do not match: "
            f"Users={len(users_df)}, Roles={len(roles_df)}, Customers={len(customers_df)}"
        )
    
    # Combine into single DataFrame
    permissions_df = pd.DataFrame({
        "Email": users_df["Email"],
        "RoleName": roles_df["RoleName"],
        "CustomerName": customers_df["CustomerName"]
    })
    
    logger.info("Loaded %d permission rows", len(permissions_df))
    
    return permissions_df


def fetch_lookup_map(cursor, sql: str) -> dict:
    """
    Execute a lookup query and return as dictionary.
    
    Args:
        cursor: Database cursor
        sql: SQL query returning two columns (key, value)
    
    Returns:
        Dictionary mapping first column to second column
    """
    return {row[0]: row[1] for row in cursor.execute(sql).fetchall()}


def run_migration():
    """
    Execute the customer permissions import migration with pandas and bulk operations.
    
    This function:
    1. Loads Excel data using pandas
    2. Connects to SQL Server
    3. Fetches lookup data (users, roles, customers)
    4. Maps IDs using pandas
    5. Validates all mappings exist
    6. Performs bulk idempotent insert
    7. Commits on success or rolls back on failure
    """
    logger.info("=" * 70)
    logger.info("Starting migration: Customer Permission Import")
    logger.info("=" * 70)
    
    conn = None
    cursor = None
    
    try:
        # -----------------------
        # VALIDATE CONFIGURATION
        # -----------------------
        Config.validate()
        
        # Ensure DB_ENGINE is SQL Server
        if Config.DB_ENGINE.lower() != 'sqlserver':
            raise ValueError(
                f"This migration is designed for SQL Server only. "
                f"Current DB_ENGINE: {Config.DB_ENGINE}"
            )
        
        excel_path = Config.EXCEL_PATH
        if not excel_path:
            raise ValueError(
                "EXCEL_PATH not set in environment configuration. "
                "Please set it in your .env file."
            )
        
        # -----------------------
        # LOAD EXCEL DATA
        # -----------------------
        permissions_df = load_excel_data(excel_path)
        
        if permissions_df.empty:
            logger.warning("No data found in Excel file. Nothing to import.")
            return
        
        # -----------------------
        # CONNECT TO SQL SERVER
        # -----------------------
        logger.info("Connecting to SQL Server: %s", Config.DB_SERVER)
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={Config.DB_SERVER};"
            f"DATABASE={Config.DB_NAME};"
            f"UID={Config.DB_USER};"
            f"PWD={Config.DB_PASSWORD};"
        )
        
        conn = pyodbc.connect(conn_str)
        conn.autocommit = False
        cursor = conn.cursor()
        cursor.fast_executemany = True  # Enable fast bulk operations
        
        logger.info("Connected to SQL Server: %s/%s", Config.DB_SERVER, Config.DB_NAME)
        
        # -----------------------
        # FETCH LOOKUP DATA
        # -----------------------
        logger.info("Fetching lookup data from database")
        
        user_map = fetch_lookup_map(cursor, "SELECT Email, UserId FROM Users")
        role_map = fetch_lookup_map(cursor, "SELECT RoleName, RoleId FROM Roles")
        customer_map = fetch_lookup_map(cursor, "SELECT CustomerName, CustomerId FROM Customers")
        
        logger.info(
            "Loaded lookups: %d users, %d roles, %d customers",
            len(user_map), len(role_map), len(customer_map)
        )
        
        # -----------------------
        # MAP IDS USING PANDAS
        # -----------------------
        logger.info("Mapping IDs to Excel data")
        
        permissions_df["UserId"] = permissions_df["Email"].map(user_map)
        permissions_df["RoleId"] = permissions_df["RoleName"].map(role_map)
        permissions_df["CustomerId"] = permissions_df["CustomerName"].map(customer_map)
        
        # -----------------------
        # VALIDATE MAPPINGS
        # -----------------------
        missing = permissions_df[
            permissions_df[["UserId", "RoleId", "CustomerId"]]
            .isnull()
            .any(axis=1)
        ]
        
        if not missing.empty:
            logger.error("Missing mappings detected for %d rows:", len(missing))
            logger.error("\n%s", missing[["Email", "RoleName", "CustomerName", "UserId", "RoleId", "CustomerId"]])
            raise ValueError(
                f"Migration aborted: {len(missing)} rows have missing database references. "
                f"Check the log file for details."
            )
        
        logger.info("All mappings validated successfully")
        
        # -----------------------
        # BULK INSERT (IDEMPOTENT)
        # -----------------------
        logger.info("Inserting data into CustomerPermission table")
        
        insert_sql = """
        INSERT INTO CustomerPermission (UserId, RoleId, CustomerId)
        SELECT ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM CustomerPermission
            WHERE UserId = ? AND RoleId = ? AND CustomerId = ?
        )
        """
        
        # Prepare rows for bulk insert
        # Each row needs (UserId, RoleId, CustomerId) twice for the WHERE NOT EXISTS clause
        rows = [
            (
                int(r.UserId), int(r.RoleId), int(r.CustomerId),
                int(r.UserId), int(r.RoleId), int(r.CustomerId)
            )
            for r in permissions_df.itertuples(index=False)
        ]
        
        cursor.executemany(insert_sql, rows)
        
        rows_affected = cursor.rowcount
        logger.info(
            "Inserted %d records (duplicates skipped: %d)",
            rows_affected,
            len(permissions_df) - rows_affected
        )
        
        # -----------------------
        # COMMIT TRANSACTION
        # -----------------------
        conn.commit()
        logger.info("Transaction committed successfully")
        
        logger.info("=" * 70)
        logger.info("Migration completed successfully")
        logger.info("Total rows processed: %d", len(permissions_df))
        logger.info("New records inserted: %d", rows_affected)
        logger.info("Duplicate records skipped: %d", len(permissions_df) - rows_affected)
        logger.info("Log file: %s", LOG_FILE)
        logger.info("=" * 70)
        
    except Exception as e:
        logger.exception("Migration failed — rolling back")
        
        if conn:
            conn.rollback()
            logger.info("Transaction rolled back")
        
        raise
    
    finally:
        # Clean up resources
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Database connection closed")


if __name__ == '__main__':
    try:
        run_migration()
        logger.info("Migration execution completed successfully")
    except Exception as e:
        logger.error("Migration terminated with errors: %s", e)
        sys.exit(1)
