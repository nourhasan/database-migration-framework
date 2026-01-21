# Database Migration Framework

A modular, database-agnostic Python migration framework that enables independent, repeatable, and safe migrations across multiple database engines (SQL Server, PostgreSQL, MySQL).

## 🎯 Key Features

- **Database Agnostic**: Seamlessly switch between SQL Server, PostgreSQL, and MySQL via environment configuration
- **Self-Contained Migrations**: Each migration is an independent Python script that can be executed directly
- **No Central Runner**: Run migrations with a simple `python migrations/001_migration.py` command
- **Transaction Safety**: Automatic commit/rollback handling with explicit transaction control
- **Zero Hardcoded Credentials**: All configuration loaded from environment variables
- **Production-Ready**: Comprehensive logging, error handling, and idempotent operations

## 📋 Architecture

```
database-migration-framework/
├── config/
│   └── env_config.py          # Environment configuration and logging setup
├── lib/
│   └── db_connection.py       # Database abstraction layer (factory pattern)
├── migrations/
│   └── 001_import_customer_permissions.py   # Example migration
├── .env.example               # Environment configuration template
├── requirements.txt           # Python dependencies
└── README.md
```

### Core Components

#### 1. Configuration Module (`config/env_config.py`)
- Loads environment variables from `.env` file using `python-dotenv`
- Validates required configuration on startup
- Provides centralized logging setup
- Exposes `Config` class for accessing DB settings

#### 2. Database Abstraction Layer (`lib/db_connection.py`)
- Factory pattern for creating database connections based on `DB_ENGINE`
- Unified interface across SQL Server, PostgreSQL, and MySQL
- Transaction management (commit/rollback)
- Context manager for automatic transaction handling
- Connection pooling ready

#### 3. Migration Files (`migrations/*.py`)
- Self-contained Python scripts
- Each migration manages its own:
  - Database connection
  - Transaction lifecycle
  - Logging and error handling
- Executable independently: `python migrations/001_migration.py`

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Database-specific drivers (installed via requirements.txt)
- Access to target database (SQL Server, PostgreSQL, or MySQL)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/database-migration-framework.git
   cd database-migration-framework
   ```

2. **Create a virtual environment** (recommended)
   ```bash
   python -m venv venv
   
   # Windows
   venv\Scripts\activate
   
   # Linux/Mac
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**
   ```bash
   # Copy the example configuration
   copy .env.example .env   # Windows
   cp .env.example .env     # Linux/Mac
   
   # Edit .env with your database credentials
   ```

### Configuration

Edit the `.env` file with your database connection details:

```env
# Choose your database engine
DB_ENGINE=sqlserver          # Options: sqlserver, postgresql, mysql

# Connection details
DB_SERVER=localhost
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password

# Optional: Override default ports
DB_PORT=                     # SQL Server: 1433, PostgreSQL: 5432, MySQL: 3306

# Application settings
EXCEL_PATH=./data/import.xlsx
LOG_LEVEL=INFO
```

### Database-Specific Setup

#### SQL Server
Requires ODBC Driver 17 for SQL Server:
- **Windows**: Usually pre-installed
- **Linux/Mac**: Download from [Microsoft ODBC Driver](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

```env
DB_ENGINE=sqlserver
DB_SERVER=localhost
DB_PORT=1433
```

#### PostgreSQL
```env
DB_ENGINE=postgresql
DB_SERVER=localhost
DB_PORT=5432
```

#### MySQL
```env
DB_ENGINE=mysql
DB_SERVER=localhost
DB_PORT=3306
```

## 📝 Usage

### Running a Migration

Execute any migration directly:

```bash
python migrations/001_import_customer_permissions.py
```

**That's it!** No command-line arguments, no central runner, no configuration flags.

### Migration Output

```
2026-01-21 10:30:45 - migrations.001_import_customer_permissions - INFO - ======================================================================
2026-01-21 10:30:45 - migrations.001_import_customer_permissions - INFO - Starting migration: Import Customer Permissions
2026-01-21 10:30:45 - migrations.001_import_customer_permissions - INFO - ======================================================================
2026-01-21 10:30:45 - migrations.001_import_customer_permissions - INFO - Reading Excel file: ./data/import.xlsx
2026-01-21 10:30:45 - migrations.001_import_customer_permissions - INFO - Read 50 rows from Excel file
2026-01-21 10:30:45 - lib.db_connection - INFO - Connected to SQL Server: localhost/mydb
2026-01-21 10:30:46 - migrations.001_import_customer_permissions - INFO - Row 1: Inserted permission for user@example.com (Admin) on CustomerA
...
2026-01-21 10:30:50 - migrations.001_import_customer_permissions - INFO - ======================================================================
2026-01-21 10:30:50 - migrations.001_import_customer_permissions - INFO - Migration completed successfully
2026-01-21 10:30:50 - migrations.001_import_customer_permissions - INFO - Total rows processed: 50
2026-01-21 10:30:50 - migrations.001_import_customer_permissions - INFO - New permissions inserted: 45
2026-01-21 10:30:50 - migrations.001_import_customer_permissions - INFO - Existing permissions skipped: 5
2026-01-21 10:30:50 - migrations.001_import_customer_permissions - INFO - ======================================================================
```

### Switching Databases

Simply change `DB_ENGINE` in your `.env` file:

```bash
# Switch from SQL Server to PostgreSQL
DB_ENGINE=postgresql

# Run the same migration on a different database
python migrations/001_import_customer_permissions.py
```

## 🔧 Creating New Migrations

### Step 1: Create a new migration file

```bash
# Naming convention: <sequence>_<description>.py
migrations/002_add_user_roles.py
```

### Step 2: Use the migration template

```python
"""
Migration: <Your Migration Name>

Description of what this migration does.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.env_config import Config, setup_logging
from lib.db_connection import get_db_connection

logger = setup_logging(__name__)


def run_migration():
    """Execute the migration."""
    logger.info("=" * 70)
    logger.info("Starting migration: <Your Migration Name>")
    logger.info("=" * 70)
    
    db = None
    
    try:
        # Validate configuration
        Config.validate()
        
        # Open database connection
        logger.info(f"Connecting to database: {Config.DB_ENGINE}")
        db = get_db_connection()
        
        # Your migration logic here
        db.execute("CREATE TABLE example (id INT, name VARCHAR(100))")
        
        # Commit transaction
        db.commit()
        logger.info("Migration completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        
        if db:
            db.rollback()
            logger.info("Transaction rolled back")
        
        raise
    
    finally:
        if db:
            db.close()
            logger.info("Database connection closed")


if __name__ == '__main__':
    try:
        run_migration()
    except Exception as e:
        logger.error(f"Migration execution failed: {e}")
        sys.exit(1)
```

### Step 3: Run your migration

```bash
python migrations/002_add_user_roles.py
```

**No other files need to be modified!**

## 🛡️ Best Practices

### Idempotent Migrations

Design migrations to be safely re-runnable:

```python
# ✅ Good: Check if record exists before insert
if not record_exists(db, user_id):
    db.execute("INSERT INTO users (id, name) VALUES (?, ?)", (user_id, name))

# ❌ Bad: Insert without checking
db.execute("INSERT INTO users (id, name) VALUES (?, ?)", (user_id, name))
```

### Transaction Safety

Always use try/except/finally blocks:

```python
try:
    # Migration logic
    db.commit()
except Exception as e:
    db.rollback()
    raise
finally:
    db.close()
```

### Database-Agnostic SQL

Write SQL that works across all supported databases:

```python
# ✅ Good: Parameter placeholders work everywhere
db.execute("SELECT * FROM users WHERE id = ?", (user_id,))

# Use CURRENT_TIMESTAMP instead of NOW() or GETDATE()
db.execute("INSERT INTO logs (created_at) VALUES (CURRENT_TIMESTAMP)")
```

### Logging

Use structured logging throughout:

```python
logger.info(f"Processing user: {email}")
logger.warning(f"User not found: {email}")
logger.error(f"Failed to insert: {e}")
logger.debug(f"Query result: {result}")
```

## 🔍 Advanced Usage

### Using the Transaction Context Manager

For simpler transaction handling:

```python
from lib.db_connection import transaction

with transaction() as db:
    db.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
    # Automatically commits on success, rolls back on exception
```

### Custom Configuration

Access configuration values in your migrations:

```python
from config.env_config import Config

excel_path = Config.EXCEL_PATH
log_level = Config.LOG_LEVEL
db_config = Config.get_db_config()
```

### Database-Specific Queries

When necessary, use engine-specific logic:

```python
if Config.DB_ENGINE == 'sqlserver':
    db.execute("SELECT TOP 10 * FROM users")
elif Config.DB_ENGINE == 'postgresql':
    db.execute("SELECT * FROM users LIMIT 10")
elif Config.DB_ENGINE == 'mysql':
    db.execute("SELECT * FROM users LIMIT 10")
```

## 🧪 Testing Migrations

Before running on production:

1. **Test on development database first**
   ```bash
   # Update .env to point to dev database
   DB_NAME=dev_database
   
   # Run migration
   python migrations/001_migration.py
   ```

2. **Verify idempotency**
   ```bash
   # Run the same migration twice
   python migrations/001_migration.py
   python migrations/001_migration.py
   
   # Should complete successfully both times
   ```

3. **Test rollback scenarios**
   - Introduce a deliberate error midway
   - Verify transaction rolls back
   - Verify database state is unchanged

## 🐛 Troubleshooting

### Connection Errors

**Error**: `Failed to connect to sqlserver: [Database error]`

**Solution**:
- Verify database server is running
- Check credentials in `.env` file
- Ensure firewall allows connection
- For SQL Server: Verify ODBC driver is installed

### Module Import Errors

**Error**: `ModuleNotFoundError: No module named 'config'`

**Solution**:
- Run migrations from project root: `python migrations/001_migration.py`
- Or ensure Python path includes project root

### Missing Dependencies

**Error**: `ImportError: pyodbc is required for SQL Server`

**Solution**:
```bash
pip install -r requirements.txt
```

## 📦 Dependencies

- **python-dotenv**: Environment variable management
- **openpyxl**: Excel file reading
- **pyodbc**: SQL Server driver (Windows)
- **psycopg2-binary**: PostgreSQL adapter
- **mysql-connector-python**: MySQL connector

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add your migration or enhancement
4. Test across all supported databases
5. Submit a pull request

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details

## 🔗 Related Resources

- [SQL Server ODBC Driver](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [MySQL Documentation](https://dev.mysql.com/doc/)
- [Python dotenv Documentation](https://pypi.org/project/python-dotenv/)

---

**Built with ❤️ for database migration simplicity**.
