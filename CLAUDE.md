# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Flask-based household ledger processing and electronic ledger generation system (住户台账处理与电子台账生成系统). The application has been cleaned up for local development, with Docker/deployment configurations removed in favor of direct local execution.

## Development Setup

### Environment Setup
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate

# Install dependencies
pip install -U pip
pip install -r requirements.txt
```

### Database Configuration
The application supports SQLite (current) and SQL Server. Database configuration is loaded in this priority order:
1. Environment variables: `DATABASE_HOST`, `DATABASE_NAME`, `DATABASE_USER`, `DATABASE_PASSWORD`
2. Configuration file: `config/mssql.json` (copy from `config/mssql.json.example`)

For local development:
```bash
cp config/mssql.json.example config/mssql.json
# Edit mssql.json with actual database credentials
```

### Running the Application
```bash
# Optional: Copy environment variables template
cp .env.example .env

# Start application (default port 8888)
python app.py

# Or specify custom port
FLASK_RUN_PORT=5000 python app.py
```

### Testing
```bash
# Run tests using pytest
pytest

# Run specific test file
pytest test_file.py

# Run with verbose output
pytest -v
```

## Architecture Overview

### Core Components

- **app.py**: Main Flask application entry point with blueprint registration
- **src/database.py**: Database abstraction layer using SQLite connection pooling
- **src/database_pool.py**: SQLite connection pool management for thread safety
- **src/data_processing.py**: Core business logic for household ledger processing

### Blueprint Structure
The application uses Flask blueprints for modular organization:

- **data_generation**: Electronic ledger and summary table generation (`/src/blueprints/data_generation.py`)
- **data_import**: Household list import/export functionality (`/src/blueprints/data_import.py`)
- **statistics**: Statistical analysis by household, township, and month (`/src/blueprints/statistics.py`)
- **household_analysis**: Advanced household analysis features (`/src/blueprints/household_analysis.py`)

### Key Modules

- **src/electronic_ledger_generator.py**: Generates electronic ledgers with Excel formatting
- **src/excel_operations.py**: Excel file read/write operations using openpyxl
- **src/query_service.py**: Centralized database query service
- **src/response_helper.py**: API response formatting utilities
- **src/param_validator.py**: Request parameter validation and sanitization

### Database Architecture

The system migrated from SQL Server to SQLite with the following key tables:
- `调查点户名单` (Household list)
- `调查点台账合并` (Merged ledger data)
- Statistical and analysis tables for reporting

Connection pooling (`src/database_pool.py`) ensures thread safety for concurrent database operations.

### Error Handling

The application uses a centralized error handling approach with:
- `@handle_errors` decorator for route-level error catching
- `ResponseHelper` for consistent API response formatting
- Graceful degradation when database connection fails (restricted mode)

## Development Notes

### Database Migration Context
This codebase recently migrated from SQL Server to SQLite. Files like `migrate_mssql_to_sqlite.py` contain migration utilities. The `src/database_pool_mssql_backup.py` preserves the original SQL Server pool implementation.

### File Upload Handling
- Upload directory: `uploads/` (auto-created on startup)
- Maximum file size: 16MB (configurable via `MAX_CONTENT_LENGTH`)
- Supported formats: Excel files (.xlsx, .xls) for household data import

### Restricted Mode
When database connection fails, the application starts in "restricted mode" where:
- Health check endpoints remain available
- Database-dependent features are disabled
- Excel operations may still function independently

### Threading Considerations
- Uses connection pooling for thread-safe database access
- Progress tracking for long-running operations (electronic ledger generation)
- Thread locks for shared cache management (township codes)

## Key Endpoints

- `/` - Main application interface
- `/health` - Health check endpoint
- `/api/system/status` - System status including database connectivity
- `/debug` - Debug page (non-production only)
- Blueprint-specific endpoints for data generation, import, statistics, and analysis

## Migration and Utility Scripts

Several migration and utility scripts are available in the root directory:
- `migrate_mssql_to_sqlite.py`: Database migration from SQL Server
- `create_sqlite_database.py`: SQLite database creation
- `add_test_data.py`: Test data population
- `optimize_database_performance.py`: Database optimization utilities