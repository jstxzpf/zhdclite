# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## System Overview

This is a Flask-based household data management system that processes Excel files, synchronizes with external databases, and generates electronic ledgers. The system uses SQL Server as its database and is containerized for deployment.

## Core Architecture

### Application Structure
- **app.py**: Main Flask application entry point with blueprint registration
- **src/**: Core application modules
  - **blueprints/**: Modular functionality as Flask blueprints
    - `data_generation.py`: Electronic ledger generation
    - `data_import.py`: Excel file upload and processing
    - `database_sync.py`: External database synchronization
    - `direct_coding.py`: Direct coding/matching functionality
    - `statistics.py`: Statistical analysis and reporting
  - **database.py**: Database connection management with retry logic and thread safety
  - **data_processing.py**: Core data transformation logic
  - **excel_operations.py**: Excel file reading/writing operations
  - **electronic_ledger_generator.py**: Electronic ledger creation logic

### Database Architecture
- Primary database: SQL Server (configured via `config/mssql.json` or environment variables)
- External database synchronization support
- Connection pooling and retry mechanisms implemented
- Thread-safe database operations

#### Core Tables
- **调查点台账合并**: Main household survey data consolidation table
- **调查点户名单**: Household registry with household head information
- **调查点村名单**: Village registry with village codes and names
- **调查品种编码**: Item/category coding table for classification
- **已经编码完成**: Completed coding records
- **国家点待导入**: National survey points pending import

#### Connection Management
- Retry mechanism: Up to 5 attempts with exponential backoff
- Thread safety: RLock implementation for concurrent access
- Connection validation: Health checks before query execution
- Resource cleanup: Automatic connection and cursor management

### Blueprint Pattern
Each blueprint is initialized with dependencies via `init_blueprint()` functions, promoting loose coupling and testability.

## Development Commands

### Running the Application
```bash
# Development (local)
python app.py

# Production (Docker)
docker compose up -d

# Check system status
./check_system.sh
```

### Docker Operations
```bash
# Full build (base + app images)
./scripts/build_multistage.sh -f

# Smart build (checks dependencies)
./scripts/build_multistage.sh -c

# Base image only
./scripts/build_multistage.sh -b

# App image only  
./scripts/build_multistage.sh -a

# Deploy with Docker
./deploy_docker.sh

# Quick rebuild for development
./scripts/quick_build.sh
```

#### Advanced Build Options
- **Multi-stage build**: Base image + App image separation for optimization
- **Smart dependency checking**: Rebuilds base image only when requirements.txt changes
- **DaoCloud mirror support**: Optimized registry acceleration for Chinese users
- **Build caching**: Git commit tracking and intelligent rebuild decisions

### Database Configuration
- Configuration files: `config/mssql.json` (use `config/mssql.json.example` as template)
- Environment variables take precedence over config files
- External database config: `config/wbfwq.json`

### System Health Monitoring
```bash
# Comprehensive system check
./check_system.sh

# Enhanced health check
python healthcheck_enhanced.py

# View logs
docker compose logs -f household-data-app
```

#### Monitoring Architecture
- **System Monitor** (`src/system_monitor.py`): Comprehensive system health tracking
- **Memory Monitor** (`src/memory_monitor.py`): Memory usage optimization and cleanup
- **Database Health Checker** (`src/database_health_checker.py`): Connection monitoring with recovery
- **Error Handler** (`src/error_handler.py`): Centralized error management and categorization
- **Health endpoint**: `/api/system/status` for automated monitoring

## Key Patterns and Conventions

### Error Handling
- Use `@handle_errors` decorator from `src.utils` for consistent error handling
- All blueprint endpoints should be wrapped with error handling

### File Operations
- Upload validation via `allowed_file()` and `validate_file_size()` from `src.utils`
- Excel operations centralized in `src.excel_operations`
- File uploads go to `uploads/` directory

### Database Operations
- Use `Database` class with context managers for connection management
- Implement retry logic for database operations
- Thread-safe operations via `_lock` mechanism

### Configuration Priority
1. Environment variables (highest priority)
2. JSON configuration files
3. Default values

#### Configuration Files
- **Primary DB**: `config/mssql.json` (use `config/mssql.json.example` as template)
- **External DB**: `config/wbfwq.json` for synchronization
- **Environment Variables**: `DB_*`, `EXTERNAL_DB_*`, `FLASK_*`, `MAX_CONTENT_LENGTH`

## Testing Framework

### Running Tests
```bash
# Run all tests
pytest

# Run with verbose output  
pytest -v

# Run specific test file
pytest tests/test_app.py
```

### Test Setup
- **Framework**: pytest 8.2.2
- **Configuration**: PYTHONPATH automatically configured in Docker test stage
- **Coverage**: API endpoint validation and system status testing

## Important Notes

### System Characteristics
- System uses Chinese language in logs and error messages
- Database connections are automatically managed with retry logic
- The system includes memory monitoring and system health checks
- Large file support: 50MB+ Excel file handling with memory-efficient processing
- Thread-safe operations with RLock implementation for concurrent access

### Performance Optimizations
- **Memory management**: Automatic garbage collection and memory usage tracking
- **Connection pooling**: Efficient database resource usage with health validation
- **Docker optimization**: Multi-stage builds with layer caching and registry acceleration
- **File processing**: Streaming operations for large Excel files