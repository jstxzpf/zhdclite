# GEMINI.md

## Project Overview

This project is a Flask-based web application for analyzing household financial data. It provides a RESTful API to generate analysis reports on household income and expenditure. The application uses a SQL Server database as its data source and can generate reports for individual households or for entire areas (towns or villages).

The core functionality of the application is to:
-   Connect to a SQL Server database to retrieve household financial data.
-   Provide API endpoints for analyzing household data.
-   Generate detailed analysis reports, including consumption profiles, anomaly detection, and data quality assessment.
-   The application is structured using Flask Blueprints to separate different functionalities.

## Building and Running

### Prerequisites

-   Python 3.10+
-   SQL Server ODBC Driver

### Installation

1.  **Create and activate a virtual environment:**

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

2.  **Install the required dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

### Configuration

1.  **Copy the example configuration file:**

    ```bash
    cp config/mssql.json.example config/mssql.json
    ```

2.  **Edit `config/mssql.json` with your database credentials.**

### Running the Application

```bash
python app.py
```

The application will be available at `http://localhost:8888`.

## Development Conventions

-   **Project Structure:** The project follows a standard Flask application structure, with the main application logic in `app.py` and blueprints in the `src/blueprints` directory. The core business logic is separated into different modules within the `src` directory, such as `analysis_report_generator.py`, `consumption_profile_engine.py`, `anomaly_detection_engine.py`, and `recording_quality_engine.py`.
-   **Database Interaction:** The application uses a Data Access Layer (DAL) pattern to interact with the database. The DAL is implemented in `src/household_analysis_dal.py` and uses the `pyodbc` library to connect to the SQL Server database.
-   **API Design:** The application provides a RESTful API for accessing the analysis functionality. The API endpoints are defined in the blueprints and use JSON for request and response formats.
-   **Testing:** The project includes `pytest` in its dependencies, which suggests that there are tests written for the application. However, the tests are not included in the provided file list.
-   **Logging:** The application uses the `logging` module to log important events and errors. Logs are written to `app.log`.
