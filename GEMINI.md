# GEMINI.md - Project Overview

This document provides a comprehensive overview of the `zhdcsqlite` project, intended as a guide for AI-powered development assistance.

## Project Overview

`zhdcsqlite` is a web application built with **Flask**, a Python web framework. Its primary purpose is to process, analyze, and manage household financial data. The application facilitates the import of financial records from various sources (Excel and CSV files), supports the manual coding of uncoded data entries, and generates detailed financial reports and electronic ledgers.

The backend is written in Python and relies on a Microsoft SQL Server database for data storage and querying. The frontend is composed of HTML templates, CSS, and JavaScript.

### Key Technologies

*   **Backend:** Python, Flask
*   **Database:** Microsoft SQL Server (via `pyodbc`)
*   **Data Manipulation:** pandas, numpy
*   **Frontend:** HTML, CSS, JavaScript
*   **Dependencies:** See `requirements.txt` for a complete list.

### Architecture

The application follows a modular structure, with core functionalities organized into Flask Blueprints. The main components are:

*   **`app.py`:** The main entry point of the application. It initializes the Flask app, database connection, and registers the blueprints.
*   **`src/`:** Contains the core application logic.
    *   **`src/blueprints/`:**  Houses the different modules of the application, such as data import, data generation, and statistical analysis.
    *   **`src/database.py`:** Manages the database connection and data access operations.
    *   **`src/data_processing.py`:**  Handles the business logic for processing financial data.
    *   **`src/excel_operations.py`:** Provides utility functions for reading and writing Excel files.
*   **`static/`:** Contains frontend assets like CSS and JavaScript files.
*   **`src/templates/`:**  Contains the HTML templates for the web interface.
*   **`uploads/`:**  A directory for storing uploaded and generated files.

## Building and Running

### Prerequisites

*   Python 3.10+
*   Microsoft SQL Server ODBC Driver

### Setup and Installation

1.  **Create and activate a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure the database:**
    *   Copy the example configuration file:
        ```bash
        cp config/mssql.json.example config/mssql.json
        ```
    *   Edit `config/mssql.json` with your SQL Server connection details.

### Running the Application

1.  **Start the Flask development server:**
    ```bash
    python app.py
    ```

2.  The application will be available at `http://localhost:5000`.

## Development Conventions

*   **Modular Design:** The application is organized into Blueprints, promoting separation of concerns. New features should be implemented in their own blueprints.
*   **Error Handling:** The `@handle_errors` decorator is used to provide consistent error handling for API endpoints.
*   **Database Interaction:** All database operations are centralized in the `src/database.py` module.
*   **Logging:** The application uses the standard Python `logging` module to log events to `app.log` and the console.
*   **Dependencies:** Project dependencies are managed in the `requirements.txt` file.
