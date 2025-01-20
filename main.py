"""
Main module for processing customer journey data through the IHC Attribution API.

This module:
1. Sets up the SQLite database using SQL schema file
2. Processes customer journeys in batches through the IHC Attribution API
3. Stores the attribution results back in the database
4. Generates channel-level reporting metrics
5. Outputs results to a CSV file

Environment variables required:
- IHC_CONV_TYPE_ID: Conversion type identifier for the IHC API
- BATCH_SIZE: Optional batch size for processing (defaults to 100)

The module uses SQLite for data storage and the IHC Attribution API for 
computing attribution values across customer journeys.
"""
import os
from dotenv import load_dotenv

from dags.lib.db import execute_sql_file


# Load environment variables from .env file
load_dotenv()


SQL_FILE_PATH = os.environ.get("SQL_FILE_PATH", "fixtures/challenge_db_create.sql")
DB_PATH = os.environ.get("DB_PATH", "challenge.db")


def main():
    execute_sql_file(DB_PATH, SQL_FILE_PATH)

    
if __name__ == "__main__":
    main()
