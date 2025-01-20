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
from datetime import datetime
import argparse
import os

from dotenv import load_dotenv

from src.batch_processor import process_batches, process_responses
from src.db import execute_sql_file, fill_channel_reporting
from src.ihc_attribution_client import ConfigError
from src.report import save_channel_metrics


# Load environment variables from .env file
load_dotenv()

SQL_FILE_PATH="challenge_db_create.sql"
DB_PATH = "challenge.db"

CONV_TYPE_ID = os.getenv('IHC_CONV_TYPE_ID')  # Optional: can also get conv_type_id from env
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Optional: can configure batch size in env
CSV_FILE = os.getenv('CSV_FILE', 'output/channel_metrics.csv')


def main():
    if not CONV_TYPE_ID:
        raise ConfigError("Conversion type ID not found. Please provide IHC_CONV_TYPE_ID in your .env file")

    start_date, end_date = parse_dates()

    execute_sql_file(DB_PATH, SQL_FILE_PATH)

    responses = process_batches(
        db_path=DB_PATH,
        conv_type_id=CONV_TYPE_ID,
        batch_size=BATCH_SIZE,
        start_date=start_date,
        end_date=end_date
    )
    process_responses(DB_PATH, responses)

    fill_channel_reporting(DB_PATH)

    save_channel_metrics(DB_PATH, CSV_FILE)


    
if __name__ == "__main__":
    main()
