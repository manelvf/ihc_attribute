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
        batch_size=BATCH_SIZE
        start_date=start_date,
        end_date=end_date
    )
    process_responses(DB_PATH, responses)

    fill_channel_reporting(DB_PATH)

    save_channel_metrics(DB_PATH, CSV_FILE)


def parse_dates():
    # Create argument parser
    parser = argparse.ArgumentParser(description='Process two dates from command line')
    
    # Add arguments for start and end dates
    parser.add_argument('--start_date', type=validate_date, default=None,
                        help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end_date', type=validate_date, default=None,
                        help='End date in YYYY-MM-DD format')

    # Parse arguments
    try:
        args = parser.parse_args()
        
        # Additional validation: check if end date is after start date
        if args.end_date < args.start_date:
            raise ValueError("End date must be after start date")
            
        # If all validations pass, print the dates
        print(f"Start date: {args.start_date.date()}")
        print(f"End date: {args.end_date.date()}")
        
    except ValueError as e:
        print(f"Error: {e}")

    return args.start_date, args.end_date


def validate_date(date_string):
    try:
        # Try to parse the date string in YYYY-MM-DD format
        return datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        # If parsing fails, raise an error with a helpful message
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Please use YYYY-MM-DD format")

    
if __name__ == "__main__":
    main()
