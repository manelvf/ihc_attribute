"""Auxiliary functions for parsing dates from command line arguments"""
import argparse
from datetime import datetime


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

        start_date = args.start_date
        end_date = args.end_date
        
        # Additional validation: check if end date is after start date
        if start_date and end_date:
            start_date_obj = datetime.strptime(args.start_date, '%Y-%m-%d')
            end_date_obj = datetime.strptime(args.end_date, '%Y-%m-%d')
            
            if end_date_obj < start_date_obj:
                raise ValueError("End date must be after start date")
        
    except ValueError as e:
        print(f"Error: {e}")

    return start_date, end_date


def validate_date(date_string):
    try:
        # Try to parse the date string in YYYY-MM-DD format
        return datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        # If parsing fails, raise an error with a helpful message
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Please use YYYY-MM-DD format")
