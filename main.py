import os

from dotenv import load_dotenv

from src.db import execute_sql_file, insert_customer_journey, fill_channel_reporting
from src.ihc_attribution_client import ConfigError
from src.batch_processor import process_batches


# Load environment variables from .env file
load_dotenv()

SQL_FILE_PATH="challenge_db_create.sql"
DB_PATH = "challenge.db"

CONV_TYPE_ID = os.getenv('IHC_CONV_TYPE_ID')  # Optional: can also get conv_type_id from env
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Optional: can configure batch size in env


def main():
    if not CONV_TYPE_ID:
        raise ConfigError("Conversion type ID not found. Please provide IHC_CONV_TYPE_ID in your .env file")
    
    execute_sql_file(DB_PATH, SQL_FILE_PATH)

    try:
        responses = process_batches(
            db_path=DB_PATH,
            conv_type_id=CONV_TYPE_ID,
            batch_size=BATCH_SIZE
        )
    except ConfigError as e:
        print(f"Configuration error: {e}")
        raise

    for response in responses:
        conv_id = response["conversion_id"]
        session_id = response["session_id"]
        ihc = response["ihc"]

        insert_customer_journey(DB_PATH, conv_id, session_id, ihc)

    fill_channel_reporting(DB_PATH)

    
if __name__ == "__main__":
    main()
