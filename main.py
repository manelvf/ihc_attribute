import sqlite3
from datetime import datetime
import requests
from typing import List, Dict, Any, Optional, Iterator
from time import sleep
from datetime import date
import os
import json
import sqlite3

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SQL_FILE_PATH="challenge_db_create.sql"
DB_PATH = "challenge.db"

CONV_TYPE_ID = os.getenv('IHC_CONV_TYPE_ID')  # Optional: can also get conv_type_id from env
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Optional: can configure batch size in env


class ConfigError(Exception):
    """Raised when required configuration is missing"""


class IHCAttributionClient:
    """Client for interacting with the IHC Attribution API"""
    
    def __init__(self, 
                 api_key: Optional[str] = None, 
                 base_url: str = "https://api.ihc-attribution.com/v1/",
                 max_retries: int = 3,
                 retry_delay: float = 1.0):
        # Try to get API key from environment if not provided
        self.api_key = api_key or os.getenv('IHC_API_KEY')
        if not self.api_key:
            raise ConfigError("IHC API key not found. Please set IHC_API_KEY in your .env file")
            
        self.base_url = base_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
    def compute_ihc(self, 
                    customer_journeys: List[Dict[str, Any]], 
                    conv_type_id: str,
                    redistribution_parameter: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Send customer journeys to the IHC API for attribution computation.
        
        Args:
            customer_journeys: List of journey sessions in the required format
            conv_type_id: Conversion type identifier
            redistribution_parameter: Optional redistribution parameters
            
        Returns:
            Dict containing the API response
        """
        headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }
        
        payload = {
            "customer_journeys": customer_journeys
        }
        
        if redistribution_parameter:
            payload["redistribution_parameter"] = redistribution_parameter

        api_url = "https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}".format(conv_type_id = conv_type_id)

        try:
            response = requests.post(
                api_url,
                headers=headers,
                data=json.dumps(payload)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise

def get_customer_journeys_batch(db_path: str, batch_size: int = 100) -> Iterator[Dict[str, List[Dict[str, Any]]]]:
    """
    Query and build customer journeys from session_sources and conversions tables in batches.
    
    Args:
        db_path: Path to the SQLite database file
        batch_size: Number of conversions to process in each batch
        
    Yields:
        Dictionary with conv_id as key and list of session details as value for each batch
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    # First, get all conversion IDs
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT conv_id FROM conversions ORDER BY conv_id")
    all_conv_ids = [row[0] for row in cursor.fetchall()]
    
    # Process conversions in batches
    for i in range(0, len(all_conv_ids), batch_size):
        batch_conv_ids = all_conv_ids[i:i + batch_size]
        conv_ids_str = ','.join(f"'{conv_id}'" for conv_id in batch_conv_ids)
        
        query = f"""
        WITH conversion_sessions AS (
            SELECT 
                c.conv_id,
                c.user_id,
                c.conv_date,
                c.conv_time,
                s.session_id,
                s.event_date,
                s.event_time,
                s.channel_name,
                s.holder_engagement,
                s.closer_engagement,
                s.impression_interaction,
                CASE 
                    WHEN c.conv_date = s.event_date 
                    AND c.conv_time = s.event_time 
                    THEN 1 
                    ELSE 0 
                END as conversion
            FROM conversions c
            JOIN session_sources s 
                ON c.user_id = s.user_id
                AND (
                    s.event_date < c.conv_date 
                    OR (
                        s.event_date = c.conv_date 
                        AND s.event_time <= c.conv_time
                    )
                )
            WHERE c.conv_id IN ({conv_ids_str})
        )
        SELECT *
        FROM conversion_sessions
        ORDER BY 
            conv_id,
            event_date,
            event_time
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        journeys = {}
        for row in rows:
            conv_id = row['conv_id']
            if conv_id not in journeys:
                journeys[conv_id] = []
            journeys[conv_id].append(dict(row))
        
        yield journeys
    
    cursor.close()
    conn.close()

def format_journeys_for_api(journeys: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Format customer journeys into the structure required by the IHC API.
    
    Args:
        journeys: Dictionary of customer journeys
        
    Returns:
        List of formatted customer journeys
    """
    formatted_journeys = []
    
    for conv_id, sessions in journeys.items():
        journey_sessions = []
        
        for session in sessions:
            timestamp = f"{session['event_date']} {session['event_time']}"
            
            formatted_session = {
                "conversion_id": conv_id,
                "session_id": session['session_id'],
                "timestamp": timestamp,
                "channel_label": session['channel_name'],
                "holder_engagement": session['holder_engagement'],
                "closer_engagement": session['closer_engagement'],
                "conversion": session['conversion'],
                "impression_interaction": session['impression_interaction']
            }
            journey_sessions.append(formatted_session)
            
        formatted_journeys.extend(journey_sessions)
    
    return formatted_journeys

def process_batches(db_path: str, 
                   conv_type_id: str, 
                   batch_size: int = 100,
                   redistribution_parameter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Process and send customer journeys in batches.
    
    Args:
        db_path: Path to the SQLite database
        conv_type_id: Conversion type identifier
        batch_size: Number of conversions to process in each batch
        redistribution_parameter: Optional redistribution parameters
        
    Returns:
        List of API responses for each batch
    """
    client = IHCAttributionClient()  # Will use API key from environment
    responses = []
    total_conversions = 0
    
    for batch_num, journey_batch in enumerate(get_customer_journeys_batch(db_path, batch_size), 1):
        total_conversions += len(journey_batch)
        formatted_journeys = format_journeys_for_api(journey_batch)
        
        try:
            response = client.compute_ihc(
                customer_journeys=formatted_journeys,
                conv_type_id=conv_type_id,
                redistribution_parameter=redistribution_parameter
            )
            responses.append(response)
            print(f"Batch {batch_num}: Successfully processed {len(journey_batch)} conversions")
            print(f"Running total: {total_conversions} conversions processed")
            
        except requests.exceptions.RequestException as e:
            print(f"Error processing batch {batch_num}: {e}")
            raise
        except ConfigError as e:
            print(f"Configuration error: {e}")
            raise

        # TODO: REMOVE THIS
        break
    
    print(f"response: {responses}")
    return responses


def execute_sql_file(db_path, sql_file_path):
    try:
        # Connect to SQLite database (creates it if it doesn't exist)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Read the SQL file
        with open(sql_file_path, 'r') as sql_file:
            sql_script = sql_file.read()
        
        # Execute the SQL script
        cursor.executescript(sql_script)
        
        # Commit the changes
        conn.commit()
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except IOError as e:
        print(f"Error reading SQL file: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()


def insert_customer_journey(db_path, conv_id, session_id, ihc):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO attribution_customer_journey (conv_id, session_id, ihc)
            VALUES (?, ?, ?)
        ''', (conv_id, session_id, ihc))
        
        conn.commit()
        
    except sqlite3.Error as e:
        print(f"Error inserting record: {e}")
        conn.rollback()
        
    finally:
        conn.close()


def insert_channel_reporting(db_path, records):
    """
    Insert or update records in the channel_reporting table.
    
    Args:
        db_path: Path to the SQLite database
        records: List of tuples containing (channel_name, date, cost, ihc, ihc_revenue)
                date should be in 'YYYY-MM-DD' format
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Using INSERT OR REPLACE to handle cases where the record might already exist
        cursor.executemany('''
            INSERT OR REPLACE INTO channel_reporting 
            (channel_name, date, cost, ihc, ihc_revenue)
            VALUES (?, ?, ?, ?, ?)
        ''', records)
        
        conn.commit()
        
    except sqlite3.Error as e:
        print(f"Error inserting channel reporting records: {e}")
        conn.rollback()
        
    finally:
        conn.close()


def get_session_costs(db_path, session_id):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT cost_real
        FROM session_sources
        WHERE session_id = ?
    ''', (session_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    return row[0] if row else 0


def fill_channel_reporting(db_path):
    """Fill the channel_reporting table with aggregated data"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # First, clear the existing data from channel_reporting
    cursor.execute('DELETE FROM channel_reporting')
    
    # Insert the aggregated data
    cursor.execute('''
    INSERT INTO channel_reporting (channel_name, date, cost, ihc, ihc_revenue)
    WITH session_metrics AS (
        SELECT 
            ss.channel_name,
            ss.event_date as date,
            SUM(COALESCE(sc.cost, 0)) as total_cost,
            SUM(COALESCE(acj.ihc, 0)) as total_ihc,
            SUM(COALESCE(acj.ihc * c.revenue, 0)) as total_ihc_revenue
        FROM session_sources ss
        LEFT JOIN session_costs sc 
            ON ss.session_id = sc.session_id
        LEFT JOIN attribution_customer_journey acj 
            ON ss.session_id = acj.session_id
        LEFT JOIN conversions c 
            ON acj.conv_id = c.conv_id
        GROUP BY 
            ss.channel_name,
            ss.event_date
    )
    SELECT 
        channel_name,
        date,
        total_cost as cost,
        total_ihc as ihc,
        total_ihc_revenue as ihc_revenue
    FROM session_metrics
    ''')
    
    conn.commit()

    print("\nSample results from channel_reporting:")
    print("Channel Name | Date | Cost | IHC | IHC Revenue")
    print("-" * 60)
    for row in cursor.fetchall():
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")
    
    # Close the connection
    conn.close()

    
if __name__ == "__main__":
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


        session_cost = get_session_costs(DB_PATH, session_id)

        insert_customer_journey(DB_PATH, conv_id, session_id, ihc)

    fill_channel_reporting(DB_PATH)
