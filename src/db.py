"""Database utility functions"""
from typing import Dict, List, Any, Iterator
import sqlite3


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


def get_channel_reporting(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM channel_reporting')
    conn.commit()

    for row in cursor.fetchall():
        yield row

    conn.close()
    

def get_channel_metrics(db_path):
    """
    Reads channel reporting data from SQLite, 
    """
    
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all data from channel_reporting table
        cursor.execute("""
            SELECT channel_name, date, cost, ihc, ihc_revenue 
            FROM channel_reporting
            ORDER BY date, channel_name
        """)
        
        # Fetch all rows
        rows = cursor.fetchall()
        for row in rows:
            yield row
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")

    finally:
        # Close database connection
        if 'conn' in locals():
            conn.close()