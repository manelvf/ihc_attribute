"""Process batches of customer journeys and send them to the IHC Attribution API."""
from typing import Optional, Dict, Any, List

import requests

from src.db import get_customer_journeys_batch, insert_customer_journey
from src.ihc_attribution_client import IHCAttributionClient, ConfigError


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

    customer_journeys = get_customer_journeys_batch(db_path, batch_size)

    for batch_num, journey_batch in enumerate(customer_journeys, 1):
        total_conversions += len(journey_batch)
        formatted_journeys = format_journeys_for_api(journey_batch)
        
        try:
            response = client.compute_ihc(
                customer_journeys=formatted_journeys,
                conv_type_id=conv_type_id,
                redistribution_parameter=redistribution_parameter
            )
            responses.extend(response["value"])
            print(f"Batch {batch_num}: Successfully processed {len(journey_batch)} conversions")
            print(f"Running total: {total_conversions} conversions processed")
            
        except requests.exceptions.RequestException as e:
            print(f"Error processing batch {batch_num}: {e}")
            raise
        except ConfigError as e:
            print(f"Configuration error: {e}")
            raise

    return responses


def process_responses(db_path, responses: list):
    """Process the responses from the IHC API and insert the results into the database."""
    for response in responses:
        conv_id = response["conversion_id"]
        session_id = response["session_id"]
        ihc = response["ihc"]

        insert_customer_journey(db_path, conv_id, session_id, ihc)


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
