"""Client for interacting with the IHC Attribution API"""
import json
import os
from typing import Optional, List, Dict, Any

import requests


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
            payload["redistribution_parameter"] = redistribution_parameter  # type: ignore

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
