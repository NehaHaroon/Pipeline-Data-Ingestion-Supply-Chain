"""
Centralized API communication utilities.
"""

import logging
from typing import Dict, List, Any, Optional

import requests

log = logging.getLogger(__name__)


def send_records_to_api(
    records: List[Dict[str, Any]],
    api_url: str,
    api_token: str,
    timeout: int = 30
) -> bool:
    """
    Send records to ingestion API with proper error handling.
    
    Args:
        records: List of record dictionaries to ingest
        api_url: Full API endpoint URL
        api_token: Bearer token for authentication
        timeout: Request timeout in seconds
    
    Returns:
        True if successful, False otherwise
    """
    if not records:
        return True
    
    try:
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        
        body = {"records": records}
        
        response = requests.post(api_url, headers=headers, json=body, timeout=timeout)
        response.raise_for_status()
        
        log.info(f"Successfully sent {len(records)} records to {api_url}")
        return True
    
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to send records to API: {e}")
        return False
    except Exception as e:
        log.error(f"Unexpected error sending records: {e}")
        return False


def build_api_url(base_url: str, source_id: str, endpoint: str = "ingest") -> str:
    """
    Build API URL for a specific source.
    
    Args:
        base_url: Base API URL (e.g., 'http://ingestion-api:8000')
        source_id: Source identifier
        endpoint: API endpoint (default: 'ingest')
    
    Returns:
        Complete API URL
    """
    # Remove trailing slash from base_url if present
    base_url = base_url.rstrip('/')
    return f"{base_url}/{endpoint}/{source_id}"
