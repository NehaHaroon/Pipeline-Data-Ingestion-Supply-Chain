# tests/test_api.py
import pytest
import sys
import os
from pathlib import Path

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient
import config
from api import app

client = TestClient(app)

# Get API token from config (loaded from .env)
API_TOKEN = config.API_TOKEN

def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert "Supply Chain Ingestion API" in response.json()["message"]

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_sources_unauthorized():
    response = client.get("/sources")
    assert response.status_code == 403  # No token

def test_sources_authorized():
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    response = client.get("/sources", headers=headers)
    assert response.status_code == 200
    assert "sources" in response.json()

# Add more tests for contracts, ingestion, etc.