# tests/test_api.py
import pytest
from fastapi.testclient import TestClient
from api import app

client = TestClient(app)

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
    headers = {"Authorization": "Bearer default_token_change_in_prod"}
    response = client.get("/sources", headers=headers)
    assert response.status_code == 200
    assert "sources" in response.json()

# Add more tests for contracts, ingestion, etc.