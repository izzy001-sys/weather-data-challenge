import os
import pytest
import json
from api import app


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


def test_get_weather_no_filters(client):
    response = client.get("/api/weather")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert isinstance(data["data"], list)


def test_get_weather_with_station_id(client):
    response = client.get("/api/weather?station_id=TEST123")
    assert response.status_code == 200
    data = response.get_json()
    # Data may be empty if TEST123 is not a real station in your DB
    assert "data" in data


def test_get_weather_stats_no_filters(client):
    response = client.get("/api/weather/stats")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert isinstance(data["data"], list)


def test_get_weather_stats_with_filters(client):
    response = client.get("/api/weather/stats?station_id=TEST123&year=2020")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
