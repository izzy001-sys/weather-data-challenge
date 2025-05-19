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


def test_get_weather_with_valid_station(client):
    response = client.get("/api/weather?station_id=USC00110072")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert isinstance(data["data"], list)


def test_get_weather_with_invalid_date_format(client):
    response = client.get("/api/weather?date=1985-06-15")
    assert response.status_code in [200, 400]
    data = response.get_json()
    assert "data" in data or "error" in data


def test_get_weather_stats_no_filters(client):
    response = client.get("/api/weather/stats")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert isinstance(data["data"], list)


def test_get_weather_stats_with_valid_filters(client):
    response = client.get("/api/weather/stats?station_id=USC00110072&year=1985")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert isinstance(data["data"], list)


def test_get_weather_stats_with_invalid_year(client):
    response = client.get("/api/weather/stats?year=1800")
    assert response.status_code in [400, 500]  # could raise ValueError or DB error


def test_get_weather_stats_with_empty_result(client):
    response = client.get("/api/weather/stats?station_id=USC00110072&year=2099")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert isinstance(data["data"], list)
    assert len(data["data"]) == 0


def test_get_weather_with_invalid_pagination(client):
    response = client.get("/api/weather?page=-1&per_page=6000")
    assert response.status_code in [400, 500]


def test_get_weather_stats_with_valid_pagination(client):
    response = client.get("/api/weather/stats?page=1&per_page=2")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert isinstance(data["data"], list)