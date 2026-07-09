"""Smoke test for the FastAPI surface. Health stays up even without a database."""

from fastapi.testclient import TestClient

from app.main import app


def test_health_reports_ok():
    with TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_query_rejects_missing_question():
    # Body validation runs before the graph, so this never reaches external APIs.
    with TestClient(app) as client:
        resp = client.post("/query/sync", json={})
    assert resp.status_code == 422
