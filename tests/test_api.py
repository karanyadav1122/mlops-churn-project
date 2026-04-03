from fastapi.testclient import TestClient
from api.app import app

client = TestClient(app)


def test_predict_endpoint_exists():
    payload = {
        "gender": "Male",
        "location": "Texas",
        "subscription_type": "Basic",
        "tenure_months": 4,
        "monthly_charges": 120.5,
        "support_tickets": 8,
        "late_payments": 3,
        "tenure_bucket": "new",
        "charge_bucket": "high"
    }

    response = client.post("/predict", json=payload)
    assert response.status_code in (200, 422)
