from fastapi.testclient import TestClient
from unittest.mock import MagicMock
import api.app as app_module

client = TestClient(app_module.app)


def test_predict(monkeypatch):
    fake_spark = MagicMock()
    fake_df = MagicMock()
    fake_predictions = MagicMock()
    fake_model = MagicMock()
    fake_result = {"churn_prob": 0.95, "prediction_custom": 1}

    fake_spark.createDataFrame.return_value = fake_df
    fake_model.transform.return_value = fake_predictions

    fake_after_first_withcolumn = MagicMock()
    fake_after_second_withcolumn = MagicMock()

    fake_predictions.withColumn.return_value = fake_after_first_withcolumn
    fake_after_first_withcolumn.withColumn.return_value = fake_after_second_withcolumn
    fake_after_second_withcolumn.select.return_value.collect.return_value = [
        fake_result]

    fake_probability_col = MagicMock()
    fake_probability_col.__getitem__.return_value = MagicMock()

    fake_churn_prob_col = MagicMock()
    fake_churn_prob_col.__ge__.return_value = MagicMock()

    fake_when_obj = MagicMock()
    fake_when_obj.otherwise.return_value = MagicMock()

    monkeypatch.setattr(app_module, "get_spark", lambda: fake_spark)
    monkeypatch.setattr(app_module, "get_model", lambda: fake_model)
    monkeypatch.setattr(
        app_module,
        "col",
        lambda name: fake_probability_col if name == "probability" else fake_churn_prob_col,
    )
    monkeypatch.setattr(
        app_module,
        "vector_to_array",
        lambda x: fake_probability_col)
    monkeypatch.setattr(
        app_module,
        "when",
        lambda condition,
        value: fake_when_obj)

    payload = {
        "gender": "Male",
        "location": "Texas",
        "subscription_type": "Basic",
        "tenure_months": 4,
        "monthly_charges": 120.5,
        "support_tickets": 8,
        "late_payments": 3,
        "tenure_bucket": "new",
        "charge_bucket": "high",
    }

    response = client.post("/predict", json=payload)

    assert response.status_code == 200
    assert response.json()["prediction"] == 1
    assert response.json()["churn_probability"] == 0.95
