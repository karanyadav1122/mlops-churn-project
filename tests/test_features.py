def make_gold_features(record: dict) -> dict:
    return {
        **record,
        "is_high_support_customer": 1 if record["support_tickets"] >= 5 else 0,
        "is_payment_risky": 1 if record["late_payments"] >= 3 else 0,
    }

def test_gold_features_logic():
    record = {
        "support_tickets": 8,
        "late_payments": 3
    }
    out = make_gold_features(record)
    assert out["is_high_support_customer"] == 1
    assert out["is_payment_risky"] == 1