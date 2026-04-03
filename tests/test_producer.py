from streaming.producer import generate_event


def test_generate_events_has_required_keys():
    event = generate_event()
    required = {
        "gender",
        "location",
        "subscription_type",
        "tenure_months",
        "monthly_charges",
        "support_tickets",
        "late_payments",
        "tenure_bucket",
        "charge_bucket"

    }

    assert required.issubset(event.keys())
