import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

CUSTOMER_LOCATIONS = ["Texas", "California", "Florida", "New York", "Ohio"]
GENDERS = ["Male", "Female"]
SUBSCRIPTION_TYPES = ["Basic", "Standard", "Premium"]


def generate_customer_event():
    return {
        "customer_id": random.randint(1000, 9999),
        "age": random.randint(18, 70),
        "gender": random.choice(GENDERS),
        "location": random.choice(CUSTOMER_LOCATIONS),
        "subscription_type": random.choice(SUBSCRIPTION_TYPES),
        "monthly_charges": round(random.uniform(20, 150), 2),
        "tenure_months": random.randint(1, 72),
        "support_tickets": random.randint(0, 10),
        "late_payments": random.randint(0, 5),
        "timestamp": datetime.utcnow().isoformat()
    }


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
                         )

TOPIC_NAME = 'churn_events'

if __name__ == "__main__":
    print("🚀 Producer started...")
    while True:
        event = generate_customer_event()
        producer.send(TOPIC_NAME, value=event)
        print(f"Sent:{event}")
        time.sleep(2)
