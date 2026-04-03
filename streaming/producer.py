import os
import time
import json

TOPIC_NAME = "churn_input"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

def create_producer():
    
    from kafka import KafkaProducer
    
    print(f"connecting to kafka at: {KAFKA_BOOTSTRAP_SERVERS}", flush = True)
    time.sleep(15)
    return KafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        value_serializer = lambda v: json.dumps(v).encode("utf-8")
        )



def generate_event():
    return {
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

if __name__ == "__main__":
    producer = create_producer()
    print("Producer started...", flush=True)
    
    
    while True:
        event = generate_event()
        producer.send(TOPIC_NAME, value=event)
        producer.flush()
        print(f"Sent: {event}", flush=True)
        time.sleep(5)