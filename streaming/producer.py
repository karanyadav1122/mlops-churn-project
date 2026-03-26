import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers= 'localhost:9092',
    value_serializer= lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME= 'churn_input'

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
    print("Inference producer started")
    while True:
        event= generate_event()
        producer.send(TOPIC_NAME, value= event)
        print(f"Sent: {event}")
        time.sleep(30)
                