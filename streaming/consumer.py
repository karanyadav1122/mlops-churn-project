import os
import time
import json
import requests
from kafka import KafkaConsumer, KafkaProducer

INPUT_TOPIC = "churn_input"
OUTPUT_TOPIC = "churn_output"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
API_URL = os.getenv("API_URL", "http://api:8000/predict")

print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Calling API at: {API_URL}")

time.sleep(15)

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="churn-prediction-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

if __name__ == "__main__":
    print("Consumer started...Listening for Kafka events")

    for message in consumer:
        event = message.value
        print(f"\nReceived event: {event}")

        try:
            response = requests.post(API_URL, json=event, timeout=30)
            response.raise_for_status()
            result = response.json()

            output_event = {
                "input": event,
                "prediction": result
            }

            producer.send(OUTPUT_TOPIC, value=output_event)
            producer.flush()

            print(f"Prediction result: {result}")
            print(f"Sent to {OUTPUT_TOPIC}: {output_event}")

        except Exception as e:
            print(f"Error calling FastAPI: {e}")