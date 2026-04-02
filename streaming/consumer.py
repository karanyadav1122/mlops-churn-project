import os
import time
import json
import requests
from kafka import KafkaConsumer, KafkaProducer

INPUT_TOPIC = "churn_features"
OUTPUT_TOPIC = "churn_output"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
API_URL = os.getenv("API_URL", "http://host.docker.internal:5001/invocations")

print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
print(f"Calling API at: {API_URL}", flush=True)

time.sleep(15)

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="churn-prediction-group-mlflow-v2",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

if __name__ == "__main__":
    print("Consumer started...Listening for Kafka events", flush=True)

    for message in consumer:
        event = message.value
        print(f"\nReceived event: {event}", flush=True)

        try:
            response = requests.post(
                API_URL,
                json={"dataframe_records": [event]},
                timeout=30
            )
            response.raise_for_status()
            result = response.json()["predictions"][0]

            output_event = {
                "input": event,
                "prediction": result
            }

            producer.send(OUTPUT_TOPIC, value=output_event)
            producer.flush()

            print(f"Prediction result: {result}", flush=True)
            print(f"Sent to {OUTPUT_TOPIC}: {output_event}", flush=True)

        except Exception as e:
            print(f"Error calling MLflow serving endpoint: {e}", flush=True)
