import json
import requests
from kafka import KafkaConsumer, KafkaProducer

INPUT_TOPIC = "churn_input"
OUTPUT_TOPIC = "churn_output"
API_URL = "http://127.0.0.1:8000/predict"

consumer = KafkaConsumer(
  INPUT_TOPIC,
  bootstrap_servers = "localhost:9092",
  auto_offset_reset = "earliest",
  enable_auto_commit = True,
  group_id = "churn-prediction-group",
  value_deserializer= lambda m : json.loads(m.decode("utf-8"))
  
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

if __name__  == "__main__":
  print("Consumer started...Listening for Kafka events")
  
  for message in consumer:
    event = message.value
    print(f"\nReceived event: {event}")
    
    try:
      response = requests.post(API_URL,json=event, timeout= 30)
      response.raise_for_status()
      result = response.json()
      
      output_event = {
        "input": event,
        "prediction":result
      }
      
      producer.send(OUTPUT_TOPIC, value = output_event)
      producer.flush()
      
      print(f"Prediction result: {result}")
      print(f"Sent to {OUTPUT_TOPIC}:{output_event}")
      
    except Exception as e:
      print(f"Error calling FastAPI: {e}")
        