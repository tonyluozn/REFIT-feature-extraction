import json
import time
import random
import sys
import os
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka")
            return producer
        except Exception as e:
            print(f"Error connecting to Kafka: {e}. Retrying in 10 seconds...")
            time.sleep(10)

producer = connect_kafka()

def send_data(key):
    sensor_data = {
        "project_guid": "iot-streaming",
        "id": str(key),
        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "doubles": {"temperature": random.choice([10,20]),
                    "humidity": random.choice([10,20])},
    }
    producer.send(KAFKA_TOPIC, sensor_data)
    producer.flush() # flush data to Kafka
    print(f"Sent data: {sensor_data}")
    sys.stdout.flush() # flush print statement to terminal log

id = 0
while True:
    send_data(id)
    id+=1
    time.sleep(random.choice([5, 10, 20]))

