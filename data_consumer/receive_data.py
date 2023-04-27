from kafka import KafkaConsumer
import time
import sys
import os 

KAFKA_BROKER = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

def connect_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda value: value.decode('utf-8'),
                auto_offset_reset='latest',
                enable_auto_commit=True,
            )
            print("Connected to Kafka")
            return consumer
        except Exception as e:
            print(f"Error connecting to Kafka: {e}. Retrying in 10 seconds...")
            time.sleep(10)

consumer = connect_kafka()

print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")

for message in consumer:
    print(f"Received message: {message.value}")
    sys.stdout.flush() # flush print statement to terminal log
