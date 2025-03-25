import csv
import json
from kafka import KafkaProducer
import time
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_TOPIC = "iplscores"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_csv_to_kafka(file_path):
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send(KAFKA_TOPIC, row)
            print(f"Sent row to Kafka: {row}")
            time.sleep(8)

stream_csv_to_kafka('deliveries.csv')