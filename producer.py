import csv
import json
from kafka import KafkaProducer
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
KAFKA_TOPIC = "iplscores"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to read ball.csv and stream to Kafka
def stream_csv_to_kafka(file_path):
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send(KAFKA_TOPIC, row)
            print(f"Sent row to Kafka: {row}")
            time.sleep(5)  # Simulate real-time streaming

# Stream ball.csv to Kafka
stream_csv_to_kafka('deliveries.csv')