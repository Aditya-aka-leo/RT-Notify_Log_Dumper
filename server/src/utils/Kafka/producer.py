from kafka import KafkaProducer
import json

def ProducerClient():
    # Create a KafkaProducer with JSON serialization for the value
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',  # Adjust this if running inside Docker
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize dictionary to JSON bytes
    )
    return producer
    