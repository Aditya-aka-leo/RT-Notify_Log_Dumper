from kafka import KafkaProducer


def get_kafka_producer():
    producer = KafkaProducer(bootstrap_servers=["kafka:9092"], client_id="LogDumper")
    return producer
