import logging
import json
import time
from kafka import KafkaConsumer
from controller.LogViewer import LogViewer
from controller.LogDbPusher import LogDbPusher
from controller.ErrorDbPusher import ErrorDbPusher
from controller.ChannelAggregator import ChannelAggregator
from threading import Thread
import signal
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_consumer(group_id, topic):
    logging.info(f"Creating consumer for topic: {topic}")
    return KafkaConsumer(
        topic,
        group_id=group_id,
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )

def consume_topic(consumer, process_message):
    try:
        logging.info(f"Consumer Connected To The {consumer.subscription()} Queue")
        for message in consumer:
            logging.info(f"Msg Pulled From {message.topic} Queue")
            try:
                process_message(message.value)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
    except Exception as error:
        logging.error(f"Error consuming topic {consumer.subscription()}: {error}")
    finally:
        consumer.close()

def signal_handler(signum, frame):
    logging.info("Interrupt received, stopping consumers...")
    sys.exit(0)

def run():
    logging.info("Starting consumer script")
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    ErrorDumperConsumer = create_consumer("Error-Dumper", "Error-Dumper")
    LogDumperConsumer = create_consumer("Log-Dumper", "Log-Dumper")
    LogViewerConsumer = create_consumer("Log-Viewer", "Log-Viewer")
    ChannelAggregatorConsumer = create_consumer("Channel-Aggregator", "Channel-Aggregator")

    threads = [
        Thread(target=consume_topic, args=(ErrorDumperConsumer, ErrorDbPusher)),
        Thread(target=consume_topic, args=(LogDumperConsumer, LogDbPusher)),
        Thread(target=consume_topic, args=(LogViewerConsumer, LogViewer)),
        Thread(target=consume_topic, args=(ChannelAggregatorConsumer, ChannelAggregator))
    ]

    for thread in threads:
        thread.start()

    logging.info("All consumer threads started")

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        import traceback
        logging.error(traceback.format_exc())