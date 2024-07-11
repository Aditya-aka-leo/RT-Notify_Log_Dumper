import json
from utils import producer  # Adjust this import based on your actual producer module
import logging

def ChannelAggregator(msg):
    logging.info("Received message in ChannelAggregator")
    
    # Ensure msg is a Python dictionary
    if isinstance(msg, str):
        try:
            msg = json.loads(msg)
        except json.JSONDecodeError:
            logging.error("Failed to parse message as JSON")
            return
    elif not isinstance(msg, dict):
        logging.error("Message is neither a string nor a dictionary")
        return

    logging.info(f"Processing message: {msg}")
    
    ProducerClient = producer.ProducerClient()
    
    if 'status' in msg and isinstance(msg['status'], int):
        if 200 <= msg['status'] < 300:
            ProducerClient.send('Log-Dumper', json.dumps(msg))
            logging.info("Sent to Log-Dumper")
        else:
            ProducerClient.send('Error-Dumper', json.dumps(msg))
            logging.info("Sent to Error-Dumper")
    else:
        logging.warning("Message does not contain a valid 'status' field")
    
    ProducerClient.send('Log-Viewer', json.dumps(msg))
    logging.info("Sent to Log-Viewer")

    logging.info("Message processing completed in ChannelAggregator")