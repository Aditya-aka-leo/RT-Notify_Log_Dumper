from  src.utils.Kafka.producer import ProducerClient

producer = ProducerClient()
try:
    producer.send('Error-Dumper', {'message': 'Hello Kafka'})
    producer.send('Log-Dumper', {'message': 'Hello Kafka'})
    producer.send('Log-Viewer', {'message': 'Hello Kafka'})
    producer.flush()
    print("Messages sent successfully")
except Exception as e:
        print(f"Error sending messages: {e}")
finally:
        producer.close()

