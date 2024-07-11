from kafka.admin import KafkaAdminClient, NewTopic
import time

def create_kafka_admin_client():
    return KafkaAdminClient(
        bootstrap_servers=['kafka:9092'],  # Use 'kafka' if running in Docker
        client_id='LogDumper'
    )

topic_configurations = [
    NewTopic(name="Error-Dumper", num_partitions=1, replication_factor=1),
    NewTopic(name="Log-Dumper", num_partitions=1, replication_factor=1),
    NewTopic(name="Log-Viewer", num_partitions=1, replication_factor=1),
]

def create_topics(admin):
    try:
        existing_topics = set(admin.list_topics())
        new_topic_names = set(topic.name for topic in topic_configurations)
        
        if existing_topics.issuperset(new_topic_names):
            print("Topics Were Already Created")
            return
        
        admin.create_topics(new_topics=topic_configurations, validate_only=False)
        print("New topics created successfully")
    except Exception as error:
        print("Error creating topics:", error)
        raise
    finally:
        admin.close()

def kafka_admin():
    admin = create_kafka_admin_client()
    try:
        create_topics(admin)
        print("Kafka Admin Client connected successfully")
    except Exception as error:
        print("Error connecting with Kafka Admin Client:", error)

if __name__ == "__main__":
    kafka_admin()
    print("Admin tasks finished, keeping process alive")
    while True:
        time.sleep(1)
