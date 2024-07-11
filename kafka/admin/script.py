from kafka.admin import KafkaAdminClient, NewTopic

kafka = KafkaAdminClient(bootstrap_servers=["localhost:9092"], client_id="LogDumper")

topic_configurations = [
    NewTopic(name="Error-Dumper", num_partitions=1, replication_factor=1),
    NewTopic(name="Log-Dumper", num_partitions=1, replication_factor=1),
    NewTopic(name="Log-Viewer", num_partitions=1, replication_factor=1),
]


def create_topics(admin):
    try:
        existing_topics = admin.list_topics()
        if existing_topics:
            print("Topics Were Already Created")
            return
        admin.create_topics(new_topics=topic_configurations, validate_only=False)
        print("New topics created successfully")
    except Exception as error:
        print("Error creating topics:", error)
        raise error


def kafka_admin():
    try:
        create_topics(kafka)
        print("Kafka Admin Client connected successfully")
    except Exception as error:
        print("Error connecting with Kafka Admin Client:", error)
    finally:
        kafka.close()


if __name__ == "__main__":
    kafka_admin()
    print("Admin tasks finished, keeping process alive")
    import time

    while True:
        time.sleep(1)