from kafka import KafkaAdminClient

# Create a Kafka Admin client and connect to the local Kafka broker
admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092,localhost:9093,localhost:9094")

# Specify the topic to retrieve information about
topic_name = "jacky-demo-topic"

# Fetch partition metadata for the specified topic
try:
    topic_metadata = admin_client.describe_topics([topic_name])

    # Print metadata details for each partition of the topic
    for topic in topic_metadata:
        print(topic)
        print(f"Topic: {topic['topic']}")  # Display the topic name
        for partition in topic['partitions']:
            partition_id = partition['partition']
            leader = partition['leader']
            replicas = partition['replicas']
            isr = partition['isr']

            print(f"Partition: {partition_id}")
            print(f"Leader: {leader}")
            print(f"Replicas: {replicas}")
            print(f"In-Sync Replicas (ISR): {isr}")

except Exception as e:
    print(f"Failed to describe topic '{topic_name}': {e}")

# Close the Kafka Admin client to release resources
admin_client.close()
