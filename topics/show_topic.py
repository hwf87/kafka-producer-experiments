from kafka.admin import KafkaAdminClient

# Initialize KafkaAdminClient and connect to the Kafka broker
admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092,localhost:9093,localhost:9094',  # Replace with your Kafka broker address if needed
    client_id='admin-client'  # Optional: Specify a client ID for easier request tracking
)

# Fetch the list of all available topics in the Kafka cluster
topics = admin_client.list_topics()

# Function to display metadata for a specific topic
def show_topic_info(topic_name: str):
    try:
        # Describe topics to get detailed metadata
        topic_metadata = admin_client.describe_topics([topic_name])
        for topic in topic_metadata:
            print(f"\nTopic: {topic['topic']}")
            for partition in topic['partitions']:
                print(f"  Partition: {partition['partition']}")
                print(f"    Leader: {partition['leader']}")
                print(f"    Replicas: {partition['replicas']}")
                print(f"    In-Sync Replicas (ISR): {partition['isr']}")
    except Exception as e:
        print(f"Error retrieving metadata for topic '{topic_name}': {e}")

# Display the list of topics and their metadata
print("Topics in Kafka cluster:")
for topic in topics:
    print(f"- {topic}")  # Print each topic name
    show_topic_info(topic_name=topic)  # Show detailed info for each topic

# Close the Kafka Admin client to release resources
admin_client.close()
