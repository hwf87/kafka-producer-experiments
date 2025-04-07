import argparse
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_topic(topic_name, num_partitions, replication_factor, bootstrap_servers, dry_run=False, skip_if_exists=False):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='topic-creator'
    )

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    try:
        if dry_run:
            print(f"🔍 Dry-run: would create topic '{topic_name}' with {num_partitions} partitions and replication factor {replication_factor}")
        else:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"✅ Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        if skip_if_exists:
            print(f"⚠️ Topic '{topic_name}' already exists. Skipping.")
        else:
            print(f"❌ Topic '{topic_name}' already exists. Use --skip-if-exists to ignore.")
    except Exception as e:
        print(f"❌ Error creating topic '{topic_name}': {e}")
    finally:
        admin_client.close()


if __name__ == "__main__":
    '''
    # Create topic and skip if topic already exists
    python create_topic.py --topic demo-topic --partitions 3 --replicas 2 --skip-if-exists

    # dry-run mode
    python create_topic.py --topic demo-topic --partitions 3 --replicas 2 --dry-run
    '''
    parser = argparse.ArgumentParser(description="Create a Kafka topic.")
    parser.add_argument('--topic', required=True, help='Topic name')
    parser.add_argument('--partitions', type=int, default=1, help='Number of partitions (default: 1)')
    parser.add_argument('--replicas', type=int, default=1, help='Replication factor (default: 1)')
    parser.add_argument('--brokers', default='localhost:9092,localhost:9093,localhost:9094',
                        help='Comma-separated list of bootstrap servers')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without creating the topic')
    parser.add_argument('--skip-if-exists', action='store_true', help='Skip topic creation if topic already exists')

    args = parser.parse_args()
    brokers_list = args.brokers.split(',')

    create_topic(
        topic_name=args.topic,
        num_partitions=args.partitions,
        replication_factor=args.replicas,
        bootstrap_servers=brokers_list,
        dry_run=args.dry_run,
        skip_if_exists=args.skip_if_exists
    )
