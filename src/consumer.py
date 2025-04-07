import os
import argparse
from kafka import KafkaConsumer


class KafkaConsumerWrapper:
    def __init__(self, topic, brokers, group_id, offset_reset, output_file):
        self.topic = topic
        self.output_file = output_file
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            auto_offset_reset=offset_reset
        )

    def save_to_file(self, message):
        with open(self.output_file, 'a') as f:
            f.write(f"Received message: {message}\n")

    def start_consuming(self):
        print(f"Starting consumer for topic '{self.topic}'...")
        try:
            for message in self.consumer:
                decoded_message = message.value.decode('utf-8')
                print(f"Received message: {decoded_message}")
                self.save_to_file(decoded_message)
        except KeyboardInterrupt:
            print("\nConsumer stopped manually.")
        finally:
            self.consumer.close()
            print("Kafka consumer connection closed.")


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Consumer for acks demo")
    parser.add_argument('--topic', default='demo-topic', help='Kafka topic name')
    parser.add_argument('--brokers', default='localhost:9092,localhost:9093,localhost:9094', help='Bootstrap servers')
    parser.add_argument('--group', default='my-group', help='Consumer group id')
    parser.add_argument('--offset', choices=['earliest', 'latest'], default='earliest', help='Offset reset policy')
    parser.add_argument('--acks', choices=['0', 'all'], default='all', help='Acks mode (for output file name)')
    return parser.parse_args()


if __name__ == "__main__":
    """
    # Test with acks=all
    python src/consumer.py --acks all

    # Test with acks=0
    python src/consumer.py --acks 0 --offset earliest
    """
    args = parse_args()
    output_file = f'./logs/consumer_report_ack_{args.acks}.log'

    consumer = KafkaConsumerWrapper(
        topic=args.topic,
        brokers=args.brokers.split(','),
        group_id=args.group,
        offset_reset=args.offset,
        output_file=output_file
    )
    consumer.start_consuming()
