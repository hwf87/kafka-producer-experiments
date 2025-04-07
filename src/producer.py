import os
import time
import argparse
from confluent_kafka import Producer


class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers, topic, acks, output_path, idempotent):
        self.topic = topic
        self.output_path = output_path

        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'demo-producer',
            'acks': acks,
            'enable.idempotence': idempotent,
            'max.in.flight.requests.per.connection': 5
        })

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

    def delivery_report(self, err, msg):
        """Callback function to log delivery status."""
        with open(self.output_path, 'a') as f:
            if err is not None:
                log_msg = f"Message {msg.value().decode()} delivery failed: {err}"
            else:
                log_msg = f"Message {msg.value().decode()} delivered to {msg.topic()} [{msg.partition()}]"
            print(log_msg)
            f.write(log_msg + "\n")

    def send_messages(self, num_messages=500, interval=0.1, message_prefix="Test message"):
        for i in range(num_messages):
            message = f"{message_prefix} {i}"
            self.producer.produce(self.topic, key=None, value=message.encode(), callback=self.delivery_report)
            time.sleep(interval)
        self.producer.flush()


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Producer for acks demo")
    parser.add_argument('--acks', choices=['0', 'all'], default='all', help='Kafka acks setting')
    parser.add_argument('--topic', default='demo-topic', help='Kafka topic name')
    parser.add_argument('--brokers', default='localhost:9092,localhost:9093,localhost:9094', help='Bootstrap servers')
    parser.add_argument('--messages', type=int, default=500, help='Number of messages to send')
    parser.add_argument('--interval', type=float, default=0.1, help='Delay between messages')
    return parser.parse_args()


if __name__ == "__main__":
    """
    # run acks=0
    python src/producer.py --acks 0 --messages 1000 --interval 0.05

    # run acks=all
    python src/producer.py --acks all --messages 1000 --interval 0.05
    """
    args = parse_args()
    output_path = f"./logs/delivery_report_ack_{args.acks}.log"
    idempotent = True if args.acks == 'all' else False
    message_prefix = f"Test message with acks={args.acks}"

    producer = KafkaProducerWrapper(
        bootstrap_servers=args.brokers,
        topic=args.topic,
        acks=args.acks,
        output_path=output_path,
        idempotent=idempotent
    )

    producer.send_messages(
        num_messages=args.messages,
        interval=args.interval,
        message_prefix=message_prefix
    )
