# Kafka Acks & Idempotence Demo

This repository demonstrates how Kafka's `acks` settings (`acks=0` and `acks=all`) and producer idempotence affect message delivery reliability in various scenarios. By simulating an unstable environment where the Kafka container is randomly stopped and started, this demo highlights the impact of these settings on message consistency and the overall reliability of data transmission.

## Key Features

### Two Kafka Producer Modes:
- **`acks=0`**: No delivery guarantee — The producer does not wait for any acknowledgment from the broker before continuing. This can result in lost messages in case of failures.
  
- **`acks=all`**: Strongest delivery guarantee — The producer waits for acknowledgment from all in-sync replicas before proceeding. This ensures message durability and replication, even under unstable conditions.

### Simulated Unstable Environment:
- Randomly toggling the Kafka Docker container during message production to simulate network failures or service instability.
- The demo showcases how message delivery behaves under unreliable conditions, demonstrating that `acks=all` ensures higher message reliability and consistency in delivery.

### Unified Kafka Consumer:
- A single consumer that listens to the Kafka topic and logs messages.
- The consumer log is used to validate whether the messages from the producer were successfully delivered and received, demonstrating the differences between the two `acks` configurations.

### Logs & Monitoring:
- Detailed log output stored in the `./logs` directory to track the message flow from production to consumption.
- Logs help you analyze the effects of the different `acks` settings on message durability and delivery.

### Topic Management:
- A dedicated folder with scripts to manage Kafka topics, including:
  - Creating new topics with customized configurations.
  - Checking the status of existing topics and verifying their health and replication status.

## Setup & Execution

### Prerequisites:
- Docker and Docker Compose installed on your machine.
- Kafka and Zookeeper running via Docker Compose.

### Running the Demo:

1. **Clone this repository to your local machine:**

2. **Start the Kafka and Zookeeper services using Docker Compose:**
    ```bash
    docker-compose up -d
    ```
3. **For topic management, use the provided scripts in the `topics` folder to create and inspect topics:**
    ```bash
    # Create a topic and skip if it already exists
    python topics/create_topic.py --topic ${demo-topic} --partitions 3 --replicas 2 --skip-if-exists
    
    # Dry-run mode
    python topics/create_topic.py --topic ${demo-topic} --partitions 3 --replicas 2 --dry-run
    ```

4. **Run the producer in either `acks=0` or `acks=all` mode** by setting the appropriate environment variable or using the corresponding script:
    ```bash
    # Run acks=0
    python src/producer.py --acks 0 --messages 1000 --interval 0.05

    # Run acks=all
    python src/producer.py --acks all --messages 1000 --interval 0.05
    ```

5. **Simulate network instability by randomly restarting the Kafka container during the message production phase:**

6. **Consume messages with the consumer script:**
    ```bash
    # Test with acks=all
    python src/consumer.py --acks all

    # Test with acks=0
    python src/consumer.py --acks 0 --offset earliest
    ```

### Logs:
- Log output goes to the `./logs/` directory:
  - **Producer logs**: `delivery_report_ack_*.log`
  - **Consumer logs**: `consumer_report_ack_*.log`
  
  These logs track the message flow from production to consumption and help analyze the effects of the different `acks` settings on message durability and delivery.

## Expected Results:
- When using `acks=0`, you may notice that some messages are lost due to network instability or container restarts.
- When using `acks=all`, the consumer should successfully receive all messages, demonstrating the enhanced reliability of Kafka's acknowledgment system.

## Conclusion:
This demo highlights the critical differences between Kafka’s `acks=0` and `acks=all` settings in terms of reliability. By simulating a less stable environment with Docker, it becomes clear how the stronger guarantee of `acks=all` ensures that messages are properly delivered and replicated across all brokers, even in the face of failures.