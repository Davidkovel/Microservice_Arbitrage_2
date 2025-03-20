import json
from typing import Dict, Any, Optional
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from utils.logger import logger


class KafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize the Kafka producer

        Args:
            bootstrap_servers: Comma-separated list of broker addresses
            topic: Kafka topic to publish messages to
        """
        self.topic = topic
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'arbitrage_dex_cex-notifications',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 5,  # Retry on temporary failures
        }
        self.producer = Producer(self.producer_config)
        self._ensure_topic_exists(bootstrap_servers, topic)

    def _ensure_topic_exists(self, bootstrap_servers: str, topic: str):
        """Create the topic if it doesn't exist"""
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        try:
            # Check if topic exists
            topics = admin_client.list_topics(timeout=10).topics
            if topic not in topics:
                # Create the topic with reasonable defaults
                topic_list = [NewTopic(
                    topic,
                    num_partitions=3,
                    replication_factor=1,
                    config={"retention.ms": str(7 * 24 * 60 * 60 * 1000)}  # 7 days retention
                )]
                admin_client.create_topics(topic_list)
                logger.info(f"Created Kafka topic: {topic}")
        except KafkaException as e:
            logger.warning(f"Failed to create topic: {e}")

    def delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    async def send_message(self, key: str, value: Dict[str, Any]):
        """
        Send a message to Kafka topic

        Args:
            key: Message key (e.g., token name)
            value: Message content as dictionary
        """
        try:
            # Convert dictionary to JSON string
            serialized_data = json.dumps(value).encode('utf-8')
            # Produce message
            self.producer.produce(
                topic=self.topic,
                key=key.encode('utf-8'),
                value=serialized_data,
                callback=self.delivery_callback
            )
            # Trigger any delivery callbacks from previous produce calls
            self.producer.poll(0)
            logger.info(f"Sent message with key {key} to topic {self.topic}")
        except Exception as e:
            logger.error(f"Error producing message: {e}")
            raise

    def flush(self, timeout=30):
        """Flush the producer to ensure all messages are sent"""
        self.producer.flush(timeout)
