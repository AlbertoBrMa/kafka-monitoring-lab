import json
import random
import time
import uuid
import logging
import os
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "system-metrics-topic")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "1.0"))

SERVERS = ["web01", "web02", "db01", "app01", "cache01"]


def generate_metrics(server_id: str) -> dict:
    return {
        "server_id": server_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "cpu_percent": round(random.uniform(5.0, 95.0), 2),
            "memory_percent": round(random.uniform(20.0, 90.0), 2),
            "disk_io_mbps": round(random.uniform(0.5, 200.0), 2),
            "network_mbps": round(random.uniform(0.1, 100.0), 2),
            "error_count": random.randint(0, 10),
        },
        "message_uuid": str(uuid.uuid4()),
    }


def on_send_error(exc: Exception) -> None:
    logger.error("Failed to deliver message: %s", exc)


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        retry_backoff_ms=500,
        acks="all",
    )


def main() -> None:
    logger.info("Starting producer — broker=%s topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    producer = None
    try:
        producer = create_producer()
        logger.info("Producer connected successfully.")
        while True:
            for server in SERVERS:
                message = generate_metrics(server)
                producer.send(KAFKA_TOPIC, value=message).add_errback(on_send_error)
                logger.info(
                    "Sent → server=%-8s uuid=%s cpu=%.1f%% mem=%.1f%%",
                    message["server_id"],
                    message["message_uuid"],
                    message["metrics"]["cpu_percent"],
                    message["metrics"]["memory_percent"],
                )
            producer.flush()
            time.sleep(SEND_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    except KafkaError as exc:
        logger.exception("Kafka error: %s", exc)
        raise
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        raise
    finally:
        if producer:
            producer.close()
            logger.info("Producer closed.")


if __name__ == "__main__":
    main()
