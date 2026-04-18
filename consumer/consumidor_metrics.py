import json
import logging
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "system-metrics-topic")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "monitoring-consumer-group-v1")
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "kafka_monitoring")
KPI_WINDOW_SIZE = int(os.getenv("KPI_WINDOW_SIZE", "20"))

RAW_COLLECTION = "system_metrics_raw"
KPI_COLLECTION = "system_metrics_kpis"


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )


def create_db(connection_string: str):
    client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client[MONGODB_DATABASE]


def calculate_kpis(window: list, window_start: datetime) -> dict:
    count = len(window)
    duration = (datetime.now(timezone.utc) - window_start).total_seconds()
    metrics = [m["metrics"] for m in window]

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "window_duration_seconds": round(duration, 3),
        "message_count": count,
        "kpis": {
            "avg_cpu_percent": round(sum(m["cpu_percent"] for m in metrics) / count, 2),
            "avg_memory_percent": round(sum(m["memory_percent"] for m in metrics) / count, 2),
            "avg_disk_io_mbps": round(sum(m["disk_io_mbps"] for m in metrics) / count, 2),
            "avg_network_mbps": round(sum(m["network_mbps"] for m in metrics) / count, 2),
            "total_error_count": sum(m["error_count"] for m in metrics),
            "throughput_msg_per_sec": round(count / duration, 3) if duration > 0 else 0.0,
        },
    }


def main() -> None:
    if not MONGODB_CONNECTION_STRING:
        raise RuntimeError("MONGODB_CONNECTION_STRING is not set — check your .env file")

    logger.info("Connecting to MongoDB…")
    db = create_db(MONGODB_CONNECTION_STRING)
    raw_col = db[RAW_COLLECTION]
    kpi_col = db[KPI_COLLECTION]
    logger.info("MongoDB connected — database='%s'", MONGODB_DATABASE)

    logger.info(
        "Starting consumer — broker=%s group=%s topic=%s window=%d",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_GROUP_ID,
        KAFKA_TOPIC,
        KPI_WINDOW_SIZE,
    )

    window: list = []
    window_start = datetime.now(timezone.utc)
    consumer = None

    try:
        consumer = create_consumer()
        logger.info("Consumer connected. Listening…")

        for message in consumer:
            doc = dict(message.value)
            doc["_kafka_offset"] = message.offset
            doc["_kafka_partition"] = message.partition
            doc["_kafka_topic"] = message.topic

            try:
                raw_col.insert_one(doc)
                logger.info(
                    "RAW stored → server=%-8s offset=%d uuid=%s",
                    doc.get("server_id"),
                    message.offset,
                    doc.get("message_uuid"),
                )
            except PyMongoError as exc:
                logger.error("Failed to insert raw document: %s", exc)
                continue

            window.append(message.value)

            if len(window) >= KPI_WINDOW_SIZE:
                kpi_doc = calculate_kpis(window, window_start)
                try:
                    kpi_col.insert_one(kpi_doc)
                    logger.info(
                        "KPI window closed — msgs=%d duration=%.2fs "
                        "avg_cpu=%.2f%% avg_mem=%.2f%% errors=%d throughput=%.3f msg/s",
                        kpi_doc["message_count"],
                        kpi_doc["window_duration_seconds"],
                        kpi_doc["kpis"]["avg_cpu_percent"],
                        kpi_doc["kpis"]["avg_memory_percent"],
                        kpi_doc["kpis"]["total_error_count"],
                        kpi_doc["kpis"]["throughput_msg_per_sec"],
                    )
                except PyMongoError as exc:
                    logger.error("Failed to insert KPI document: %s", exc)
                window = []
                window_start = datetime.now(timezone.utc)

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    except KafkaError as exc:
        logger.exception("Kafka error: %s", exc)
        raise
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        raise
    finally:
        if consumer:
            consumer.close()
            logger.info("Consumer closed.")


if __name__ == "__main__":
    main()
