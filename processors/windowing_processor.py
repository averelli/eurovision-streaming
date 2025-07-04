from datetime import datetime, timedelta, timezone
import json
from kafka import KafkaConsumer, KafkaProducer

from config import settings, setup_logging
from window_logic import WindowAggregator

EVENT_LABEL = "Semi Final 1"

logger = setup_logging()
logger.info("Connecting to Kafka")

posts_consumer = KafkaConsumer(
    settings.KAFKA_PROCESSED_TOPIC,
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    group_id='windowing-group',
    enable_auto_commit=True
)

window_producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

logger.info("Starting windowing aggregator")
aggregator = WindowAggregator(logger=logger, event_label=EVENT_LABEL, window_size_seconds=30)
last_flush = None

for msg in posts_consumer:
    post = json.loads(msg.value)
    print(post["timestamp"])
    aggregator.add_post(post)

    if not last_flush:
        last_flush = datetime.now()
    elif datetime.now() - last_flush > timedelta(seconds=30):
        window = aggregator.aggregate_window()
        window_producer.send(settings.KAFKA_WINDOW_TOPIC, value=window)
        window_producer.flush()
        last_flush = datetime.now()
        logger.info(f"Flushed window to Kafka: {window}")