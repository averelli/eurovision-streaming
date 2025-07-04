from kafka import KafkaConsumer
from pg_client import PostgresClient
import json
from config import settings, setup_logging

logger = setup_logging()
logger.info("Starting the pg_win_saver")

consumer = KafkaConsumer(
    settings.KAFKA_WINDOW_TOPIC,
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="pg_win_saver",
    enable_auto_commit=True
)

logger.info("Connecting to the database")
pg = PostgresClient()

logger.info("Starting to consume the window topic")
for msg in consumer:
    window = json.loads(msg.value)
    window_id = pg.insert_window(window)
    logger.info(f"Inserted window: {window_id}")

logger.info("Closing the database connection")
pg.close()
