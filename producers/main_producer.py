from kafka import KafkaProducer
from tumblr_poller import poll_tumblr
from bluesky_poller import poll_bsky, get_bsky_client
import json
import time
from config import settings, setup_logging

logger = setup_logging()

producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

last_ts_tumblr = int(time.time())
last_ts_bsky = "2025-05-15T00:00:00Z"
bsky_client = get_bsky_client()

while True:
    logger.info(f"Started main producer loop with ts_t: {last_ts_tumblr}, ts_b: {last_ts_bsky}")
    # TODO: implement just sort by latest
    last_ts_tumblr, tumblr_posts = poll_tumblr(last_ts_tumblr, logger)
    last_ts_bsky, bsky_posts = poll_bsky(bsky_client, last_ts_bsky, logger)

    for post in tumblr_posts + bsky_posts:
        producer.send(settings.KAFKA_RAW_TOPIC, value=post)
        producer.flush()

    time.sleep(10)


