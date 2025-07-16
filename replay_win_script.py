import json
import logging
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from kafka import KafkaProducer

from processors.window_logic import WindowAggregator
from config import settings

# --- Setup logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ReplaySimulator")

# --- Constants ---
WINDOW_SECONDS = 60
WINDOW_SIZE = timedelta(seconds=WINDOW_SECONDS)
GRAND_FINAL_DATES = {"2025-05-17", "2025-05-18"}

# --- Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

# --- MongoDB Client ---
mongo = MongoClient(settings.MONGO_URI)
collection = mongo["eurovison_streaming"]["processed_posts"]

# --- Load and normalize posts ---
logger.info("Fetching posts from MongoDB...")
raw_posts = list(collection.find())

logger.info(f"Fetched {len(raw_posts)} posts. Normalizing timestamps and sorting...")

# Convert all timestamps to UTC-aware datetime objects
for post in raw_posts:
    ts = post["timestamp"]
    if isinstance(ts, int):
        ts = datetime.fromtimestamp(ts, tz=timezone.utc)
    elif isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    elif isinstance(ts, datetime) and ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    post["timestamp"] = ts

# Sort posts by timestamp
posts = sorted(raw_posts, key=lambda x: x["timestamp"])

# --- Label mapping based on date ---
def get_event_label(date_str):
    if date_str == "2025-05-13":
        return "Semi Final 1"
    elif date_str == "2025-05-15":
        return "Semi Final 2"
    elif date_str in GRAND_FINAL_DATES:
        return "Grand Final"
    else:
        return "Unknown"

# --- Initialize aggregator ---
aggregator = WindowAggregator(logger, window_size_seconds=WINDOW_SECONDS)

# --- Simulate windowing ---
window_start = None
window_count = 0
last_date = None
current_event_label = None

for post in posts:
    ts = post["timestamp"]
    current_date = ts.date().isoformat()

    if window_start is None:
        window_start = ts
        last_date = current_date
        current_event_label = get_event_label(current_date)

    # Reset aggregator if date changes, except for grand final dates
    if current_date != last_date and not ({current_date, last_date} <= GRAND_FINAL_DATES):
        logger.info(f"Date changed from {last_date} to {current_date}, resetting aggregator")
        aggregator = WindowAggregator(logger, window_size_seconds=WINDOW_SECONDS)
        last_date = current_date
        current_event_label = get_event_label(current_date)

    # Check if still in the same window
    if ts - window_start <= WINDOW_SIZE:
        aggregator.add_post(post)
    else:
        # Flush the current window
        if aggregator.ROLLING_WINDOW:
            logger.info(f"Flushing window #{window_count} | {len(aggregator.ROLLING_WINDOW)} posts")
            result = aggregator.aggregate_window()
            result["event_label"] = current_event_label
            producer.send(settings.KAFKA_WINDOW_TOPIC, value=result)
            producer.flush()
            window_count += 1

        # Start new window
        aggregator.debug_clear_window()
        window_start = ts
        aggregator.add_post(post)

# --- Final flush for remaining posts ---
if aggregator.ROLLING_WINDOW:
    logger.info(f"Final flush | {len(aggregator.ROLLING_WINDOW)} posts")
    result = aggregator.aggregate_window()
    result["event_label"] = current_event_label
    producer.send(settings.KAFKA_WINDOW_TOPIC, value=result)
    producer.flush()

logger.info("Replay complete. Shutting down.")
