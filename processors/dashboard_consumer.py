from kafka import KafkaConsumer
import json
from config import settings

import time

def consume_windows(pause_time:int=None):
    """
    Generator that yields aggregated window as they arrive from Kafka
    """
    consumer = KafkaConsumer(
        settings.KAFKA_WINDOW_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='dashboard-group-debug-199',
        enable_auto_commit=True
    )
    for msg in consumer:
        window = json.loads(msg.value)
        yield window
        if pause_time:
            time.sleep(pause_time)

