from kafka import KafkaConsumer, KafkaProducer
import json
from config import settings, setup_logging
from .sentiment_model import SentimentService

logger = setup_logging()
logger.info("Connecting to Kafka")

consumer_raw = KafkaConsumer(
    settings.KAFKA_RAW_TOPIC,
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    group_id='test-group',
    enable_auto_commit=True
)

producer_processed = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


logger.info("Starting up the models")
sent_serv = SentimentService()
logger.info("Models started")

for msg in consumer_raw:
    post = json.loads(msg.value)
    data = sent_serv.process_post(post)    
    logger.info(f"Post processed: {data["post_id"]}, tags: {data["tags"]}, sentiment: {data["overall_vibe"]}")
    producer_processed.send(settings.KAFKA_PROCESSED_TOPIC, data)
    producer_processed.flush()