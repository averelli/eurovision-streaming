from kafka import KafkaConsumer
from config import settings
from db.mongo_client import MongoDBClient
import json

consumer = KafkaConsumer(
    settings.KAFKA_PROCESSED_TOPIC,
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="mongo_saver",
    enable_auto_commit=True
)
mongo = MongoDBClient("eurovison_streaming")

for msg in consumer:
    post = json.loads(msg.value)
    mongo.insert_post(post, "processed_posts")
    for clause in post["clauses"]:
        mongo.insert_post(clause, "processed_clauses")