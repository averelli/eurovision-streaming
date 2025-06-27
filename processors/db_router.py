from kafka import KafkaConsumer
from config import settings
from db.mongo import MongoDBClient
import json

consumer_raw = KafkaConsumer(
    settings.KAFKA_PROCESSED_TOPIC,
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    group_id='test-router',
    enable_auto_commit=True
)
mongo = MongoDBClient("eurovison_streaming")

for msg in consumer_raw:
    post = json.loads(msg.value)
    print("Got a message:" + str(post["post_id"]))
    mongo.insert_post(post, "processed_posts")
    for clause in post["clauses"]:
        mongo.insert_post(clause, "processed_clauses")