from dotenv import load_dotenv
import os

load_dotenv()

class Settings:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC")
    KAFKA_PROCESSED_TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC")

    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB = os.getenv("MONGO_DB")

    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    POSTGRES_DB = os.getenv("POSTGRES_DB")

    TUMBLR_API_KEY = os.getenv("TUMBLR_API_KEY")
    BSKY_USER = os.getenv("BSKY_USER")
    BSKY_PASSWORD = os.getenv("BSKY_PASSWORD")

    @property
    def postgres_uri(self):
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

settings = Settings()