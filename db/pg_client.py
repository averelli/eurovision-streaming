import psycopg2
from psycopg2.extras import Json
from config import settings

class PostgresClient:
    def __init__(self):
        self.conn = psycopg2.connect(settings.postgres_uri)
        self.conn.autocommit = True

    def execute_query(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                return None
            
    def insert_window(self, window) -> int:
        query = """
        INSERT INTO live_data.window_aggregates (
            window_start,
            window_end,
            window_posts,
            total_posts,
            window_sentiment,
            total_sentiment,
            window_avg_sentiment,
            total_avg_sentiment,
            window_platform_counter,
            total_platform_counter,
            window_top_song,
            window_top_country,
            window_top_contestant,
            total_top_song,
            total_top_country,
            total_top_contestant,
            event_label
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (window_start) DO NOTHING
        RETURNING id
        """
        return self.execute_query(
            query,
            (
                window["window_start"],
                window["window_end"],
                window["window_posts"],
                window["total_posts"],
                window["window_sentiment"],
                window["total_sentiment"],
                window["window_avg_sentiment"],
                window["total_avg_sentiment"],
                Json(window["window_platform_counter"]),
                Json(window["total_platform_counter"]),
                Json(window["window_top_song"]),
                Json(window["window_top_country"]),
                Json(window["window_top_contestant"]),
                Json(window["total_top_song"]),
                Json(window["total_top_country"]),
                Json(window["total_top_contestant"]),
                window["event_label"]
            )
        )

    def close(self):
        self.conn.close()
