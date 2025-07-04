from collections import  Counter, deque
from datetime import datetime, timedelta, timezone
from logging import Logger
from data import COUNTRIES

class WindowAggregator:
    def __init__(self, logger: Logger, event_label:str, window_size_seconds=60):
        self.logger = logger
        self.WINDOW_SIZE = timedelta(seconds=window_size_seconds)
        self.ROLLING_WINDOW = deque()
        self.event_label = event_label
        self.total_stats = {
            "total_posts": 0,
            "total_sentiment": 0.0,
            "total_platform_counter": Counter(),
            "total_song_counter": Counter(),
            "total_contestant_counter": Counter(),
            "total_country_counter": Counter()
        }

    def prune_window(self):
        self.logger.info(f"Prunning the window")
        now = datetime.now(timezone.utc)
        while self.ROLLING_WINDOW and now - self.ROLLING_WINDOW[0][0] > self.WINDOW_SIZE:
            self.ROLLING_WINDOW.popleft()

    def aggregate_window(self):
        self.logger.info("Aggregating the window")
        window_sentiment = 0.0
        window_posts = len(self.ROLLING_WINDOW)
        platform_counter = Counter()
        song_counter = Counter()
        contestant_counter = Counter()
        country_counter = Counter()

        for _, post in self.ROLLING_WINDOW:
            window_sentiment += post["overall_vibe"]
            platform_counter[post["platform"]] += 1

            # count window tags
            for tag in post.get("tags", []):
                if tag["label"] == "GPE" and tag["text"].lower() in COUNTRIES:
                    country_counter[tag["text"].capitalize()] += 1
                elif tag["label"] == "CONTESTANT":
                    contestant_counter[tag["text"].capitalize()] += 1
                elif tag["label"] == "SONG":
                    song_counter[tag["text"].capitalize()] += 1

        # add to the total counters
        self.total_stats["total_posts"] += window_posts
        self.total_stats["total_sentiment"] += window_sentiment
        self.total_stats["total_platform_counter"] += platform_counter
        self.total_stats["total_contestant_counter"] += contestant_counter
        self.total_stats["total_country_counter"] += country_counter
        self.total_stats["total_song_counter"] += song_counter

        # get the top values or None if empty
        window_top_song = song_counter.most_common(1)[0] if song_counter else None #("song": count) type
        total_top_song = self.total_stats["total_song_counter"].most_common(1)[0] if self.total_stats["total_song_counter"] else None
        window_top_country = country_counter.most_common(1)[0] if country_counter else None
        total_top_country = self.total_stats["total_country_counter"].most_common(1)[0] if self.total_stats["total_country_counter"] else None
        window_top_contestant = contestant_counter.most_common(1)[0] if contestant_counter else None
        total_top_contestant = self.total_stats["total_contestant_counter"].most_common(1)[0] if self.total_stats["total_contestant_counter"] else None

        return {
            "window_start":             self.ROLLING_WINDOW[0][0].isoformat(),
            "window_end":               self.ROLLING_WINDOW[-1][0].isoformat(),

            "window_posts":             window_posts,
            "total_posts":              self.total_stats["total_posts"],

            "window_sentiment":         window_sentiment,
            "total_sentiment":          self.total_stats["total_sentiment"],
            "window_avg_sentiment":     round(window_sentiment / window_posts, 2),
            "total_avg_sentiment":      round(self.total_stats["total_sentiment"] / self.total_stats["total_posts"], 2),

            "window_platform_counter":  platform_counter,
            "total_platform_counter":   self.total_stats["total_platform_counter"],
            
            "window_top_song":          window_top_song,
            "window_top_country":       window_top_country,
            "window_top_contestant":    window_top_contestant,
            "total_top_song":           total_top_song,
            "total_top_country":        total_top_country,
            "total_top_contestant":     total_top_contestant,
            "event_label":              self.event_label
        }

    def add_post(self, post):
        ts = post["timestamp"]
        self.logger.info(f"Appending post: {post["post_id"]}")
        self.ROLLING_WINDOW.append((ts, post))
        self.prune_window() 

    def debug_clear_window(self):
        self.ROLLING_WINDOW = deque()