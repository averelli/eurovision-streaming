from collections import  Counter, deque
from datetime import datetime, timedelta

# global metrics
total_stats = {
    "total_posts": 0,
    "total_sentiment": 0.0,
    "tag_counter": Counter(),
    "platform_counter": Counter()
}

# window
ROLLING_WINDOW = deque()
WINDOW_SIZE = timedelta(seconds=60)

def prune_window():
    now = datetime.now()
    while ROLLING_WINDOW and now - ROLLING_WINDOW[0][0] > WINDOW_SIZE:
        ROLLING_WINDOW.popleft()

def aggregate_window():
    tag_counter = Counter()
    platform_counter = Counter()
    window_sentiment = 0.0

    for _, post in ROLLING_WINDOW:
        window_sentiment += post["overall_vibe"]
        platform_counter[post["platform"]] += 1

        for tag in post.get("tags", []):
            tag_counter[tag["text"]] += 1

    return {
        "window_start": ROLLING_WINDOW[0][0],
        "window_end": ROLLING_WINDOW[-1][0],
        "window_posts": len(ROLLING_WINDOW),
        "widow_sentiment": window_sentiment,
        "avg_sentiment": round(window_sentiment / len(ROLLING_WINDOW), 2),
        "total_posts": total_stats["total_posts"],
        "win_platform_counter": platform_counter.most_common(),
        "total_platform_counter"
        "total_sentiment": total_stats["total_sentiment"],
        
    }


def add_post(post):
    # add to the window
    ts = datetime.fromisoformat(post["timestamp"].replace("Z", "+00:00"))
    ROLLING_WINDOW.append((ts, post))
    prune_window()

    # update the stats
    total_stats["total_posts"] += 1
    total_stats["total_sentiment"] += post["overall_vibe"]
    total_stats["platform_counter"][post["platform"]] += 1
    for tag in post.get("tags", []):
        total_stats["tag_counter"][tag["text"]] += 1

