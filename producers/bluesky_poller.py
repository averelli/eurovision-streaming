import atproto
from config import settings
import re
from logging import Logger

def get_bsky_client():
    client = atproto.Client()
    client.login(settings.BSKY_USER, settings.BSKY_PASSWORD)
    return client

def remove_urls(text):
    # Regex pattern to match http, https, www, or just domain-based URLs
    url_pattern = re.compile(
        r'(https?://\S+|www\.\S+|(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(?:/\S*)?)'
    )
    return url_pattern.sub('', text)


def poll_bsky(client: atproto.Client, last_ts, logger: Logger):
    params = {
        "q":"eurovision", 
        "limit":50, 
        "lang":"en",
        "since": last_ts
    }
    try:
        results = client.app.bsky.feed.search_posts(params)

        posts = []
        timestamps = []
        for post in results["posts"]:
            author_obj = post.author
            post_obj = post.record
            cleaned_post = {
                "post_id": post.cid,
                "post_author": author_obj.handle,
                "body": remove_urls(post_obj.text),
                "timestamp": post_obj.created_at,
                "platform": "Bluesky"
            }
            posts.append(cleaned_post)
            timestamps.append(post_obj.created_at)
        
        logger.info(f"Got {len(posts)} new posts")
        if len(posts) == 0:
            return last_ts, []
        return max(timestamps), posts
    except Exception as e:
        logger.error(f"Error while polling Bluesky: {e}", exc_info=True)
        return last_ts, []
    