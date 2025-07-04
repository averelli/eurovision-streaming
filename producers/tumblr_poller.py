import requests
from config import settings, setup_logging
from datetime import datetime, timezone


def clean_post(post: dict):
    post_id = post.get("id")
    author = post.get("blog", {}).get("name")
    timestamp = post.get("timestamp")
    body = post.get("body")

    # Convert timestamp to ISO 
    iso_timestamp = None
    if timestamp is not None:
        try:
            iso_timestamp = datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()
        except Exception:
            iso_timestamp = str(timestamp)

    return {
        "post_id": post_id,
        "post_author": author,
        "timestamp": iso_timestamp,
        "body": body,
        "platform": "Tumblr"
    }
        

def poll_tumblr(max_ts:int, logger):

    posts = fetch_posts(logger)

    if not posts:
        return max_ts, []

    new_posts = [post for post in posts if int(post["timestamp"]) >= max_ts]
    if len(new_posts) >= 1:
        max_ts = max([post["timestamp"] for post in new_posts])
        logger.info(f"Fetched {len(new_posts)} new posts")
        # Clean posts after filtering
        cleaned_posts = [clean_post(post) for post in new_posts]
        return max_ts, cleaned_posts
    else:
        logger.info("No new posts")
        return max_ts, []


def fetch_posts(logger):
    URL = "https://api.tumblr.com/v2/tagged"
    API_KEY = settings.TUMBLR_API_KEY

    params = {
            "tag": "Eurovision",
            "api_key": API_KEY,
            "limit": 20,
            "filter": "text"
        }
    
    try:
        response = requests.get(URL, params)
        if response.status_code == 200:
            raw_posts = response.json()['response']
            posts = []
            
            for post in raw_posts:
                if not post.get("body", None):
                    print(post.get("summary"))
                    logger.warning("No post body found")
                    continue
                posts.append(post)  
            return posts
        else:
            logger.error(f"Error: {response.status_code}. {response.text}")
    except Exception as e:
            logger.error(f"Exception occured while polling Tumblr:  {e}", exc_info=True)
            return[]
