from datetime import datetime

# Clean BlueSky streaming posts
def clean_bluesky_post(raw: dict) -> dict:
    if not raw.get("uri") or not raw.get("text") or not raw.get("created_at"):
        return None
    return {
        "uri": raw["uri"],
        "text": raw["text"],
        "created_at": datetime.fromisoformat(raw["created_at"].replace("Z", "+00:00")).timestamp(),
        "like_count": int(raw.get("like_count", 0)),
        "reply_count": int(raw.get("reply_count", 0)),
        "repost_count": int(raw.get("repost_count", 0)),
        "quote_count": int(raw.get("quote_count", 0)),
        "source": "bluesky",
        **raw.get("_meta", {}),
    }

def clean_bluesky_like(raw: dict) -> dict:
    if not raw.get("post_uri") or not raw.get("actor_did") or not raw.get("created_at"):
        return None
    
    # ADD - If two rows have the same post_uri and actor_did, keep the one with the latest created_at

    return {
        "post_uri": raw["post_uri"],
        "text": raw["text"],
        "created_at": datetime.fromisoformat(raw["created_at"].replace("Z", "+00:00")).timestamp(),
        "actor_handle": raw["actor_handle"],
        "actor_display_name": raw["actor_display_name"],
        "source": "bluesky",
        **raw.get("_meta", {}),
    }