from datetime import datetime

_latest_bluesky_likes: dict[tuple[str,str], float] = {}

# Clean BlueSky streaming likes
def clean_bluesky_likes(raw: dict) -> dict:
    if not raw.get("post_uri") or not raw.get("actor_did") or not raw.get("created_at"):
        return None
    
    # If two rows have the same post_uri and actor_did, keep the one with the latest created_at
    # parse timestamp
    created_ts = (
        datetime.fromisoformat(raw["created_at"].replace("Z", "+00:00"))
                .timestamp()
    )

    key = (raw["post_uri"], raw["actor_did"])
    prev_ts = _latest_bluesky_likes.get(key)

    # if we've seen this key before at an equal or later time, skip it
    if prev_ts is not None and created_ts <= prev_ts:
        return None

    # otherwise update our state and emit
    _latest_bluesky_likes[key] = created_ts

    return {
        "post_uri": raw["post_uri"],
        "text": raw["text"],
        "created_at": created_ts,
        "actor_handle": raw["actor_handle"],
        "actor_display_name": raw["actor_display_name"],
        "source": "bluesky",
        **raw.get("_meta", {}),
    }