from datetime import datetime
from typing import Optional, Dict, Any

def clean_youtube_comment(raw: dict) -> dict:
    """
    Clean a raw YouTube comment record for streaming.

    Returns a dict with:
      - channelId (str)
      - channelName (str)
      - threadId (str)
      - videoId  (str)
      - comment  (str)
      - publishedAt (float)  unix timestamp in seconds
      - likes     (int)
      - replies   (int)
      - source    "youtube"
      - any extra raw["_meta"] if present
    """
    # drop if any mandatory field is missing or blank
    if not raw.get("threadId") or not raw.get("videoId") \
       or not raw.get("comment") or not raw.get("publishedAt"):
        return None
    
    # clean up channelId
    ch = raw.get("channelId")
    channel_id = None if ch == "Unknown" else ch

    # clean up publishedAt
    pub_raw = raw.get("publishedAt")
    if pub_raw == "Unknown":
        # drop the record
        return None
    else:
        ts = (
            datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
            .timestamp()
        )

    return {
        "threadId":    raw["threadId"],
        "videoId":     raw["videoId"],
        "comment":     raw["comment"],
        "publishedAt": ts,
        "likes":       int(raw.get("likes", 0)),
        "replies":     int(raw.get("replies", 0)),
        "channelId":   channel_id,
        "channelName": raw.get("channelName"),
        "source":      "youtube",
        **raw.get("_meta", {}),
    }

