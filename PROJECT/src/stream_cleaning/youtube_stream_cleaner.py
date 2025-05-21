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
    
    # ADD : Convert the "Unknown" values to None in the columns channelId and publishedAt

    # parse publishedAt from ISO to unix epoch
    #   YouTube gives e.g. "2025-03-28T03:36:22Z"
    # .replace("Z","+00:00") makes it iso-8601 parseable
    ts = datetime.fromisoformat(raw["publishedAt"].replace("Z", "+00:00")) \
                .timestamp()

    return {
        "threadId":    raw["threadId"],
        "videoId":     raw["videoId"],
        "comment":     raw["comment"],
        "publishedAt": ts,
        "likes":       int(raw.get("likes", 0)),
        "replies":     int(raw.get("replies", 0)),
        "channelId":   raw.get("channelId"),
        "channelName": raw.get("channelName"),
        "source":      "youtube",
        **raw.get("_meta", {}),
    }

def clean_youtube_video(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a raw YouTube video metadata record for streaming.
    
    Optional numeric fields will default to 0 if missing.
    All other fields are passed through or set to None.
    """
    # If videoId or publishedAt are not present, drop the row
    if not raw.get("videoId") or not raw.get("publishedAt"):
        return None
    
    # ADD: # If two rows have the same videoId, keep the one with the latest publishedAt

    # parse the publishedAt into a timestamp
    try:
        ts = datetime.fromisoformat(
            raw["publishedAt"].replace("Z", "+00:00")
        ).timestamp()
    except Exception:
        return None

    return {
        "videoId":      raw["videoId"],
        "title":        raw.get("title"),
        "description":  raw.get("description"),
        "url":          raw.get("url"),
        "channel":      raw.get("channel"),
        "publishedAt":  ts,
        # numeric defaults
        "viewCount":    int(raw.get("viewCount", 0)),
        "likeCount":    int(raw.get("likeCount", 0)),
        "commentCount": int(raw.get("commentCount", 0)),
        # optional strings
        "tags":         raw.get("tags"),
        "thumbnail":    raw.get("thumbnail"),
        "duration":     raw.get("duration"),
        "definition":   raw.get("definition"),
        "captions":     raw.get("captions"),
   
        "source":       "youtube",
        **raw.get("_meta", {}),
    }