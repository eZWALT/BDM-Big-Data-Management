from datetime import datetime
from typing import Optional, Dict, Any

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