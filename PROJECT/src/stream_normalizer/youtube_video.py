from datetime import datetime
from typing import Any, Dict, Optional


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
        ts = datetime.fromisoformat(raw["publishedAt"].replace("Z", "+00:00")).timestamp()
    except Exception:
        return None

    return {"uri": raw["videoId"], "created_at": ts, "text": raw.get("captions")}
