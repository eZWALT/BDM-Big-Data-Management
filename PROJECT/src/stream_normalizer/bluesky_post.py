from datetime import datetime


# Clean BlueSky streaming posts
def clean_bluesky_post(raw: dict) -> dict:
    post_did: str = raw.get("did")
    post_text: str = raw.get("commit", {}).get("record", {}).get("text")
    created_at: str = raw.get("commit", {}).get("record", {}).get("createdAt")
    if not post_did or not post_text or not created_at:
        return None

    return {
        "uri": post_did,
        "text": post_text,
        "created_at": datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp(),
    }
