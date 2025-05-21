from hashlib import sha256


def hash_query(query: str) -> str:
    """
    Hash the query using SHA256 and return the first 8 characters.
    This is used to create a unique identifier for the query.
    """
    # Use sha256 to hash the query for a consistent length, and take the first 8 characters
    return sha256(query.encode("utf-8")).hexdigest()[:8]
