from config.settings import USER_AGENT, RETRY_MAX_ATTEMPTS, RETRY_WAIT_SECONDS
import time
from functools import wraps

def retry(max_attempts=RETRY_MAX_ATTEMPTS, wait_seconds=RETRY_WAIT_SECONDS, exceptions=(Exception,)):
    """
    Retry decorator with exponential backoff.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    print(f"[Retry] Attempt {attempt} failed with {e}")
                    if attempt == max_attempts:
                        raise
                    time.sleep(wait_seconds * (2 ** (attempt - 1)))
        return wrapper
    return decorator