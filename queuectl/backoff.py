"""Exponential backoff delay computation."""


def compute_delay(attempts: int, base: int) -> float:
    """Seconds to wait before a job becomes retryable again.

    `attempts` is the number of completed (failed) attempts, so with the
    default base=2: 1st failure -> 2s, 2nd -> 4s, 3rd -> 8s.
    """
    return float(base) ** attempts
