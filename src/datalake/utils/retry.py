from collections.abc import Callable

from tenacity import retry, stop_after_attempt, wait_exponential


def with_network_retry[T](fn: Callable[..., T]) -> Callable[..., T]:
    return retry(
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(3),
        reraise=True,
    )(fn)
