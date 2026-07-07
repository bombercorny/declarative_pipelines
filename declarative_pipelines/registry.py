from typing import Callable

TRANSFORMATION_REGISTRY: dict[str, Callable] = {}


def register(fn: Callable) -> Callable:
    """Decorator that registers a transformation function by its name."""
    TRANSFORMATION_REGISTRY[fn.__name__] = fn
    return fn
