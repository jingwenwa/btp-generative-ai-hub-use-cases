from __future__ import annotations

import warnings
import time

from functools import lru_cache, wraps
from typing import Dict, Literal, Optional, List, Any, Tuple


def _get_cache_refresh_time(cache_refresh_time: float, recache: bool, timeout: Optional[int] = None) -> float:
    current_time = time.time()
    if recache or (timeout is not None and (current_time - cache_refresh_time) > timeout):
        cache_refresh_time = current_time
    return cache_refresh_time


def _get_cache_key_and_args(cache_refresh_time: float, args: List[Any], first_arg_self: bool) -> Tuple[
    Optional[Any], List[Any], float]:
    if first_arg_self and args:
        id_ = id(args[0])
        cache_key = (id_, cache_refresh_time)
        obj, args = args[0], args[1:]
    else:
        cache_key = (cache_refresh_time,)
        obj = None
    return cache_key, args, obj


def lru_cache_extended(timeout: Optional[int] = None,
                       maxsize: Optional[int] = None,
                       typed: bool = False,
                       first_arg_self: bool = False):
    """
    A decorator that adds LRU caching with an optional timeout to methods.
    Handles 'self' as a weak reference for instance methods if required.

    Args:
        timeout: Time in seconds after which the cache will be refreshed. If None, never expires.
        maxsize: Maximum size of the cache.
        typed: If True, arguments of different types will be cached separately.
        first_arg_self: If True, treats the first argument as 'self' and uses its id for caching.

    Returns:
        Callable: Decorated method with cache and optional timeout.
    """

    def decorator(func):
        cache_refresh_time = time.time()

        objs = {}

        @wraps(func)
        @lru_cache(maxsize=maxsize, typed=typed)
        def cached_method(cache_key, *args, **kwargs):
            if first_arg_self:
                id_, _ = cache_key
                args = (objs.pop(id_),) + (args or tuple([]))

            return func(*args, **kwargs)

        @wraps(func)
        def wrapped_func(*args, _recache=False, **kwargs):
            nonlocal cache_refresh_time
            cache_refresh_time = _get_cache_refresh_time(cache_refresh_time, _recache, timeout)
            cache_key, args, obj = _get_cache_key_and_args(cache_refresh_time, args, first_arg_self)
            if obj:
                objs[id(obj)] = obj
            try:
                ret = cached_method(cache_key, *args, **kwargs)
            finally:
                if first_arg_self:
                    objs.pop(id(obj), None)
            return ret

        wrapped_func.cache_info = cached_method.cache_info
        wrapped_func.cache_clear = cached_method.cache_clear

        return wrapped_func

    return decorator


class PredictionURLs:
    """
    A class to manage prediction URLs with dynamic suffixes for different models.

    Args:
        suffixes (Optional[Dict[str, str]]): A dictionary mapping model names to their URL suffixes.
    """

    def __init__(self, suffixes: Optional[Dict[str, str]] = None) -> None:
        self._suffixes: Dict[str, str] = {}
        if suffixes:
            self.register(suffixes)

    def register(self, suffixes: Dict[str, str | Omit]) -> None:
        """
        Register new model suffixes.

        Args:
            suffixes (Dict[str, str]): A dictionary of model names and their corresponding URL suffixes.
        """

        cleaned_suffixes = {model_name: '/' + suffix.lstrip('/') if suffix is not OMIT else OMIT for
                            model_name, suffix in suffixes.items()}
        self._suffixes.update(cleaned_suffixes)

    def __call__(self, model_name: str, url: str, fixed_suffix: Optional[str] = None) -> str:
        """
        Generate a complete URL for a given model.

        Args:
            model_name (str): The name of the model.
            url (str): The base URL.
            fixed_suffix (Optional[str]): A fixed suffix to override the registered one.

        Returns:
            str or None: The complete URL for the model. None means there is no suffix registered for the model and usally the url should be used.
        """
        suffix = fixed_suffix if fixed_suffix is not None else self._suffixes.get(model_name)
        if suffix is OMIT or suffix is None:
            return None
        else:
            return url.rstrip('/?') + suffix if url else ''


try:
    # Don't duplicate the definition if openai offers it
    from openai._types import NOT_GIVEN, NotGiven, Omit, OMIT
except ImportError:
    class NotGiven:

        def __bool__(self) -> Literal[False]:
            return False

    NOT_GIVEN = NotGiven()
    class Omit:

        def __bool__(self) -> Literal[False]:
            return False

    OMIT = Omit()


def if_set(value, alternative=NOT_GIVEN):
    return value if not isinstance(value, NotGiven) and value is not None else alternative

def if_str_set(value: str, alternative: str = ""):
    return value if value != "" else alternative

def kwargs_if_set(**kwargs):
    filtered_kwargs = {}
    for name in [*kwargs.keys()]:
        if if_set(kwargs[name]):
            filtered_kwargs[name] = kwargs[name]
    return filtered_kwargs

def warn_once(msg, category=None):
    if not getattr(warn_once, 'log', None):
        warn_once.log = set()
    if msg not in warn_once.log:
        warnings.warn(msg, category, stacklevel=2)
        warn_once.log.add(msg)
