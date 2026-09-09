"""Bounded LRU cache for URI -> URI (or set of URIs) lookups.

Lives here rather than alongside the redis idmap because config.py caches
canonicalize() with it, and config sits at the bottom of the import stack --
it must not pull in a storage backend (and `import redis` with it).
"""

from collections import OrderedDict
from typing import Optional, Set, Union

# Distinct from None, which is a legitimate cached value: canonicalize()
# returns None for a URI it can't match, and those negatives are worth
# caching. A cache probed with a plain .get() would treat them as misses and
# recompute every time.
_MISSING = object()


class URICache:
    # Use __slots__ to prevent dynamic dictionary creation for the instance,
    # saving memory overhead and slightly speeding up attribute access.
    __slots__ = ('capacity', 'cache')
    missing = _MISSING

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("Capacity must be greater than 0")
        self.capacity = capacity
        # Keys are URIs (str), Values are either a URI (str) or Set of URIs (Set[str])
        self.cache: OrderedDict[str, Union[str, Set[str]]] = OrderedDict()

    def get(self, key: str, missing=_MISSING) -> Optional[Union[str, Set[str]]]:
        """
        Retrieve a value by its URI key.
        Returns self.missing if not found to avoid conflicts with actual None values.
        """
        if key not in self.cache:
            return missing
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key: str, value: Union[str, Set[str]]) -> None:
        """
        Insert or update a URI or Set of URIs.
        """
        if key in self.cache:
            # If it exists, update its position to most recently used.
            self.cache.move_to_end(key)
        self.cache[key] = value
        # If we exceed capacity, pop the least recently used item.
        if len(self.cache) > self.capacity:
            # popitem(last=False) removes and returns the first inserted (LRU) key-value pair in O(1) time.
            self.cache.popitem(last=False)

    def clear(self) -> None:
        self.cache.clear()

    def __len__(self) -> int:
        return len(self.cache)

    def __contains__(self, key: str) -> bool:
        return key in self.cache

    def __getitem__(self, key: str) -> Union[str, Set[str]]:
        return self.get(key)

    def __setitem__(self, key: str, value: Union[str, Set[str]]) -> None:
        self.put(key, value)

    def __delitem__(self, key: str) -> None:
        self.cache.pop(key, None)
