"""Bounded LRU cache for URI -> URI (or set of URIs) lookups.

Lives here rather than alongside the redis idmap because config.py caches
canonicalize() with it, and config sits at the bottom of the import stack --
it must not pull in a storage backend (and `import redis` with it).

Optionally segmented, for the caller whose keys are not all worth the same
amount. See `reusable` on __init__ and the note on the class.
"""

from collections import OrderedDict
from typing import Callable, Optional, Set, Union

# Distinct from None, which is a legitimate cached value: canonicalize()
# returns None for a URI it can't match, and those negatives are worth
# caching. A cache probed with a plain .get() would treat them as misses and
# recompute every time.
_MISSING = object()


class URICache:
    """LRU, optionally split into a protected and a one-shot segment.

    Plain LRU assumes every key has the same chance of being asked for
    again. The idmap's do not, and merge is where it shows: it walks a slice
    of internal records, and each one's own uri -> yuid entry is read exactly
    once in the life of the process -- the record is visited once and never
    again. The entries worth holding are the *external* members, the aat
    concepts and wikidata places that thousands of records all reference.

    Under one LRU the one-shot keys evict the reusable ones continuously:
    a 200k cache walking 100M records is 100M insertions of keys that will
    never be read, and every one of them pushes something useful out. Give it
    a `reusable` predicate and reusable keys get their own segment, which
    nothing else can evict.

    `reusable` is called on the key at insert only, so it must be cheap and
    must not depend on anything that changes while the cache lives.
    """

    __slots__ = ('capacity', 'cache', 'hot', 'hot_capacity', 'reusable',
                 'hits', 'hot_hits', 'misses')
    missing = _MISSING

    def __init__(self, capacity: int,
                 reusable: Optional[Callable[[str], bool]] = None,
                 reusable_share: float = 0.9):
        if capacity <= 0:
            raise ValueError("Capacity must be greater than 0")
        self.reusable = reusable
        if reusable is None:
            self.capacity = capacity
            self.hot_capacity = 0
        else:
            # Most of the room goes to the keys that come back. The
            # one-shot segment only has to outlive the record being worked
            # on -- get_cluster() puts a cluster's member set in and merge
            # reads it back two stages later -- so a tenth of the capacity
            # is already thousands of entries more than it needs.
            self.hot_capacity = max(1, int(capacity * reusable_share))
            self.capacity = max(1, capacity - self.hot_capacity)
        # Keys are URIs (str), Values are either a URI (str) or Set of URIs
        self.cache: OrderedDict[str, Union[str, Set[str]]] = OrderedDict()
        self.hot: OrderedDict[str, Union[str, Set[str]]] = OrderedDict()
        self.hits = 0
        self.hot_hits = 0
        self.misses = 0

    def get(self, key: str, missing=_MISSING) -> Optional[Union[str, Set[str]]]:
        """
        Retrieve a value by its URI key.
        Returns self.missing if not found to avoid conflicts with actual None values.
        """
        if key in self.hot:
            self.hot.move_to_end(key)
            self.hot_hits += 1
            self.hits += 1
            return self.hot[key]
        if key not in self.cache:
            self.misses += 1
            return missing
        self.cache.move_to_end(key)
        self.hits += 1
        return self.cache[key]

    def put(self, key: str, value: Union[str, Set[str]]) -> None:
        """
        Insert or update a URI or Set of URIs.
        """
        # An existing key keeps the segment it is in: reusable() is a
        # prediction, and one already made should not be revisited on every
        # write to the same key.
        if key in self.hot:
            self.hot.move_to_end(key)
            self.hot[key] = value
            return
        if key not in self.cache and self.reusable is not None \
                and self.reusable(key):
            self.hot[key] = value
            if len(self.hot) > self.hot_capacity:
                self.hot.popitem(last=False)
            return
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
        self.hot.clear()

    def stats(self) -> dict:
        """Whether the split is earning its keep. `hot_hits` against `hits`
        says how much of the value is coming from the protected segment; a
        hit rate that does not move when hot_capacity does means the
        predicate is wrong about which keys come back."""
        looks = self.hits + self.misses
        return {"hits": self.hits, "hot_hits": self.hot_hits,
                "misses": self.misses,
                "hit_rate": self.hits / looks if looks else 0.0,
                "hot": len(self.hot), "hot_capacity": self.hot_capacity,
                "cold": len(self.cache), "cold_capacity": self.capacity}

    def __len__(self) -> int:
        return len(self.cache) + len(self.hot)

    def __contains__(self, key: str) -> bool:
        return key in self.cache or key in self.hot

    def __getitem__(self, key: str) -> Union[str, Set[str]]:
        return self.get(key)

    def __setitem__(self, key: str, value: Union[str, Set[str]]) -> None:
        self.put(key, value)

    def __delitem__(self, key: str) -> None:
        self.cache.pop(key, None)
        self.hot.pop(key, None)
