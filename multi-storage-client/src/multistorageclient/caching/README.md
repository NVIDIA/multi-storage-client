# Caching Module

This module contains the components for the MultiStorageClient caching system.

## Components

- `cache_item.py`: Defines the `CacheItem` class which represents a cached file and its metadata.
- `eviction_policy.py`: Defines the eviction policies (LRU, FIFO, Random) used by the cache manager.

## Usage

The caching module is used by the `CacheManager` class in `cache.py` to manage cached files and implement different eviction strategies.

```python
from multistorageclient.cache import CacheConfig, CacheManager
from multistorageclient.caching.eviction_policy import LRU, FIFO, RANDOM

# Create a cache configuration
cache_config = CacheConfig(
    location="/path/to/cache",
    size_mb=1000,  # 1GB cache
    use_etag=True,
    eviction_policy=FIFO  # Default is "fifo", can also use "lru" or "random"
)

# Create a cache manager
cache_manager = CacheManager(cache_config)
``` 