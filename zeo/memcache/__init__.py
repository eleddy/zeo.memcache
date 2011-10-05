from cache import ZeoClientMemcache
from ZEO.ClientStorage import ClientStorage

class MemcachedClientStorage(ClientStorage):
    ClientCacheClass = ZeoClientMemcache

