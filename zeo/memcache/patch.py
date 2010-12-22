from ZEO.ClientStorage import ClientStorage
from cache import ZeoClientMemcache
import logging


def shakeItLikeAPolaroidPicture():
    logging.info("Replacing the on disk cache with MEMCACHE based cache")
    ClientStorage.ClientCacheClass = ZeoClientMemcache