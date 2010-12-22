import xdrlib
import BTrees.LOBTree
import logging
import os
import threading
import time
from ZODB.utils import u64, z64
import memcache
import zope.interface
from zope.interface import implements
from zeo.cache.interfaces import IClientCache, IClientStorage
from zeo.cache.registry import cacheRegistry


logger = logging.getLogger("ZEO.memcache")

def bpack(saved_oid, tid, end_tid, data):
    """
    Binary pack for a cache item
    """
    p = xdrlib.Packer()
    p.pack_list([saved_oid, tid, end_tid, data], p.pack_string)
    return p.get_buffer()
    
def bunpack(packedData):
    u = xdrlib.Unpacker(packedData)     
    items = u.unpack_list(u.unpack_string)
    u.done()
    return items
    
def keyify(oid):
    """OIDs aren't good memcache keys. Remove all control characters"""
    return repr(oid).replace(" ", "")

class locked(object):
    def __init__(self, func):
        self.func = func

    def __get__(self, inst, class_):
        if inst is None:
            return self
        def call(*args, **kw):
            inst._lock.acquire()
            try:
                return self.func(inst, *args, **kw)
            finally:
                inst._lock.release()
        return call


class ZeoClientMemcache(object):
    """A memcached based zodb cache."""
    zope.interface.implements(IClientCache)
    
    def __nonzero__(self):
        return True
    
    # XXX: cache path will be url to memcache?
    def __init__(self, cache_path, size=None):
        # The number of records in the cache.
        self._n_items = 0

        # {oid -> {tid->pos}}
        # Note that caches in the wild seem to have very little non-current
        # data, so this would seem to have little impact on memory consumption.
        # I wonder if we even need to store non-current data in the cache.
        self.noncurrent = BTrees.LOBTree.LOBTree()

        # tid for the most recent transaction we know about.  This is also
        # stored near the start of the file.
        self.tid = None

        self.clearStats()

        self.cache = memcache.Client(['127.0.0.1:11211'], debug=0)

        self._lock = threading.RLock()
        

    def clear(self):
        self.cache.flush_all()

    def clearStats(self):
        self._n_adds = 0
        self._n_accesses = 0

    def getStats(self):
        return (self._n_adds, 0,
                0, 0,
                self._n_accesses,
               )
               
    def logStats(self):
        logging.info("Thread: %s --- Adds: %s      Accesses: %s"%(threading.current_thread(),self._n_adds, self._n_accesses))
        logging.info(self.cache.get_stats())

    def __len__(self):
        return self.cache.get_stats()[0][1]['total_items']

    ##
    # Close the underlying file.  No methods accessing the cache should be
    # used after this.
    def close(self):
        self.cache.disconnect_all()

    ##
    # Update our idea of the most recent tid.  This is stored in the
    # instance, and also written out near the start of the cache file.  The
    # new tid must be strictly greater than our current idea of the most
    # recent tid.
    @locked
    def setLastTid(self, tid):
        if (self.tid is not None) and (tid <= self.tid) and self:
            raise ValueError("new last tid (%s) must be greater than "
                             "previous one (%s)" % (u64(tid),
                                                    u64(self.tid)))
        assert isinstance(tid, str) and len(tid) == 8, tid
        self.tid = tid
        self.cache.set('LAST_TID',tid)
        
    ##
    # Return the last transaction seen by the cache.
    # @return a transaction id
    # @defreturn string, or None if no transaction is yet known
    def getLastTid(self):
        tid = self.tid
        if not tid:
            tid = self.cache.get('LAST_TID')
        return tid

    ##
    # Return the current data record for oid.
    # @param oid object id
    # @return (data record, serial number, tid), or None if the object is not
    #         in the cache
    # @defreturn 3-tuple: (string, string, string)

    @locked
    def load(self, oid):
        obj = self.cache.get(keyify(oid))
        if not obj:
            return None            
        
        saved_oid, tid, end_tid, data = bunpack(obj)
            
        self._n_accesses += 1
        
        return data, tid

    ##
    # Return a non-current revision of oid that was current before tid.
    # @param oid object id
    # @param tid id of transaction that wrote next revision of oid
    # @return data record, serial number, start tid, and end tid
    # @defreturn 4-tuple: (string, string, string, string)

    @locked
    def loadBefore(self, oid, before_tid):
        noncurrent_for_oid = self.noncurrent.get(u64(oid))
        if noncurrent_for_oid is None:
            return None

        items = noncurrent_for_oid.items(None, u64(before_tid)-1)
        if not items:
            return None
        tid, obj = items[-1]

        saved_oid, saved_tid, end_tid, data = bunpack(obj)

        if end_tid < before_tid:
            return None

        self._n_accesses += 1
        return data, saved_tid, end_tid

    ##
    # Store a new data record in the cache.
    # @param oid object id
    # @param start_tid the id of the transaction that wrote this revision
    # @param end_tid the id of the transaction that created the next
    #                revision of oid.  If end_tid is None, the data is
    #                current.
    # @param data the actual data

    @locked
    def store(self, oid, start_tid, end_tid, data):
        ofs = self.cache.get(keyify(oid))
        if end_tid is None and ofs:
            saved_oid, saved_tid, end_tid, data = bunpack(ofs)
            if saved_tid == start_tid:
                return
            raise ValueError("already have current data for oid")
        else:
            noncurrent_for_oid = self.noncurrent.get(u64(oid))
            if noncurrent_for_oid and (u64(start_tid) in noncurrent_for_oid):
                return

        self._n_adds += 1

        self.cache.set(keyify(oid), bpack(oid, start_tid, end_tid or z64, data))
        self._n_items += 1

        if end_tid:
            self._set_noncurrent(oid, start_tid, ofs)
            

    ##
    # If `tid` is None,
    # forget all knowledge of `oid`.  (`tid` can be None only for
    # invalidations generated by startup cache verification.)  If `tid`
    # isn't None, and we had current
    # data for `oid`, stop believing we have current data, and mark the
    # data we had as being valid only up to `tid`.  In all other cases, do
    # nothing.
    #
    # Paramters:
    #
    # - oid object id
    # - tid the id of the transaction that wrote a new revision of oid,
    #        or None to forget all cached info about oid.
    # - server_invalidation, a flag indicating whether the
    #       invalidation has come from the server. It's possible, due
    #       to threading issues, that when applying a local
    #       invalidation after a store, that later invalidations from
    #       the server may already have arrived.

    @locked
    def invalidate(self, oid, tid, server_invalidation=True):
        if tid is not None:
            if tid > self.tid:
                self.setLastTid(tid)
            elif tid < self.tid:
                if server_invalidation:
                    raise ValueError("invalidation tid (%s) must not be less"
                                     " than previous one (%s)" %
                                     (u64(tid), u64(self.tid)))
        self.cache.delete(keyify(oid))
        self._n_items -= 1

    ##
    # Generates (oid, serial) pairs for all objects in the
    # cache.  This generator is used by cache verification.
    def contents(self):
        for oid, ofs in self.cache.items():
            self._lock.acquire()
            try:
                saved_oid, tid, end_tid, data = bunpack(ofs)
                result = oid, tid
            finally:
                self._lock.release()

            yield result

    def dump(self):
        from ZODB.utils import oid_repr
        print "cache size", len(self)
        L = list(self.contents())
        L.sort()
        for oid, tid in L:
            print oid_repr(oid), oid_repr(tid)
        print "dll contents"
        L = list(self)
        L.sort(lambda x, y: cmp(x.key, y.key))
        for x in L:
            end_tid = x.end_tid or z64
            print oid_repr(x.key[0]), oid_repr(x.key[1]), oid_repr(end_tid)
        print
        
    def sync():
        # nothing to do here on fs
        pass


cacheRegistry.register([IClientStorage], IClientCache, '', ZeoClientMemcache)