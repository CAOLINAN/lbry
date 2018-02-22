import binascii
import collections
import logging
import datetime

from twisted.internet import defer, task
from lbrynet.core import utils

log = logging.getLogger(__name__)


class DummyHashAnnouncer(object):
    def __init__(self):
        pass

    def run_manage_loop(self):
        pass

    def stop(self):
        pass

    def hash_queue_size(self):
        return 0

    def immediate_announce(self, blob_hashes):
        pass

    def get_next_announce_time(self):
        return 0


class DHTHashAnnouncer(DummyHashAnnouncer):
    ANNOUNCE_CHECK_INTERVAL = 60
    CONCURRENT_ANNOUNCERS = 5

    # 1 hour is the min time hash will be reannounced
    MIN_HASH_REANNOUNCE_TIME = 60 * 60
    # conservative assumption of the time it takes to announce
    # a single hash
    DEFAULT_SINGLE_HASH_ANNOUNCE_DURATION = 1

    """This class announces to the DHT that this peer has certain blobs"""
    STORE_RETRIES = 3

    def __init__(self, dht_node):
        self.dht_node = dht_node
        self.peer_port = dht_node.peerPort
        self.next_manage_call = None
        self.hash_queue = collections.deque()
        self._concurrent_announcers = 0
        self._manage_call_lc = task.LoopingCall(self.manage_lc)
        self._manage_call_lc.clock = dht_node.clock
        self._lock = utils.DeferredLockContextManager(defer.DeferredLock())
        self._last_checked = dht_node.clock.seconds(), self.CONCURRENT_ANNOUNCERS
        self._total = None
        self.single_hash_announce_duration = self.DEFAULT_SINGLE_HASH_ANNOUNCE_DURATION
        self._hashes_to_announce = []

    def run_manage_loop(self):
        log.info("Starting hash announcer")
        if not self._manage_call_lc.running:
            self._manage_call_lc.start(self.ANNOUNCE_CHECK_INTERVAL)

    def manage_lc(self):
        last_time, last_hashes = self._last_checked
        hashes = len(self.hash_queue)
        if hashes:
            t, h = self.dht_node.clock.seconds() - last_time, last_hashes - hashes
            blobs_per_second = float(h) / float(t)
            if blobs_per_second > 0:
                estimated_time_remaining = int(float(hashes) / blobs_per_second)
                remaining = str(datetime.timedelta(seconds=estimated_time_remaining))
            else:
                remaining = "unknown"
            log.info("Announcing blobs: %i blobs left to announce, %i%s complete, "
                     "est time remaining: %s", hashes + self._concurrent_announcers,
                     100 - int(100.0 * float(hashes + self._concurrent_announcers) /
                               float(self._total)), "%", remaining)
            self._last_checked = t + last_time, hashes
        else:
            self._total = 0
        if self.peer_port is not None:
            return self._announce_available_hashes()

    def stop(self):
        log.info("Stopping DHT hash announcer.")
        if self._manage_call_lc.running:
            return self._manage_call_lc.stop()

    def immediate_announce(self, blob_hashes):
        if self.peer_port is not None:
            return self._announce_hashes(blob_hashes, immediate=True)
        else:
            return defer.succeed(False)

    def hash_queue_size(self):
        return len(self.hash_queue)

    @defer.inlineCallbacks
    def _announce_available_hashes(self):
        log.debug('Announcing available hashes')
        hashes = yield self.hashes_to_announce()
        yield self._announce_hashes(hashes)

    @defer.inlineCallbacks
    def _announce_hashes(self, hashes, immediate=False):
        if not hashes:
            defer.returnValue(None)
        if not self.dht_node.can_store:
            log.warning("Client only DHT node cannot store, skipping announce")
            defer.returnValue(None)
        log.info('Announcing %s hashes', len(hashes))
        # TODO: add a timeit decorator
        start = self.dht_node.clock.seconds()

        ds = []
        with self._lock:
            for h in hashes:
                announce_deferred = defer.Deferred()
                if immediate:
                    self.hash_queue.appendleft((h, announce_deferred))
                else:
                    self.hash_queue.append((h, announce_deferred))
            if not self._total:
                self._total = len(hashes)

        log.debug('There are now %s hashes remaining to be announced', self.hash_queue_size())

        @defer.inlineCallbacks
        def do_store(blob_hash, announce_d, retry_count=0):
            if announce_d.called:
                defer.returnValue(announce_deferred.result)
            try:
                store_nodes = yield self.dht_node.announceHaveBlob(binascii.unhexlify(blob_hash))
                if not store_nodes:
                    retry_count += 1
                    if retry_count <= self.STORE_RETRIES:
                        log.debug("No nodes stored %s, retrying", blob_hash)
                        result = yield do_store(blob_hash, announce_d, retry_count)
                    else:
                        result = {}
                        log.warning("No nodes stored %s", blob_hash)
                else:
                    result = store_nodes
                if not announce_d.called:
                    announce_d.callback(result)
                defer.returnValue(result)
            except Exception as err:
                if not announce_d.called:
                    announce_d.errback(err)
                raise err

        @defer.inlineCallbacks
        def announce(progress=None):
            progress = progress or {}
            if len(self.hash_queue):
                with self._lock:
                    h, announce_deferred = self.hash_queue.popleft()
                log.debug('Announcing blob %s to dht', h[:16])
                stored_to_nodes = yield do_store(h, announce_deferred)
                progress[h] = stored_to_nodes
                log.debug("Stored %s to %i peers (hashes announced by this announcer: %i)",
                          h.encode('hex')[:16],
                          len(stored_to_nodes), len(progress))

                yield announce(progress)
            else:
                with self._lock:
                    self._concurrent_announcers -= 1
            defer.returnValue(progress)

        for i in range(self._concurrent_announcers, self.CONCURRENT_ANNOUNCERS):
            self._concurrent_announcers += 1
            ds.append(announce())
        announcer_results = yield defer.DeferredList(ds)
        stored_to = {}
        for _, announced_to in announcer_results:
            stored_to.update(announced_to)

        log.info('Took %s seconds to announce %s hashes', self.dht_node.clock.seconds() - start, len(hashes))
        seconds_per_blob = (self.dht_node.clock.seconds() - start) / len(hashes)
        self.set_single_hash_announce_duration(seconds_per_blob)
        defer.returnValue(stored_to)

    @defer.inlineCallbacks
    def add_hashes_to_announce(self, blob_hashes):
        yield self._lock._lock.acquire()
        self._hashes_to_announce.extend(blob_hashes)
        yield self._lock._lock.release()

    @defer.inlineCallbacks
    def hashes_to_announce(self):
        hashes_to_announce = []
        yield self._lock._lock.acquire()
        while self._hashes_to_announce:
            hashes_to_announce.append(self._hashes_to_announce.pop())
        yield self._lock._lock.release()
        defer.returnValue(hashes_to_announce)

    def set_single_hash_announce_duration(self, seconds):
        """
        Set the duration it takes to announce a single hash
        in seconds, cannot be less than the default single
        hash announce duration
        """
        seconds = max(seconds, self.DEFAULT_SINGLE_HASH_ANNOUNCE_DURATION)
        self.single_hash_announce_duration = seconds

    def get_next_announce_time(self, num_hashes_to_announce=1):
        """
        Hash reannounce time is set to current time + MIN_HASH_REANNOUNCE_TIME,
        unless we are announcing a lot of hashes at once which could cause the
        the announce queue to pile up.  To prevent pile up, reannounce
        only after a conservative estimate of when it will finish
        to announce all the hashes.

        Args:
            num_hashes_to_announce: number of hashes that will be added to the queue
        Returns:
            timestamp for next announce time
        """
        queue_size = self.hash_queue_size() + num_hashes_to_announce
        reannounce = max(self.MIN_HASH_REANNOUNCE_TIME,
                         queue_size * self.single_hash_announce_duration)
        return self.dht_node.clock.seconds() + reannounce
