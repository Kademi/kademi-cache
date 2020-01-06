/*
 * Kademi
 */
package co.kademi.kademi.cache;

import co.kademi.kademi.cache.channel.InvalidateItemMessage;
import co.kademi.kademi.channel.Channel;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class InvalidationManager {

    private static final Logger log = LoggerFactory.getLogger(InvalidationManager.class);

    private final Channel channel;
    private final ThreadLocal<InvalidationState> tlInvalidationActionsList = new ThreadLocal();
    private final Map<String, KademiCacheRegion> mapOfRegions;
    private final CachePartitionService cachePartitionService;

    public InvalidationManager(Channel channel, CachePartitionService cachePartitionService, Map<String, KademiCacheRegion> mapOfRegions) {
        this.channel = channel;
        this.mapOfRegions = mapOfRegions;
        this.cachePartitionService = cachePartitionService;
    }

    public void clearInvalidationState() {
        tlInvalidationActionsList.remove();
    }

    private InvalidationState getInvalidationState(boolean autocreate) {
        InvalidationState list = tlInvalidationActionsList.get();
        if (list == null && autocreate) {
            list = new InvalidationState();
            tlInvalidationActionsList.set(list);
        }
        return list;
    }

    private List<InvalidationAction> enqueuedInvalidations(boolean autocreate) {
        InvalidationState is = getInvalidationState(autocreate);
        if (is != null) {
            return is.invalidationsList;
        }
        return null;
    }

    public void lockCacheForTransaction(Serializable id) {
        InvalidationState is = getInvalidationState(true);
        is.cacheLocked = true;
        Serializable partitionId = cachePartitionService.currentPartitionKey(null);
        enqueueInvalidation(null, null, null, partitionId);
    }

    public boolean isCacheLockedForTransaction() {
        InvalidationState is = getInvalidationState(false);
        if (is == null) {
            return false;
        }
        return is.cacheLocked;
    }

    public void enqueueInvalidation(String cacheName, KademiCacheRegion.KademiCacheAccessor cacheAccessor, String key, Serializable partitionId) {
        //log.info("enqueueInvalidation: cacheName={} key={}", cacheName, key);
        List<InvalidationAction> list = enqueuedInvalidations(true);
        InvalidationAction ia = new InvalidationAction(cacheName, cacheAccessor, key, partitionId);
        list.add(ia);

        // also need to flush the query cache immediately (as well as after the transaction) to ennsure if the query is called again within the transaction it doesnt get stale results
        // flush all query caches for this partiton
        for (KademiCacheRegion r : this.mapOfRegions.values()) {
            if (r instanceof KademiQueryResultsRegion) {
                KademiQueryResultsRegion qrr = (KademiQueryResultsRegion) r;
                qrr.getCache().invalidateAll(partitionId);
            }
        }
    }

    public void onCommit(Transaction tx) {
        List<InvalidationAction> list = enqueuedInvalidations(false);
        tlInvalidationActionsList.remove();
        if (list != null && !list.isEmpty() ) {
            log.info("onCommit: invalidating {} items", list.size());
            Set<Serializable> partitionIds = new HashSet<>();
            for (InvalidationAction ia : list) {
                //log.info("onCommit key={} partition={}", ia.key, ia.partitionId);
                doInvalidation(ia);
                partitionIds.add(ia.partitionId);
            }

            // flush all query caches for this partiton
            for (Serializable pId : partitionIds) {
                for (KademiCacheRegion r : this.mapOfRegions.values()) {
                    if (r instanceof KademiQueryResultsRegion) {
                        KademiQueryResultsRegion qrr = (KademiQueryResultsRegion) r;
                        qrr.getCache().invalidateAll(pId);
                    }
                }
            }
        }
    }

    public void onRollback(Transaction tx) {
        tlInvalidationActionsList.remove();
    }

    private void doInvalidation(InvalidationAction ia) {
        if (ia.key != null) {
            ia.cacheAccessor.invalidate(ia.key, ia.partitionId);
        }
        if (channel != null) {
            InvalidateItemMessage m = new InvalidateItemMessage(ia.cacheName, ia.key, ia.partitionId);
            channel.sendNotification(m);
        }
    }

    public void onInvalidateMessage(InvalidateItemMessage iim) {
        //lookup cache, and remove item. do not call invalidate otherwise will recur
        KademiCacheRegion r = null;
        if (iim.getCacheName() != null) {
            r = mapOfRegions.get(iim.getCacheName());
            if (r == null) {
                log.warn("onInvalidateMessage: cache not found: {}", iim.getCacheName());
            } else {
                r.getCache().invalidate(iim.getKey(), iim.getPartitionId());
            }
        }

        if (r == null || r instanceof KademiEntityRegion) {
            // need to invalidate all query results caches
            for (KademiCacheRegion r2 : mapOfRegions.values()) {
                if (r2 instanceof KademiQueryResultsRegion) {
                    KademiQueryResultsRegion qrr = (KademiQueryResultsRegion) r2;
                    //log.info("onInvalidateMessage: Invalidate query cache {} in partition {} due to network message", qrr.getName(), iim.getPartitionId());
                    qrr.getCache().invalidateAll(iim.getPartitionId()); // this will flush the entire cache, for the current partiton only
                }
            }
        }

    }

    private class InvalidationState {

        private final List<InvalidationAction> invalidationsList = new ArrayList<>();
        private boolean cacheLocked;    // if true, do not add to the cache for this transaction/thread

    }

    private class InvalidationAction {

        private final String cacheName;
        private final KademiCacheRegion.KademiCacheAccessor cacheAccessor;
        private final String key;
        private final Serializable partitionId;

        public InvalidationAction(String cacheName, KademiCacheRegion.KademiCacheAccessor cacheAccessor, String key, Serializable partitionId) {
            this.cacheAccessor = cacheAccessor;
            this.key = key;
            this.cacheName = cacheName;
            this.partitionId = partitionId;
            
            if( key == null ) {
                //log.warn("Null key!");
            }
            if( partitionId == null ) {
                //log.warn("Null partition!");
            }
        }

    }
}
