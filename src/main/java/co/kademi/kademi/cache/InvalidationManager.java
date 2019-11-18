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
    private final ThreadLocal<List<InvalidationAction>> tlInvalidationActionsList = new ThreadLocal();
    private final Map<String, KademiCacheRegion> mapOfRegions;

    public InvalidationManager(Channel channel, Map<String, KademiCacheRegion> mapOfRegions) {
        this.channel = channel;
        this.mapOfRegions = mapOfRegions;
    }

    private List<InvalidationAction> enqueuedInvalidations(boolean autocreate) {
        List<InvalidationAction> list = tlInvalidationActionsList.get();
        if (list == null && autocreate) {
            list = new ArrayList<>();
            tlInvalidationActionsList.set(list);
        }
        return list;
    }

    public void enqueueInvalidation(String cacheName, KademiCacheRegion.KademiCacheAccessor cacheAccessor, String key, Serializable partitionId) {
        log.info("enqueueInvalidation: cacheName={} key={}", cacheName, key);
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
        if (list != null) {
            log.info("onCommit: invalidating {} items", list.size());
            Set<Serializable> partitionIds = new HashSet<>();
            for (InvalidationAction ia : list) {
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
        ia.cacheAccessor.invalidate(ia.key);
        if (channel != null) {
            InvalidateItemMessage m = new InvalidateItemMessage(ia.cacheName, ia.key, ia.partitionId);
            channel.sendNotification(m);
        }
    }

    public void onInvalidateMessage(InvalidateItemMessage iim) {
        //lookup cache, and remove item. do not call invalidate otherwise will recur
        KademiCacheRegion r = mapOfRegions.get(iim.getCacheName());
        if (r != null) {
            //r.remove(iim.getKey());
            r.getCache().invalidate(iim.getKey(), iim.getPartitionId());
            if (r instanceof KademiEntityRegion) {
                // need to invalidate all query results caches
                for (KademiCacheRegion r2 : mapOfRegions.values()) {
                    if (r2 instanceof KademiQueryResultsRegion) {
                        KademiQueryResultsRegion qrr = (KademiQueryResultsRegion) r2;
                        log.info("onInvalidateMessage: Invalidate query cache {} in partition {} due to network message", qrr.getName(), iim.getPartitionId());
                        qrr.getCache().invalidateAll(iim.getPartitionId()); // this will flush the entire cache, for the current partiton only
                    }
                }
            }
        } else {
            log.warn("---- CACHE NOT FOUND " + iim.getCacheName() + " -----");
        }
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
        }

    }
}
