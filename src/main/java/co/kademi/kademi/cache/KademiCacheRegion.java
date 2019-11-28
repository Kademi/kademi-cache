package co.kademi.kademi.cache;

import co.kademi.kademi.cache.channel.InvalidateAllMessage;
import co.kademi.kademi.channel.Channel;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public abstract class KademiCacheRegion implements org.hibernate.cache.spi.Region {

    private static final Logger log = LoggerFactory.getLogger(KademiCacheRegion.class);

    protected final String cacheName;
    private final Channel channel;
    private final InvalidationManager imgr;
    protected final Properties props;
    protected final CacheDataDescription cdd;
    private final CachePartitionService cachePartitionService;
    private final int ttlMins;
    private final int timeout;
    private final int maxSize;

    private final KademiCacheAccessor cacheAccessor = new KademiCacheAccessor();

    public KademiCacheRegion(String name, Channel channel, Properties props, CacheDataDescription cdd, InvalidationManager imgr, CachePartitionService cachePartitionService) {
        this.cacheName = name;
        this.imgr = imgr;
        this.channel = channel;
        this.props = props;
        this.cdd = cdd;
        this.cachePartitionService = cachePartitionService;

        ttlMins = Integer.parseInt(props.getProperty("hibernate.cache.ttl_mins", "5"));

        int i = Integer.parseInt(props.getProperty("hibernate.cache.max_size", "1000"));
        String k = "hibernate.cache." + name + ".max_size";
        if (props.containsKey(k)) {
            i = Integer.parseInt(props.getProperty(k));
        }
        this.maxSize = i;

        timeout = 600; // not sure of units

    }

    public void remove(Serializable key) {
        String sKey = key.toString();
        cacheAccessor.invalidate(sKey);
    }

    protected void invalidate(Object key) {
        // only process invalidations on transaction complete
        String sKey = key.toString();
        Serializable partitionId = cachePartitionService.currentPartitionKey(null);
        imgr.enqueueInvalidation(cacheName, cacheAccessor, sKey, partitionId);
    }

    protected void invalidateAll() {
        cacheAccessor.invalidateAll();
        if (channel != null) {
            InvalidateAllMessage m = new InvalidateAllMessage(cacheName);
            channel.sendNotification(m);
        }
    }

    @Override
    public String getName() {
        return cacheName;
    }

    @Override
    public void destroy() throws CacheException {

    }

    @Override
    public boolean contains(Object o) {
        return cacheAccessor.contains(o);
    }

    @Override
    public long getSizeInMemory() {
        return cacheAccessor.getElementCountInMemory();
    }

    @Override
    public long getElementCountInMemory() {
        return cacheAccessor.getElementCountInMemory();
    }

    @Override
    public long getElementCountOnDisk() {
        return -1;
    }

    @Override
    public Map toMap() {
        return null;
    }

    @Override
    public long nextTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public int getTimeout() {
        return timeout;
    }

    public KademiCacheAccessor getCache() {
        return cacheAccessor;
    }

    public void removeAll() {
        cacheAccessor.invalidateReallyAll();
    }

    public int getTtlMins() {
        return ttlMins;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public class KademiCacheAccessor {

        private final Map<Serializable, Cache<String, Object>> mapOfCaches = new HashMap<>();
        private final Cache<String, Object> defaultCache;

        public KademiCacheAccessor() {
            defaultCache = createCache(10); // very short TTL
        }

        public boolean contains(Object o) {
            Object v = defaultCache.getIfPresent(o);
            if (v != null) {
                return true;
            }

            for (Cache<String, Object> c : mapOfCaches.values()) {
                v = defaultCache.getIfPresent(o);
                if (v != null) {
                    return true;
                }
            }

            return false;
        }

        public long getSizeInMemory() {
            long s = defaultCache.size();
            for (Cache<String, Object> c : mapOfCaches.values()) {
                s += c.size();
            }
            return s;
        }

        public long getElementCountInMemory() {
            long s = defaultCache.size();
            for (Cache<String, Object> c : mapOfCaches.values()) {
                s += c.size();
            }
            return s;
        }

        void invalidateReallyAll() {
            defaultCache.invalidateAll();
            for (Cache<String, Object> c : mapOfCaches.values()) {
                c.invalidateAll();
            }
        }

        //private final Cache<String, Object> cache;
        private Serializable getPartitionId() {
            return cachePartitionService.currentPartitionKey(null);
        }

        private Cache<String, Object> cache() {
            Serializable id = getPartitionId();
            return cache(id);
        }

        private Cache<String, Object> cache(Serializable id) {
            if (id != null) {
                Cache<String, Object> cache = mapOfCaches.get(id);
                if (cache == null) {
                    cache = createCache(ttlMins*60);
                    mapOfCaches.put(id, cache);
                }
                return cache;
            } else {
                return defaultCache;
            }
        }


        private Cache<String, Object> createCache(int seconds) {
            return CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    .expireAfterWrite(seconds, TimeUnit.SECONDS)
                    .build();
        }

        public Object getIfPresent(String key) {
            return cache().getIfPresent(key);
        }

        public void put(String key, Object value) {
            Serializable id = getPartitionId();
            //log.info("put: partition={} key={} value={}", id, key, value);
            cache().put(key, value);
        }

        public void invalidate(Serializable key) {
            cache().invalidate(key);
            defaultCache.invalidate(key);
        }

        public void invalidate(Serializable key, Serializable partitionId) {
            Cache<String, Object> c = cache(partitionId);
            //log.info("invalidate: part={} key={} size before={}", partitionId, key, c.size());
            c.invalidate(key);
            //log.info("invalidate: part={} key={} size after={} does contain?={}", partitionId, key, c.size(), c.getIfPresent(key));
            defaultCache.invalidate(key); // must always invalidate from the default cache, because this is used prior to locating the rootfolder
        }


        public void invalidateAll() {
            cache().invalidateAll();
            defaultCache.invalidateAll();
        }

        public void invalidateAll(Serializable partitionId) {
            Cache<String, Object> c = cache(partitionId);
            if( c.size() == 0 ) {
                return ;
            }
            log.info("invalidateAll: cache: {} partition: {} current size={}", KademiCacheRegion.this.cacheName, partitionId, c.size());
            c.invalidateAll();
            //log.info("invalidateAll: partition: {} after invalidation size={}", partitionId, c.size());
            defaultCache.invalidateAll();
        }

        public Map asMap() {
            Map m = new HashMap();
            m.putAll(defaultCache.asMap());
            for (Cache<String, Object> c : mapOfCaches.values()) {
                m.putAll(c.asMap());
            }
            return m;
        }

        public Map<Serializable,Long> getPartitionCounts() {
            Map<Serializable,Long> map = new HashMap<>();
            for( Map.Entry<Serializable, Cache<String, Object>> entry : this.mapOfCaches.entrySet() ) {
                map.put(entry.getKey(), entry.getValue().size());
            }
            return map;
        }


    }

}
