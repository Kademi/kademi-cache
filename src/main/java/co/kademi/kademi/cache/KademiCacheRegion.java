package co.kademi.kademi.cache;

import co.kademi.kademi.cache.channel.InvalidateAllMessage;
import co.kademi.kademi.cache.channel.InvalidateItemMessage;
import co.kademi.kademi.channel.Channel;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.Serializable;
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
    private final Cache<String, Object> cache;
    private final int ttlMins;
    private final int timeout;
    private final int maxSize;

    public KademiCacheRegion(String name, Channel channel, Properties props, CacheDataDescription cdd, InvalidationManager imgr) {
        this.cacheName = name;
        this.imgr = imgr;
        this.channel = channel;
        this.props = props;
        this.cdd = cdd;

        ttlMins = Integer.parseInt(props.getProperty("hibernate.cache.ttl_mins", "5"));

        int i = Integer.parseInt(props.getProperty("hibernate.cache.max_size", "1000"));
        String k = "hibernate.cache." + name + ".max_size";
        if( props.containsKey(k)) {
            i = Integer.parseInt(props.getProperty(k));
        }
        this.maxSize = i;

        cache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(ttlMins, TimeUnit.MINUTES)
                .build();
        timeout = 600; // not sure of units

    }

    public void remove(Serializable key) {
        String sKey = key.toString();
        cache.invalidate(sKey);
    }

    protected void invalidate(Object key) {
        // only process invalidations on transaction complete
        String sKey = key.toString();
        imgr.enqueueInvalidation(cacheName, cache, sKey);
    }

    protected void invalidateAll() {
        cache.invalidateAll();
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
        Object v = cache.getIfPresent(o);
        return v != null;
    }

    @Override
    public long getSizeInMemory() {
        return cache.size();
    }

    @Override
    public long getElementCountInMemory() {
        return cache.size();
    }

    @Override
    public long getElementCountOnDisk() {
        return -1;
    }

    @Override
    public Map toMap() {
        return cache.asMap();
    }

    @Override
    public long nextTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public int getTimeout() {
        return timeout;
    }

    public Cache<String, Object> getCache() {
        return cache;
    }

    public void removeAll() {
        cache.invalidateAll();
    }

    public int getTtlMins() {
        return ttlMins;
    }

    public int getMaxSize() {
        return maxSize;
    }




}
