package co.kademi.kademi.cache;

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
    protected final Properties props;
    protected final CacheDataDescription cdd;
    private final Cache<Object, Object> cache;
    private final int timeout;

    public KademiCacheRegion(String name, Channel channel, Properties props, CacheDataDescription cdd) {
        this.cacheName = name;
        this.channel = channel;
        this.props = props;
        this.cdd = cdd;
        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
        timeout = 600; // not sure of units

    }

    public void remove(Serializable key) {
        cache.invalidate(key);
    }

    protected void invalidate(Serializable key) {
        cache.invalidate(key);
        if (channel != null) {
            InvalidateItemMessage m = new InvalidateItemMessage(cacheName, key);
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

    public Cache<Object, Object> getCache() {
        return cache;
    }

}
