package co.kademi.kademi.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;

/**
 *
 * @author brad
 */
public class KademiCacheRegion implements org.hibernate.cache.spi.Region {

    protected final String name;
    protected final Properties props;
    protected final CacheDataDescription cdd;
    private final Cache<Object, Object> cache;
    private final int timeout;

    public KademiCacheRegion(String string, Properties props, CacheDataDescription cdd) {
        this.name = string;
        this.props = props;
        this.cdd = cdd;
        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
        timeout = 600; // not sure of units
    }



    @Override
    public String getName() {
        return name;
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
