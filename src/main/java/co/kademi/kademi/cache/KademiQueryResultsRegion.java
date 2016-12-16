/*
 * Kademi
 */
package co.kademi.kademi.cache;

import java.util.Properties;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.QueryResultsRegion;

/**
 *
 * @author brad
 */
public class KademiQueryResultsRegion extends KademiCacheRegion implements QueryResultsRegion {

    public KademiQueryResultsRegion() {
        super(null, null, null);
    }

    public KademiQueryResultsRegion(String string, Properties props, CacheDataDescription cdd) {
        super(string, props, cdd);
    }

    @Override
    public Object get(Object key) throws CacheException {
        return getCache().getIfPresent(key);
    }

    @Override
    public void put(Object key, Object value) throws CacheException {
        getCache().put(key, value);
    }

    @Override
    public void evict(Object key) throws CacheException {
        getCache().invalidate(key);
    }

    @Override
    public void evictAll() throws CacheException {
        getCache().invalidateAll();
    }

}
