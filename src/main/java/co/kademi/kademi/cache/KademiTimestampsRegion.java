/*
 * Kademi
 */
package co.kademi.kademi.cache;

import java.util.Properties;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.TimestampsRegion;

/**
 *
 * @author brad
 */
public class KademiTimestampsRegion extends KademiCacheRegion implements TimestampsRegion{

    public KademiTimestampsRegion(String string, Properties props) {
        super(string, props, null);
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
