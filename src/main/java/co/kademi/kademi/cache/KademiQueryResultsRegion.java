/*
 * Kademi
 */
package co.kademi.kademi.cache;

import co.kademi.kademi.channel.Channel;
import java.util.Properties;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsRegion;

/**
 *
 * @author brad
 */
public class KademiQueryResultsRegion extends KademiCacheRegion implements QueryResultsRegion {
    
    
    public KademiQueryResultsRegion(String string,Channel channel, Properties props, CacheDataDescription cdd, InvalidationManager imgr, CachePartitionService cachePartitionService) {
        super(string, channel, props, cdd, imgr, cachePartitionService);
    }

    @Override
    public Object get(Object key) throws CacheException {
        return getCache().getIfPresent(key.toString());
    }

    @Override
    public void put(Object key, Object value) throws CacheException {
        getCache().put(key.toString(), value);
    }

    @Override
    public void evict(Object key) throws CacheException {
        QueryKey ck = (QueryKey) key;
        getCache().invalidate(ck.toString());
    }

    @Override
    public void evictAll() throws CacheException {
        getCache().invalidateAll();
    }

}
