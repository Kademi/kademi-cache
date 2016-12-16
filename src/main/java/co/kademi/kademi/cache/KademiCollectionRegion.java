/*
 * Kademi
 */
package co.kademi.kademi.cache;

import java.util.Properties;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;

/**
 *
 * @author brad
 */
public class KademiCollectionRegion extends KademiCacheRegion implements CollectionRegion {

    public KademiCollectionRegion() {
        super(null, null, null);
    }

    public KademiCollectionRegion(String string, Properties props, CacheDataDescription cdd) {
        super(string, props, cdd);
    }


    @Override
    public CollectionRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
        return new KademiCollectionRegionAccessStrategy();
    }

    @Override
    public boolean isTransactionAware() {
        return false;
    }

    @Override
    public CacheDataDescription getCacheDataDescription() {
        return cdd;
    }

    public class KademiCollectionRegionAccessStrategy implements CollectionRegionAccessStrategy {

        @Override
        public CollectionRegion getRegion() {
            return KademiCollectionRegion.this;
        }

        @Override
        public Object get(Object key, long txTimestamp) throws CacheException {
            return getCache().getIfPresent(key);
        }

        @Override
        public boolean putFromLoad(Object key, Object value, long txTimestamp, Object version) throws CacheException {
            getCache().put(key, value);
            return true;
        }

        @Override
        public boolean putFromLoad(Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
            getCache().put(key, value);
            return true;
        }

        @Override
        public SoftLock lockItem(Object key, Object version) throws CacheException {
            return new KademiSoftLock();
        }

        @Override
        public SoftLock lockRegion() throws CacheException {
            return new KademiSoftLock();
        }

        @Override
        public void unlockItem(Object key, SoftLock lock) throws CacheException {

        }

        @Override
        public void unlockRegion(SoftLock lock) throws CacheException {
            
        }

        @Override
        public void remove(Object key) throws CacheException {
            getCache().invalidate(key);
        }

        @Override
        public void removeAll() throws CacheException {
            getCache().invalidateAll();
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
}
