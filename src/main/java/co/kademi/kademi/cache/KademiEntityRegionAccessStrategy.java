/*
 * Kademi
 */
package co.kademi.kademi.cache;

import java.io.Serializable;
import java.util.Properties;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheKey;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;

/**
 *
 * @author brad
 */
public class KademiEntityRegionAccessStrategy implements EntityRegionAccessStrategy {

    private final KademiEntityRegion entityRegion;
    private final Properties props;

    public KademiEntityRegionAccessStrategy(KademiEntityRegion aThis, Properties props) {
        this.entityRegion = aThis;
        this.props = props;
    }

    @Override
    public EntityRegion getRegion() {
        return entityRegion;
    }

    @Override
    public boolean insert(Object key, Object value, Object version) throws CacheException {
        return false;
    }

    @Override
    public boolean afterInsert(Object key, Object value, Object version) throws CacheException {
        CacheKey ck = (CacheKey) key;
        entityRegion.getCache().put(ck.toString(), value);
        return true;
    }

    @Override
    public boolean update(Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException {
        return false;

    }

    @Override
    public boolean afterUpdate(Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) throws CacheException {
        entityRegion.invalidate(key);
        return true;

    }

    @Override
    public Object get(Object key, long txTimestamp) throws CacheException {
        CacheKey ck = (CacheKey) key;
        return entityRegion.getCache().getIfPresent(ck.toString());
    }

    @Override
    public boolean putFromLoad(Object key, Object value, long txTimestamp, Object version) throws CacheException {
        CacheKey ck = (CacheKey) key;
        entityRegion.getCache().put(ck.toString(), value);
        return true;
    }

    @Override
    public boolean putFromLoad(Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
        CacheKey ck = (CacheKey) key;
        entityRegion.getCache().put(ck.toString(), value);
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
        entityRegion.invalidate(key);
        entityRegion.getCache().invalidate(key.toString());
    }

    @Override
    public void removeAll() throws CacheException {
        entityRegion.getCache().invalidateAll();
    }

    @Override
    public void evict(Object key) throws CacheException {
        CacheKey ck = (CacheKey) key;
        entityRegion.getCache().invalidate(ck.getKey());
        entityRegion.invalidate(ck);
    }

    @Override
    public void evictAll() throws CacheException {
        entityRegion.getCache().invalidateAll();
    }

}
