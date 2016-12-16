/*
 * Kademi
 */
package co.kademi.kademi.cache;

import java.util.Properties;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.NaturalIdRegionAccessStrategy;

/**
 *
 * @author brad
 */
public class KademiNaturalIdRegion extends KademiCacheRegion implements NaturalIdRegion{

    public KademiNaturalIdRegion(String string, Properties props, CacheDataDescription cdd) {
        super(string, props, cdd);
    }

    public KademiNaturalIdRegion() {
        super(null, null, null);
    }

    @Override
    public NaturalIdRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isTransactionAware() {
        return false;
    }

    @Override
    public CacheDataDescription getCacheDataDescription() {
        return cdd;
    }

}
