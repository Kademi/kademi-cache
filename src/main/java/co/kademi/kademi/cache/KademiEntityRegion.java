/*
 * Kademi
 */
package co.kademi.kademi.cache;

import co.kademi.kademi.channel.Channel;
import java.util.Properties;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;

/**
 *
 * @author brad
 */
public class KademiEntityRegion extends KademiCacheRegion implements EntityRegion{
    
    private final KademiRegionFactory regionFactory;

    public KademiEntityRegion( KademiRegionFactory regionFactory, String string,Channel channel, Properties props, CacheDataDescription cdd, InvalidationManager imgr, CachePartitionService cachePartitionService) {
        super(string, channel, props, cdd, imgr, cachePartitionService);
        this.regionFactory = regionFactory;
    }


    @Override
    public EntityRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
        return new KademiEntityRegionAccessStrategy(this, props, regionFactory);

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
