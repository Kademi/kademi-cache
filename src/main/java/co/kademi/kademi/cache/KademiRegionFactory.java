package co.kademi.kademi.cache;

import java.util.Properties;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Settings;

/**
 *
 * @author brad
 */
public class KademiRegionFactory implements RegionFactory {

    @Override
    public void start(Settings stngs, Properties prprts) throws CacheException {

    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isMinimalPutsEnabledByDefault() {
        return true;
    }

    @Override
    public AccessType getDefaultAccessType() {
        return AccessType.NONSTRICT_READ_WRITE;
    }

    @Override
    public long nextTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public EntityRegion buildEntityRegion(String string, Properties prprts, CacheDataDescription cdd) throws CacheException {
        return new KademiEntityRegion(string, prprts, cdd);
    }

    @Override
    public NaturalIdRegion buildNaturalIdRegion(String string, Properties prprts, CacheDataDescription cdd) throws CacheException {
        return new KademiNaturalIdRegion(string, prprts, cdd);
    }

    @Override
    public CollectionRegion buildCollectionRegion(String string, Properties prprts, CacheDataDescription cdd) throws CacheException {
        return new KademiCollectionRegion(string, prprts, cdd);
    }

    @Override
    public QueryResultsRegion buildQueryResultsRegion(String string, Properties prprts) throws CacheException {
        return new KademiQueryResultsRegion(string, prprts, null);
    }

    @Override
    public TimestampsRegion buildTimestampsRegion(String string, Properties prprts) throws CacheException {
        return new KademiTimestampsRegion(string, prprts);
    }

}
