package co.kademi.kademi.cache;

import co.kademi.kademi.cache.channel.InvalidateAllMessage;
import co.kademi.kademi.cache.channel.InvalidateItemMessage;
import co.kademi.kademi.channel.Channel;
import co.kademi.kademi.channel.ChannelListener;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class KademiRegionFactory implements RegionFactory {

    private static final Logger log = LoggerFactory.getLogger(KademiRegionFactory.class);

    private Properties props;
    private Channel channel;
    private Map<String,KademiCacheRegion> mapOfRegions;

    @Override
    public void start(Settings stngs, Properties prprts) throws CacheException {
        this.props = prprts;
        this.mapOfRegions = new ConcurrentHashMap<>();

        String channelName = (String) props.get("hibernate.cache.provider_name");
        this.channel = Channel.get(channelName);
        channel.registerListener(new ChannelListener() {

            @Override
            public void handleNotification(UUID sourceId, Serializable msg) {
                if( msg instanceof InvalidateItemMessage) {
                    InvalidateItemMessage iim = (InvalidateItemMessage) msg;
                    //lookup cache, and remove item. do not call invalidate otherwise will recur
                    KademiCacheRegion r = mapOfRegions.get(iim.getCacheName());
                    if( r != null ) {
                        r.remove(iim.getKey());
                    }
                } else if( msg instanceof InvalidateAllMessage) {
                    InvalidateAllMessage iam = (InvalidateAllMessage) msg;
                    KademiCacheRegion r = mapOfRegions.get(iim.getCacheName());
                    if( r != null ) {
                        r.removeAll();
                    }
                }
            }

            @Override
            public void memberRemoved(UUID sourceId) {
                log.info("memberRemoved: {}", sourceId);
            }

            @Override
            public void onConnect(UUID sourceId, InetAddress remoteAddress) {
                log.info("onConnect: remote={}", remoteAddress);
            }
        });
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
    public EntityRegion buildEntityRegion(String regionName, Properties prprts, CacheDataDescription cdd) throws CacheException {
        KademiEntityRegion r = new KademiEntityRegion(regionName, channel, prprts, cdd);
        mapOfRegions.put(regionName, r);
        return r;
    }

    @Override
    public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties prprts, CacheDataDescription cdd) throws CacheException {
        KademiNaturalIdRegion r = new KademiNaturalIdRegion(regionName, channel, prprts, cdd);
        mapOfRegions.put(regionName, r);
        return r;
    }

    @Override
    public CollectionRegion buildCollectionRegion(String regionName, Properties prprts, CacheDataDescription cdd) throws CacheException {
        KademiCollectionRegion r = new KademiCollectionRegion(regionName, channel, prprts, cdd);
        mapOfRegions.put(regionName, r);
        return r;
    }

    @Override
    public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties prprts) throws CacheException {
        KademiQueryResultsRegion r = new KademiQueryResultsRegion(regionName, channel, prprts, null);
        mapOfRegions.put(regionName, r);
        return r;
    }

    @Override
    public TimestampsRegion buildTimestampsRegion(String regionName, Properties prprts) throws CacheException {
        KademiTimestampsRegion r = new KademiTimestampsRegion(regionName, channel, prprts);
        mapOfRegions.put(regionName, r);
        return r;
    }

}
