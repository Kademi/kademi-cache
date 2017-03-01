package co.kademi.kademi.cache;

import co.kademi.kademi.cache.channel.InvalidateAllMessage;
import co.kademi.kademi.cache.channel.InvalidateItemMessage;
import co.kademi.kademi.channel.Channel;
import co.kademi.kademi.channel.ChannelListener;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

    private final List<BroadcastEventListener2> broadcastEventListeners = new ArrayList<>();


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
                    } else {
                        log.warn("---- CACHE NOT FOUND " + iim.getCacheName() + " -----");
                    }
                } else if( msg instanceof InvalidateAllMessage) {
                    InvalidateAllMessage iam = (InvalidateAllMessage) msg;
                    KademiCacheRegion r = mapOfRegions.get(iam.getCacheName());
                    if( r != null ) {
                        r.removeAll();
                    }
                } else if( msg instanceof BroadcastMessage) {
                    BroadcastMessage m = (BroadcastMessage) msg;
                    for( BroadcastEventListener2 l : broadcastEventListeners) {
                        l.receive(l.topicName, m.key, m.value);
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

    public Channel getChannel() {
        return channel;
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
        if( mapOfRegions.containsKey(regionName)) {
//            throw new CacheException("Duplicate cache region");
            log.info("buildCollectionRegion: return existing cache region: {}", regionName);
            return (CollectionRegion) mapOfRegions.get(regionName);
        } else {
            log.info("buildCollectionRegion: create new cache region {}", regionName);
        }
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

    public Collection<KademiCacheRegion> getRegions() {
        return this.mapOfRegions.values();
    }

    public void broadcast(String topicName, Object key, Object value) {
        BroadcastMessage m = new BroadcastMessage(topicName, (Serializable) key, (Serializable) value);
        channel.sendNotification(m);
    }

    public void registerBroadcastListener(String topicName, BroadcastEventListener l) {
        BroadcastEventListener2 l2 = new BroadcastEventListener2(topicName, l);
        broadcastEventListeners.add(l2);
    }

    private class BroadcastEventListener2{
        private final String topicName;
        private final BroadcastEventListener listener;

        public BroadcastEventListener2(String topicName, BroadcastEventListener listener) {
            this.topicName = topicName;
            this.listener = listener;
        }

        void receive(String topicName, Serializable key, Serializable value) {
            if( topicName.equals(this.topicName)) {
                listener.receive(key, value);
            }
        }

    }


    public static interface BroadcastEventListener {

        void receive(Serializable key, Serializable value);

    }

    public static class BroadcastMessage implements Serializable {
        private final String topicName;
        private final Serializable key;
        private final Serializable value;

        public BroadcastMessage(String topicName,Serializable key, Serializable value) {
            this.topicName = topicName;
            this.key = key;
            this.value = value;
        }

        public String getTopicName() {
            return topicName;
        }

        public Serializable getKey() {
            return key;
        }

        public Serializable getValue() {
            return value;
        }
    }

}
