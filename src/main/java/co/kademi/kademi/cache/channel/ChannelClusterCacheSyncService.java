/*
 * Kademi
 */
package co.kademi.kademi.cache.channel;

import co.kademi.kademi.cache.ClusterCacheSyncService;
import co.kademi.kademi.channel.Channel;
import java.io.Serializable;

/**
 *
 * @author brad
 */
public class ChannelClusterCacheSyncService implements ClusterCacheSyncService{

    private final Channel channel;

    public ChannelClusterCacheSyncService() {
        this.channel = null;
    }

    public ChannelClusterCacheSyncService(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void invalidate(String cacheName, Object key) {
        InvalidateItemMessage msg = new InvalidateItemMessage(cacheName, (Serializable) key);
        channel.sendNotification(msg);
    }
}
