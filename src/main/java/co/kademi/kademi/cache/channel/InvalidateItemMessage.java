/*
 * Kademi
 */
package co.kademi.kademi.cache.channel;

import java.io.Serializable;

/**
 *
 * @author brad
 */
public class InvalidateItemMessage implements Serializable {
    private final String cacheName;
    private final String key;
    private final Serializable partitionId;

    public InvalidateItemMessage() {
        this.cacheName = null;
        this.key = null;
        this.partitionId = null;
    }

    public InvalidateItemMessage(String cacheName, String key, Serializable partitionId) {
        this.cacheName = cacheName;
        this.key = key;
        this.partitionId = partitionId;
    }

    public String getCacheName() {
        return cacheName;
    }

    public Serializable getKey() {
        return key;
    }

    public Serializable getPartitionId() {
        return partitionId;
    }



    @Override
    public String toString() {
        return "Invalidate: cache=" + cacheName + "; key=" + key + "; partition=" + partitionId;
    }


}
