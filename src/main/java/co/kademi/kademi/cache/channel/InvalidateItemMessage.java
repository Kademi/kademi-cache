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

    public InvalidateItemMessage() {
        this.cacheName = null;
        this.key = null;
    }

    public InvalidateItemMessage(String cacheName, String key) {
        this.cacheName = cacheName;
        this.key = key;
    }

    public String getCacheName() {
        return cacheName;
    }

    public Serializable getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "Invalidate: " + cacheName + "/" + key;
    }


}
