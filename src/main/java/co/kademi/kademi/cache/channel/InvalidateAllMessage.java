/*
 * Kademi
 */
package co.kademi.kademi.cache.channel;

import java.io.Serializable;

/**
 *
 * @author brad
 */
public class InvalidateAllMessage implements Serializable {
    private final String cacheName;


    public InvalidateAllMessage(String cacheName) {
        this.cacheName = cacheName;
    }

    public String getCacheName() {
        return cacheName;
    }


    @Override
    public String toString() {
        return "InvalidateAll: " + cacheName;
    }


}
