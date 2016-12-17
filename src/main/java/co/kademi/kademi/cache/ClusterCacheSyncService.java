/*
 * Kademi
 */
package co.kademi.kademi.cache;

/**
 *
 * @author brad
 */
public interface ClusterCacheSyncService {
    void invalidate(String cacheName, Object key);
}
