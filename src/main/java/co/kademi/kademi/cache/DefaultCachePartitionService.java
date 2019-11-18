/*
 * Kademi
 */
package co.kademi.kademi.cache;

import java.io.Serializable;

/**
 * Just uses a single partition.
 *
 *
 * @author brad
 */
public class DefaultCachePartitionService implements CachePartitionService {

    private final ThreadLocal<Serializable> tlCurrentPartition = new ThreadLocal<>();

    @Override
    public Serializable currentPartitionKey(Object changed) {
        Serializable id = tlCurrentPartition.get();
        if (id != null) {
            return id;
        }
        return null;
    }

}
