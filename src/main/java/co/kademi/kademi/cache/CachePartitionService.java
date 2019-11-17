/*
 * Kademi
 */
package co.kademi.kademi.cache;

import java.io.Serializable;

/**
 * Allows determining the logical partition for data changes
 *
 * @author brad
 */
public interface CachePartitionService {
    /**
     * Return an immutable identifier for the current partition (ie tenant).
     *
     * @param changed - an optional data object, may be null
     * @return
     */
    Serializable currentPartitionKey(Object changed);

}
