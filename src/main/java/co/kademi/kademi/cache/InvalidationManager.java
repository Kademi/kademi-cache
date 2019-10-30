/*
 * Kademi
 */
package co.kademi.kademi.cache;

import co.kademi.kademi.cache.channel.InvalidateItemMessage;
import co.kademi.kademi.channel.Channel;
import com.google.common.cache.Cache;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.Transaction;

/**
 *
 * @author brad
 */
public class InvalidationManager {

    private final Channel channel;
    private final ThreadLocal<List<InvalidationAction>> tlInvalidationActionsList = new ThreadLocal();

    public InvalidationManager(Channel channel) {
        this.channel = channel;
    }

    private List<InvalidationAction> enqueuedInvalidations(boolean autocreate) {
        List<InvalidationAction> list = tlInvalidationActionsList.get();
        if (list == null && autocreate) {
            list = new ArrayList<>();
            tlInvalidationActionsList.set(list);
        }
        return list;
    }

    public void enqueueInvalidation(String cacheName, Cache<String, Object> cache, String key) {
        List<InvalidationAction> list = enqueuedInvalidations(true);
        InvalidationAction ia = new InvalidationAction(cacheName, cache, key);
        list.add(ia);
    }

    public void onCommit(Transaction tx) {
        List<InvalidationAction> list = enqueuedInvalidations(false);
        tlInvalidationActionsList.remove();
        if (list != null) {
            for (InvalidationAction ia : list) {
                doInvalidation(ia);
            }
        }
    }

    public void onRollback(Transaction tx) {
        tlInvalidationActionsList.remove();
    }

    private void doInvalidation(InvalidationAction ia) {
        ia.cache.invalidate(ia.key);
        if (channel != null) {
            InvalidateItemMessage m = new InvalidateItemMessage(ia.cacheName, ia.key);
            channel.sendNotification(m);
        }
    }

    private class InvalidationAction {

        private final String cacheName;
        private final Cache<String, Object> cache;
        private final String key;

        public InvalidationAction(String cacheName, Cache<String, Object> cache, String key) {
            this.cache = cache;
            this.key = key;
            this.cacheName = cacheName;
        }

    }
}
