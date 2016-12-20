/*
 * Kademi
 */
package co.kademi.kademi.channel.map;

import co.kademi.kademi.channel.Channel;
import co.kademi.kademi.channel.ChannelListener;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author brad
 */
public class DistributedMap<K,V> implements Map<K,V> {

    private final Channel channel;
    private final String name;
    private Map<K,V> map = new ConcurrentHashMap<>();

    public DistributedMap(String name, Channel channel) {
        this.name = name;
        this.channel = channel;
        this.channel.registerListener(new ChannelListener() {

            @Override
            public void handleNotification(UUID sourceId, Serializable msg) {
                if( msg instanceof AbstractMapMsg ) {
                    AbstractMapMsg m = (AbstractMapMsg) msg;
                    m.doIt(map);
                }
            }

            @Override
            public void memberRemoved(UUID sourceId) {

            }

            @Override
            public void onConnect(UUID sourceId, InetAddress remoteAddress) {

            }
        });
    }



    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        AddItemMsg m = new AddItemMsg(name, key, value);
        channel.sendNotification(m);
        return map.put(key, value);
    }

    @Override
    public V remove(Object key) {
        RemoveItemMsg m = new RemoveItemMsg(name, key.toString());
        channel.sendNotification(m);
        return map.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void clear() {
        RemoveAllMsg m = new RemoveAllMsg(name);
        channel.sendNotification(m);

        map.clear();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    public static abstract class AbstractMapMsg implements Serializable {
        private final String mapName;

        public abstract void doIt(Map map);

        public AbstractMapMsg(String mapName) {
            this.mapName = mapName;
        }

        public String getMapName() {
            return mapName;
        }


    }

    public static class AddItemMsg<K,V> extends AbstractMapMsg {
        private final K key;
        private final V value;

        public AddItemMsg(String mapName, K key, V value) {
            super(mapName);
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public void doIt(Map map) {
            map.put(key, value);
        }

    }

    public static class RemoveItemMsg<K> extends AbstractMapMsg {
        private final K key;

        public RemoveItemMsg(String mapName, K key) {
            super(mapName);
            this.key = key;
        }

        public K getKey() {
            return key;
        }

        @Override
        public void doIt(Map map) {
            map.remove(key);
        }
    }

    public static class RemoveAllMsg<K> extends AbstractMapMsg {

        public RemoveAllMsg(String mapName) {
            super(mapName);
        }

        @Override
        public void doIt(Map map) {
            map.clear();
        }

    }

}
