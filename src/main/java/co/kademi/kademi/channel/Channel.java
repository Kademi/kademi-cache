package co.kademi.kademi.channel;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author brad
 */
public interface Channel extends Service {

    static Map<String,Channel> mapOfChannels = new ConcurrentHashMap<>();

    public static void register(Channel channel) {
        mapOfChannels.put(channel.getName(), channel);
    }

    public static Channel get(String name) {
        return mapOfChannels.get(name);
    }

    String getName();

    /**
     * Broadcast a message to the cluster
     *
     * @param msg
     */
    void sendNotification( Serializable msg );

    void registerListener( ChannelListener channelListener );

    void removeListener( ChannelListener channelListener );

}
