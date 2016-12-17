package co.kademi.kademi.channel;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.UUID;

/**
 *
 * @author brad
 */
public interface Channel extends Service {

    /**
     * Broadcast a message to the cluster
     *
     * @param msg
     */
    void sendNotification( Serializable msg );

    void registerListener( ChannelListener channelListener );

    void removeListener( ChannelListener channelListener );

}
