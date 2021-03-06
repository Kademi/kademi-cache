package co.kademi.kademi.channel;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.UUID;

/**
 *
 * @author brad
 */
public interface ChannelListener {

    /**
     * Called on receipt of a message from the channel
     *
     * @param sourceId
     * @param msg
     */
    void handleNotification( UUID sourceId, Serializable msg );

    void memberRemoved(UUID sourceId);

    /**
     * Called when we get a connection to the hub
     * @param sourceId
     * @param remoteAddress
     */
    void onConnect(UUID sourceId, InetAddress remoteAddress);

}
