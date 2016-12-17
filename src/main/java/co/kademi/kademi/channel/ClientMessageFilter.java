package co.kademi.kademi.channel;

import java.io.Serializable;
import java.util.UUID;

/**
 *
 * @author brad
 */
public interface ClientMessageFilter {

    void handleNotification( ChannelListener listener, UUID sourceId, Serializable msg );

    void memberRemoved(ChannelListener listener, UUID sourceId);

    /**
     * Called when we get a connection to the hub
     */
    void onConnect(ChannelListener listener);
}
