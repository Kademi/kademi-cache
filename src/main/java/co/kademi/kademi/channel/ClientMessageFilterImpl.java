package co.kademi.kademi.channel;

import java.io.Serializable;
import java.util.UUID;

/**
 *
 * @author brad
 */
public class ClientMessageFilterImpl implements ClientMessageFilter {

    public void handleNotification( ChannelListener listener, UUID sourceId, Serializable msg ) {
        listener.handleNotification( sourceId, msg );
    }

    public void memberRemoved( ChannelListener listener, UUID sourceId ) {
        listener.memberRemoved( sourceId );
    }

    public void onConnect( ChannelListener listener ) {
        listener.onConnect();
    }

}
