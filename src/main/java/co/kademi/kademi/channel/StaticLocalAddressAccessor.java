package co.kademi.kademi.channel;

import java.net.InetSocketAddress;

/**
 *
 * @author brad
 */
public class StaticLocalAddressAccessor implements LocalAddressAccessor{

    private final InetSocketAddress address;

    public StaticLocalAddressAccessor( InetSocketAddress address ) {
        this.address = address;
    }


    @Override
    public InetSocketAddress getLocalAddress() {
        return address;
    }

}
