/*
 * Kademi
 */
package co.kademi.kademi.channel.p2p;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 *
 * @author brad
 */
public interface P2PMemberDiscoveryService {
    Collection<InetSocketAddress> getRegisteredAddresses();

    void registerAddresses(Collection<InetSocketAddress> addrs);

    void unregisterAddresses(Collection<InetSocketAddress> addrs);
}
