/*
 * Kademi
 */
package co.kademi.kademi.channel.p2p;

import co.kademi.kademi.channel.Channel;
import co.kademi.kademi.channel.ChannelListener;
import co.kademi.kademi.channel.TcpChannelClient;
import co.kademi.kademi.channel.TcpChannelHub;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This opens a TCP server, that other members of the cluster can connect to
 *
 * It also uses the discovery service to find other members on startup, and
 * connects to their servers
 *
 * @author brad
 */
public class P2PTcpChannel implements Channel {

    private static final Logger log = LoggerFactory.getLogger(P2PTcpChannel.class);

    private final TcpChannelHub server;
    private final List<TcpChannelClient> clients;
    private final List<ChannelListener> channelListeners;
    private final P2PMemberDiscoveryService discoveryService;

    public P2PTcpChannel(int port, P2PMemberDiscoveryService discoveryService, String bindPrefix) throws UnknownHostException, SocketException {
        this.discoveryService = discoveryService;
        InetAddress bindAddress = findBindAddress(bindPrefix);
        this.server = new TcpChannelHub(bindAddress, port, new ChannelListener() {

            @Override
            public void handleNotification(UUID sourceId, Serializable msg) {
                log.info("handleNotification: source={} receiver={} msg class={}", sourceId, server.getBindAddress(), msg.getClass());
                for (ChannelListener l : channelListeners) {
                    l.handleNotification(sourceId, msg);
                }
            }

            @Override
            public void memberRemoved(UUID sourceId) {
                log.info("memberRemoved: {}", sourceId);
            }

            @Override
            public void onConnect(UUID sourceId, InetAddress remoteAddress) {
                log.info("onConnect: sourceId={} remoteAddress={}", sourceId, remoteAddress);
            }
        });
        this.clients = new CopyOnWriteArrayList();
        this.channelListeners = new CopyOnWriteArrayList<>();
    }

    public void start() {
        server.start();

        Collection<InetSocketAddress> peerAddresses = discoveryService.getRegisteredAddresses();

        InetAddress host = server.getBindAddress();
        InetSocketAddress myAddress = new InetSocketAddress(host, server.getPort());

        log.info("Connect to {} peers", peerAddresses.size());
        for (InetSocketAddress peerAddress : peerAddresses) {
            if (!peerAddress.equals(myAddress)) {
                connectToServer(peerAddress);
            }
        }

        // Add me
        discoveryService.registerAddresses(Arrays.asList(myAddress));
    }

    public void stop() {
        server.stop();
        for (TcpChannelClient c : clients) {
            c.stop();
        }
        clients.clear();
    }

    @Override
    public void sendNotification(Serializable msg) {
        for (TcpChannelClient client : clients) {
            client.sendNotification(msg);
        }
    }

    @Override
    public void registerListener(ChannelListener channelListener) {
        channelListeners.add(channelListener);
    }

    @Override
    public void removeListener(ChannelListener channelListener) {
        channelListeners.remove(channelListener);
    }

    private void connectToServer(InetSocketAddress peerAddress) {
        log.info("Connect to {}", peerAddress);
        InetAddress add = peerAddress.getAddress();
        TcpChannelClient c = new TcpChannelClient(add, this.server.getPort(), channelListeners);
        c.start();
        this.clients.add(c);
    }

    private InetAddress findBindAddress(String bindPrefix) throws SocketException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

        while (interfaces.hasMoreElements()) {
            NetworkInterface nic = interfaces.nextElement();
            Enumeration<InetAddress> addresses = nic.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();

                String localIp = address.getHostAddress();
                if (localIp.startsWith(bindPrefix)) {
                    return address;
                }
            }
        }
        return null;
    }

}
