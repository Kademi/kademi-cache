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
import java.util.Iterator;
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
public final class P2PTcpChannel implements Channel {

    private static final Logger log = LoggerFactory.getLogger(P2PTcpChannel.class);

    public static final int MAX_REGISTRATION_ATTEMPTS = 3;

    private final String name;
    private final TcpChannelHub server;
    private final List<TcpChannelClient> clients;
    private final List<ChannelListener> channelListeners;
    private final P2PMemberDiscoveryService discoveryService;
    private final InetAddress bindAddress;
    private final int serverPort;
    private final String bindPrefix;

    private final String registerAddress;
    private InetSocketAddress myAddress;    // this is determined during start


    /**
     * Use this constructor when the address to bind to is different from the
     * address to register. Eg when using port forwarding from the host to
     * docker
     *
     * @param name
     * @param port
     * @param discoveryService
     * @param bindAddress - a specific IP address to bind to, eg 0.0.0.0
     * @param registerAddress - a specific IP address to register for inbound
     * cluster connections
     * @throws UnknownHostException
     * @throws SocketException
     */
    public P2PTcpChannel(String name, int port, P2PMemberDiscoveryService discoveryService, String sBindAddress, String registerAddress) throws UnknownHostException, SocketException {
        this.name = name;
        this.serverPort = port;
        this.bindPrefix = null;
        this.discoveryService = discoveryService;
        bindAddress = InetAddress.getByName(sBindAddress);
        this.registerAddress = registerAddress;
        this.server = new TcpChannelHub(bindAddress, port, new ChannelListener() {

            @Override
            public void handleNotification(UUID sourceId, Serializable msg) {
                //log.info("handleNotification: source={} receiver={} msg class={}", sourceId, server.getBindAddress(), msg.getClass());
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
                connectToServers();
            }
        });
        this.clients = new CopyOnWriteArrayList();
        this.channelListeners = new CopyOnWriteArrayList<>();
        Channel.register(this);
    }

    public P2PTcpChannel(String name, int port, P2PMemberDiscoveryService discoveryService, String bindPrefix) throws UnknownHostException, SocketException {
        this.name = name;
        this.serverPort = port;
        this.bindPrefix = bindPrefix;
        this.discoveryService = discoveryService;
        bindAddress = findBindAddress(bindPrefix);
        this.server = new TcpChannelHub(bindAddress, port, new ChannelListener() {

            @Override
            public void handleNotification(UUID sourceId, Serializable msg) {
                //log.info("handleNotification: source={} receiver={} msg class={}", sourceId, server.getBindAddress(), msg.getClass());
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
                connectToServers();
            }
        });
        this.clients = new CopyOnWriteArrayList();
        this.channelListeners = new CopyOnWriteArrayList<>();
        this.registerAddress = null;
        Channel.register(this);
    }

    public void start() throws UnknownHostException {
        log.info("channel-start");
        server.start();

        InetAddress hostAddressToUse;
        if (registerAddress != null) {
            hostAddressToUse = InetAddress.getByName(registerAddress);
        } else {
            hostAddressToUse = server.getBindAddress();
        }
        myAddress = new InetSocketAddress(hostAddressToUse, server.getPort());

        // Add me
        log.info("channel-start: register my address={}", myAddress);
        discoveryService.registerAddresses(Arrays.asList(myAddress));

        connectToServers();
    }

    public void connectToServers() {
        InetAddress host = server.getBindAddress();
        myAddress = new InetSocketAddress(host, server.getPort());

        // Add me to make sure is available to other servers
        int attempts = 0;
        while (attempts < MAX_REGISTRATION_ATTEMPTS) {
            attempts++;
            try {
                log.info("channel-start: register my address={}, attempt {} of {}", myAddress, attempts, MAX_REGISTRATION_ATTEMPTS);
                discoveryService.registerAddresses(Arrays.asList(myAddress));
                log.info("channel-start: registration succeeded");
                break;
            } catch (Exception ex) {
                log.info("channel-start: exception occuring in registration {}", ex);
            }
        }

        log.info("Check for connections to servers. My address={}", myAddress);
        Collection<InetSocketAddress> peerAddresses = discoveryService.getRegisteredAddresses();
        log.info("Connect to {} peers", peerAddresses.size());
        for (InetSocketAddress peerAddress : peerAddresses) {
            if (!peerAddress.equals(myAddress)) {
                if (!hasClient(peerAddress)) {
                    log.info("Not connected to {}, attempt to connect..", peerAddress);
                    connectToServer(peerAddress);
                } else {
                    log.info(".. already connected to {}", peerAddress);
                }
            }
        }
    }

    public void stop() {
        if (server != null) {
            server.stop();
        }
        if (clients != null) {
            for (TcpChannelClient c : clients) {
                c.stop();
            }
            clients.clear();
        }
    }

    @Override
    public void sendNotification(Serializable msg) {
        for (TcpChannelClient client : clients) {
            log.info("sendNotification: to={} msg={}", client, msg);
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
        TcpChannelClient c = new TcpChannelClient(add, peerAddress.getPort(), channelListeners, () -> {
            log.info("Lost connection to {}", peerAddress);
            this.discoveryService.unregisterAddresses(Arrays.asList(peerAddress));

            removeClient(peerAddress);
        });
        this.clients.add(c);
        c.start();
    }

    /**
     * True if we already have a client for the given peer address
     *
     * @param peerAddress
     * @return
     */
    private boolean hasClient(InetSocketAddress peerAddress) {
        for (TcpChannelClient c : this.clients) {
            if (c.getHubPort() == peerAddress.getPort()) {
                if (c.getHubAddress().equals(peerAddress.getAddress())) {
                    return true;
                }
            }
        }
        return false;
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

    @Override
    public String getName() {
        return name;
    }

    public InetAddress getBindAddress() {
        return bindAddress;
    }

    public InetSocketAddress getMyAddress() {
        return myAddress;
    }

    public int getServerPort() {
        return serverPort;
    }

    public String getBindPrefix() {
        return bindPrefix;
    }

    public List<TcpChannelClient> getClients() {
        return clients;
    }

    private void removeClient(InetSocketAddress peerAddress) {
        log.info("Remove connection: {}", peerAddress);

        TcpChannelClient clientToRemove = null;
        Iterator<TcpChannelClient> it = clients.iterator();
        while (it.hasNext()) {
            TcpChannelClient c = it.next();
            if (c.getHubPort() == peerAddress.getPort()) {
                if (c.getHubAddress().equals(peerAddress.getAddress())) {
                    clientToRemove = c;
                    break;
                }
            }
        }
        if (clientToRemove != null) {
            clients.remove(clientToRemove);
        }
    }

    public P2PMemberDiscoveryService getDiscoveryService() {
        return discoveryService;
    }

    public TcpChannelHub getServer() {
        return server;
    }
}
