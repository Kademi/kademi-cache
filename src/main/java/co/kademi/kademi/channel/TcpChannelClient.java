package co.kademi.kademi.channel;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang.SerializationUtils;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.FilterEvent;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client sends messages to a hub. It never receives messages. It connects to
 * a single hub.
 *
 * @author brad
 */
public class TcpChannelClient implements LocalAddressAccessor, IoHandler {

    private static final Logger log = LoggerFactory.getLogger(TcpChannelClient.class);

    private static final int MAX_CONNECTION_ATTEMPTS = 3;

    private final InetAddress hubAddress;
    private final int hubPort;
    private final List<ChannelListener> channelListeners;
    private boolean running;
    private ClientMessageFilter filter = null;
    private final Runnable onLostConnection;
    /**
     * Keeps checking for a connection
     */
    private Thread thMonitor;
    private Thread thSender;
    private LinkedBlockingQueue<QueuedMessage> sendQueue = new LinkedBlockingQueue<>();
    private NioSocketConnector connector;
    private IoSession session;
    private Long lastMessageTime;

    private int connectFailedCount;

    public TcpChannelClient(InetAddress hubAddress, int hubPort, List<ChannelListener> channelListeners, Runnable onLostConnection) {
        this.hubAddress = hubAddress;
        this.hubPort = hubPort;
        this.channelListeners = channelListeners;
        this.onLostConnection = onLostConnection;
    }

    public Map<String, Object> getInfo() {
        Map<String, Object> map = new HashMap<>();
        map.put("hubPort", hubPort);
        if( hubAddress != null ) {
            map.put("hubAddress", hubAddress.toString());
        }
        if( lastMessageTime != null ) {
            map.put("lastMessageTime", new Date(lastMessageTime));
        }
        map.put("connectFailedCount", connectFailedCount);
        return map;
    }

    public int getHubPort() {
        return hubPort;
    }

    public InetAddress getHubAddress() {
        return hubAddress;
    }

    public void start() {
        log.warn("start: address={} port={}" + this.hubAddress, hubPort);
        running = true;

        thSender = new Thread(new QueueSender(), "TcpChannelClientSender");
        thSender.setDaemon(true);

        thMonitor = new Thread(new ConnectionMonitor(), "TcpChannelClientMonitor");
        thMonitor.setDaemon(true);

        thSender.start();
        thMonitor.start();
    }

    public void stop() {
        log.warn("stop: " + this.getClass().getCanonicalName());
        running = false;
        thSender.interrupt();
        thMonitor.interrupt();
        disconnect();
    }

    public void sendNotification(Serializable msg) {
//        log.debug( "sendNotification: " + msg.getClass() + " queue: " + sendQueue.size() );
        sendQueue.add(new QueuedMessage(null, msg));
    }
//
//    public void sendNotification(UUID destination, Serializable msg) {
////        log.debug( "sendNotification2: " + msg.getClass() + " queue: " + sendQueue.size() );
//        sendQueue.add(new QueuedMessage(destination, msg));
//    }

    public void registerListener(ChannelListener channelListener) {
        channelListeners.add(channelListener);
    }

    public void removeListener(ChannelListener channelListener) {
        channelListeners.remove(channelListener);
    }

    public ClientMessageFilter getFilter() {
        return filter;
    }

    public void setFilter(ClientMessageFilter filter) {
        this.filter = filter;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if( running ) {
            log.debug("finalize called, but not stopped. Attempt to disconnect..");
            disconnect();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) session.getLocalAddress();
    }

    private synchronized boolean isConnected() {
        return session != null && session.isConnected() && session.getRemoteAddress() != null;
    }

    private synchronized void connect() {
        boolean didConnect = false;
        log.info("attempt to connect to: " + hubAddress + ":" + hubPort);
        try {
            connector = new NioSocketConnector();
            connector.getFilterChain().addLast("codec",
                    new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
            connector.setHandler(this);
            SocketSessionConfig sessionConf = (SocketSessionConfig) connector.getSessionConfig();
            sessionConf.setReuseAddress(true);
            InetSocketAddress add = new InetSocketAddress(hubAddress, hubPort);
            ConnectFuture future = connector.connect(add);
            if( future.await(5000) ) {
                try {
                    session = future.getSession();
                } catch( Exception e ) {
                    session = null;
                    log.warn("Failed to connect to: " + hubAddress + ":" + hubPort);
                    connectFailedCount++;
                }
            } else {
                session = null;
                log.warn("Failed to connect to: " + hubAddress + ":" + hubPort);
                connectFailedCount++;
            }

        } catch( InterruptedException ex ) {
            session = null;
            log.warn("Failed to connect to: " + hubAddress + ":" + hubPort + ". ex: " + ex.toString());
            connectFailedCount++;
        }
        if( didConnect ) {
            try {
                connectFailedCount = 0;
                notifyConnected();
            } catch( Exception e ) {
                log.error("exception in notifyConnected", e);
            }
        }
    }

    private synchronized void disconnect() {
        log.info("disconnect");
        if( session != null ) {
            session.closeNow();
        }
        session = null;
    }

    private void notifyConnected() {
        log.debug("notifyConnected");
        for( ChannelListener l : channelListeners ) {
            try {
                //filter.onConnect(l);
                l.onConnect(null, hubAddress);
            } catch( Exception e ) {
                log.error("Exception in memberRemoved listener: " + l.getClass(), e);
            }
        }
    }

    @Override
    public String toString() {

        String s = "server=" + this.hubAddress + ":" + hubPort + "send-queue-size=" + sizeOf(this.sendQueue) + " connectFailedCount=" + connectFailedCount;
        if( lastMessageTime != null ) {
            long tm = System.currentTimeMillis() - lastMessageTime;
            s += " last message sent: " + (tm / 1000) + "secs ago";
        }
        return s;
    }

    @Override
    public void sessionCreated(IoSession session) throws Exception {

    }

    @Override
    public void sessionOpened(IoSession session) throws Exception {

    }

    @Override
    public void sessionClosed(IoSession session) throws Exception {

    }

    @Override
    public void sessionIdle(IoSession session, IdleStatus status) throws Exception {

    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause) throws Exception {

    }

    @Override
    public void messageReceived(IoSession session, Object message) throws Exception {

    }

    @Override
    public void messageSent(IoSession session, Object message) throws Exception {

    }

    @Override
    public void inputClosed(IoSession is) throws Exception {

    }

    @Override
    public void event(IoSession is, FilterEvent fe) throws Exception {

    }

    private String sizeOf(LinkedBlockingQueue<QueuedMessage> sendQueue) {
        if( sendQueue == null ) {
            return "(null)";
        }
        return sendQueue.size() + "";
    }

    private class ConnectionMonitor implements Runnable {

        @Override
        public void run() {
            try {
                while( running ) {
                    if( !isConnected() ) {
                        connect();

                        if( connectFailedCount > MAX_CONNECTION_ATTEMPTS ) {
                            if( onLostConnection != null ) {
                                running = false;
                                onLostConnection.run();
                            }
                        }
                    } else {
                        log.trace("still connected to {}:{}", hubAddress, hubPort);
                    }
                    if( running ) {
                        Thread.sleep(1000);
                    }
                }
            } catch( InterruptedException ex ) {
                log.warn("connection monitor interrupted");
            }
        }
    }

    private class QueueSender implements Runnable {

        @Override
        public void run() {
            try {
                long tm = 0;
                int cnt = 0;
                while( running ) {
                    if( isConnected() ) {
                        //might be infinite looping here, somehow
                        cnt++;
                        QueuedMessage item = sendQueue.take();
                        consume(item);

                        Thread.yield(); // try to be nice to rest of system, in case this is spamming messages

                        if( cnt > 1000 ) {
                            tm = System.currentTimeMillis() - tm;
                            log.warn("QueueSender: have sent 1000 messages in {}ms", tm);
                            cnt = 0;
                            Thread.sleep(1000); // just in case we're in a tight loop, take a break every 1000 messages for a second
                        }
                    } else {
                        Thread.sleep(5000);
                    }
                }
            } catch( InterruptedException ex ) {
                log.warn("QueueSender: thread finished");
            }
        }

        private void consume(QueuedMessage msg) {
            if( session == null ) {
                log.warn("QueueSender: socket gone");
                sendQueue.add(msg);
            } else {
//                session.write(new InvalidateItemMessage("cache1", "hello world"));
//                Type t = new BigIntegerType();
//                session.write(new InvalidateItemMessage("cache1", new CacheKey(1, t, "ddd", "ddd", null)));
//                Serializable data2 = (Serializable) SerializationUtils.clone(msg.data);
                log.info("Transmit message to: {}", session.getRemoteAddress());
                session.write(msg.data);
                lastMessageTime = System.currentTimeMillis();

            }
        }
    }

    private class QueuedMessage {

        UUID dest;
        byte[] data;

        public QueuedMessage(UUID dest, Serializable data) {
            this.dest = dest;
            this.data = SerializationUtils.serialize(data);
        }
    }
}
