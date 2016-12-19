package co.kademi.kademi.channel;

import com.sun.xml.internal.ws.developer.Serialization;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang.SerializationUtils;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A hub receives messages from multiple clients. It never sends to clients,
 * and it never connects to clients.
 *
 * When a message is received it is passed on to the channel listeners
 *
 * @author brad
 */
public class TcpChannelHub implements Service {

    private static final Logger log = LoggerFactory.getLogger(TcpChannelHub.class);
    private final InetAddress bindAddress;
    private final int port;
    private final List<Client> clients;
    private final ChannelListener channelListener;
    private boolean started;
    private IoAcceptor acceptor;
    private Thread thSender;
    private LinkedBlockingQueue<ReceivedMessage> sendQueue;

    public TcpChannelHub(InetAddress bindAddress, int port, ChannelListener channelListener) throws UnknownHostException {
        this.port = port;
        this.bindAddress = bindAddress;
        this.clients = new CopyOnWriteArrayList<>();
        this.channelListener = channelListener;
    }

    public void start() {
        if (started) {
            throw new IllegalStateException("already started");
        }
        started = true;

        sendQueue = new LinkedBlockingQueue();
        try {
            acceptor = new NioSocketAcceptor();
            acceptor.getFilterChain().addLast("protocol", new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
            acceptor.getSessionConfig().setReadBufferSize(2048 * 8);
            acceptor.setHandler(new ChannelServerHandler());
            InetSocketAddress add = new InetSocketAddress(bindAddress, port);
            acceptor.bind(add);

        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private class ChannelServerHandler extends IoHandlerAdapter {

        @Override
        public void messageReceived(IoSession session, Object message) throws Exception {
            Client c = (Client) session.getAttribute("client");
            byte[] data = (byte[]) message;
            Serializable msgObject = (Serializable) SerializationUtils.deserialize(data);
            log.info("messageReceived: from client {} msgClass={}", c, msgObject.getClass());
            channelListener.handleNotification(null, msgObject);
        }

        @Override
        public void sessionClosed(IoSession session) throws Exception {
            Client c = (Client) session.getAttribute("client");
            if (c != null) {
                c.stop();
            }
        }

        @Override
        public void sessionOpened(IoSession session) throws Exception {
            Client client = new Client(session);
            clients.add(client);
            log.info("added new client: " + client);
            InetSocketAddress remoteAdd = (InetSocketAddress) client.session.getRemoteAddress();
            channelListener.onConnect(client.id, remoteAdd.getAddress());

        }

    }

    public void stop() {
        started = false;
        sendQueue.clear();
        acceptor.unbind();

        thSender.interrupt();
    }

    public int getPort() {
        return port;
    }

    public InetAddress getBindAddress() {
        return bindAddress;
    }

    public List<String> getClients() {
        List<String> list = new ArrayList<>();
        for (Client c : clients) {
            list.add(c.toString());
        }
        return list;
    }


    private class Client {

        private final UUID id;
        private final IoSession session;
        private boolean stopped;

        public Client(IoSession session) throws IOException {
            this.id = UUID.randomUUID();
            this.session = session;
            clients.add(this);
            session.setAttribute("client", this);
        }

        public boolean onData(Serializable message) throws IOException, BufferUnderflowException, ClosedChannelException {
            if (stopped) {
                log.info("discarding message because state is stopped");
                return true;
            }
            log.info("onData: Hub received", message);
            try {
                channelListener.handleNotification(id, message);
                return true;
            } catch (BufferUnderflowException e) {
                return true;
            }

        }

        /**
         * Stop, and notify peers
         */
        public void stop() {
            log.debug("stop: " + session.getRemoteAddress());
            clients.remove(this);
            stopped = true;
        }

        @Override
        public String toString() {
            return "Client: " + session.getRemoteAddress();
        }
    }
}
