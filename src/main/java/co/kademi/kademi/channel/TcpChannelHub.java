package co.kademi.kademi.channel;

import co.kademi.kademi.channel.TcpObjectCodec.IdAndArray;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xsocket.MaxReadSizeExceededException;
import org.xsocket.connection.ConnectionUtils;
import org.xsocket.connection.IConnectHandler;
import org.xsocket.connection.IDataHandler;
import org.xsocket.connection.IDisconnectHandler;
import org.xsocket.connection.INonBlockingConnection;
import org.xsocket.connection.IServer;
import org.xsocket.connection.Server;
import org.xsocket.datagram.IEndpoint;

/**
 *
 * @author brad
 */
public class TcpChannelHub implements Service {

    private static final Logger log = LoggerFactory.getLogger( TcpChannelHub.class );
    private final InetAddress bindAddress;
    private final int port;
    private final List<Client> clients;
    private final TcpObjectCodec codec = new TcpObjectCodec();
    private final ChannelListener channelListener;
    private boolean stopped;
    private boolean started;
    private IServer server;
    private Thread thSender;
    private Thread thClientTester;
    private LinkedBlockingQueue<ReceivedMessage> sendQueue;
    private int maxMessageSizeBytes = 100000; // 100k max message size

    public TcpChannelHub( String bindAddress, int port, ChannelListener channelListener ) throws UnknownHostException {
        this.port = port;
        this.bindAddress = InetAddress.getByName( bindAddress );
        this.clients = new CopyOnWriteArrayList<>();
        this.channelListener = channelListener;
    }

    public void start() {
        if( started ) throw new IllegalStateException( "already started" );
        started = true;

        sendQueue = new LinkedBlockingQueue();
        try {
            // This is needed for clean shutdown
            Map<String, Object> options = new HashMap<>();
            options.put( "SO_REUSEADDR", true );
            options.put( IEndpoint.SO_REUSEADDR, true );

            if( bindAddress == null ) {
                server = new Server( port, options, new ConnectHandler() );
            } else {
                server = new Server( bindAddress, port, options, new ConnectHandler(), null, false );
            }
        } catch( UnknownHostException ex ) {
            throw new RuntimeException( ex );
        } catch( IOException ex ) {
            throw new RuntimeException( ex );
        }
        try {
            ConnectionUtils.start( server );
            log.info( "hub server started ok" );
        } catch( SocketTimeoutException ex ) {
            throw new RuntimeException( ex );
        }

        thSender = new Thread( new QueueSender(), "TcpChannelHubQueueSender" );
        thSender.setDaemon( true );
        thSender.start();

        thClientTester = new Thread( new ClientTester(), "TcpChannelHubClientTester" );
        thClientTester.setDaemon( true );
        thClientTester.start();
    }

    public void stop() {
        stopped = true;
        started = false;
        sendQueue.clear();
        try {
            server.close();
        } catch( IOException ex ) {
            log.warn( "closing socket", ex );
        }
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
        for( Client c : clients ) {
            list.add( c.toString() );
        }
        return list;
    }

    public int getMaxMessageSizeBytes() {
        return maxMessageSizeBytes;
    }

    public void setMaxMessageSizeBytes( int maxMessageSizeBytes ) {
        this.maxMessageSizeBytes = maxMessageSizeBytes;
    }

    private class ConnectHandler implements IConnectHandler, IDisconnectHandler {

        @Override
        public boolean onConnect( INonBlockingConnection connection ) throws IOException {
            Client client = new Client( connection );
            log.debug( "added new client: " + client + " on id: " + client.id );

            return true;
        }

        @Override
        public boolean onDisconnect( INonBlockingConnection connection ) throws IOException {
            log.warn( "DISCONNECTED" );
            return true;
        }
    }

    private void notifyMemberRemoved( UUID id ) {
        log.debug( "notifyMemberRemoved: " + id );
        MemberRemoved mr = new MemberRemoved();
        byte[] arr = codec.encodeToBytes( mr );

        ReceivedMessage m = new ReceivedMessage( null, id, arr );
        sendQueue.add( m );
    }

    private class QueueSender implements Runnable {

        public void run() {
            try {
                while( started ) {
                    consume( sendQueue.take() );
                }
            } catch( InterruptedException ex ) {
            }
        }

        private void consume( ReceivedMessage msg ) {
            for( Client c : clients ) {
                if( !c.id.equals( msg.source ) ) {
                    c.send( msg );
                }
            }
        }
    }

    private class Client implements IDataHandler {

        private final UUID id;
        private final INonBlockingConnection sconn;
        private boolean stopped;

        public Client( INonBlockingConnection sconn ) throws IOException {
            this.id = UUID.randomUUID();
            this.sconn = sconn;
            clients.add( this );
            sconn.setHandler( this );
        }

        @Override
        public boolean onData( INonBlockingConnection con ) throws IOException, BufferUnderflowException, ClosedChannelException, MaxReadSizeExceededException {
            if( stopped ) {
                log.info( "discarding message because state is stopped" );
                return true;
            }
            try {
                TcpObjectCodec.IdAndObject iao = codec.decode( con );
                channelListener.handleNotification(id, iao.data);
                return true;
            } catch( BufferUnderflowException e ) {
                return true;
            } catch (ClassNotFoundException ex) {
                log.error("Could not process message", ex);
                return true;
            }

        }

        /**
         * Stop, and notify peers
         */
        public void stop() {
            log.debug( "stop: " + sconn.getRemoteAddress() );
            clients.remove( this );
            stopped = true;
            notifyMemberRemoved( this.id );
        }

        /**
         * Send to this client
         *
         * @param data
         */
        public synchronized void send( ReceivedMessage msg ) {
            try {
                codec.encodeHubToClient( msg.source, sconn, msg.data );
            } catch( IOException ex ) {
                log.warn( "exception sending data to client, disconnecting" );
                stop();
            }
        }

        @Override
        public String toString() {
            return "Client: " + sconn.getRemoteAddress();
        }
    }

    private class ClientTester implements Runnable {

        byte[] testMessage;

        public ClientTester() {
            testMessage = codec.encodeToBytes( new AreYouThere() );
        }

        @Override
        public void run() {
            try {
                while( !stopped ) {
                    //log.debug( "test connected clients");
                    List<Client> list = Collections.unmodifiableList( clients );
                    for( Client client : list ) {
                        try {
                            //log.debug( "test: " + client.id);
                            codec.encodeBytes( client.sconn, null, testMessage );
                        } catch( IOException ex ) {
                            // if send fails disconnect the client
                            client.stop();
                        }
                    }
                    Thread.sleep( 1000 );
                }
            } catch( InterruptedException ex ) {
                log.warn( "ClientTester interrupted" );
            }
            log.warn( "ClientTester thread has finished" );
        }
    }
}
