/*
 * Kademi
 */
package co.kademi.kademi.channel.p2p;

import co.kademi.kademi.cache.channel.InvalidateItemMessage;
import java.net.SocketException;
import java.net.UnknownHostException;
import org.junit.Assert;
import org.junit.Before;

/**
 *
 * @author brad
 */
public class P2PTcpChannelTest {

    DirectoryFileListService fileListService = new DirectoryFileListService();
    FileListP2PMemberDiscoveryService disco;

    @Before
    public void init() {
        fileListService = new DirectoryFileListService();
        fileListService.clear();
        disco = new FileListP2PMemberDiscoveryService(fileListService);
    }

    @org.junit.Test
    public void testSomeMethod() throws UnknownHostException, InterruptedException, SocketException {
        P2PTcpChannel ch1 = new P2PTcpChannel("chan", 9010, disco, "127.0");
        P2PTcpChannel ch2 = new P2PTcpChannel("chan", 9020, disco, "127.0");

        ch1.start();
        ch2.start();
        Thread.sleep(300);
        System.out.println("clients: " + ch1.getClients().size() + " - " + ch2.getClients().size());
        Assert.assertEquals(1, ch1.getClients().size());
        Assert.assertEquals(1, ch2.getClients().size());

//        while (true) {
            ch1.sendNotification(new InvalidateItemMessage("cache1", "key1"));
            System.out.println("sleep..");
            Thread.sleep(1000);
            ch2.sendNotification(new InvalidateItemMessage("cache2", "key2"));
            Thread.sleep(1000);
//        }
    }

}
