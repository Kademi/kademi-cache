/*
 * Kademi
 */
package co.kademi.kademi.channel.p2p;

import co.kademi.kademi.cache.channel.InvalidateItemMessage;
import co.kademi.kademi.channel.ChannelListener;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.UUID;

/**
 *
 * @author brad
 */
public class LocalChannelRunner {
    public static void main(String[] args) throws UnknownHostException, SocketException, InterruptedException {
        DirectoryFileListService fileListService = new DirectoryFileListService();
        FileListP2PMemberDiscoveryService disco = new FileListP2PMemberDiscoveryService(fileListService);
        int i = (int) (Math.random() * 100) + 9000;
        P2PTcpChannel ch1 = new P2PTcpChannel("chan", i, disco, "127.0");
        ch1.registerListener(new ChannelListener() {

            @Override
            public void handleNotification(UUID sourceId, Serializable msg) {
                System.out.println("notification from " + sourceId + " - msg=" + msg);
            }

            @Override
            public void memberRemoved(UUID sourceId) {
                System.out.println("cluster member removed: " + sourceId);
            }

            @Override
            public void onConnect(UUID sourceId, InetAddress remoteAddress) {
                System.out.println("onconnect");
            }
        });
        ch1.start();

        while (true) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Enter a key to send");
            String d = scanner.next();

            ch1.sendNotification(new InvalidateItemMessage("cache1", d));
            System.out.println("Sent.");
//            System.out.println("sleep..");
//            Thread.sleep(1000);
//            ch2.sendNotification(new InvalidateItemMessage("cache2", "key2"));
            Thread.sleep(1000);
        }
    }
}
