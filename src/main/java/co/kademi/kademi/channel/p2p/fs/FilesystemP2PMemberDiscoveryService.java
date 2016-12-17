/*
 * Kademi
 */
package co.kademi.kademi.channel.p2p.fs;

import co.kademi.kademi.channel.p2p.P2PMemberDiscoveryService;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class FilesystemP2PMemberDiscoveryService implements P2PMemberDiscoveryService {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(FilesystemP2PMemberDiscoveryService.class);

    private final File dir;

    public FilesystemP2PMemberDiscoveryService() {
        this.dir = new File(System.getProperty("java.io.tmpdir"));
    }

    public FilesystemP2PMemberDiscoveryService(File dir) {
        this.dir = dir;
    }

    @Override
    public Collection<InetSocketAddress> getRegisteredAddresses() {
        File fdata = new File(dir, "addresses");
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(fdata)) {
            props.load(in);
            List<InetSocketAddress> list = new ArrayList<>();
            for (Object key : props.keySet()) {
                String hostName = key.toString();
                String v = props.getProperty(key.toString());
                Integer port = Integer.parseInt(v);
                InetSocketAddress add = new InetSocketAddress(hostName, port);
                list.add(add);
            }
            return list;
        } catch (IOException ex) {
            log.info("file not found:", ex);
            return null;
        }
    }

    @Override
    public void registerAddresses(Collection<InetSocketAddress> addrs) {
        File fdata = new File(dir, "addresses");
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(fdata)) {
            props.load(in);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        for (InetSocketAddress add : addrs) {
            props.setProperty(add.getHostName(), add.getPort() + "");
        }

        try (OutputStream out = new FileOutputStream(fdata)) {
            props.store(out, null);
        } catch (IOException ex) {
            log.info("file not found:", ex);
        }

    }

    @Override
    public void unregisterAddresses(Collection<InetSocketAddress> addrs) {
        File fdata = new File(dir, "addresses");
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(fdata)) {
            props.load(in);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        for (InetSocketAddress add : addrs) {
            props.remove(add.getHostName());
        }

        try (OutputStream out = new FileOutputStream(fdata)) {
            props.store(out, null);
        } catch (IOException ex) {
            log.info("file not found:", ex);
        }

    }

}
