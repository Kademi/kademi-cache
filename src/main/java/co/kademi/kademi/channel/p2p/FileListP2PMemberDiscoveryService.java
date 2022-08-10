/*
 * Kademi
 */
package co.kademi.kademi.channel.p2p;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class FileListP2PMemberDiscoveryService implements P2PMemberDiscoveryService {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(FileListP2PMemberDiscoveryService.class);
    public static final String DELIM = "#";

    private final FileListService fileListService;

    public FileListP2PMemberDiscoveryService(FileListService fileListService) {
        this.fileListService = fileListService;
    }

    @Override
    public Collection<InetSocketAddress> getRegisteredAddresses() {
        List<InetSocketAddress> list = new ArrayList<>();
        for (String key : fileListService.getFileList()) {
            InetSocketAddress add = parse(key);
            if (add != null) {
                list.add(add);
            }
        }
        return list;

    }

    @Override
    public void registerAddresses(Collection<InetSocketAddress> addrs) {
        log.info("registerAddresses: items={}", addrs.size());
        List<String> list = new ArrayList<>();
        for (InetSocketAddress add : addrs) {
            String key = format(add);
            list.add(key);
        }
        fileListService.addFileList(list);
    }

    @Override
    public void unregisterAddresses(Collection<InetSocketAddress> addrs) {
        List<String> list = new ArrayList<>();
        for (InetSocketAddress add : addrs) {
            String key = format(add);
            list.add(key);
        }
        fileListService.removeFileList(list);
    }

    public InetSocketAddress parse(String key) {
        StringTokenizer st = new StringTokenizer(key, DELIM);

        if (st.countTokens() != 2) {
            throw new RuntimeException("Failed to parse S3 entry due to invalid format: " + key);
        } else {
            String addrStr = st.nextToken();
            String portStr = st.nextToken();

            int port = -1;

            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException ex) {
                log.error("Failed to parse port for S3 entry: " + key, ex);
            }
            if (port != -1) {
                try {
                    InetAddress ia = InetAddress.getByName(addrStr);
                    return new InetSocketAddress(ia, port);
                } catch (IllegalArgumentException ex) {
                    log.error("Failed to parse port for S3 entry: " + key, ex);
                } catch( UnknownHostException ex ) {
                    log.error("Failed to parse port for S3 entry: " + key, ex);
                }
            }
        }
        return null;
    }

    public String format(InetSocketAddress addr) {
        StringBuilder sb = new StringBuilder();

        sb.append(addr.getAddress().getHostAddress())
                .append(DELIM)
                .append(addr.getPort());

        return sb.toString();
    }

    @Override
    public Map<String, Object> getDiscoInfo() {
        Map<String, Object> map = new HashMap<>();
        map.put("fileListService", fileListService.getInfo());
        return map;
    }


}
