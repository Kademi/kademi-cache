package co.kademi.kademi.channel.p2p.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.StringTokenizer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Wesley
 */
public class S3P2PMemberDiscoveryService {

    private static final Logger log = LoggerFactory.getLogger(S3P2PMemberDiscoveryService.class);

    /**
     * Delimiter to use in S3 entries name.
     */
    public static final String DELIM = "#";

    /**
     * Entry content.
     */
    private static final byte[] ENTRY_CONTENT = new byte[]{1};

    /**
     * Entry metadata with content length set.
     */
    private static final ObjectMetadata ENTRY_METADATA;

    static {
        ENTRY_METADATA = new ObjectMetadata();

        ENTRY_METADATA.setContentLength(ENTRY_CONTENT.length);
    }

    /**
     * Config Properties
     */
    private String bucketName;
    private AWSCredentials cred;
    private ClientConfiguration cfg;

    private AmazonS3 s3;
    private boolean initFinished = false;

    public Collection<InetSocketAddress> getRegisteredAddresses() {
        initClient();

        Collection<InetSocketAddress> addrs = new LinkedList<>();

        try {
            ObjectListing list = s3.listObjects(bucketName);
            boolean truncated = list.isTruncated();
            do {
                for (S3ObjectSummary sum : list.getObjectSummaries()) {
                    String key = sum.getKey();

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
                                addrs.add(new InetSocketAddress(addrStr, port));
                            } catch (IllegalArgumentException ex) {
                                log.error("Failed to parse port for S3 entry: " + key, ex);
                            }
                        }

                    }
                }

                if (truncated) {
                    list = s3.listNextBatchOfObjects(list);
                }
            } while (list.isTruncated());
        } catch (AmazonClientException ex) {
            throw new RuntimeException(ex);
        }

        return addrs;
    }

    /**
     * Registers new addresses.
     * <p>
     * Implementation should accept duplicates quietly, but should not register
     * address if it is already registered.
     *
     * @param addrs Addresses to register. Not {@code null} and not empty.
     */
    public void registerAddresses(Collection<InetSocketAddress> addrs) {
        initClient();

        for (InetSocketAddress addr : addrs) {
            String key = key(addr);

            try {
                s3.putObject(bucketName, key, new ByteArrayInputStream(ENTRY_CONTENT), ENTRY_METADATA);
            } catch (AmazonClientException e) {
                throw new RuntimeException("Failed to put entry [bucketName=" + bucketName
                        + ", entry=" + key + ']', e);
            }
        }
    }

    public void unregisterAddresses(Collection<InetSocketAddress> addrs) {
        initClient();

        for (InetSocketAddress addr : addrs) {
            String key = key(addr);

            try {
                s3.deleteObject(bucketName, key);
            } catch (AmazonClientException e) {
                throw new RuntimeException("Failed to delete entry [bucketName=" + bucketName
                        + ", entry=" + key + ']', e);
            }
        }
    }

    /**
     * Gets S3 key for provided address.
     *
     * @param addr Node address.
     * @return Key.
     */
    private String key(InetSocketAddress addr) {
        assert addr != null;

        StringBuilder sb = new StringBuilder();

        sb.append(addr.getAddress().getHostAddress())
                .append(DELIM)
                .append(addr.getPort());

        return sb.toString();
    }

    private void initClient() {
        if (!initFinished) {
            initFinished = true;

            if (cred == null) {
                throw new RuntimeException("AWS credentials are not set.");
            }

            if (StringUtils.isBlank(bucketName)) {
                throw new RuntimeException("Bucket name is null or empty (provide bucket name and restart).");
            }

            s3 = cfg != null ? new AmazonS3Client(cred, cfg) : new AmazonS3Client(cred);

            if (!s3.doesBucketExist(bucketName)) {
                try {
                    s3.createBucket(bucketName);

                    while (!s3.doesBucketExist(bucketName)) {
                        sleep(200);
                    }

                } catch (AmazonClientException ex) {
                    throw new RuntimeException(ex);
                }
            }

            if (s3 == null) {
                throw new RuntimeException("IP finder has not been initialized properly.");
            }
        }
    }

    /**
     * Sets bucket name for IP finder.
     *
     * @param bucketName Bucket name.
     */
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    /**
     * Sets AWS credentials.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cred AWS credentials.
     */
    public void setAwsCredentials(AWSCredentials cred) {
        this.cred = cred;
    }

    /**
     * Sets Amazon client configuration.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cfg Amazon client configuration.
     */
    public void setClientConfiguration(ClientConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Sleeps for given number of milliseconds.
     *
     * @param ms Time to sleep.
     *
     * {@link InterruptedException}.
     */
    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(e);
        }
    }

}
