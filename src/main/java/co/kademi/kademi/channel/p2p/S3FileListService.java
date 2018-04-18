/*
 * Kademi
 */
package co.kademi.kademi.channel.p2p;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.ByteArrayInputStream;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class S3FileListService implements FileListService {

    private static final Logger log = LoggerFactory.getLogger(S3FileListService.class);

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
    private AWSCredentialsProvider cred;
    private ClientConfiguration cfg;

    private AmazonS3 s3;
    private String regionId;
    private boolean initFinished = false;

    @Override
    public List<String> getFileList() {
        log.info("getFileList bucket={}", bucketName);
        initClient();

        List<String> addrs = new LinkedList<>();

        if (s3 != null) {
            try {
                ObjectListing list = s3.listObjects(bucketName);
                boolean truncated = list.isTruncated();
                do {
                    log.info("getFileList: file list response num items={} truncated={}", list.getObjectSummaries().size(), truncated);
                    for (S3ObjectSummary sum : list.getObjectSummaries()) {
                        String key = sum.getKey();
                        addrs.add(key);
                    }
                    if (truncated) {
                        list = s3.listNextBatchOfObjects(list);
                    }
                } while (list.isTruncated());
            } catch (AmazonClientException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            log.warn("Cant lookup file list, no s3 client");
        }

        return addrs;
    }

    private void initClient() {
        if (!initFinished) {
            initFinished = true;

            if (cred == null) {
                throw new RuntimeException("S3-initClient: AWS credentials are not set.");
            }

            if (StringUtils.isBlank(bucketName)) {
                throw new RuntimeException("S3-initClient: Bucket name is null or empty (provide bucket name and restart).");
            }

            Regions region = null;
            if (StringUtils.isNotBlank(regionId)) {
                region = Regions.fromName(regionId);
            }
            if (region == null) {
                region = Regions.DEFAULT_REGION;
            }

            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(cred)
                    .withRegion(region);

            if (cfg != null) {
                builder.setClientConfiguration(cfg);
            }

            try {
                s3 = builder.build();
            } catch (Exception e) {                
                log.error("initClient: Exception. Region={}" + region + " Cred=" + cred + " Bucket=" + bucketName + " RegionID=" + regionId, e);
                return ;
            }

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

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    /**
     * Sets AWS credentials.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cred AWS credentials.
     */
    public void setAwsCredentials(AWSCredentialsProvider cred) {
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

    @Override
    public void addFileList(List<String> list) {
        log.info("addFileList: items={}", list.size());
        initClient();
        
        if( s3 == null ) {
            log.warn("addFileList: Cant set file list becasue dont have an s3 client");
            return ;
        }

        for (String key : list) {
            if (StringUtils.isNotBlank(key)) {
                try {
                    log.info("addFileList: item={}", key);
                    s3.putObject(bucketName, key, new ByteArrayInputStream(ENTRY_CONTENT), ENTRY_METADATA);
                } catch (AmazonClientException e) {
                    throw new RuntimeException("addFileList: Failed to put entry [bucketName=" + bucketName + ", entry=" + key + ']', e);
                }
            } else {
                log.warn("addFileList: Ignoring null or empty key in list of size {}", list.size());
            }
        }
    }

    @Override
    public void removeFileList(List<String> list) {
        log.info("removeFileList: items={}", list.size());
        initClient();

        for (String key : list) {
            try {
                log.info("removeFileList: item={}", key);
                s3.deleteObject(bucketName, key);
            } catch (AmazonClientException e) {
                throw new RuntimeException("Failed to delete entry [bucketName=" + bucketName + ", entry=" + key + ']', e);
            }
        }
    }
}
