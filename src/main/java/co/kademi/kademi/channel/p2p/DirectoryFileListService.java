/*
 * Kademi
 */
package co.kademi.kademi.channel.p2p;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * Really just for testing, simulates s3
 *
 * @author brad
 */
public class DirectoryFileListService implements FileListService {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(DirectoryFileListService.class);

    private final File dir;

    public DirectoryFileListService() {
        File tmp = new File(System.getProperty("java.io.tmpdir"));
        this.dir = new File(tmp, "filelist");
        this.dir.mkdirs();
    }

    public DirectoryFileListService(File dir) {
        this.dir = dir;
    }

    @Override
    public List<String> getFileList() {
        List<String> list = new ArrayList<>();
        if (dir.listFiles() != null) {
            for (File f : dir.listFiles()) {
                list.add(f.getName());
            }
        }
        return list;
    }

    @Override
    public void addFileList(List<String> list) {
        for (String key : list) {
            File newDir = new File(dir, key);
            if (!newDir.exists()) {
                if (!newDir.mkdir()) {
                    throw new RuntimeException("Couldnt create: " + newDir.getAbsolutePath());
                }
            }
        }
    }

    @Override
    public void removeFileList(List<String> list) {
        for (String key : list) {
            File newDir = new File(dir, key);
            if (newDir.exists()) {
                if (!newDir.delete() ) {
                    throw new RuntimeException("Couldnt delete: " + newDir.getAbsolutePath());
                }
            }
        }

    }

    public void clear() {
        if (dir.listFiles() != null) {
            for (File f : dir.listFiles()) {
                if( !f.delete() ) {
                    throw new RuntimeException("Could not delete: " + f.getAbsolutePath());
                }
            }
        }

    }

}
