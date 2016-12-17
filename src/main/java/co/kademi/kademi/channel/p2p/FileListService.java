/*
 * Kademi
 */
package co.kademi.kademi.channel.p2p;

import java.util.List;

/**
 *
 * @author brad
 */
public interface FileListService {
    List<String> getFileList();

    void addFileList(List<String> list);

    void removeFileList(List<String> list);
}
