package com.datapps.linkoopdb.worker.spi.util.lock;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class LookSupport {

    public static Lock lockFor(FileSystem fs) {
        if (fs instanceof DistributedFileSystem) {
            return new HDFSFileLock((DistributedFileSystem) fs);
        } else if (fs instanceof RawLocalFileSystem) {
            return new RawLocalFSFileLock((RawLocalFileSystem) fs);
        }
        throw new RuntimeException("unsupported FileSystem: " + fs.getUri().toString());
    }

}
