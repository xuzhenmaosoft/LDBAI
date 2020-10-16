package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaseManger {

    private static final Logger logger = LoggerFactory.getLogger(LeaseManger.class);

    private static final Map<Lease, Set<Path>> leaseMap = new HashMap<>();

    public static synchronized void addLease(DistributedFileSystem fs, Path path) {
        DFSClient dfsClient = fs.getClient();
        Lease lease = new Lease(Thread.currentThread(), dfsClient);
        Set<Path> paths = leaseMap.get(lease);
        if (paths == null) {
            Set<Path> newPaths = new HashSet<>();
            newPaths.add(path);
            leaseMap.put(lease, newPaths);
        } else {
            paths.add(path);
        }
    }

    public static synchronized void releaseLease(DistributedFileSystem fs, Path path) {
        DFSClient dfsClient = fs.getClient();
        Lease lease = new Lease(Thread.currentThread(), dfsClient);
        Set<Path> paths = leaseMap.get(lease);
        if (paths != null && !paths.isEmpty()) {
            boolean success = paths.remove(path);
            if (success) {
                logger.info("Release lease: {} {}", path.getParent().getName(), path.getName());
            }
        }
    }

    public static synchronized boolean hasLease(DistributedFileSystem fs, Path path) {
        DFSClient dfsClient = fs.getClient();
        Lease lease = new Lease(Thread.currentThread(), dfsClient);
        Set<Path> paths = leaseMap.get(lease);
        if (paths != null && !paths.isEmpty()) {
            return paths.contains(path);
        }
        return false;
    }

    public static synchronized void recoverLease(DistributedFileSystem fs, Path path) {
        try {
            boolean recoverLease = fs.recoverLease(path);
            if (!recoverLease) {
                logger.info("Release lease failed, path: {}", path.toString());
            }
        } catch (IOException ioException) {
            logger.info("Release lease failed, path: {}", path.toString());
        }
    }

}
