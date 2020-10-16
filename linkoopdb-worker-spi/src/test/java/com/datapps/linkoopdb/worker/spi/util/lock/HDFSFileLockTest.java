package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Test;

public class HDFSFileLockTest extends HDFSTestBase {

    @Test(timeout = 6000)
    public void testSingleThreadHDFSFileLock() throws IOException, InterruptedException {
        HDFSFileLock lock = new HDFSFileLock((DistributedFileSystem) fs);
        Path lockPath = new Path("/lock.lck");
        lock.lock(lockPath, 1000);
        Assert.assertTrue(fs.exists(lockPath));
        Thread.sleep(500);
        lock.unlock();
        Assert.assertFalse(fs.exists(new Path("/lock.lck.lock")));
    }

    @Test(timeout = 600000)
    public void testMultipleThreadHDFSFileLock() throws Exception {
        int threadCount = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicReference<Exception> exception = new AtomicReference<>();
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                HDFSFileLock lock = new HDFSFileLock((DistributedFileSystem) fs);
                Path lockPath = new Path("/lock.lck");
                try {
                    lock.lock(lockPath, 1000);
                    Thread.sleep(500);
                    lock.unlock();
                } catch (Exception e) {
                    exception.set(e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }
        Path lockPath = new Path("/lock.lck.lock");
        Assert.assertFalse(fs.exists(lockPath));
    }

}
