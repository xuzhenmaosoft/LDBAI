package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.File;
import java.net.URI;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;
import org.junit.Test;

public class RawLocalFSFileLockTest {

    @Test(timeout = 60000)
    public void testSingleThreadRawLocalFSFileLock() throws Exception {
        RawLocalFileSystem rawLocalFileSystem = new RawLocalFileSystem();
        rawLocalFileSystem.initialize(URI.create("file:///"), new Configuration());
        File tmpFile = File.createTempFile("test", ".tmp");
        System.out.println(tmpFile.getAbsolutePath());
        Path path = new Path(tmpFile.getAbsolutePath());

        for (int i = 0; i < 10; i++) {
            RawLocalFSFileLock lock = new RawLocalFSFileLock(rawLocalFileSystem);
            lock.lock(path, 1000);
            Thread.sleep(50);
            lock.unlock();
        }
        File lockFile = new File(tmpFile.getAbsolutePath() + ".lock");
        Assert.assertFalse(lockFile.exists());
    }

    @Test(timeout = 600000)
    public void testMultipleThreadRawLocalFSFileLock() throws Exception {
        RawLocalFileSystem rawLocalFileSystem = new RawLocalFileSystem();
        rawLocalFileSystem.initialize(URI.create("file:///"), new Configuration());
        File tmpFile = File.createTempFile("test", ".tmp");
        System.out.println(tmpFile.getAbsolutePath());
        Path path = new Path(tmpFile.getAbsolutePath());

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                try {
                    RawLocalFSFileLock lock = new RawLocalFSFileLock(rawLocalFileSystem);
                    lock.lock(path, 1000);
                    Thread.sleep(30);
                    lock.unlock();
                    latch.countDown();
                    System.out.println(latch.getCount());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
        }
        latch.await();
        File lockFile = new File(tmpFile.getAbsolutePath() + ".lock");
        Assert.assertFalse(lockFile.exists());
    }

}
