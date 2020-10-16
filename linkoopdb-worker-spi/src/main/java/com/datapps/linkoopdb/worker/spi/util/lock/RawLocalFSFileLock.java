package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawLocalFSFileLock implements Lock {

    private final Logger logger = LoggerFactory.getLogger(RawLocalFSFileLock.class);

    private File file;

    public RawLocalFSFileLock(RawLocalFileSystem fs) {
        //ignore
    }

    public void lock(Path lock, long lockExpireTime) throws IOException {
        if (this.file != null) {
            throw new IllegalArgumentException("lock can only be invoked once.");
        }
        String path = lock.toString();
        if (path.startsWith("ldb:/")) {
            path = path.substring(5);
        }
        path = path + ".lock";
        file = new File(path);
        boolean success = false;
        long lockWriteTime;
        while (!success && !Thread.currentThread().isInterrupted()) {
            boolean createFileSuccess;
            try {
                createFileSuccess = file.createNewFile();
            } catch (IOException e) {
                createFileSuccess = false;
            }
            if (createFileSuccess) {
                try {
                    FileOutputStream fileOutputStream = new FileOutputStream(file);
                    lockWriteTime = System.currentTimeMillis();
                    String lockContent = StringUtils.joinWith(" ", "flink", lockWriteTime, lockExpireTime);
                    fileOutputStream.write(lockContent.getBytes(StandardCharsets.UTF_8));
                    fileOutputStream.close();
                    success = true;
                    logger.info("Create lock {}  success.", lock.getName());
                } catch (IOException e) {
                    //ignore
                }
            }
            if (!success) {
                FileInputStream fileInputStream = new FileInputStream(file);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
                String line = bufferedReader.readLine();
                bufferedReader.close();
                long lockTimeout;
                if (line != null && !line.isEmpty()) {
                    String[] contents = line.split(" ");
                    if (contents.length == 3) {
                        logger.info("Last lock belong to {}", contents[0]);
                        lockWriteTime = Long.parseLong(contents[1]);
                        lockTimeout = Long.parseLong(contents[2]);
                    } else {
                        lockWriteTime = System.currentTimeMillis();
                        lockTimeout = lockExpireTime;
                    }
                } else {
                    lockWriteTime = System.currentTimeMillis();
                    lockTimeout = lockExpireTime;
                }
                try {
                    //等待锁超时
                    while (System.currentTimeMillis() - lockWriteTime < lockTimeout) {
                        Thread.sleep(500);
                        //优化
                        //如果发现锁已经不在了退出等待
                        if (!file.exists()) {
                            break;
                        }
                    }
                    //和HDFS不同如果文件被占用delete不会删除成功
                    if (file.exists()) {
                        file.delete();
                    }
                } catch (InterruptedException e2) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        //本地文件如果锁上了,读也会抛异常
    }

    public void unlock() {
        if (file != null) {
            boolean success = file.delete();
            if (!success) {
                logger.info("Unlock {} {} failed.", file.getParentFile().getName(), file.getName());
            } else {
                logger.info("Unlock {} {} success.", file.getParentFile().getName(), file.getName());
            }
        }
    }

}
