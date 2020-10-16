package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSFileLock implements Lock {

    private static final Logger logger = LoggerFactory.getLogger(HDFSFileLock.class);

    private final DistributedFileSystem fs;
    private Path lock;
    private FSDataOutputStream outputStream;

    public HDFSFileLock(DistributedFileSystem fs) {
        this.fs = fs;
    }

    public synchronized void lock(Path lock, long lockExpireTime) throws IOException {
        if (this.lock != null) {
            throw new IllegalArgumentException("create lock can only be invoked once.");
        }
        this.lock = new Path(lock.getParent(), lock.getName() + ".lock");
        boolean success = false;
        while (!success && !Thread.currentThread().isInterrupted()) {
            try {
                //以乐观的方式创建锁
                this.outputStream = fs.create(lock, false);
                LeaseManger.addLease(fs, lock);
                logger.info("Create lock {} {} success.", lock.getParent().getName(), lock.getName());
                success = true;
            } catch (IOException ioException) {
                HDFSExceptionProcessor.processCreateFileException(ioException);
                //锁文件已经存在,尝试读取里面的内容
                SimpleReader reader = new SimpleReader();
                //为了不影响数据处理
                //从锁中读取不到内容或内容不完整,不抛异常,使用一个默认值
                long lockWriteTime = System.currentTimeMillis();
                long lockTimeout = lockExpireTime;
                try {
                    reader.open(fs, lock);
                    String line = reader.readLine();
                    //内容不为空,读取超时时间进行等待,否则继续循环
                    if (line != null) {
                        String[] params = line.split(" ");
                        if (params.length == 3) {
                            logger.info("Previous lock belong to " + params[0]);
                            lockWriteTime = Long.parseLong(params[1]);
                            lockTimeout = Long.parseLong(params[2]);
                        }
                    }
                } catch (IOException readerIOException) {
                    HDFSExceptionProcessor.processReadFileException(readerIOException);
                } finally {
                    //reader要提早释放
                    // 因为读会占用锁不放
                    reader.close();
                }
                try {
                    //等待锁超时
                    if (LeaseManger.hasLease(fs, lock)) {
                        while (System.currentTimeMillis() - lockWriteTime < lockTimeout) {
                            Thread.sleep(100);
                            if (!LeaseManger.hasLease(fs, lock)) {
                                break;
                            }
                        }
                    } else {
                        while (System.currentTimeMillis() - lockWriteTime < lockTimeout) {
                            Thread.sleep(500);
                            //优化
                            //如果发现锁已经不在了退出等待
                            if (!fs.exists(lock)) {
                                break;
                            }
                        }
                        //锁不被当前线程占有才能删除
                        //因为Lease还在,删除后再写入会报No Lease Exception
                        if (fs.exists(lock) && !LeaseManger.hasLease(fs, lock) && fs.isFileClosed(lock)) {
                            deleteFile(fs, lock);
                        }
                    }
                } catch (InterruptedException e2) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e2);
                } catch (IOException e) {
                    //ignore
                }
            }
        }
        if (this.outputStream != null) {
            try {
                SimpleWriter writer = new SimpleWriter();
                writer.open(fs, outputStream, lock);
                String content = "flink " + System.currentTimeMillis() + " " + lockExpireTime;
                writer.writeString(content);
                //hsync可以保证数据被持久化
                writer.hsync();
                //这里不close writer,outputStream由unlock来close
            } catch (IOException e) {
                //即使没写入内容或写入了错误的内容,都会有处理
                //ignore
            }
        }
    }

    /**
     * @throws IOException IOException
     */
    public synchronized void unlock() throws IOException {
        if (outputStream != null) {
            try {
                //org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException):
                // No lease on /tmp/bucketing/202008032041/2020-08-04-10-56_2020-08-04-10-57/in-progress.lck (inode 13517737): File does not exist.
                outputStream.close();
                outputStream = null;
            } catch (IOException e) {
                logger.error(e.getMessage());
                LeaseManger.recoverLease(fs, lock);
                //可能被别的Task删掉了
                //ignore
            } finally {
                //释放Lease
                LeaseManger.releaseLease(fs, lock);
                deleteFile(fs, lock);
            }
        }
        logger.info("Release lock {} {} success", lock.getParent().getName(), lock.getName());
    }

    /**
     *如果文件存在将其删除.
     */
    private void deleteFile(FileSystem fs, Path path) throws IOException {
        try {
            if (fs.exists(path)) {
                boolean deleteSuccess = fs.delete(path, false);
                if (deleteSuccess) {
                    logger.info("Delete lock {} {} success.", path.getParent().getName(), path.getName());
                } else {
                    logger.info("Delete lock {} {} failed.", path.getParent().getName(), path.getName());
                }
            }
        } catch (IOException e) {
            HDFSExceptionProcessor.processInterrupt(e);
        }
    }

}
