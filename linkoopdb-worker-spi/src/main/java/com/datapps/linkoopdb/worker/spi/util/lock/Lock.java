package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface Lock {

    /**
     * 相应锁住的文件路径
     * @param path 文件路径
     * @param lockExpireTime 锁过期时间
     * @throws IOException IOException
     */
    void lock(Path path, long lockExpireTime) throws IOException;

    /**
     * 解锁
     * @throws IOException IOException
     */
    void unlock() throws IOException;

}
