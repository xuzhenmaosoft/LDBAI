package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;

public class HDFSTestBase {

    private static TemporaryFolder tempFolder = new TemporaryFolder();

    protected static MiniDFSCluster hdfsCluster;
    protected static org.apache.hadoop.fs.FileSystem fs;

    @BeforeClass
    public static void createHDFS() throws IOException {
        Configuration conf = new Configuration();

        tempFolder.create();
        File dataDir = tempFolder.newFolder();

        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();

        fs = hdfsCluster.getFileSystem();
    }

    @AfterClass
    public static void destroyHDFS() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
        }
    }

}
