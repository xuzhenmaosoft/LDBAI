package com.datapps.linkoopdb.worker.spi.util.lock;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class SimpleReaderWriterTest extends HDFSTestBase {

    @Test
    public void testWrite() throws Exception {
        SimpleWriter writer = new SimpleWriter();
        Path path = new Path("/log.txt");
        writer.open(fs, path);
        writer.writeString("asdf");
        writer.writeString("zxcv");
        writer.hflush();
        writer.close();

        SimpleReader reader = new SimpleReader();
        reader.open(fs, path);
        String line = reader.readLine();
        Assert.assertEquals("asdf", line);
        line = reader.readLine();
        Assert.assertEquals("zxcv", line);
        reader.close();
    }

}
