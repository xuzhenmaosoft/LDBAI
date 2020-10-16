package com.datapps.linkoopdb.worker.spi;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class NetworkUtilsTest {

    @Test
    public void testGetPortRangeFromString() {
        String portRange="1000,200-205";
        List<Integer> portList = NetworkUtils.getPortRangeFromString(portRange);
        Assert.assertEquals(7, portList.size());

        Integer[] resultPortList = new Integer[]{ 1000, 200, 201, 202, 203, 204, 205 };
        Assert.assertEquals(Arrays.asList(resultPortList), portList);
    }

    @Test
    public void testPickPortFromRange() {
        String portRange="50000,200-205";
        int freePort = NetworkUtils.pickPortFromRange(portRange);
        Assert.assertEquals(50000, freePort);
    }

    @Test
    public void testPickFreePort() throws IOException {
        int freePort = NetworkUtils.pickFreePort("127.0.0.1", 50001);
        Assert.assertEquals(50001, freePort);
    }
}
