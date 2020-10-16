package com.datapps.linkoopdb.worker.spi.plan;

import java.io.Serializable;

/**
 * Created by gloway on 2019/3/25.
 */
public class LdbColumnStat implements Serializable {

    public Long distinctCount;
    public String min;
    public String max;
    public Long nullCount;
    public Long avgLen;
    public Long maxLen;
    public String histogram;

    public LdbColumnStat(long distinctCount, String min, String max, long nullCount, long avgLen,
        long maxLen, String histogram) {
        this.distinctCount = distinctCount;
        this.min = min;
        this.max = max;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
        this.histogram = histogram;
    }
}
