package com.datapps.linkoopdb.worker.spi.plan;

import java.io.Serializable;
import java.util.Map;

import com.datapps.linkoopdb.worker.spi.plan.util.Utilities;

/**
 * Created by gloway on 2019/3/25.
 */
public class LdbStatistics implements Serializable {

    public long sizeInBytes;
    public long rowCount;
    public Map<String, LdbColumnStat> attributeStats;

    public LdbStatistics(long sizeInBytes, long rowCount,
        Map<String, LdbColumnStat> attributeStats) {
        this.sizeInBytes = sizeInBytes;
        this.rowCount = rowCount;
        this.attributeStats = attributeStats;
    }

    public String simpleString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("sizeInBytes=" + Utilities.bytesToString(sizeInBytes));
        stringBuilder.append(", ");
        stringBuilder.append("rowCount=" + rowCount);
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return "Statistics(" + simpleString() + ")";
    }
}
