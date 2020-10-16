package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.Properties;

/**
 * Created by gloway on 2019/2/26.
 */
public class LdbJdbcSource extends LdbSource {

    public static final int THREE_PARAM = 1;
    public static final int FOUR_PAEAM = 2;
    public static final int SEVEN_PAEAM = 3;

    String url;
    String sql;
    Properties properties;
    public boolean originSql = false;

    int ldbJdbcSourceType = -1;

    String sourceTable;
    String[] partsArr;

    String partitionColumn;
    long lowerBound;
    long upperBound;
    int numPartitions;


    public LdbJdbcSource(String url, String sql, Properties properties) {
        this.url = url;
        this.sql = sql;
        this.properties = properties;
        ldbJdbcSourceType = THREE_PARAM;
    }

    public LdbJdbcSource(String url, String sourceTable, String[] partsArr, Properties properties) {
        this.url = url;
        this.sourceTable = sourceTable;
        this.partsArr = partsArr;
        this.properties = properties;
        ldbJdbcSourceType = FOUR_PAEAM;
    }

    public LdbJdbcSource(String url, String sourceTable, String partitionColumn, long lowerBound,
        long upperBound, int numPartitions, Properties properties) {
        this.url = url;
        this.sourceTable = sourceTable;
        this.partitionColumn = partitionColumn;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.numPartitions = numPartitions;
        this.properties = properties;
        ldbJdbcSourceType = SEVEN_PAEAM;
    }

    public String getUrl() {
        return url;
    }

    public String getSql() {
        return sql;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getSourceTable() {
        return sourceTable == null ? this.getSql() : sourceTable;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String[] getPartsArr() {
        return partsArr;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public long getLowerBound() {
        return lowerBound;
    }

    public long getUpperBound() {
        return upperBound;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getLdbJdbcSourceType() {
        return ldbJdbcSourceType;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public String getStatistic() {
        return null;
    }
}
