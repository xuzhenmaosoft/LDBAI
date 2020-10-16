package com.datapps.linkoopdb.worker.spi.plan.core;

import java.util.Map;
import java.util.UUID;

import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;

/**
 * Created by gloway on 2019/2/26.
 */
public class LdbParquetSource extends LdbSource {

    private int bucketNum = 0;
    private String[] bucketColumns;
    private String[] sortColumns;
    private String[] paths;
    private Map<String, String> properties;
    private RexNode[][] partitionRanges = null;

    public LdbParquetSource(String[] paths) {
        this(0, null, null, paths, null);
    }

    public LdbParquetSource(int bucketNum, String[] bucketColumns, String[] sortColumns, String[] paths, Map<String, String> properties) {
        this.bucketNum = bucketNum;
        this.bucketColumns = bucketColumns;
        this.sortColumns = sortColumns;
        this.paths = paths;
        this.properties = properties;
    }

    @Override
    public String getStatistic() {
        return null;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    public String[] getBucketColumns() {
        return bucketColumns;
    }

    public void setBucketColumns(String[] bucketColumns) {
        this.bucketColumns = bucketColumns;
    }

    public String[] getSortColumns() {
        return sortColumns;
    }

    public void setSortColumns(String[] sortColumns) {
        this.sortColumns = sortColumns;
    }

    public String[] getPaths() {
        return paths;
    }

    public void setPaths(String[] paths) {
        this.paths = paths;
    }


    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public RexNode[][] getPartitionRanges() {
        return partitionRanges;
    }

    public void setPartitionRanges(RexNode[][] partitionRanges) {
        this.partitionRanges = partitionRanges;
    }

    @Override
    public boolean equals(Object o) {
        //if (this == o) {
        //    return true;
        //}
        //if (o == null || getClass() != o.getClass()) {
        //    return false;
        //}

        //LdbParquetSource that = (LdbParquetSource) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        //return Arrays.equals(paths, that.paths);

        return false;
    }

    @Override
    public int hashCode() {

        // return Arrays.hashCode(paths);
        return UUID.randomUUID().hashCode();
    }
}
