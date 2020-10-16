package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;
import java.util.Map;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;

/**
 * 对应某个transaction中一组连续的insert/load操作的结果chunk的基类，不同存储引擎会继承该类来实现各自的具体chunk类
 */
public class StorageChunk implements Serializable {

    private String tableName;
    private String schemaName;
    private String format; //only used for external table
    private String[] columns;
    private Type[] columnTypes;
    private RexNode[] defaultValues;
    private String[] partitionColumns;
    private RexNode[][] partitionRanges = null;
    private Map<String, String> parameters;
    private String[] bucketColumns;
    private int[] nullabilities;
    private int[] fileToBinaryColumn;
    private int bucketNum = 0;
    private String[] sortColumns;
    private String chunk;
    private boolean upsert = false;
    private long txnSize = 0L;
    private long perInsertSize = 0L;
    private long perIngestSize = 0L;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StorageChunk that = (StorageChunk) o;

        return chunk != null ? chunk.equals(that.chunk) : that.chunk == null;
    }

    @Override
    public int hashCode() {
        return chunk != null ? chunk.hashCode() : 0;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public Type[] getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(Type[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String[] getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(String[] partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    public RexNode[][]  getPartitionRanges() {
        return partitionRanges;
    }

    public void setPartitionRanges(RexNode[][] partitionRanges) {
        this.partitionRanges = partitionRanges;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public String[] getBucketColumns() {
        return bucketColumns;
    }

    public void setBucketColumns(String[] bucketColumns) {
        this.bucketColumns = bucketColumns;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    public RexNode[] getDefaultValues() {
        return defaultValues;
    }

    public void setDefaultValues(RexNode[] defaultValues) {
        this.defaultValues = defaultValues;
    }

    public int[] getNullabilities() {
        return nullabilities;
    }

    public void setNullabilities(int[] nullabilities) {
        this.nullabilities = nullabilities;
    }

    public int[] getFileToBinaryColumn() {
        return fileToBinaryColumn;
    }

    public void setFileToBinaryColumn(int[] fileToBinaryColumn) {
        this.fileToBinaryColumn = fileToBinaryColumn;
    }

    public String[] getSortColumns() {
        return sortColumns;
    }

    public void setSortColumns(String[] sortColumns) {
        this.sortColumns = sortColumns;
    }

    public String getChunk() {
        return chunk;
    }

    public void setChunk(String chunk) {
        this.chunk = chunk;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public void setUpsert(boolean upsert) {
        this.upsert = upsert;
    }

    public long getTxnSize() {
        return txnSize;
    }

    public void setTxnSize(long txnSize) {
        this.txnSize = txnSize;
    }

    public long getPerInsertSize() {
        return perInsertSize;
    }

    public void setPerInsertSize(long perInsertSize) {
        this.perInsertSize = perInsertSize;
    }

    public long getPerIngestSize() {
        return perIngestSize;
    }

    public void setPerIngestSize(long perIngestSize) {
        this.perIngestSize = perIngestSize;
    }
}
