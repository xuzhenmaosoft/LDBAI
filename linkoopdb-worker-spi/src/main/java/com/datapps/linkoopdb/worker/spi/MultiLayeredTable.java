/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;
import java.util.Map;

import com.datapps.linkoopdb.jdbc.StorageEngineConstant;
import com.datapps.linkoopdb.jdbc.lib.StringConverter;
import com.datapps.linkoopdb.jdbc.types.Type;

public class MultiLayeredTable implements Serializable {

    //存储引擎类型
    private int storageEngine;
    private int structureType;

    private LayeredChunk layeredChunk;
    private String name;
    private String tableId;
    private String schema;
    private Type[] columnTypes;
    private String[] columns;
    private String format; //only used for external table
    private String[] partitionColumns;
    private Map<String, String> parameters;
    private String[] bucketColumns;
    private String[] sortColumns;
    private int bucketNum;
    private boolean hasStats;
    private long statsUpdateTimeStamp;
    private transient Object stable;
    private String snapshot;
    private String indexMetastorePath;

    public MultiLayeredTable() {
    }

    public MultiLayeredTable(String name, String tableId, String schemaName, Type[] columnTypes, String[] columns) {
        this(name, tableId, schemaName, null, columnTypes, columns, false);
    }

    public MultiLayeredTable(String name, String tableId, String schemaName, LayeredChunk layeredChunk, Type[] columnTypes, String[] columns,
        boolean hasStats) {
        this.name = name;
        this.tableId = tableId;
        this.schema = schemaName;
        this.layeredChunk = layeredChunk;
        this.columnTypes = columnTypes;
        this.columns = columns;
        this.hasStats = hasStats;
    }

    public String getQualifiedName() {
        return schema + "$" + name;
    }

    public String getQuotedQualifiedName() {
        return StringConverter.toQuotedString(getQualifiedName(), '`', true);
    }

    public String getName() {
        return name;
    }

    public String getSchema() {
        return schema;
    }

    public LayeredChunk getLayeredChunk() {
        return layeredChunk;
    }

    public void setLayeredChunk(LayeredChunk layeredChunk) {
        this.layeredChunk = layeredChunk;
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

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public boolean isHasStats() {
        return hasStats;
    }

    public Object getStable() {
        return stable;
    }

    public void setStable(Object stable) {
        this.stable = stable;
    }

    public String[] getSortColumns() {
        return sortColumns;
    }

    public void setSortColumns(String[] sortColumns) {
        this.sortColumns = sortColumns;
    }

    public int getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(int storageEngine) {
        this.storageEngine = storageEngine;
    }

    public int getStructureType() {
        return structureType;
    }

    public void setStructureType(int structureType) {
        this.structureType = structureType;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(String snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultiLayeredTable table = (MultiLayeredTable) o;

        if (layeredChunk != null ? !layeredChunk.equals(table.layeredChunk) : table.layeredChunk != null) {
            return false;
        }
        return tableId != null ? tableId.equals(table.tableId) : table.tableId == null;
    }

    @Override
    public int hashCode() {
        int result = layeredChunk != null ? layeredChunk.hashCode() : 0;
        result = 31 * result + (tableId != null ? tableId.hashCode() : 0);
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    //return whether the table is palllas table
    //if storage engine is pallas engine, return true, else return false
    public boolean isPallas() {
        return this.structureType == StorageEngineConstant.STORAGE_ENGINE_PALLAS;
    }

    public void setIndexMetastorePath(String indexMetastorePath) {
        this.indexMetastorePath = indexMetastorePath;
    }

    public String getIndexMetastorePath() {
        return indexMetastorePath;
    }
}
