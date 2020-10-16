package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.Map;

import com.datapps.linkoopdb.jdbc.lib.HashMappedList;
import com.datapps.linkoopdb.worker.spi.stream.WorkerSession;

public class StreamPropertiesRpcMessage extends SessionAction {

    private String tableName;
    private Map<String, String> parameters;
    private HashMappedList columns;
    private boolean isBatch;
    private boolean isStream;

    public StreamPropertiesRpcMessage(WorkerSession streamSession, SessionActionType actionType) {
        super(streamSession, actionType);
    }

    public StreamPropertiesRpcMessage withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public StreamPropertiesRpcMessage withParameters(Map<String, String> parameters) {
        this.parameters = parameters;
        return this;
    }

    public StreamPropertiesRpcMessage withColumns(HashMappedList columns) {
        this.columns = columns;
        return this;
    }

    public StreamPropertiesRpcMessage withIsBatch(boolean isBatch) {
        this.isBatch = isBatch;
        return this;
    }

    public StreamPropertiesRpcMessage withIsStream(boolean isStream) {
        this.isStream = isStream;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public HashMappedList getColumns() {
        return columns;
    }

    public boolean isBatch() {
        return isBatch;
    }

    public boolean isStream() {
        return isStream;
    }
}
