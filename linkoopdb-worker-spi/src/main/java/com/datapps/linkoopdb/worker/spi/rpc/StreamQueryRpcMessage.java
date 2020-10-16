package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.StreamMultiLayeredTable;
import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.UDFMeta;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.stream.WorkerSession;

public class StreamQueryRpcMessage extends QueryRpcMessage {

    private WorkerSession streamSession;
    private List<StreamMultiLayeredTable> tables;
    private Type[] columnTypes;
    private List<UDFMeta> udfMetas;

    private String resultId;
    private int pageSize;
    private long statementId;

    protected StreamQueryRpcMessage(
        WorkerSession streamSession,
        QueryMessageType messageType,
        SubmitInfo submitInfo) {
        super(messageType, submitInfo);
        this.streamSession = streamSession;
    }

    protected StreamQueryRpcMessage(
        WorkerSession streamSession,
        QueryMessageType messageType,
        SubmitInfo submitInfo,
        RelNode relNode) {
        super(messageType, submitInfo);
        this.streamSession = streamSession;
        this.relNode = relNode;
    }

    public StreamQueryRpcMessage withStreamSession(WorkerSession streamSession) {
        this.streamSession = streamSession;
        return this;
    }

    public StreamQueryRpcMessage withTables(List<StreamMultiLayeredTable> tables) {
        this.tables = tables;
        return this;
    }

    public StreamQueryRpcMessage withColumnTypes(Type[] columnTypes) {
        this.columnTypes = columnTypes;
        return this;
    }

    public StreamQueryRpcMessage withUdfMetas(List<UDFMeta> udfMetas) {
        this.udfMetas = udfMetas;
        return this;
    }

    public StreamQueryRpcMessage withSql(String sql) {
        super.sql = sql;
        return this;
    }

    public StreamQueryRpcMessage withResultId(String resultId) {
        this.resultId = resultId;
        return this;
    }

    public StreamQueryRpcMessage withPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public StreamQueryRpcMessage withStatementId(long statementId) {
        this.statementId = statementId;
        return this;
    }

    public WorkerSession getStreamSession() {
        return streamSession;
    }

    public List<StreamMultiLayeredTable> getTables() {
        return tables;
    }

    public Type[] getColumnTypes() {
        return columnTypes;
    }

    public List<UDFMeta> getUdfMetas() {
        return udfMetas;
    }

    public String getResultId() {
        return resultId;
    }

    public int getPageSize() {
        return pageSize;
    }

    public long getStatementId() {
        return statementId;
    }
}
