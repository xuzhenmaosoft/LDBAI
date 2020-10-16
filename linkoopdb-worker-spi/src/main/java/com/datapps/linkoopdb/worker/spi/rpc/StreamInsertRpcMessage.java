package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.List;
import java.util.Properties;

import com.google.common.base.Preconditions;

import com.datapps.linkoopdb.worker.spi.StorageChunk;
import com.datapps.linkoopdb.worker.spi.StreamMultiLayeredTable;
import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.UDFMeta;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.stream.WorkerSession;

public class StreamInsertRpcMessage extends InsertRpcMessage {
    private WorkerSession streamSession;
    private List<StreamMultiLayeredTable> tables;
    private List<UDFMeta> udfMetas;

    private String jobName;
    private Properties sourceProperties;
    private Properties sinkProperties;
    private String sql;
    private long statementId;

    public StreamInsertRpcMessage(WorkerSession streamSession, SubmitInfo submitInfo, StorageChunk chunk) {
        super(InsertType.INSERT_BY_SUBQUERY, submitInfo, chunk);
        this.streamSession = streamSession;
    }

    public StreamInsertRpcMessage(WorkerSession streamSession, String jobName,
        Properties sourceProperties, Properties sinkProperties) {
        super(InsertType.START_SYNC_JOB);
        this.jobName = Preconditions.checkNotNull(jobName, "jobName cannot be null.");
        this.sourceProperties = Preconditions.checkNotNull(sourceProperties, "sourceProperties cannot be null.");
        this.sinkProperties = Preconditions.checkNotNull(sinkProperties, "sinkProperties cannot be null.");
        this.streamSession = streamSession;
    }

    public StreamInsertRpcMessage withTables(List<StreamMultiLayeredTable> tables) {
        this.tables = tables;
        return this;
    }

    public StreamInsertRpcMessage withUdfMetas(List<UDFMeta> udfMetas) {
        this.udfMetas = udfMetas;
        return this;
    }

    public StreamInsertRpcMessage withRelNode(RelNode relNode) {
        this.relNode = relNode;
        return this;
    }

    public StreamInsertRpcMessage withSql(String sql) {
        this.sql = sql;
        return this;
    }

    public StreamInsertRpcMessage withStatementId(long statementId) {
        this.statementId = statementId;
        return this;
    }

    public String getSql() {
        return sql;
    }

    public WorkerSession getStreamSession() {
        return streamSession;
    }

    public List<StreamMultiLayeredTable> getTables() {
        return tables;
    }

    public List<UDFMeta> getUdfMetas() {
        return udfMetas;
    }

    public String getJobName() {
        return jobName;
    }

    public Properties getSourceProperties() {
        return sourceProperties;
    }

    public Properties getSinkProperties() {
        return sinkProperties;
    }

    public long getStatementId() {
        return statementId;
    }
}
