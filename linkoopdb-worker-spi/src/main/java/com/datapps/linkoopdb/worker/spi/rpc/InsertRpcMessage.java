package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.Properties;

import com.datapps.linkoopdb.jdbc.navigator.RowSetNavigator;
import com.datapps.linkoopdb.worker.spi.PallasDmlSymbol;
import com.datapps.linkoopdb.worker.spi.ShardsChunk;
import com.datapps.linkoopdb.worker.spi.StorageChunk;
import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;
import com.datapps.linkoopdb.worker.spi.stream.WorkerSession;

/**
 * Created by gloway on 2019/9/3.
 */
public class InsertRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.INSERT;
    }

    public enum InsertType {
        ADD_INSERT_CACHE, ADD_BATCH_INSERT_CACHE, INSERT_TO_PALLAS, INSERT_BY_SUBQUERY, FLUSH_INSERT, CLEAN_INSERT_CACHE, SYSTEM_INSERT, START_SYNC_JOB
    }

    private InsertType insertType;

    private SubmitInfo submitInfo;
    private StorageChunk storageChunk;
    private RowSetNavigator rowSetNavigator;

    private boolean ispstmt = false;

    RexNode rexNode;
    int[] columnIndexs;

    boolean returnData;

    protected RelNode relNode;
    PallasDmlSymbol pallasDmlSymbol;

    public static InsertRpcMessage buildInsertCacheMessage(SubmitInfo submitInfo, StorageChunk chunk, RowSetNavigator rowSetNavigator) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.ADD_INSERT_CACHE;
        insertRpcMessage.submitInfo = submitInfo;
        insertRpcMessage.storageChunk = chunk;
        insertRpcMessage.rowSetNavigator = rowSetNavigator;
        return insertRpcMessage;
    }

    public static InsertRpcMessage buildBatchInsertCacheMessage(SubmitInfo submitInfo, StorageChunk chunk, RowSetNavigator rowSetNavigator) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.ADD_BATCH_INSERT_CACHE;
        insertRpcMessage.submitInfo = submitInfo;
        insertRpcMessage.storageChunk = chunk;
        insertRpcMessage.rowSetNavigator = rowSetNavigator;
        return insertRpcMessage;
    }

    public static InsertRpcMessage buildInsertCacheMessage(SubmitInfo submitInfo, StorageChunk chunk, RexNode rexNode, int[] columnIndexs) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.ADD_INSERT_CACHE;
        insertRpcMessage.submitInfo = submitInfo;
        insertRpcMessage.storageChunk = chunk;
        insertRpcMessage.rexNode = rexNode;
        insertRpcMessage.columnIndexs = columnIndexs;
        return insertRpcMessage;
    }

    public static InsertRpcMessage buildBatchInsertCacheMessage(SubmitInfo submitInfo, StorageChunk chunk, RexNode rexNode, int[] columnIndexs) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.ADD_BATCH_INSERT_CACHE;
        insertRpcMessage.submitInfo = submitInfo;
        insertRpcMessage.storageChunk = chunk;
        insertRpcMessage.rexNode = rexNode;
        insertRpcMessage.columnIndexs = columnIndexs;
        return insertRpcMessage;
    }

    public static InsertRpcMessage buildInsertPallasMessage(SubmitInfo submitInfo, ShardsChunk chunk, RowSetNavigator rowSetNavigator) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.INSERT_TO_PALLAS;
        insertRpcMessage.submitInfo = submitInfo;
        insertRpcMessage.storageChunk = chunk;
        insertRpcMessage.rowSetNavigator = rowSetNavigator;
        return insertRpcMessage;
    }

    public static InsertRpcMessage buildInsertPallasMessage(SubmitInfo submitInfo, ShardsChunk chunk,
            RexNode rexNode, int[] columnIndexs, boolean returnData, boolean ispstmt) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.INSERT_TO_PALLAS;
        insertRpcMessage.submitInfo = submitInfo;
        insertRpcMessage.storageChunk = chunk;
        insertRpcMessage.rexNode = rexNode;
        insertRpcMessage.columnIndexs = columnIndexs;
        insertRpcMessage.returnData = returnData;
        insertRpcMessage.ispstmt = ispstmt;
        return insertRpcMessage;
    }

    public static InsertRpcMessage buildSysInsertMessage(SubmitInfo submitInfo, ShardsChunk chunk, RexNode rexNode, int[] columnIndexs) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.SYSTEM_INSERT;
        insertRpcMessage.submitInfo = submitInfo;
        insertRpcMessage.storageChunk = chunk;
        insertRpcMessage.rexNode = rexNode;
        insertRpcMessage.columnIndexs = columnIndexs;
        return insertRpcMessage;
    }

    public static InsertRpcMessage buildInsertBySubqueryMessage(SubmitInfo submitInfo, RelNode relNode, StorageChunk chunk, PallasDmlSymbol pallasDmlSymbol) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.INSERT_BY_SUBQUERY;
        insertRpcMessage.submitInfo = submitInfo;
        insertRpcMessage.storageChunk = chunk;
        insertRpcMessage.relNode = relNode;
        insertRpcMessage.pallasDmlSymbol = pallasDmlSymbol;
        return insertRpcMessage;
    }

    public static InsertRpcMessage buildFlushInsertMessage(SubmitInfo submitInfo, StorageChunk incrementalChunk) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.FLUSH_INSERT;
        insertRpcMessage.submitInfo = submitInfo;
        insertRpcMessage.storageChunk = incrementalChunk;
        return insertRpcMessage;
    }

    public static InsertRpcMessage buildCleanInsertMessage(SubmitInfo submitInfo) {
        InsertRpcMessage insertRpcMessage = new InsertRpcMessage();
        insertRpcMessage.insertType = InsertType.CLEAN_INSERT_CACHE;
        insertRpcMessage.submitInfo = submitInfo;
        return insertRpcMessage;
    }

    public static StreamInsertRpcMessage buildStreamInsertBySubQuery(WorkerSession streamSession, SubmitInfo submitInfo, StorageChunk chunk) {
        return new StreamInsertRpcMessage(streamSession, submitInfo, chunk);
    }

    public static StreamInsertRpcMessage buildStreamStartSyncJob(WorkerSession streamSession,
        String jobName,Properties sourceProperties, Properties sinkProperties) {
        return new StreamInsertRpcMessage(streamSession, jobName, sourceProperties, sinkProperties);
    }

    private InsertRpcMessage() {

    }

    protected InsertRpcMessage(InsertType insertType) {
        this.insertType = insertType;
    }

    protected InsertRpcMessage(InsertType insertType, SubmitInfo submitInfo, StorageChunk chunk) {
        this.insertType = insertType;
        this.submitInfo = submitInfo;
        this.storageChunk = chunk;
    }

    public InsertType getInsertType() {
        return insertType;
    }

    public SubmitInfo getSubmitInfo() {
        return submitInfo;
    }

    public StorageChunk getStorageChunk() {
        return storageChunk;
    }

    public RowSetNavigator getRowSetNavigator() {
        return rowSetNavigator;
    }

    public RexNode getRexNode() {
        return rexNode;
    }

    public int[] getColumnIndexs() {
        return columnIndexs;
    }

    public boolean isReturnData() {
        return returnData;
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public PallasDmlSymbol getPallasDmlSymbol() {
        return pallasDmlSymbol;
    }

    public boolean isIspstmt() {
        return ispstmt;
    }

    public void setIspstmt(boolean ispstmt) {
        this.ispstmt = ispstmt;
    }

}
