package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.jdbc.Row;
import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by gloway on 2019/9/4.
 */
public class BatchResultRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.BATCH_RESULT;
    }

    String iteratorId;
    String physicalPlanString;
    String[] columns;
    Type[] workerTypes;

    boolean hasNext;
    long executeTime;

    private Row[] rows;

    public static BatchResultRpcMessage buildQueryBatchResultMessage(String iteratorId, String[] columns, Type[] workerTypes, String physicalPlanString) {
        BatchResultRpcMessage batchRpcMessage = new BatchResultRpcMessage();
        batchRpcMessage.iteratorId = iteratorId;
        batchRpcMessage.columns = columns;
        batchRpcMessage.workerTypes = workerTypes;
        batchRpcMessage.physicalPlanString = physicalPlanString;
        return batchRpcMessage;
    }

    public static BatchResultRpcMessage buildGetNextBatchResultMessage(Row[] rows) {
        BatchResultRpcMessage batchRpcMessage = new BatchResultRpcMessage();
        batchRpcMessage.rows = rows;
        return batchRpcMessage;
    }

    public static BatchResultRpcMessage buildGetBatchResultMessage(Row[] rows, boolean hasNext, long executeTime) {
        BatchResultRpcMessage batchRpcMessage = new BatchResultRpcMessage();
        batchRpcMessage.rows = rows;
        batchRpcMessage.hasNext = hasNext;
        batchRpcMessage.executeTime = executeTime;
        return batchRpcMessage;
    }

    public static BatchResultRpcMessage buildHasBatchResultMessage(boolean hasNext) {
        BatchResultRpcMessage batchRpcMessage = new BatchResultRpcMessage();
        batchRpcMessage.hasNext = hasNext;
        return batchRpcMessage;
    }

    private BatchResultRpcMessage() {
    }

    public String getIteratorId() {
        return iteratorId;
    }

    public String getPhysicalPlanString() {
        return physicalPlanString;
    }

    public String[] getColumns() {
        return columns;
    }

    public Type[] getWorkerTypes() {
        return workerTypes;
    }

    public boolean isHasNext() {
        return hasNext;
    }

    public Row[] getRows() {
        return rows;
    }

    public long getExecuteTime() {
        return executeTime;
    }
}
