package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;

/**
 * Created by gloway on 2019/9/3.
 */
public class BatchRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.BATCH_ACTION;
    }

    public enum BatchMessageType {
        QUERY_BATCH, HAS_BATCH, GET_NEXT_BATCH, GET_BATCH, RELEASE
    }

    private BatchMessageType batchMessageType;

    private SubmitInfo submitInfo;
    private RelNode relNode;
    private String iteratorId;
    private long sessionId;
    private int size;

    public static BatchRpcMessage buildQueryBatchMessage(SubmitInfo submitInfo, RelNode relNode) {
        BatchRpcMessage batchRpcMessage = new BatchRpcMessage();
        batchRpcMessage.batchMessageType = BatchMessageType.QUERY_BATCH;
        batchRpcMessage.submitInfo = submitInfo;
        batchRpcMessage.relNode = relNode;
        return batchRpcMessage;
    }

    public static BatchRpcMessage buildHasBatchMessage(String iteratorId) {
        BatchRpcMessage batchRpcMessage = new BatchRpcMessage();
        batchRpcMessage.batchMessageType = BatchMessageType.HAS_BATCH;
        batchRpcMessage.iteratorId = iteratorId;
        return batchRpcMessage;
    }

    public static BatchRpcMessage buildGetNextBatchMessage(String iteratorId, int size) {
        BatchRpcMessage batchRpcMessage = new BatchRpcMessage();
        batchRpcMessage.batchMessageType = BatchMessageType.GET_NEXT_BATCH;
        batchRpcMessage.iteratorId = iteratorId;
        batchRpcMessage.size = size;
        return batchRpcMessage;
    }

    public static BatchRpcMessage buildGetBatchMessage(String iteratorId, int size) {
        BatchRpcMessage batchRpcMessage = new BatchRpcMessage();
        batchRpcMessage.batchMessageType = BatchMessageType.GET_BATCH;
        batchRpcMessage.iteratorId = iteratorId;
        batchRpcMessage.size = size;
        return batchRpcMessage;
    }

    public static BatchRpcMessage buildReleaseBatchMessage(long sessionId, String iteratorId) {
        BatchRpcMessage batchRpcMessage = new BatchRpcMessage();
        batchRpcMessage.batchMessageType = BatchMessageType.RELEASE;
        batchRpcMessage.iteratorId = iteratorId;
        batchRpcMessage.sessionId = sessionId;
        return batchRpcMessage;
    }


    private BatchRpcMessage() {
    }

    public BatchMessageType getBatchMessageType() {
        return batchMessageType;
    }

    public SubmitInfo getSubmitInfo() {
        return submitInfo;
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public String getIteratorId() {
        return iteratorId;
    }

    public long getSessionId() {
        return sessionId;
    }

    public int getSize() {
        return size;
    }
}
