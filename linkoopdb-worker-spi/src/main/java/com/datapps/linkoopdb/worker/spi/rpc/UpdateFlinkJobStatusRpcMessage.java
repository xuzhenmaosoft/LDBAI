package com.datapps.linkoopdb.worker.spi.rpc;

/**
 * Created by gloway on 2019/9/19.
 */
public class UpdateFlinkJobStatusRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.UPDATE_FLINK_JOBSTATUS;
    }

    String statementId;
    String status;

    public static UpdateFlinkJobStatusRpcMessage buildUpdateFlinkJobStatusMessage(String statementId, String status) {
        UpdateFlinkJobStatusRpcMessage heartbeatMessage = new UpdateFlinkJobStatusRpcMessage();
        heartbeatMessage.statementId = statementId;
        heartbeatMessage.status = status;
        return heartbeatMessage;
    }

    private UpdateFlinkJobStatusRpcMessage() {

    }

    public String getStatementId() {
        return statementId;
    }

    public String getStatus() {
        return status;
    }
}
