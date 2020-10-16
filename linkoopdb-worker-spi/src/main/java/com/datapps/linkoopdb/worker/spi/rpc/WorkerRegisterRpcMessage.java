package com.datapps.linkoopdb.worker.spi.rpc;

/**
 * Created by gloway on 2019/9/19.
 */
public class WorkerRegisterRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return messageType;
    }

    RpcMessageType messageType;

    String workerId;
    String workerUrl;

    public static WorkerRegisterRpcMessage buildBatchRegisterMessage(String workerId, String workerUrl) {
        WorkerRegisterRpcMessage workerRegisterRpcMessage = new WorkerRegisterRpcMessage();
        workerRegisterRpcMessage.workerId = workerId;
        workerRegisterRpcMessage.workerUrl = workerUrl;
        workerRegisterRpcMessage.messageType = RpcMessageType.SPARK_WORKER_REGISTER;
        return workerRegisterRpcMessage;
    }

    public static WorkerRegisterRpcMessage buildStreamRegisterMessage(String workerId, String workerUrl) {
        WorkerRegisterRpcMessage workerRegisterRpcMessage = new WorkerRegisterRpcMessage();
        workerRegisterRpcMessage.workerId = workerId;
        workerRegisterRpcMessage.workerUrl = workerUrl;
        workerRegisterRpcMessage.messageType = RpcMessageType.FLINK_WORKER_REGISTER;
        return workerRegisterRpcMessage;
    }

    public static WorkerRegisterRpcMessage buildStreamRegisterDebugMessage(String workerId, String workerUrl) {
        WorkerRegisterRpcMessage workerRegisterRpcMessage = new WorkerRegisterRpcMessage();
        workerRegisterRpcMessage.workerId = workerId;
        workerRegisterRpcMessage.workerUrl = workerUrl;
        workerRegisterRpcMessage.messageType = RpcMessageType.FLINK_WORKER_REGISTER_DEBUG;
        return workerRegisterRpcMessage;
    }

    private WorkerRegisterRpcMessage() {

    }

    public String getWorkerId() {
        return workerId;
    }

    public String getWorkerUrl() {
        return workerUrl;
    }
}
