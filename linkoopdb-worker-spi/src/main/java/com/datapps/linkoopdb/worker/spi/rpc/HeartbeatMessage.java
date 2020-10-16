package com.datapps.linkoopdb.worker.spi.rpc;

/**
 * Created by gloway on 2019/9/19.
 */
public class HeartbeatMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.HEARTBEAT;
    }

    String workerId;

    public static HeartbeatMessage buildHeartBeatMessage(String workerId) {
        HeartbeatMessage heartbeatMessage = new HeartbeatMessage();
        heartbeatMessage.workerId = workerId;
        return heartbeatMessage;
    }

    private HeartbeatMessage() {

    }

    public String getWorkerId() {
        return workerId;
    }
}
