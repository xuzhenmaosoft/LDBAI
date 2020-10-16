package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.worker.spi.rpc.RpcMessage.RpcMessageType;

/**
 * Created by gloway on 2019/9/19.
 */
public class HeartBeatResponse implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.HEARTBEAT_RESPONSE;
    }

    boolean isMainDb;

    public static HeartBeatResponse buildHeartBeatResponseMessage(boolean isMainDb) {
        HeartBeatResponse heartbeatMessage = new HeartBeatResponse();
        heartbeatMessage.isMainDb = isMainDb;
        return heartbeatMessage;
    }

    private HeartBeatResponse() {

    }

    public boolean isMainDb() {
        return isMainDb;
    }
}
