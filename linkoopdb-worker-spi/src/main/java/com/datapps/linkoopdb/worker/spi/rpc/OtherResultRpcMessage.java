package com.datapps.linkoopdb.worker.spi.rpc;

/**
 * Created by gloway on 2019/9/4.
 */
public class OtherResultRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.OTHER_MESSAGE;
    }

    Object obj;

    public static OtherResultRpcMessage builOtherResultMessage(Object obj) {
        OtherResultRpcMessage otherResultRpcMessage = new OtherResultRpcMessage();
        otherResultRpcMessage.obj = obj;
        return otherResultRpcMessage;
    }

    private OtherResultRpcMessage() {

    }

    public Object getObj() {
        return obj;
    }
}
