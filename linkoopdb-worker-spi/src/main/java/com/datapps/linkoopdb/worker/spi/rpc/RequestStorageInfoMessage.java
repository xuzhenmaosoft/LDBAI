package com.datapps.linkoopdb.worker.spi.rpc;

/**
 * Created by gloway on 2019/9/19.
 */
public class RequestStorageInfoMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.GET_STORAGE_INFO;
    }

    public enum GetType {
        GET_SHARD_INFO
    }

    private GetType requestType;

    String tableName;

    public static RequestStorageInfoMessage buildGetShardInfoMessage(String tableName) {
        RequestStorageInfoMessage getStorageInfoMessage = new RequestStorageInfoMessage();
        getStorageInfoMessage.requestType = GetType.GET_SHARD_INFO;
        getStorageInfoMessage.tableName = tableName;
        return getStorageInfoMessage;
    }

    private RequestStorageInfoMessage() {

    }

    public GetType getRequestType() {
        return requestType;
    }

    public String getTableName() {
        return tableName;
    }
}
