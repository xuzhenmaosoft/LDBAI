package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.Map;

/**
 * Created by gloway on 2019/9/3.
 */
public class RequestInfoRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.REQUEST_WORKER_INFO;
    }

    private RequestInfoMessageType requestInfoMessageType;

    public enum RequestInfoMessageType {
        WORKERID, JOBSTATUS, AI_SERVICE_URL, TABLE_INFO, PARQUET_META
    }

    Map<String, String> parameters;
    int type;

    String location;

    public static RequestInfoRpcMessage buildRequestWorkerId() {
        RequestInfoRpcMessage requestInfoRpcMessage = new RequestInfoRpcMessage();
        requestInfoRpcMessage.requestInfoMessageType = RequestInfoMessageType.WORKERID;
        return requestInfoRpcMessage;
    }

    public static RequestInfoRpcMessage buildRequestJobStatus() {
        RequestInfoRpcMessage requestInfoRpcMessage = new RequestInfoRpcMessage();
        requestInfoRpcMessage.requestInfoMessageType = RequestInfoMessageType.JOBSTATUS;
        return requestInfoRpcMessage;
    }

    public static RequestInfoRpcMessage buildRequestAiService() {
        RequestInfoRpcMessage requestInfoRpcMessage = new RequestInfoRpcMessage();
        requestInfoRpcMessage.requestInfoMessageType = RequestInfoMessageType.AI_SERVICE_URL;
        return requestInfoRpcMessage;
    }

    public static RequestInfoRpcMessage buildRequestTableInfoService(Map<String, String> parameters, int type) {
        RequestInfoRpcMessage requestInfoRpcMessage = new RequestInfoRpcMessage();
        requestInfoRpcMessage.requestInfoMessageType = RequestInfoMessageType.TABLE_INFO;
        requestInfoRpcMessage.parameters = parameters;
        requestInfoRpcMessage.type = type;
        return requestInfoRpcMessage;
    }

    public static RequestInfoRpcMessage buildParquetMetaService(String location) {
        RequestInfoRpcMessage requestInfoRpcMessage = new RequestInfoRpcMessage();
        requestInfoRpcMessage.requestInfoMessageType = RequestInfoMessageType.PARQUET_META;
        requestInfoRpcMessage.location = location;
        return requestInfoRpcMessage;
    }

    private RequestInfoRpcMessage() {

    }

    public String getLocation() {
        return location;
    }

    public RequestInfoMessageType getRequestInfoMessageType() {
        return requestInfoMessageType;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public int getType() {
        return type;
    }
}
