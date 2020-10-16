package com.datapps.linkoopdb.worker.spi.rpc;

import static com.datapps.linkoopdb.worker.spi.rpc.RpcMessage.RpcMessageType.PROCESS_STREAM_TRUNK;

public class StreamWriteTrunkMessage implements RpcMessage {

    private String streamTableSchema;
    private String streamTableId;
    private String dbTableSchema;
    private String tableId;

    @Override
    public RpcMessageType getMessageType() {
        return PROCESS_STREAM_TRUNK;
    }

    public static StreamWriteTrunkMessage buildStreamWriteTrunkMessage(String streamTableSchema, String streamTableId, String dbTableSchema, String tableId) {
        StreamWriteTrunkMessage streamWriteTrunkMessage = new StreamWriteTrunkMessage();
        streamWriteTrunkMessage.streamTableSchema = streamTableSchema;
        streamWriteTrunkMessage.streamTableId = streamTableId;
        streamWriteTrunkMessage.dbTableSchema = dbTableSchema;
        streamWriteTrunkMessage.tableId = tableId;
        return streamWriteTrunkMessage;
    }

    private StreamWriteTrunkMessage() {

    }

    public String getStreamTableSchema() {
        return streamTableSchema;
    }

    public String getStreamTableId() {
        return streamTableId;
    }

    public String getDbTableSchema() {
        return dbTableSchema;
    }

    public String getTableId() {
        return tableId;
    }
}
