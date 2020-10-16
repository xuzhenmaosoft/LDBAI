package com.datapps.linkoopdb.worker.spi.rpc;

import java.io.Serializable;

/**
 * Created by gloway on 2019/9/3.
 */
public interface RpcMessage extends Serializable {

    RpcMessage empty = new RpcMessage() {
        @Override
        public RpcMessageType getMessageType() {
            return RpcMessageType.EMPTY_MESSAGE;
        }
    };

    enum RpcMessageType {
        // server message
        SESSION_ACTION, SESSION_ACTION_RESULT, INSERT, COMPACT, BATCH_ACTION,
        TABLE_STATISTICS, REGISTER_FUNCTION, SET_WORKER, DFS_INDEX_ACTION,
        REQUEST_WORKER_INFO, QUERY, DELETE, UPDATE, MERGE_INTO, BUILD_DB_OBJECT,

        // worker message
        QUERY_RESULT, INSERT_RESULT, UPDATE_RESULT, DELETE_RESULT, BATCH_RESULT, HEARTBEAT,
        BUILD_DB_OBJECT_RESULT, START_SYNC_JOB_RESULT,

        // worker to server message
        SPARK_WORKER_REGISTER, FLINK_WORKER_REGISTER, FLINK_WORKER_REGISTER_DEBUG, GET_STORAGE_INFO, HEARTBEAT_RESPONSE, UPDATE_FLINK_JOBSTATUS,

        MERGE_RESULT, EMPTY_MESSAGE, OTHER_MESSAGE, ERROR_MESSAGE,

        // other
        PROCESS_STREAM_TRUNK
    }

    RpcMessageType getMessageType();
}
