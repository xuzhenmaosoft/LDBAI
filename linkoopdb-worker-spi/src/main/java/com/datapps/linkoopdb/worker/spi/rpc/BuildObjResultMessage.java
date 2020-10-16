package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.HashMap;
import java.util.Map;

import com.datapps.linkoopdb.jdbc.Row;
import com.datapps.linkoopdb.worker.spi.MetaDataRowSet;
import com.datapps.linkoopdb.worker.spi.SimpleRowSet;

/**
 * @author xingbu
 * @version 1.0
 *
 * created by　19-10-24 上午11:32
 */
public class BuildObjResultMessage implements RpcMessage {

    /**
     * Metadata for building ldb object
     */
    SimpleRowSet metaRowSet;

    /**
     * Other parameters for building ldb object
     */
    Map<String, Object> otherParameters;

    public static BuildObjResultMessage buildBuildObjResultMessage(Row[] rows) {
        return buildBuildObjResultMessage(rows, new HashMap<>(0));
    }

    public static BuildObjResultMessage buildBuildObjResultMessage(Row[] rows, Map<String, Object> otherParameters) {
        BuildObjResultMessage buildObjResultMessage = new BuildObjResultMessage();
        buildObjResultMessage.metaRowSet = new MetaDataRowSet(rows);
        return buildObjResultMessage;
    }

    private BuildObjResultMessage() {
    }

    public SimpleRowSet getMetaRowSet() {
        return metaRowSet;
    }

    public Map<String, Object> getOtherParameters() {
        return otherParameters;
    }

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.BUILD_DB_OBJECT_RESULT;
    }
}
