package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.worker.spi.StorageChunk;
import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbGraph;

/**
 * @author xingbu
 * @version 1.0
 *
 * created by　19-10-30 上午11:01
 */
public class BuildGraphRpcMessage extends BuildObjRpcMessage {

    StorageChunk vertexStorageChunk;
    StorageChunk edgeStorageChunk;

    private BuildGraphRpcMessage() {

    }

    public static BuildGraphRpcMessage buildGraphRpcMessage(SubmitInfo submitInfo, LdbGraph graph, StorageChunk vertexStorageChunk,
        StorageChunk edgeStorageChunk) {
        BuildGraphRpcMessage buildGraphRpcMessage = new BuildGraphRpcMessage();
        buildGraphRpcMessage.buildMessageType = BuildMessageType.BUILD_GRAPH;
        buildGraphRpcMessage.submitInfo = submitInfo;
        buildGraphRpcMessage.ldbObj = graph;
        buildGraphRpcMessage.vertexStorageChunk = vertexStorageChunk;
        buildGraphRpcMessage.edgeStorageChunk = edgeStorageChunk;
        return buildGraphRpcMessage;
    }

    public StorageChunk getVertexStorageChunk() {
        return vertexStorageChunk;
    }

    public StorageChunk getEdgeStorageChunk() {
        return edgeStorageChunk;
    }

}
