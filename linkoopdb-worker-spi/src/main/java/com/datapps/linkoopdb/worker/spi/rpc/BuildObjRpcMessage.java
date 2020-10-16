package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbGraph;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbModel;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbPipeline;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;

/**
 * Created by gloway on 2019/9/3.
 */
public class BuildObjRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.BUILD_DB_OBJECT;
    }

    public enum BuildMessageType {
        TRAIN_MODEL, BUILD_GRAPH_SIMPLE, BUILD_GRAPH, BUILD_PIPELINE
    }

    protected BuildMessageType buildMessageType;

    SubmitInfo submitInfo;
    RelNode ldbObj;

    public static BuildObjRpcMessage buildTrainModelRpcMessage(SubmitInfo submitInfo, LdbModel ldbModel) {
        BuildObjRpcMessage buildObjRpcMessage = new BuildObjRpcMessage();
        buildObjRpcMessage.buildMessageType = BuildMessageType.TRAIN_MODEL;
        buildObjRpcMessage.submitInfo = submitInfo;
        buildObjRpcMessage.ldbObj = ldbModel;
        return buildObjRpcMessage;
    }

    public static BuildObjRpcMessage buildGraphSimpleRpcMessage(SubmitInfo submitInfo, LdbGraph ldbGraph) {
        BuildObjRpcMessage buildObjRpcMessage = new BuildObjRpcMessage();
        buildObjRpcMessage.buildMessageType = BuildMessageType.BUILD_GRAPH_SIMPLE;
        buildObjRpcMessage.submitInfo = submitInfo;
        buildObjRpcMessage.ldbObj = ldbGraph;
        return buildObjRpcMessage;
    }

    public static BuildObjRpcMessage buildPipelineRpcMessage(SubmitInfo submitInfo, LdbPipeline ldbPipeline) {
        BuildObjRpcMessage buildObjRpcMessage = new BuildObjRpcMessage();
        buildObjRpcMessage.buildMessageType = BuildMessageType.BUILD_PIPELINE;
        buildObjRpcMessage.submitInfo = submitInfo;
        buildObjRpcMessage.ldbObj = ldbPipeline;
        return buildObjRpcMessage;
    }

    protected BuildObjRpcMessage() {

    }

    public SubmitInfo getSubmitInfo() {
        return submitInfo;
    }

    public BuildMessageType getBuildMessageType() {
        return buildMessageType;
    }

    public RelNode getLdbObj() {
        return ldbObj;
    }
}
