package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.worker.spi.IncrementalChunk;
import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;

/**
 * Created by gloway on 2019/9/3.
 */
public class CompactRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.COMPACT;
    }

    private SubmitInfo submitInfo;
    private IncrementalChunk compactedChunk;
    private RelNode relNode;
    private int outputParallel;

    public static CompactRpcMessage buildCompactMessage(SubmitInfo submitInfo, IncrementalChunk compactedChunk, RelNode relNode, int outputParallel) {
        CompactRpcMessage compactRpcMessage = new CompactRpcMessage();
        compactRpcMessage.submitInfo = submitInfo;
        compactRpcMessage.compactedChunk = compactedChunk;
        compactRpcMessage.relNode = relNode;
        compactRpcMessage.outputParallel = outputParallel;
        return compactRpcMessage;
    }

    private CompactRpcMessage() {

    }

    public SubmitInfo getSubmitInfo() {
        return submitInfo;
    }

    public IncrementalChunk getCompactedChunk() {
        return compactedChunk;
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public int getOutputParallel() {
        return outputParallel;
    }
}
