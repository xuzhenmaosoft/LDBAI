package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.worker.spi.PallasDmlSymbol;
import com.datapps.linkoopdb.worker.spi.StorageChunk;
import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;
import com.datapps.linkoopdb.worker.spi.plan.expression.RexNode;

/**
 * Created by gloway on 2019/9/3.
 */
public class MergeIntoRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.MERGE_INTO;
    }

    SubmitInfo submitInfo;
    RelNode insert;
    RelNode updateOrDelete;
    RexNode insertRexNode;
    int[] updateCols;
    int[] insertCols;
    StorageChunk chunk;
    boolean hasSubQuery;
    PallasDmlSymbol symbol;

    public static MergeIntoRpcMessage buildMergeIntoMessage(SubmitInfo submitInfo, RelNode insert, RelNode updateOrDelete,
        RexNode insertRexNode, int[] updateCols, int[] insertCols, StorageChunk chunk, boolean hasSubQuery, PallasDmlSymbol symbol) {
        MergeIntoRpcMessage updateRpcMessage = new MergeIntoRpcMessage();
        updateRpcMessage.submitInfo = submitInfo;
        updateRpcMessage.insert = insert;
        updateRpcMessage.insertRexNode = insertRexNode;
        updateRpcMessage.updateOrDelete = updateOrDelete;
        updateRpcMessage.updateCols = updateCols;
        updateRpcMessage.insertCols = insertCols;
        updateRpcMessage.chunk = chunk;
        updateRpcMessage.hasSubQuery = hasSubQuery;
        updateRpcMessage.symbol = symbol;
        return updateRpcMessage;
    }

    private MergeIntoRpcMessage() {

    }

    public SubmitInfo getSubmitInfo() {
        return submitInfo;
    }

    public RelNode getInsert() {
        return insert;
    }

    public RelNode getUpdateOrDelete() {
        return updateOrDelete;
    }

    public RexNode getInsertRexNode() {
        return insertRexNode;
    }

    public int[] getUpdateCols() {
        return updateCols;
    }

    public int[] getInsertCols() {
        return insertCols;
    }

    public StorageChunk getChunk() {
        return chunk;
    }

    public boolean isHasSubQuery() {
        return hasSubQuery;
    }

    public PallasDmlSymbol getSymbol() {
        return symbol;
    }
}
