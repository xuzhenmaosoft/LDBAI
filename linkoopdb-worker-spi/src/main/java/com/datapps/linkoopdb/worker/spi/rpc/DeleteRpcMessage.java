package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.PallasDmlSymbol;
import com.datapps.linkoopdb.worker.spi.ShardsChunk;
import com.datapps.linkoopdb.worker.spi.StorageChunk;
import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;

/**
 * Created by gloway on 2019/9/3.
 */
public class DeleteRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.DELETE;
    }

    public enum DeleteType {
        DELETE_PALLAS, SYSTEM_DELETE
    }

    private DeleteType deleteType;

    SubmitInfo submitInfo;
    RelNode relNode;
    StorageChunk chunk;
    boolean returnData;
    PallasDmlSymbol pallasDmlSymbol;

    Object[] values;
    int[] columnIndexs;
    Type[] types;
    int[] lobCols;
    int columnNum;
    int delPallasIdPos;

    public static DeleteRpcMessage buildDeleteMessage(SubmitInfo submitInfo,
        RelNode relNode, StorageChunk chunk, boolean returnData, PallasDmlSymbol pallasDmlSymbol) {
        DeleteRpcMessage updateRpcMessage = new DeleteRpcMessage();
        updateRpcMessage.deleteType = DeleteType.DELETE_PALLAS;
        updateRpcMessage.submitInfo = submitInfo;
        updateRpcMessage.relNode = relNode;
        updateRpcMessage.chunk = chunk;
        updateRpcMessage.returnData = returnData;
        updateRpcMessage.pallasDmlSymbol = pallasDmlSymbol;
        return updateRpcMessage;
    }

    public static DeleteRpcMessage buildSysDeleteMessage(SubmitInfo submitInfo,
        ShardsChunk chunk, Object[] values, int[] columnIndexs, Type[] types, int[] lobCols, int columnNum, int delPallasIdPos) {
        DeleteRpcMessage updateRpcMessage = new DeleteRpcMessage();
        updateRpcMessage.deleteType = DeleteType.SYSTEM_DELETE;
        updateRpcMessage.submitInfo = submitInfo;
        updateRpcMessage.chunk = chunk;
        updateRpcMessage.values = values;
        updateRpcMessage.columnIndexs = columnIndexs;
        updateRpcMessage.types = types;
        updateRpcMessage.lobCols = lobCols;
        updateRpcMessage.columnNum = columnNum;
        updateRpcMessage.delPallasIdPos = delPallasIdPos;
        return updateRpcMessage;
    }

    private DeleteRpcMessage() {

    }

    public SubmitInfo getSubmitInfo() {
        return submitInfo;
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public StorageChunk getChunk() {
        return chunk;
    }

    public boolean isReturnData() {
        return returnData;
    }

    public PallasDmlSymbol getPallasDmlSymbol() {
        return pallasDmlSymbol;
    }

    public DeleteType getDeleteType() {
        return deleteType;
    }

    public Object[] getValues() {
        return values;
    }

    public int[] getColumnIndexs() {
        return columnIndexs;
    }

    public Type[] getTypes() {
        return types;
    }

    public int[] getLobCols() {
        return lobCols;
    }

    public int getColumnNum() {
        return columnNum;
    }

    public int getDelPallasIdPos() {
        return delPallasIdPos;
    }
}
