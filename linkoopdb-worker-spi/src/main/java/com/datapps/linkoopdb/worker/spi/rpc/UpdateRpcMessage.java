package com.datapps.linkoopdb.worker.spi.rpc;

import com.datapps.linkoopdb.jdbc.types.Type;
import com.datapps.linkoopdb.worker.spi.PallasDmlSymbol;
import com.datapps.linkoopdb.worker.spi.StorageChunk;
import com.datapps.linkoopdb.worker.spi.SubmitInfo;
import com.datapps.linkoopdb.worker.spi.plan.core.RelNode;

/**
 * Created by gloway on 2019/9/3.
 */
public class UpdateRpcMessage implements RpcMessage {

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.UPDATE;
    }

    public enum UpdateType {
        UPDATE_PALLAS, SYSTEM_UPDATE
    }

    private UpdateType updateType;

    SubmitInfo submitInfo;
    int[] updateCols;
    RelNode relNode;
    StorageChunk chunk;
    boolean returnData;
    boolean hasSubQuery;
    PallasDmlSymbol pallasDmlSymbol;

    Object[] values;
    Type[] types;
    int[] columnMap;
    int[] lobCols;

    public static UpdateRpcMessage buildUpdateMessage(SubmitInfo submitInfo,
        int[] updateCols, RelNode relNode, StorageChunk chunk, boolean returnData, boolean hasSubQuery, PallasDmlSymbol pallasDmlSymbol) {
        UpdateRpcMessage updateRpcMessage = new UpdateRpcMessage();
        updateRpcMessage.updateType = UpdateType.UPDATE_PALLAS;
        updateRpcMessage.submitInfo = submitInfo;
        updateRpcMessage.updateCols = updateCols;
        updateRpcMessage.relNode = relNode;
        updateRpcMessage.chunk = chunk;
        updateRpcMessage.returnData = returnData;
        updateRpcMessage.hasSubQuery = hasSubQuery;
        updateRpcMessage.pallasDmlSymbol = pallasDmlSymbol;
        return updateRpcMessage;
    }

    public static UpdateRpcMessage buildSysUpdateMessage(SubmitInfo submitInfo, StorageChunk chunk, Object[] values, Type[] types, int[] columnMap,
        int[] lobCols) {
        UpdateRpcMessage updateRpcMessage = new UpdateRpcMessage();
        updateRpcMessage.updateType = UpdateType.SYSTEM_UPDATE;
        updateRpcMessage.submitInfo = submitInfo;
        updateRpcMessage.chunk = chunk;
        updateRpcMessage.values = values;
        updateRpcMessage.types = types;
        updateRpcMessage.columnMap = columnMap;
        updateRpcMessage.lobCols = lobCols;

        return updateRpcMessage;
    }

    private UpdateRpcMessage() {

    }

    public UpdateType getUpdateType() {
        return updateType;
    }

    public Object[] getValues() {
        return values;
    }

    public Type[] getTypes() {
        return types;
    }

    public int[] getColumnMap() {
        return columnMap;
    }

    public int[] getLobCols() {
        return lobCols;
    }

    public SubmitInfo getSubmitInfo() {
        return submitInfo;
    }

    public int[] getUpdateCols() {
        return updateCols;
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

    public boolean isHasSubQuery() {
        return hasSubQuery;
    }

    public PallasDmlSymbol getPallasDmlSymbol() {
        return pallasDmlSymbol;
    }
}
