package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datapps.linkoopdb.jdbc.Row;
import com.datapps.linkoopdb.jdbc.types.LobLocation;
import com.datapps.linkoopdb.jdbc.types.RowType;
import com.datapps.linkoopdb.worker.spi.LocatedShardWriteState;
import com.datapps.linkoopdb.worker.spi.RowSet;
import com.datapps.linkoopdb.worker.spi.SimpleRowSet;

/**
 * Created by gloway on 2019/9/4.
 */
public class DmlResultRpcMessage implements RpcMessage {

    RpcMessageType type;

    @Override
    public RpcMessageType getMessageType() {
        return type;
    }

    long affectCount;

    // 有返回更新的数据
    RowSet partResult;
    int[] updateCols;

    // 返回lob信息
    Map<Long, LobLocation> lobLocationMap = Collections.emptyMap();
    List<Long> updateLdbLobIds;

    Set<LocatedShardWriteState> lshardWriteStates = null;

    private String jobId;


    public static DmlResultRpcMessage buildInsertResultWithDataMessage(RowType rowType, String[] columns, Row[] rows, Map<Long, LobLocation> lobLocationMap) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.INSERT_RESULT;
        insertRpcMessage.partResult = new SimpleRowSet(rowType, columns, rows);
        insertRpcMessage.affectCount = rows.length;
        insertRpcMessage.lobLocationMap = lobLocationMap;
        return insertRpcMessage;
    }

    public static DmlResultRpcMessage buildInsertResultWithDataMessage(RowType rowType, String[] columns,
        Row[] rows, Map<Long, LobLocation> lobLocationMap, Set<LocatedShardWriteState> lshardWriteStates) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.INSERT_RESULT;
        insertRpcMessage.partResult = new SimpleRowSet(rowType, columns, rows);
        insertRpcMessage.affectCount = rows.length;
        insertRpcMessage.lobLocationMap = lobLocationMap;
        insertRpcMessage.lshardWriteStates = lshardWriteStates;
        return insertRpcMessage;
    }

    public static DmlResultRpcMessage buildInsertResultWithoutDataMessage(long affectCount) {
        return buildInsertResultWithoutDataMessage(affectCount, null);
    }

    public static DmlResultRpcMessage buildInsertResultWithoutDataMessage(long affectCount, Map<Long, LobLocation> lobLocationMap) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.INSERT_RESULT;
        insertRpcMessage.affectCount = affectCount;
        insertRpcMessage.lobLocationMap = lobLocationMap;
        return insertRpcMessage;
    }

    public static DmlResultRpcMessage buildInsertResultWithoutDataMessage(long affectCount,
        Map<Long, LobLocation> lobLocationMap, Set<LocatedShardWriteState> lshardWriteStates) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.INSERT_RESULT;
        insertRpcMessage.affectCount = affectCount;
        insertRpcMessage.lobLocationMap = lobLocationMap;
        insertRpcMessage.lshardWriteStates = lshardWriteStates;
        return insertRpcMessage;
    }

    public static DmlResultRpcMessage buildUpdateResultWithDataMessage(RowType rowType, String[] columns, Row[] rows, long affectCount, int[] updateCols) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.UPDATE_RESULT;
        insertRpcMessage.partResult = new SimpleRowSet(rowType, columns, rows);
        insertRpcMessage.affectCount = affectCount;
        insertRpcMessage.updateCols = updateCols;
        return insertRpcMessage;
    }

    public static DmlResultRpcMessage buildUpdateResultWithoutDataMessage(long affectCount, Map<Long, LobLocation> lobLocationMap,
        Set<LocatedShardWriteState> lshardWriteStates) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.UPDATE_RESULT;
        insertRpcMessage.affectCount = affectCount;
        insertRpcMessage.lobLocationMap = lobLocationMap == null ? Collections.EMPTY_MAP : lobLocationMap;
        insertRpcMessage.lshardWriteStates = lshardWriteStates == null ? Collections.EMPTY_SET : lshardWriteStates;
        return insertRpcMessage;
    }

    public static DmlResultRpcMessage buildMergeResultDataMessage(long affectCount, Map<Long, LobLocation> lobLocationMap, List<Long> ldbLobIds) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.MERGE_RESULT;
        insertRpcMessage.affectCount = affectCount;
        insertRpcMessage.lobLocationMap = lobLocationMap;
        insertRpcMessage.updateLdbLobIds = ldbLobIds;
        return insertRpcMessage;
    }

    public static DmlResultRpcMessage buildDeleteResultWithDataMessage(RowType rowType, String[] columns, Row[] rows, long affectCount) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.DELETE_RESULT;
        insertRpcMessage.partResult = new SimpleRowSet(rowType, columns, rows);
        insertRpcMessage.affectCount = affectCount;
        return insertRpcMessage;
    }

    public static DmlResultRpcMessage buildDeleteResultWithoutDataMessage(long affectCount) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.DELETE_RESULT;
        insertRpcMessage.affectCount = affectCount;
        return insertRpcMessage;
    }

    public static DmlResultRpcMessage buildStartSyncJobMessage(String jobId) {
        DmlResultRpcMessage insertRpcMessage = new DmlResultRpcMessage();
        insertRpcMessage.type = RpcMessageType.START_SYNC_JOB_RESULT;
        insertRpcMessage.jobId = jobId;
        return insertRpcMessage;
    }


    private DmlResultRpcMessage() {

    }


    public RowSet getPartResult() {
        return partResult;
    }

    public long getAffectCount() {
        return affectCount;
    }

    public int[] getUpdateCols() {
        return updateCols;
    }

    public RpcMessageType getType() {
        return type;
    }

    public Map<Long, LobLocation> getLobLocationMap() {
        return lobLocationMap;
    }

    public List<Long> getUpdateLdbLobIds() {
        return updateLdbLobIds;
    }

    public Set<LocatedShardWriteState> getLshardWriteStates() {
        return lshardWriteStates;
    }

    public void setLshardWriteStates(Set<LocatedShardWriteState> lshardWriteStates) {
        this.lshardWriteStates = lshardWriteStates;
    }

    public String getJobId() {
        return jobId;
    }
}
