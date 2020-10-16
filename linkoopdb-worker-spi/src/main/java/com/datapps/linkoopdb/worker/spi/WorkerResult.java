/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.datapps.linkoopdb.jdbc.types.LobLocation;
import com.datapps.linkoopdb.jdbc.types.Type;

public class WorkerResult implements Serializable {

    public static final long NO_TRACABLE_ID = -1;

    public long id;
    public long sessionId;
    public long affectCount;
    public RowSet partResult;
    public boolean remain;
    public String tableName;
    public String schemaName;
    public String explainPlan;
    public String status;

    public String iteratorId;
    public String[] columns;
    public Type[] workerTypes;

    // 用于Insert key为ldbLobId
    public Map<Long, LobLocation> lobLocationMap = Collections.EMPTY_MAP;

    // 用于update
    public int[] updateCols;

    public Set<LocatedShardWriteState> lshardWriteStates = null;

    public WorkerResult() {
    }

    public WorkerResult(long id, long sessionId, long affectCount, RowSet partResult, boolean remain) {
        this.id = id;
        this.sessionId = sessionId;
        this.affectCount = affectCount;
        this.partResult = partResult;
        this.remain = remain;
        if (partResult != null) {
            this.columns = partResult.getColumns();
            if (partResult.getRowType() != null) {
                this.workerTypes = partResult.getRowType().colTypes;
            }
        }
    }

    // 返回插入数据的值
    public WorkerResult(long id, long sessionId, long affectCount, RowSet partResult, int[] updateCols) {
        this.id = id;
        this.sessionId = sessionId;
        this.affectCount = affectCount;
        this.partResult = partResult;
        if (partResult != null) {
            this.columns = partResult.getColumns();
            if (partResult.getRowType() != null) {
                this.workerTypes = partResult.getRowType().colTypes;
            }
        }
        this.updateCols = updateCols;
    }

    // 返回更新数据的值
    public WorkerResult(long id, long sessionId, long affectCount, RowSet partResult,
        Map<Long, LobLocation> lobLocationMap, Set<LocatedShardWriteState> states) {
        this.id = id;
        this.sessionId = sessionId;
        this.affectCount = affectCount;
        this.partResult = partResult;
        if (partResult != null) {
            this.columns = partResult.getColumns();
            if (partResult.getRowType() != null) {
                this.workerTypes = partResult.getRowType().colTypes;
            }
        }
        this.lobLocationMap = lobLocationMap;
        this.lshardWriteStates = states;
    }

    public WorkerResult(long id, long sessionId, long affectCount, RowSet partResult) {
        this.id = id;
        this.sessionId = sessionId;
        this.affectCount = affectCount;
        this.partResult = partResult;
        if (partResult != null) {
            this.columns = partResult.getColumns();
            if (partResult.getRowType() != null) {
                this.workerTypes = partResult.getRowType().colTypes;
            }
        }
    }

    // 用于查询返回物理计划
    public WorkerResult(long id, long sessionId, long affectCount, RowSet partResult, boolean remain, String explainPlan) {
        this.id = id;
        this.sessionId = sessionId;
        this.affectCount = affectCount;
        this.partResult = partResult;
        this.remain = remain;
        this.explainPlan = explainPlan;
        if (partResult != null) {
            this.columns = partResult.getColumns();
            if (partResult.getRowType() != null) {
                this.workerTypes = partResult.getRowType().colTypes;
            }
        }
    }

    public static WorkerResult buildEmptyResult(long id, long sessionId) {
        return new WorkerResult(id, sessionId, 0,
            new SimpleRowSet(null, null, new com.datapps.linkoopdb.jdbc.Row[0]), false);
    }

    public static WorkerResult buildCountResult(long id, long sessionId, long count) {
        return new WorkerResult(id, sessionId, count,
            new SimpleRowSet(null, null, new com.datapps.linkoopdb.jdbc.Row[0]), false);
    }

    public Set<LocatedShardWriteState> getLshardWriteStates() {
        return lshardWriteStates;
    }

    public void setLshardWriteStates(Set<LocatedShardWriteState> lshardWriteStates) {
        this.lshardWriteStates = lshardWriteStates;
    }
}
