package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;

import com.datapps.linkoopdb.jdbc.types.LobLocation;
import com.datapps.linkoopdb.worker.spi.plan.core.LdbPallasSource.WriteMode;

/**
 * Created by gloway on 2019/7/29.
 */
public class SubmitInfo implements Serializable{
    long sessionId;
    long statementId;
    long transactionId;
    LobLocation logLobLocation;
    public boolean checkUniqueRow = true; // for pallas update
    private boolean dmlWithCount = true;
    private boolean isSimpleCount = false;
    private boolean isSimpleQuery = true;
    boolean statsTablePallas = false;
    private WriteMode pallasWriteMode;
    private int pallasMaxBufferSize;
    private boolean isLoadInTxn = false;

    public SubmitInfo(long sessionId, long statementId, long transactionId, LobLocation logLobLocation) {
        this.sessionId = sessionId;
        this.statementId = statementId;
        this.transactionId = transactionId;
        this.logLobLocation = logLobLocation;
    }

    public SubmitInfo(long sessionId, long statementId, long transactionId, LobLocation logLobLocation, boolean dmlWithCount) {
        this.sessionId = sessionId;
        this.statementId = statementId;
        this.transactionId = transactionId;
        this.logLobLocation = logLobLocation;
        this.dmlWithCount = dmlWithCount;
    }

    public SubmitInfo(long sessionId, long statementId) {
        this(sessionId, statementId, -1, null);
    }

    public SubmitInfo(long sessionId, long statementId, boolean dmlWithCount) {
        this(sessionId, statementId, -1, null, dmlWithCount);
    }

    public SubmitInfo(long sessionId, long statementId, long transactionId) {
        this(sessionId, statementId, transactionId, null);
    }

    public SubmitInfo(long sessionId, long statementId, long transactionId, WriteMode writeMode, int pallasMaxBufferSize, boolean isLoadInTxn) {
        this(sessionId, statementId, transactionId, null);
        this.pallasWriteMode = writeMode;
        this.pallasMaxBufferSize = pallasMaxBufferSize;
        this.isLoadInTxn = isLoadInTxn;
    }

    public SubmitInfo(long sessionId, long statementId, long transactionId, boolean dmlWithCount, WriteMode writeMode,
        int pallasMaxBufferSize, boolean isLoadInTxn) {
        this(sessionId, statementId, transactionId, null, dmlWithCount);
        this.pallasWriteMode = writeMode;
        this.pallasMaxBufferSize = pallasMaxBufferSize;
        this.isLoadInTxn = isLoadInTxn;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public long getStatementId() {
        return statementId;
    }

    public void setStatementId(long statementId) {
        this.statementId = statementId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public LobLocation getLogLobLocation() {
        return logLobLocation;
    }

    public void setLogLobLocation(LobLocation logLobLocation) {
        this.logLobLocation = logLobLocation;
    }

    public boolean isDmlWithCount() {
        return dmlWithCount;
    }

    public void setDmlWithCount(boolean dmlWithCount) {
        this.dmlWithCount = dmlWithCount;
    }

    public boolean isSimpleCount() {
        return isSimpleCount;
    }

    public boolean isSimpleQuery() {
        return isSimpleQuery;
    }

    public void setSimpleCount(boolean simpleCount) {
        isSimpleCount = simpleCount;
    }

    public void setSimpleQuery(boolean simpleQuery) {
        isSimpleQuery = simpleQuery;
    }

    public boolean isStatsTablePallas() {
        return statsTablePallas;
    }

    public void setStatsTablePallas(boolean statsTablePallas) {
        this.statsTablePallas = statsTablePallas;
    }

    public WriteMode getPallasWriteMode() {
        return pallasWriteMode;
    }

    public void setPallasWriteMode(WriteMode pallasWriteMode) {
        this.pallasWriteMode = pallasWriteMode;
    }

    public int getPallasMaxBufferSize() {
        return pallasMaxBufferSize;
    }

    public void setPallasMaxBufferSize(int pallasMaxBufferSize) {
        this.pallasMaxBufferSize = pallasMaxBufferSize;
    }

    public boolean isLoadInTxn() {
        return isLoadInTxn;
    }

    public void setLoadInTxn(boolean loadInTxn) {
        isLoadInTxn = loadInTxn;
    }
}
