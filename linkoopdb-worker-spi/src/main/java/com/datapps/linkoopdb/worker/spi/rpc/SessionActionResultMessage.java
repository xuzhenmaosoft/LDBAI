package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.List;
import java.util.Objects;

import com.datapps.linkoopdb.jdbc.lib.HashMappedList;
import com.datapps.linkoopdb.worker.spi.StreamJobInfo;

public class SessionActionResultMessage implements RpcMessage {

    private long sessionId;
    private String confKey;
    private String confValue;
    private StreamJobInfo streamJobInfo;

    private List<StreamJobInfo> jobInfos;

    private HashMappedList resetField;

    private String errorMessage;

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.SESSION_ACTION_RESULT;
    }

    public static SessionActionResultMessage instance() {
        return new SessionActionResultMessage();
    }

    public SessionActionResultMessage withSessionId(long sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public SessionActionResultMessage withConfKey(String confKey) {
        this.confKey = confKey;
        return this;
    }

    public SessionActionResultMessage withConfValue(String confValue) {
        this.confValue = confValue;
        return this;
    }

    public SessionActionResultMessage withErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }

    public SessionActionResultMessage withStreamJobInfo(StreamJobInfo streamJobInfo) {
        this.streamJobInfo = streamJobInfo;
        return this;
    }

    public SessionActionResultMessage withJobInfos(List<StreamJobInfo> jobInfos) {
        this.jobInfos = jobInfos;
        return this;
    }

    public SessionActionResultMessage withResetField(HashMappedList resetField) {
        this.resetField = resetField;
        return this;
    }

    public String getConfKey() {
        return confKey;
    }

    public String getConfValue() {
        return confValue;
    }

    public long getSessionId() {
        return sessionId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isError() {
        return this.errorMessage != null;
    }

    public StreamJobInfo getStreamJobInfo() {
        return streamJobInfo;
    }

    public List<StreamJobInfo> getJobInfos() {
        return jobInfos;
    }

    public HashMappedList getResetField() {
        return resetField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionActionResultMessage that = (SessionActionResultMessage) o;
        return sessionId == that.sessionId
            && Objects.equals(confKey, that.confKey)
            && Objects.equals(confValue, that.confValue)
            && Objects.equals(streamJobInfo, that.streamJobInfo)
            && Objects.equals(jobInfos, that.jobInfos)
            && Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, confKey, confValue, streamJobInfo, jobInfos, errorMessage);
    }

    @Override
    public String toString() {
        return "SessionActionResultMessage{"
            + "sessionId=" + sessionId
            + ", confKey='" + confKey + '\''
            + ", confValue='" + confValue + '\''
            + ", streamJobInfo=" + streamJobInfo
            + ", errorMessage='" + errorMessage + '\''
            + '}';
    }
}
