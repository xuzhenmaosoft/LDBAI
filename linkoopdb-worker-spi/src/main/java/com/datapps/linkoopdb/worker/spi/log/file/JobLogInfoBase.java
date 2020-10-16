package com.datapps.linkoopdb.worker.spi.log.file;

import java.io.Serializable;
import java.util.Objects;

import org.apache.log4j.MDC;
import org.slf4j.Logger;

public class JobLogInfoBase implements Serializable {

    public static final String JOB_LOG_TAG = "JOB_LOG";

    private String sessionId;
    private String taskGroupId;
    private String statementId;
    /**
     * begin or end logging flag.
     */
    private boolean openFlag = true;
    /**
     * can logging flag.
     */
    private boolean loggingFlag;

    public JobLogInfoBase() {
        this.loggingFlag = false;
    }

    public JobLogInfoBase(String sessionId, String taskGroupId, String statementId) {
        this.sessionId = sessionId;
        this.taskGroupId = taskGroupId;
        this.statementId = statementId;
        this.loggingFlag = true;
    }

    public String genLogKey() {
        // format is : sessionId() + "$" +taskGroupId() + "$" + statementId;
        return String.format("%s$%s$%s", sessionId, taskGroupId, statementId);
    }

    public void enabledLogging(String jobName, Logger logger) {
        if (this.loggingFlag) {
            this.openFlag = true;
            MDC.put(JOB_LOG_TAG, this);
            logger.info("begin logging with job {}", jobName);
        }
    }

    public void disabledLogging(Logger logger) {
        if (this.loggingFlag) {
            this.openFlag = false;
            logger.info("ended logging with job task group id {}", this.getTaskGroupId());
            MDC.remove(JOB_LOG_TAG);
        }
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getTaskGroupId() {
        return taskGroupId;
    }

    public void setTaskGroupId(String taskGroupId) {
        this.taskGroupId = taskGroupId;
    }

    public String getStatementId() {
        return statementId;
    }

    public void setStatementId(String statementId) {
        this.statementId = statementId;
    }

    public boolean isOpenFlag() {
        return openFlag;
    }

    public void setOpenFlag(boolean openFlag) {
        this.openFlag = openFlag;
    }

    public boolean isLoggingFlag() {
        return loggingFlag;
    }

    public void setLoggingFlag(boolean loggingFlag) {
        this.loggingFlag = loggingFlag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobLogInfoBase jobInfo = (JobLogInfoBase) o;
        return openFlag == jobInfo.openFlag
            && loggingFlag == jobInfo.loggingFlag
            && Objects.equals(sessionId, jobInfo.sessionId)
            && Objects.equals(taskGroupId, jobInfo.taskGroupId)
            && Objects.equals(statementId, jobInfo.statementId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, taskGroupId, statementId, openFlag, loggingFlag);
    }

    @Override
    public String toString() {
        return "JobLogInfoBase{"
            + "sessionId='" + sessionId + '\''
            + ", taskGroupId='" + taskGroupId + '\''
            + ", statementId='" + statementId + '\''
            + ", openFlag=" + openFlag
            + ", loggingFlag=" + loggingFlag
            + '}';
    }

}
