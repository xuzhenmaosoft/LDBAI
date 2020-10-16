package com.datapps.linkoopdb.worker.spi.rpc;

import java.util.List;

import com.google.common.base.Preconditions;

import com.datapps.linkoopdb.worker.spi.DbSession4Worker;
import com.datapps.linkoopdb.worker.spi.stream.WorkerSession;

/**
 * Created by gloway on 2019/9/3.
 */
public class SessionAction implements RpcMessage {

    public enum SessionActionType {
        SESSION_INIT, SESSION_DESTORY, UPDATE_TASK_GROUP_ID, SET_CONFIG, GET_CONFIG,
        SET_JOB_PRIORITY, CANCEL_JOB, CANCEL_JOB_WITH_SAVEPOINT, SHOW_RUNNING_JOBS,
        SHOW_ALL_JOBS, SHOW_JOB_WITH_IDS, DESC_JOB, SAVE_JOB_WITH_SAVEPOINT,
        CHECK_STREAM_PROPERTIES, RESET_STREAM_FIELD, STOP_JOB_WITH_SAVEPOINT
    }

    private SessionActionType sessionActionType;

    private DbSession4Worker dbSession4Worker;

    private long sessionId;

    private String message;

    private long statementId;

    private WorkerSession streamSession;

    private String confKey;

    private String confValue;

    private String jobId;

    private List<String> jobIds;

    public SessionActionType getSessionActionType() {
        return sessionActionType;
    }

    public DbSession4Worker getDbSession4Worker() {
        return dbSession4Worker;
    }

    public long getSessionId() {
        return sessionId;
    }

    public String getMessage() {
        return message;
    }

    public long getStatementId() {
        return statementId;
    }

    public static SessionAction buildInitSessionAction(DbSession4Worker dbSession4Worker) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.SESSION_INIT;
        sessionAction.dbSession4Worker = dbSession4Worker;
        return sessionAction;
    }

    public static SessionAction buildDestorySessionAction(long sessionId) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.SESSION_DESTORY;
        sessionAction.sessionId = sessionId;
        return sessionAction;
    }

    public static SessionAction buildUpdateTaskGroupIdSessionAction(long sessionId, String taskGroupId) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.UPDATE_TASK_GROUP_ID;
        sessionAction.message = taskGroupId;
        sessionAction.sessionId = sessionId;
        return sessionAction;
    }

    public static SessionAction buildCancelJobSessionAction(long sessionId, long statementId) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.CANCEL_JOB;
        sessionAction.statementId = statementId;
        sessionAction.sessionId = sessionId;
        return sessionAction;
    }

    public static SessionAction buildSetJobProSessionAction(long sessionId, String jobPriority) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.SET_JOB_PRIORITY;
        sessionAction.message = jobPriority;
        sessionAction.sessionId = sessionId;
        return sessionAction;
    }

    public static SessionAction buildGetConfigSessionAction(long sessionId, String confKey) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.GET_CONFIG;
        sessionAction.message = confKey;
        sessionAction.sessionId = sessionId;
        return sessionAction;
    }

    public static SessionAction buildSetConfigSessionAction(long sessionId, String confKey, String confValue) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.SET_CONFIG;
        sessionAction.message = confKey + "|" + confValue;
        sessionAction.sessionId = sessionId;
        return sessionAction;
    }

    public static SessionAction buildSetStreamSessionAction(WorkerSession streamSession, String confKey, String confValue) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.SET_CONFIG;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        sessionAction.confKey = confKey;
        sessionAction.confValue = confValue;
        return sessionAction;
    }

    public static SessionAction buildGetStreamSessionAction(WorkerSession streamSession, String confKey) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.GET_CONFIG;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        sessionAction.confKey = confKey;
        return sessionAction;
    }

    public static SessionAction buildCancelJobSessionAction(WorkerSession streamSession, String jobId) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.CANCEL_JOB;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        sessionAction.jobId = Preconditions.checkNotNull(jobId, "jobId cannot be null.");
        return sessionAction;
    }

    public static SessionAction buildShowJobWithIds(WorkerSession streamSession, List<String> jobIds) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.SHOW_JOB_WITH_IDS;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        sessionAction.jobIds = jobIds;
        return sessionAction;
    }

    public static SessionAction buildShowRunningJobsSessionAction(WorkerSession streamSession) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.SHOW_RUNNING_JOBS;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        return sessionAction;
    }

    public static SessionAction buildSaveJobWithSavepointSessionAction(WorkerSession streamSession, String jobId) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.SAVE_JOB_WITH_SAVEPOINT;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        sessionAction.jobId = Preconditions.checkNotNull(jobId, "jobId cannot be null.");
        return sessionAction;
    }

    public static SessionAction buildCancelJobWithSavepointSessionAction(WorkerSession streamSession, String jobId) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.CANCEL_JOB_WITH_SAVEPOINT;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        sessionAction.jobId = Preconditions.checkNotNull(jobId, "jobId cannot be null.");
        return sessionAction;
    }

    public static SessionAction buildStopJobWithSavepointSessionAction(WorkerSession streamSession, String jobId) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.STOP_JOB_WITH_SAVEPOINT;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        sessionAction.jobId = Preconditions.checkNotNull(jobId, "jobId cannot be null.");
        return sessionAction;
    }

    public static SessionAction buildDescJobSessionAction(WorkerSession streamSession, String jobId) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.DESC_JOB;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        sessionAction.jobId = Preconditions.checkNotNull(jobId, "jobId cannot be null.");
        return sessionAction;
    }

    public static SessionAction buildShowAllJobsSessionAction(WorkerSession streamSession) {
        SessionAction sessionAction = new SessionAction();
        sessionAction.sessionActionType = SessionActionType.SHOW_ALL_JOBS;
        sessionAction.streamSession = Preconditions.checkNotNull(streamSession, "WorkerSession cannot be null.");
        sessionAction.sessionId = streamSession.getSessionId();
        return sessionAction;
    }

    public static StreamPropertiesRpcMessage buildStreamPropertiesRpcMessage(WorkerSession streamSession, SessionActionType actionType) {
        return new StreamPropertiesRpcMessage(streamSession, actionType);
    }

    private SessionAction() {

    }

    protected SessionAction(WorkerSession streamSession, SessionActionType actionType) {
        this.sessionActionType = actionType;
        this.streamSession = streamSession;
    }

    @Override
    public RpcMessageType getMessageType() {
        return RpcMessageType.SESSION_ACTION;
    }

    public WorkerSession getStreamSession() {
        return streamSession;
    }

    public String getConfKey() {
        return confKey;
    }

    public String getConfValue() {
        return confValue;
    }

    public String getJobId() {
        return jobId;
    }

    public List<String> getJobIds() {
        return jobIds;
    }

}
