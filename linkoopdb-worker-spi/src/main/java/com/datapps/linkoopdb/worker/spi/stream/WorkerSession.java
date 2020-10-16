package com.datapps.linkoopdb.worker.spi.stream;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 对应的Session.
 */
public class WorkerSession implements Serializable {

    private static final long serialVersionUID = -233786609426219302L;
    // Session id
    private long sessionId;
    // resource group name
    private String groupName;
    // 一些属性
    private Map<String, String> sessionProperties;
    // watermarks生成间隔
    private long periodicWatermarksInterval;
    // Session级别的时间特征
    private String timeCharacteristic;
    // 异步查询标志，默认是false，既查询时，等待一会就返回结果；true时，则异步执行，返回jobid
    private Boolean streamAsyncQueryEnable = false;
    // Statement级别的savepoint路径设置，执行一次sql后就会清理
    private String savepointPath;
    // 用户名 原始登录密码和server的端口，这三个信息用于构造到db的jdbc connect
    private String username;
    private String passwd;
    private String dburl;
    private boolean isDBSingle;
    /**
     * 存放temporary级别参数
     */
    private Map<String, String> temporaryProperties;

    // note： 同步查询最大等待时间放到了userProperty里
    //private long queryLatencyTimeMills = 10000L;
    // sql所属任务组，由studio使用，记录sql执行日志使用。
    private String taskGroupId = "0";

    public WorkerSession(long sessionId, boolean isDBSingle, String dburl, String username, String passwd) {
        this.sessionId = sessionId;
        this.isDBSingle = isDBSingle;
        this.dburl = dburl;
        this.username = username;
        this.passwd = passwd;
        this.sessionProperties = new HashMap<>();
        this.temporaryProperties = new HashMap<>();
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public long getPeriodicWatermarksInterval() {
        return periodicWatermarksInterval;
    }

    public void setPeriodicWatermarksInterval(long periodicWatermarksInterval) {
        this.periodicWatermarksInterval = periodicWatermarksInterval;
    }

    public String getTimeCharacteristic() {
        return timeCharacteristic;
    }

    public void setTimeCharacteristic(String timeCharacteristic) {
        this.timeCharacteristic = timeCharacteristic;
    }

    public Boolean getStreamAsyncQueryEnable() {
        return streamAsyncQueryEnable;
    }

    public void setStreamAsyncQueryEnable(Boolean streamAsyncQueryEnable) {
        this.streamAsyncQueryEnable = streamAsyncQueryEnable;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String jdbcUrl() {
        if (this.isDBSingle) {
            return "jdbc:linkoopdb:tcp://" + this.dburl + "/ldb";
        } else {
            return "jdbc:linkoopdb:cluster://" + this.dburl + "/ldb";
        }
    }

    public void setDburl(String dburl) {
        this.dburl = dburl;
    }

    public String getDburl() {
        return dburl;
    }

    public boolean isDBSingle() {
        return isDBSingle;
    }

    public void setDBSingle(boolean DBSingle) {
        isDBSingle = DBSingle;
    }

    /*public long getQueryLatencyTimeMills() {
        return queryLatencyTimeMills;
    }

    public void setQueryLatencyTimeMills(long queryLatencyTimeMills) {
        this.queryLatencyTimeMills = queryLatencyTimeMills;
    }*/

    public String getTaskGroupId() {
        return taskGroupId;
    }

    public void setTaskGroupId(String taskGroupId) {
        this.taskGroupId = taskGroupId;
    }

    public Map<String, String> getSessionProperties() {
        return sessionProperties;
    }

    public void setSessionProperty(String key, String value) {
        sessionProperties.put(key, value);
    }

    public String getSessionProperty(String key) {
        return sessionProperties.get(key);
    }

    public void resetSessionProperties() {
        sessionProperties.clear();
    }

    public Map<String, String> getTemporaryProperties() {
        return temporaryProperties;
    }

    public String getTemporaryProperty(String key) {
        return temporaryProperties.get(key);
    }

    public void setTemporaryProperty(String key, String value) {
        temporaryProperties.put(key, value);
    }

    public void resetTemporaryProperties() {
        temporaryProperties.clear();
    }

    public boolean retJobId() {
        return this.streamAsyncQueryEnable;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public void setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
    }

    /**
     * read value by priority. first is temporary properties, second is session properties.
     * @param key property key.
     * @param defaultVal default value if read value is null.
     */
    public boolean getValuePriorityOrDefault(String key, boolean defaultVal) {
        String value = temporaryProperties.getOrDefault(key, sessionProperties.getOrDefault(key, null));

        if (value == null) {
            return defaultVal;
        } else {
            return Boolean.valueOf(value);
        }
    }

}
