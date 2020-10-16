package com.datapps.linkoopdb.worker.spi;

import java.io.InputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.datapps.linkoopdb.jdbc.LdbSqlException;
import com.datapps.linkoopdb.jdbc.Scanner;
import com.datapps.linkoopdb.jdbc.SessionInterface;
import com.datapps.linkoopdb.jdbc.SqlInvariants;
import com.datapps.linkoopdb.jdbc.impl.JDBCConnection;
import com.datapps.linkoopdb.jdbc.navigator.RowSetNavigatorClient;
import com.datapps.linkoopdb.jdbc.persist.LdbSqlProperties;
import com.datapps.linkoopdb.jdbc.result.Result;
import com.datapps.linkoopdb.jdbc.result.ResultLob;
import com.datapps.linkoopdb.jdbc.types.BlobDataID;
import com.datapps.linkoopdb.jdbc.types.ClobDataID;
import com.datapps.linkoopdb.jdbc.types.TimestampData;

/**
 * Created by gloway on 2019/3/5.
 */
public class DbSession4Worker implements SessionInterface, Serializable {

    String jobPriority;
    String zoneString;
    int isolationLevel = SessionInterface.TX_READ_COMMITTED;
    String taskGroupId;
    private long sessionId;
    private Calendar calendar;
    private Scanner scanner;

    public DbSession4Worker(long sessionId, String jobPriority, String zoneString, String taskGroupId) {
        this.sessionId = sessionId;
        this.jobPriority = jobPriority;
        this.zoneString = zoneString;
        this.taskGroupId = taskGroupId;
    }

    public DbSession4Worker(long sessionId) {
        this.sessionId = sessionId;
        this.jobPriority = SqlInvariants.DEFAULT_JOB_PRIORITY;
        this.zoneString = null;
        this.taskGroupId = "0";
    }

    @Override
    public Result execute(Result r) {
        // 不支持
        throw new RuntimeException("Operations performed on workers are not supported");
    }

    @Override
    public RowSetNavigatorClient getRows(long navigatorId, int offset, int size) {
        // 不支持
        return null;
    }

    @Override
    public void closeNavigator(long id) {
        // 不支持
    }

    @Override
    public void close() {
        // 无需
    }

    @Override
    public boolean isClosed() {
        // 无需
        return false;
    }

    @Override
    public boolean isReadOnlyDefault() {
        return false;
    }

    @Override
    public void setReadOnlyDefault(boolean readonly) {

    }

    @Override
    public boolean isAutoCommit() {
        return false;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        // 没用
    }

    @Override
    public int getIsolation() {
        // 没用
        return isolationLevel;
    }

    @Override
    public void setIsolationDefault(int level) {
        // 没用
    }

    @Override
    public void startPhasedTransaction() {
        // 没用
    }

    @Override
    public void prepareCommit() {
        // 没用
    }

    @Override
    public void commit(boolean chain) {
        // 没用
    }

    @Override
    public void rollback(boolean chain) {
        // 没用
    }

    @Override
    public void rollbackToSavepoint(String name) {
        // 没用
    }

    @Override
    public void savepoint(String name) {
        // 没用
    }

    @Override
    public void releaseSavepoint(String name) {
        // 没用
    }

    @Override
    public void addWarning(LdbSqlException warning) {
        // 没用
    }

    @Override
    public Result cancel(Result r) {
        // 没用
        return null;
    }

    @Override
    public Object getAttribute(int id) {
        // 没用
        return null;
    }

    @Override
    public void setAttribute(int id, Object value) {
        // 没用
    }

    @Override
    public long getId() {
        return sessionId;
    }

    @Override
    public int getRandomId() {
        // 没用
        return 0;
    }

    @Override
    public void resetSession() {
        // 没用
    }

    @Override
    public String getInternalConnectionURL() {
        return null;
    }

    @Override
    public BlobDataID createBlob(long length) {
        return null;
    }

    @Override
    public ClobDataID createClob(long length) {
        return null;
    }

    @Override
    public void allocateResultLob(ResultLob result, InputStream dataInput) {

    }

    @Override
    public Scanner getScanner() {
        if (scanner == null) {
            scanner = new Scanner();
        }
        return scanner;
    }

    @Override
    public Calendar getCalendar() {
        if (calendar == null) {
            if (zoneString == null) {
                calendar = new GregorianCalendar();
            } else {
                TimeZone zone = TimeZone.getTimeZone(zoneString);

                calendar = new GregorianCalendar(zone);
            }
        }
        return calendar;
    }

    @Override
    public Calendar getCalendarGMT() {
        return null;
    }

    @Override
    public SimpleDateFormat getSimpleDateFormatGMT() {
        return null;
    }

    @Override
    public TimestampData getCurrentDate() {
        return null;
    }

    @Override
    public int getZoneSeconds() {
        return 0;
    }

    @Override
    public int getStreamBlockSize() {
        return 0;
    }

    @Override
    public LdbSqlProperties getClientProperties() {
        return null;
    }

    @Override
    public JDBCConnection getJDBCConnection() {
        return null;
    }

    @Override
    public void setJDBCConnection(JDBCConnection connection) {

    }

    @Override
    public String getDatabaseUniqueName() {
        return null;
    }

    @Override
    public boolean isDataCheckStrict() {
        return false;
    }

    public String getTaskGroupId() {
        return taskGroupId;
    }

    public void setTaskGroupId(String taskGroupId) {
        this.taskGroupId = taskGroupId;
    }

    public String getJobPriority() {
        return jobPriority;
    }

    public void setJobPriority(String jobPriority) {
        this.jobPriority = jobPriority;
    }
}
